# "Thick KV": offset writes, versioned reads, batches, Data-on-MDT, native KV API

The OSTs used to be a thin put/get/stat/delete store. Every partial-stripe
write was a client-side read-modify-write: stat, get the whole stripe, stat
again, then a CAS put, with retries. Every small file cost an MDT entry plus
an OST object. That design is worst exactly where IO500's "hard" phases
press:

- **ior-hard**: 47 008-byte transfers into one shared file. None are
  stripe-aligned, so every write took about 4 round trips plus a
  whole-stripe transfer.
- **mdtest-hard**: 3 901-byte files in one directory. Each file took an MDT
  create, an OST PUT, an OST GET and an OST DELETE.

This work made the servers "thick" (priorities 1-3 as requested), then added
a native KV client on top.

## 1. OST ops (`roce/native/rocepool_servers.c`, `roce/pool_node.py`; encodings in `roce/protocol.py`)

| op | request value | reply | replaces |
|---|---|---|---|
| `WRITE_AT` 11/12 | u64 LE offset + data | PUT_OK (u64 BE version, u64 BE size) | stat/get/stat/CAS-put, retried |
| `GET_V` 13/14 | - | u64 BE version + data | stat/get/stat |
| `READ_AT` 15/16 | u64 LE offset + u32 LE length | u64 BE version + those bytes | whole-stripe GET |
| `TRUNCATE` 17/18 | u64 LE size | PUT_OK (0/0 if absent) | get + CAS put |
| `BATCH` 40/41 | u32 n + n x (op, klen, vlen, key, value) | u32 n + n x (op, vlen, value) | n round trips |
| `VTOKEN` 42/43 | - | u64 BE version token | (new, see section 3) |

Semantics of each op:

- `WRITE_AT` is atomic under the object's lock. It creates the object if
  needed and zero-fills any gap.
- `TRUNCATE` only shrinks; a short stripe reads as zeros.
- `BATCH` does not nest. The whole frame is validated before any
  sub-request runs, so a malformed batch has no effect. Sub-requests then
  run in order, and each gets its own reply (errors included).

Both server implementations (C and Python) implement every op. Parity is
checked op by op in `tests/test_native_servers.py`.

## 2. Data-on-MDT: small files inline in the metadata entry (`fs/mdt_node.py`, C MDT)

A new file starts **inline**: its bytes live in the MDT entry, and the OSTs
are never touched.

- **Inline limit.** `ROCEPOOL_INLINE_MAX`, default 32 KiB, never more than
  one stripe.
- **Ops.** `write_inline` (7), `read_inline` (8), `freeze` (9) and
  `spill_commit` (10). `read_inline` returns the entry and the bytes in one
  reply.
- **Create flags.** The create request carries `inline_ok`. Clients can opt
  out with `ROCEPOOL_CLIENT_INLINE=0` (C client) or `inline=False` (Python
  client).

For mdtest-hard this means:

| step | before | now |
|---|---|---|
| create + write | 3 RPCs | 2 RPCs |
| read | 1 MDT + 1 OST | 1 MDT |
| remove | 1 MDT + 1 OST | 1 MDT |

**Spill** happens when a file grows past the limit. Any client may run it,
and any client may finish one that another client began:

1. `freeze`. The entry stops accepting inline writes, and the frozen bytes
   are returned.
2. `t = VTOKEN(stripe 0)`.
3. `freeze` again, and check the entry is still inline. This confirms the
   token was read while the file was frozen.
4. `PUT stripe 0 = frozen bytes, if token == t`. On a conflict, go back
   to step 2.
5. `spill_commit` (idempotent). The entry becomes striped.

MDT rules while a spill is pending:

- `FROZEN` blocks inline writes and resizes. A client that receives it runs
  the spill itself.
- `set_size(max)` on an inline entry answers `INLINE`. This catches a client
  whose cached attributes still say "striped"; it redoes its write inline.
  If the entry is frozen, `set_size(max)` answers `FROZEN` instead, and the
  client finishes the spill and then redoes the write.

**Clients skip pointless inline attempts.** If `offset + length` exceeds the
limit, the client spills first. It does not ship the data to the MDT only
to hear `INLINE_FULL`. Without this, every new large file sent its first
flush twice, and a flush bigger than one frame failed with EFBIG.

## 3. Correctness fixes from the independent review (each with a regression test that fails without it)

| # | bug | fix | test |
|---|---|---|---|
| 1 | C `fs_spill` called `spill_commit` even after all CAS attempts failed. That would publish a striped file whose stripe 0 lacked the inline bytes (**data loss**). | Commit only after our own successful PUT, or after a freeze shows another client already committed; otherwise return `-EIO`. | code path; the C client shares the spill tests below |
| 2 | **ABA in the spill CAS.** A slow helper read stripe 0 as "absent (version 0)". Meanwhile the spill finished, the file was removed and recreated with stripe 0 as a hole. The helper's `expected 0` CAS then succeeded and wrote the **old file's bytes into the new file**. | A **version token** that is never reused per key. Deleting an object raises a per-bucket watermark (4 096 buckets). A recreated key's versions start above it. An absent key's token is that watermark. `PUT` marker 2 compares against the token. Plain `put_if(expected=0)` keeps its "must not exist" meaning. Sharing a bucket can only cause a spurious retry, never a false success. | `test_a_slow_spill_helper_cannot_write_into_a_recreated_file` (old file's bytes leaked before the fix); `KVEdgeTests.tokens` on both server implementations |
| 3 | A stale "striped" writer whose `set_size(max)` hit a *frozen* file got `FROZEN`, which surfaced as EIO. | Finish the spill, then redo the write (C and Python clients). | `test_stale_striped_attrs_meet_a_file_frozen_mid_spill` (C, through the kernel; EIO before the fix); `test_stale_striped_attr_on_a_file_frozen_mid_spill_...` (Python) |
| 4 | Batch sizing: see the list below the table. | See the list below the table. | `KVEdgeTests` with a 1 MiB frame: 8 x 300 KiB mget, a 700 KiB mput item, a 2 MiB item fails alone with EFBIG, a 30 000-item Python batch |
| 5 | The server's `BATCH` ran sub-requests while still parsing. A frame malformed at item k had already applied items 0..k-1. | Validate every header first, then execute (C and Python). | `KVEdgeTests.batch_val` |

Details of fix 4 (batch sizing):

- **KV `mget`.** It split batches only by request size, so large values
  could make the reply exceed a frame. It now reads each key with `READ_AT`,
  capped at half a frame divided by the keys in the batch. A value that
  fills its cap is re-read on its own.
- **Oversize items.** An item too big to share a batch used to abort its
  whole OST group. It is now sent alone, and fails alone with `EFBIG` if it
  exceeds a frame.
- **Chunking.** The FUSE client's truncate/unlink batches and the Python
  `RemoteEngine.batch` never split. Both now chunk at half a frame.

**Staleness contract (unchanged, stated precisely).** A client trusts its
cached attributes for `attr_ttl` (default 3 s). If another client removes
and recreates a path inside that window, the first client's
**size-extending** writes are caught by the MDT (`INLINE`/`FROZEN`) and
redone correctly.

A **non-extending** write in that window goes to the old layout's stripe
keys, like a write through a stale NFS handle. It is lost, or it is left as
an orphan stripe that the new file only sees if it later grows into that
stripe with a partial write.

Set `--attr-ttl 0` for strict cross-client semantics.

## 4. Client use (C `rocepool_fuse.c` + shared core `rocepool_client.c`, and Python `fs/posix_api.py`)

**Writes.**

- A partial stripe goes as one `WRITE_AT`; a full stripe as a blind `PUT`.
- Inline files take one MDT call.

**Reads.**

- **Sequential reads** (the handle's `next_off` matches) keep the
  whole-stripe cache with readahead.
- **Random and strided reads** use `READ_AT` for exactly the bytes asked.
  Before, ior-hard reads fetched whole 1 MiB stripes for 47 KB.

**Truncate and unlink.**

- One `BATCH` per OST: `TRUNCATE` for the boundary stripe, `DELETE` for the
  rest.
- Inline files never touch an OST.

## 5. Native KV client API (`roce/native/rocepool_kv.h`, Python `roce/kv.py`)

```c
rpkv_t *kv = rpkv_open("h1:19951,h2:19952,...", "app/");   /* thread-safe handle */
rpkv_put(kv, "user:42", buf, len, &ver);
rpkv_put_if(kv, "user:42", buf2, len2, ver, &ver2);        /* CAS; 0 = must not exist */
rpkv_get(kv, "user:42", &val, &n, &ver);                    /* GET_V: value + version, 1 RTT */
rpkv_write_at(kv, "log", off, rec, reclen);                 /* in-place partial update */
rpkv_read_at(kv, "log", off, len, &val, &got);
rpkv_mget(kv, n, keys, vals, lens, status);                 /* one BATCH per OST, OSTs in parallel */
```

- **Placement.** `sha256(namespace + key)[:8] mod #OSTs`, the same rule as
  `PoolNode`. KV keys and file stripes can share OSTs.
- **Status.** `mput`/`mget`/`mdelete` report per-key status.
- **Libraries.**
  - `librocepool_kv.so` and the `rocepool-kv-bench` driver are built by
    `build.sh` (real rdma-core) and by `build_lib.ensure_kv_lib()`
    (emulator).
  - Python: `from roce.kv import KVClient`.

## 6. Results (2-core rig, native C servers and client, 4 OSTs + 2 MDTs)

IO500 "hard" pair, `tools/bench_ior.py --backend dist --hard`: ior-hard with
4 tasks x 200 segments x 47 008 B on a shared file, and mdtest-hard with
`-w 3901 -e 3901 -N 1` in one directory.

| | before (thin KV, 2 runs) | **thick KV (after the review fixes, 3 runs)** |
|---|---|---|
| ior-hard write | 21.3 MiB/s | **195 MiB/s (9.2x)** |
| ior-hard read | 231 MiB/s | 265 MiB/s |
| mdtest-hard create | 2 983 /s | 3 660 /s |
| mdtest-hard read | 10 114 /s | 10 489 /s |
| mdtest-hard remove | 6 111 /s | **9 062 /s** |
| hard-pair score | 0.853 | **1.727 (2.0x)** |
| combined with the standard matrix (same easy run) | 2.62 | **3.07** |

Reports: `stress_reports/24_ior_hard_before_thick_kv.json`,
`25_ior_hard_thick_kv.json`, `26_ior_standard_scale4_thick_kv.json` and
`27_kv_api_bench.json`.

Notes on these numbers:

- **Before the review fixes.** The thick version averaged 1.77 over 2 runs.
  The difference is within run-to-run noise: ior-hard read ranged
  255-308 MiB/s across the 5 runs.
- **mdtest-hard create moved least.** The path fell from 5 RPCs to 3, but on
  two cores FUSE and CPU dominate.
- **mdtest "stat" is omitted.** It is cache-served, and its figure is timer
  noise.
- **Standard matrix at 4x ranks.** 4.38, up from 4.13; the thick ops did not
  cost the easy cases anything.

Native KV API vs the Python client, `tools/bench_kv.py`, same OSTs, client
on node A:

| | native `rocepool_kv` | Python client |
|---|---|---|
| get, 1 thread (64 B) | 13.8 k ops/s, p50 71 us | 11.0 k ops/s, p50 87 us |
| get, 16 threads (64 B) | 37.7 k ops/s | 19.8 k ops/s |
| put, 16 threads (64 B) | 36.2 k ops/s | 18.5 k ops/s |
| mget x64, 16 threads | **235 k keys/s** (64 B), 145 k keys/s (4 KiB) | - |

### Against the WEKA 10-node reference (UVA Afton: score 105.94, 33.28 GiB/s, 337 kIOPS)

The gap is still mostly hardware:

- **Scale.** 10 client nodes with real NICs, against one 2-core container
  running every server, the client and a TCP-based RDMA emulator.
- **Combined proxy score.** 3.07 vs 105.94, about 35x. Before this work it
  was 2.62 (about 40x).
- **Easy matrix.** 4.38, about 24x behind (unchanged).

The thick-KV work closes the part that was design rather than hardware.
WEKA-class systems do have in-place sub-block writes, small files inline in
metadata, and batched metadata. Our hard phases were an order of magnitude
behind our own easy phases, and ior-hard write was 10-25x slower than
the ior-easy writes (229-570 MiB/s). They are now in the same range.

## 7. Tests

| test file | tests | what it covers |
|---|---|---|
| `tests/test_thick_kv.py` | 12 | Data-on-MDT, spill, truncate, a frozen spill finished by another client, stale writers (`INLINE` and `FROZEN`), concurrent writers across the spill, the ABA helper, inline-disabled client, single `WRITE_AT` calls, batched truncate/remove |
| `tests/test_kv_api.py` | 4 | KV API semantics against native and Python OSTs; edge cases with a 1 MiB frame |
| `tests/test_native_fuse.py` | 23 | adds inline, spill, Python interop, strided reads, the spill race through the kernel, a 40 MiB first flush, a stale writer meeting a frozen file |

Full suite: 305 tests, all passing.
