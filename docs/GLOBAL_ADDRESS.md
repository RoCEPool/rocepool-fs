# Global addresses: the OSTs' memory as one address space

`roce/native/rocepool_ga.h` (C, in `librocepool_kv.so`) and `roce/ga.py` (Python) give a
client the memory of every OST as one address space. GPU code reads and writes it by
address, straight into and out of GPU memory.

```python
from roce.ga import GlobalMemory
ga = GlobalMemory(osts)                       # the same OST list, in the same order, everywhere
base = ga.alloc(1 << 30, name="kv-pool-0")    # a 1 GiB region on the OST its name hashes to
buf = ga.register_device(t.data_ptr(), t.numel() * t.element_size())
buf.write(base + off, offset=0, length=n)     # GPU -> region: the OST pulls the bytes (RDMA READ)
buf.read(base + off, offset=0, length=n)      # region -> GPU: one RDMA READ by this host's NIC
buf.mread(addrs, offsets=..., lengths=...)    # many, every OST in parallel
addr, size = ga.lookup("kv-pool-0")           # in another process
ga.free(base)
```

## Addresses

| bits | field | range |
|---|---|---|
| 63..56 | OST (its place in the list) | 256 OSTs: 128 nodes x 2 |
| 55..40 | region (the OST's number for it) | 65535 per OST |
| 39..0 | byte offset | regions up to 1 TiB |

## What a read costs, and what it does not check

A read is one RDMA READ from the OST's registered memory into the buffer. There is no
key lookup, no lease RPC, no seqlock, and no copy or CPU on the OST. A region's
descriptor (remote address, rkey, size) is fetched once per handle and cached. #110
measured what this saves against the KV paths on the crossover rig:

- against a read by key through an RPC: 1.4 to 2.4 times;
- against today's cached lease: 1.2 to 1.9 times for 4 KiB blocks, 1.4 times for
  64 KiB, the same for 2 MiB.

The price is that nothing is checked. A region never moves and is never evicted or
written to the tier, so its bytes stay where its address says. But there is no lock and
no version: a read racing a write to the same bytes may see a mix, and ordering between
processes is the application's (for example, a flag word written last and polled).

## What a region is

- **Server side.** Memory an OST maps (`mmap`, huge pages advised), registers with its
  NIC and hands out on request (`OP_GA`, in `rocepool_servers.c`).
- **Limit.** `ROCEPOOL_OST_GA_MIB` per OST (default 4096). Past it, `-ENOSPC`.
- **Not durable.** A region lives while its OST process runs. A restart loses it.
  Persisting regions (to the OST's NVMe tier) is a later step.
- **Freeing.** A region is deregistered from the NIC before its memory is unmapped, so a
  client still holding its address gets `-ENOENT`, not another region's bytes. That holds
  until the region number is reused, after 65535 more allocations on that OST: do not
  keep an address past its region's free.
- **Writes.** A write is one request per frame (16 MiB by default). The OST RDMA-reads
  the data out of the buffer (GPU memory included) and copies it into the region.

## When the NIC cannot reach the buffer

If GPU memory has no dma-buf export and no nvidia-peermem, or with
`ROCEPOOL_GDS_BOUNCE=1`, or if the transport cannot publish a region, the same calls go
through host memory and RPCs. Then `rpga_mem_direct()` / `Buffer.direct` is false, and
`rpga_stats()` / `GlobalMemory.stats()` count `read_rpc` / `write_bounced` instead of
`read_direct` / `write_direct`.

## Tests

`tests/test_ga.py` runs native OSTs and the emulated device (`ROCEPOOL_DEV_EMU=1`: its
memory faults on any CPU access, and the RDMA emulator reaches it through dma-buf), over
softverbs and over libfabric tcp. It covers:

- alloc, lookup from another process, and free;
- one-sided reads (no RPC, bytes counted as direct) and writes pulled from device memory;
- 64 transfers across 16 regions on 4 OSTs;
- bounds: `-ERANGE`, nothing written;
- `-EEXIST`, `-ENOSPC`, `-ENOENT`, `-EINVAL`;
- a freed region read through a cached address after its memory was handed out again
  (an error, not other bytes);
- the bounced path, and host buffers.
