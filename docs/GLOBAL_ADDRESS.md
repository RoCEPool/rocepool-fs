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

## exiom memory servers

An entry of the list can name an exiom memory server pair (RoCEPool/exiom-native-fiber: FPGA
cards in dedicated memory servers, the pool's DRAM backed by NVMe) instead of an OST:

```python
ga = GlobalMemory(["exiom://team-a@10.0.1.1:7000|10.0.2.1:7000"])   # tenant @ its paths, primary first
a = ga.stage("kv/blk-17", 65536, ost=0)       # a new value of the key, not visible yet
buf.write(a, offset=0, length=65536)          # one RDMA WRITE, from GPU memory
ga.commit(a)                                  # now the key's value, in one step
b, size = ga.lookup("kv/blk-17", ost=0)       # elsewhere: one request, then reads by address
buf.read(b, offset=0, length=size)            # one RDMA READ, no CPU on the memory server
ga.path(0)                                    # (endpoint in use, moves to another path)
```

The same calls and the same addresses as for OSTs (`rocepool_ga_exiom.c`); what differs:

| | OST | exiom memory server |
|---|---|---|
| control | `OP_GA` RPCs to the OST | TCP to the memory server (exiom `connect.py`): connect, alloc, commit, lookup, free |
| queue pair | RDMA CM to the OST | connected by hand (`roce_raw_qp_*`): the FPGA has no CM; this host's QPN, PSN, IP and MAC go in the connect request |
| read | one RDMA READ | one RDMA READ |
| write | the OST pulls the bytes and copies them | one RDMA WRITE |
| a region | memory set aside by name | the value of a key in the tenant's space |
| versions | none | `stage` / `commit`; `alloc` is both at once (and opens a key that exists) |
| an address kept | until the region is freed | for the lease of its lookup, then checked again: a key with another value since reads `-ENOENT` |
| durable | no | yes (the memory server writes pages back to NVMe) |
| alignment | none | offsets and lengths multiples of 4 bytes (`-EINVAL`) |
| path failure | `-EIO` | the next path of the entry: primary, backup, then through the switch |

- **Paths.** A failed request or transfer closes the path and connects through the next endpoint
  of the entry (a new queue pair); the request or the transfer goes again there once. A commit
  or a free that the first path did carry finds nothing to do the second time, and that counts
  as done.
- **Tenants.** The tenant's region and its protection domain are given at connect: a queue pair
  reaches only its tenant's region, and keys of other tenants are not found.
- **Path MTU.** The memory server says its RoCE target's MTU at connect (1024 when it does
  not); the queue pair uses it, and so the network card cuts its messages the way the target
  expects them.
- **Network cards.** RoCE only (verbs backend; the ofi backend has no queue pairs connected by
  hand). The GID used is the one that carries the local IPv4 address (the last one: RoCEv2), or
  `ROCEPOOL_GID_INDEX`; the ACK timeout is `ROCEPOOL_RAW_TIMEOUT` (verbs exponent, default 17:
  about 0.5 s, room for the memory server to bring a page in from NVMe).
- **Without a direct path to the buffer** (`ROCEPOOL_GDS_BOUNCE=1`, or no dma-buf or peermem),
  reads and writes go through a 4 MiB host bounce buffer, counted as `read_rpc` and
  `write_bounced` (the memory server's CPU still does not see them).

`tests/test_ga_exiom.py` runs it against `tests/fakes/exiom_server.py`: the control protocol of
exiom `connect.py` and a responder-only queue pair per connection on softverbs, two paths. It
covers values by key read and written by address in device memory (a handful of control
requests in all), stage / commit with a reader holding the old address through and after its
lease, the errors, a cut primary path (reads and requests go on through the backup), the
bounced path, and many transfers over two memory servers. softverbs connects such queue pairs
over TCP: the reader dials the destination GID's IPv4 address at `SOFTVERBS_RAW_PORT` (default
4791), and a process that serves them listens on `SOFTVERBS_RAW_LISTEN`. Not covered here: a
real network card against the FPGA (its RoCE target is tested in exiom's own simulation), the
FPGA's protection domains (the emulator checks tenants on the control path only).
