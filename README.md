# rocepool

A distributed file system whose data lives in a memory pool spread over many
hosts and is moved with RDMA (RoCE v2, or AWS EFA through libfabric).

- **Servers in C**: metadata (MDT) and object (OST) servers with
  per-NUMA-node placement, an optional NVMe tier per OST, and an RDMA-readable
  lookup table so clients can resolve paths and read data one-sided.
- **Three clients**: a native FUSE client (classic `/dev/fuse` or FUSE over
  io_uring), an `LD_PRELOAD` interception library (no mount, no kernel on the
  I/O path), and a key-value API (`librocepool_kv`).
- **GPUDirect Storage**: a cuFile-compatible `libcufile.so.0`. A GDS
  program's reads go from the OSTs' memory straight into GPU memory
  (RDMA into a dma-buf / peer-memory registration of the buffer), and its
  writes are RDMA-read by the OSTs out of GPU memory, with no host bounce
  buffer and no CPU copy. See [docs/GPUDIRECT_STORAGE.md](docs/GPUDIRECT_STORAGE.md).
- **Two transports, one API** (`roce/native/roce_transport.h`): verbs/rdma_cm
  for RoCE NICs and Soft-RoCE, libfabric `FI_EP_RDM` for EFA. A software RDMA
  emulator (`roce/native/softverbs/`) runs the whole system, tests included,
  on a machine with no RDMA hardware.
- **Benchmarked with the official IO500**, end to end and unattended: one
  command checks the hosts, builds everything (offline: all third-party
  sources are in `deps/`), starts the servers, picks the fastest client mode,
  runs IO500 and writes a report with a bottleneck analysis.

## Quick start

On every host: Linux, root, an RDMA-capable NIC (RoCE v2), the same path for
this tree.

```bash
cp cluster.conf.example cluster.conf     # list every host: ssh address, RoCE IP, hostname
sudo ./run.sh                            # on any one of them
```

`run.sh` installs packages, builds, runs the self-test, sets up root SSH between
the hosts (once, from the login in `cluster.conf`), then runs
`tools/roce_onsite.py`: preflight of every host (RoCE mode, MTU, PFC/ECN,
power states, NVMe, GPUs), transport and functional checks, benchmarks (GPU streaming included), a 60 s IO500
and the official 300 s IO500. Results: `onsite_report.json`,
`onsite_io500*.json`, and one `rocepool-results-*.tar.gz` with every host's logs.

Without `cluster.conf`: `sudo ./run.sh` runs on this host alone (or, with no
RDMA NIC, a 10-node simulation in network namespaces). On AWS:
`python3 tools/aws_cluster.py run --mode efa --nodes 10` creates the cluster,
runs, fetches the reports and releases everything.

Full guide (Chinese): [docs/运行说明书.md](docs/运行说明书.md).

## Layout

| path | |
|---|---|
| `roce/native/` | the C code: transports, servers, FUSE client, interception library, KV API, GPUDirect (cuFile) library, NVMe tier |
| `fs/`, `storage_engine/`, `mpi/`, `wlp/` | Python reference implementations, the MPI-IO layer, the storage-engine adapters |
| `tools/` | deployment and benchmarking: `run.sh`'s steps, IO500 driver and model, preflight, cluster setup, AWS |
| `tests/` | the test suite (runs on the RDMA emulator: `python3 -m unittest discover -s tests -t .`, as root) |
| `deps/` | pinned third-party sources for offline builds (see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)) |
| `third_party/rapidsse/` | the RapidsSE sources `wlp/` was written against, unmodified, with their license |
| `docs/` | design notes and the record of each development round |

## Storage-engine adapters

`storage_engine/rapidsse_engine.py` and `wlp/` speak the client wire protocol
(WLP v5) of RapidsSE, the Rapids Storage Engine (Apache License 2.0,
Copyright (C) 2026 Boray Data; https://bitbucket.org/rapidsdataproject/rapidsse/).
The RapidsSE sources they were written
against are kept unmodified in [third_party/rapidsse/](third_party/rapidsse/UPSTREAM.md),
with RapidsSE's license, and a test checks the two agree.

## License

Apache License 2.0, see [LICENSE](LICENSE). The third-party sources in `deps/` and
`third_party/` keep their own licenses: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
