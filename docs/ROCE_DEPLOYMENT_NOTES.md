# RoCE Deployment Notes

How rocepool goes onto real RoCE servers (written for Broadcom NetXtreme-E /
`bnxt_re` NICs; nothing in it is Broadcom-only). The short version is one
command per site. Everything below it explains what that command decides.

```bash
# one server (traffic through the NIC's own loopback)
sudo python3 tools/roce_onsite.py --ip 192.168.50.1

# two servers (this host + a peer reachable with ssh key auth)
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 --peer root@nodeb --peer-ip 192.168.50.2

#   --bench quick (default: ladder 1..100 + IOR/mdtest quick) | full (1..500 + full IOR/mdtest) | none
#   IOR/mdtest run through the native C FUSE client (roce/native/rocepool-fuse, built by build.sh),
#   then the IO500 "hard" pair (ior-hard + mdtest-hard, step "ior_hard") and the native KV API
#   benchmark (rocepool-kv-bench vs the Python client, step "kv"); see docs/THICK_KV.md
# N servers: --peers root@n2=192.168.50.2,root@n3=192.168.50.3,...  -> official IO500 on all hosts with a
#   bottleneck analysis + projection (docs/IO500_CLUSTER.md). It checks hostname resolution on every host
#   (MPI finds ranks by name) and, as root, fixes only broken names in a marked /etc/hosts block
```

It prints a PASS / WARN / FAIL line per step, stops at the first FAIL (so a
FAIL is always the thing to fix next), and writes `onsite_report.json` plus
`onsite_topology.json`. With the topology file, any later benchmark runs
on the same hosts:
`ROCEPOOL_TOPOLOGY=onsite_topology.json python3 tools/stress_concurrency.py ...`.

## What has and has not been verified

| | how | status |
|---|---|---|
| transport logic (RC semantics, RNR, SRQ, flush, CM events, teardown) | `roce_transport.c` unmodified against softverbs, a strict userspace RC emulator; unit tests + a two-namespace crossover rig, 40/40 repeated runs | verified |
| builds against the **real** rdma-core headers and libraries | rdma-core v59.2 built from source here; `-Wall -Wextra -Werror` clean; every `ibv_*`/`rdma_*` symbol resolves against the real `libibverbs.so.1` / `librdmacm.so.1` | verified |
| real library with no RDMA device | loads, and `serve`/`send_request` fail with a clear error instead of hanging | verified |
| on-site automation (preflight decisions, build paths, two-host ssh orchestration, report) | preflight against fake `bnxt_re` sysfs/configfs trees (15 cases); `roce_onsite.py` end to end with an ssh shim and the emulator standing in for the NIC | verified |
| **real verbs on a real NIC** | needs hardware: this is what `roce_onsite.py` runs on site | **not yet run** |

The environment this was developed in has no RDMA device, no `rdma_rxe`
module and no kernel modules at all, so the last row cannot be done here.
Softverbs is deliberately strict (a SEND with no RECV posted is NAKed RNR,
a length overrun is a remote invalid request, a lkey from another
protection domain is a protection error, SRQ receives are not flushed on
QP error, ...), and the real-header build removes the old caveat about
hand-written stub headers. What remains unproven is only what hardware
itself adds: driver/firmware behaviour, timing, and performance.

## What the preflight decides (`tools/roce_preflight.py`)

| check | PASS | otherwise |
|---|---|---|
| kernel | >= 5.13: fork() is safe with registered memory | WARN, exports `RDMAV_FORK_SAFE=1` |
| memlock | `RLIMIT_MEMLOCK` covers what the transport pins (SRQ slab + buffer pool; 2.5 GiB at the default 16 MiB frames) | `--fix` raises it (root); else FAIL with the `ulimit -l` / `LimitMEMLOCK` fix |
| device | an RDMA device exists; Broadcom recognised by PCI vendor 0x14e4 | FAIL: bnxt_re not loaded / RoCE not enabled in NIC firmware |
| gid | a **RoCE v2** GID carries `--ip` (auto-detected if omitted) | FAIL, listing the IPs that do have RoCE v2 GIDs |
| port | ACTIVE + LinkUp, Ethernet link layer | FAIL |
| mtu | netdev MTU allows RoCE MTU 4096 | WARN with the RoCE MTU you actually get (jumbo frames on NIC **and** switch) |
| roce_mode | rdma_cm `default_roce_mode` = RoCE v2 | `--fix` sets it (root); else FAIL |
| lossless | PFC on a priority and a DSCP mapped to it (iproute2 `dcb`) -> `ROCE_TOS = DSCP<<2` | no PFC but bnxt_re ECN congestion control -> WARN, `ROCE_ACK_TIMEOUT=14`; neither -> WARN |
| tuning | >= 16 CPUs: `ROCE_POLL_SPIN_US=20` (short busy-poll before sleeping) | none |

The derived settings go to `roce_env.sh`; `roce_onsite.py`, the `--real`
crossover run and ssh-launched peer processes all apply it. Switch-side
settings (PFC, MTU, ECN marking) are never changed automatically: the host
must match the switch, so the preflight reports and derives, it does not
guess.

## Build (`roce/native/build.sh`)

Finds rdma-core's development files by itself: already installed (or
`ROCE_RDMA_CORE=<dir>`), else the distro packages (root: `apt-get` /
`dnf` / `yum`), else rdma-core built from source into
`~/.cache/rocepool` (git + cmake; libnl optional). Output:
`roce/native/libroce_transport.so` (transport + native MDT/OST servers),
`roce/native/rocepool-fuse` (native client), `roce/native/librocepool_kv.so`
+ `roce/native/rocepool-kv-bench` (native KV API),
`roce/native/librocepool_intercept.so` (the LD_PRELOAD client), all
`-Werror` clean. `SPDK_DIR=<built spdk tree> build.sh` adds the OST tier's
SPDK backend (`ROCEPOOL_OST_TIER=spdk:<traddr>`); without it the tier has
the kernel backend only (`file:PATH[:GIB]`, a file or block device).

## Transport properties that matter on hardware

- **Pooled persistent connections** (client side, `ROCE_CONN_POOL=0`
  disables): a request is only re-sent on a new connection when it provably
  never left the host (the connection was already known dead before
  posting, or posting failed); anything after the SEND is posted is
  reported, never silently retried. Idle connections close after
  `ROCE_POOL_IDLE_S` (30 s); up to `ROCE_POOL_MAX_IDLE` (8) per peer.
- **Registered memory is bounded, not per connection.** Servers receive
  through a shared receive queue (`ROCE_SRQ_DEPTH` buffers, default 32,
  clamped to the device's `max_srq_wr`); clients and replies borrow frame
  buffers from one process-wide pool registered in chunks
  (`ROCE_BUFPOOL_MAX`, default 512). Previously every connection pinned
  2 x 16 MiB. When the SRQ is empty the NIC NAKs RNR and the sender
  retransmits: back-pressure in the transport.
- **No busy-polling by default**: completion channels + `poll()`; CM events
  have timeouts (a lost CM reply no longer blocks forever); the accept loop
  polls, so `stop()` cannot hang in `rdma_get_cm_event` on real rdma-core.
- **`roce_server_stop()` is a clean shutdown**: drains replies already in
  progress, disconnects every live connection (pooled clients see the CM
  DISCONNECTED and reconnect), and waits for connection threads (bounded
  by `ROCE_SERVER_DRAIN_MS`, 2000).
- `ROCE_TOS` / `ROCE_ACK_TIMEOUT` are applied with `rdma_set_option`;
  registration failures name the locked-memory limit when that is the
  cause.

## What the on-site run decides by itself (this round)

- **Client.** `io500_run.py` runs a short official IO500 three ways --
  the FUSE client over classic /dev/fuse, over io_uring (needs Linux 6.14+
  with `fuse.enable_uring=Y`, which `io500_run.py` turns on itself), and
  the LD_PRELOAD interception library in every rank with no mount -- and
  uses the fastest for the real runs; all results are in the report.
- **NVMe tier and P2P memory.** `tools/nvme_preflight.py` on every host
  (sysfs only) picks the NVMe-backed file system with the most room for
  each OST's tier file and, when the kernel is 6.2+, the NIC is mlx5 and a
  drive publishes its CMB under the same root port, the drive's p2pmem for
  the OST's request buffers. A raw block device or SPDK is never chosen by
  itself: `ROCEPOOL_OST_TIER=file:/dev/nvmeXnY` / `spdk:<traddr>` in the
  cluster file's `env` does that.
- **RDMA READ resources.** The bulk path needs `responder_resources` /
  `initiator_depth` on both sides; the transport asks for 8 within the
  device's `max_qp_rd_atom`. A NIC that refuses fails the connection setup
  with a clear CM error rather than a hang.
- **Locked memory.** The client now pins a registered arena (64 MiB
  chunks, grown on demand) plus the buffer pool; the preflight's
  `ulimit -l` check covers it, the write cap per file is 16 MiB
  (`ROCEPOOL_DIRTY_CAP_MIB`).

## Known gaps

- No replication / failover: `PeerUnreachable` is surfaced to the caller.
- Large transfers now go by server-driven RDMA READ (requests) and RDMA
  WRITE with immediate (replies) from and into registered client memory
  (`docs/SCALE_OUT_ROUND.md`); small frames stay two-sided SEND/RECV.
  Not yet exercised on a real NIC -- softverbs models the verbs' rules,
  the on-site run is the first hardware test of this path.
- The OST tier's SPDK backend and the P2P (CMB) buffer path compile
  against the real headers and are gated by the preflight, but have not
  run on hardware (`docs/KERNEL_BYPASS_ROUND.md`); the kernel (file /
  O_DIRECT) backend has, here, on a file.
- One `ibv_context` per device is used as rdma_cm selects it; no explicit
  multi-NIC striping.
