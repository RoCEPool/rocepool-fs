# Running on AWS: Soft-RoCE and an EFA transport backend

EC2 has no RoCE NIC. Two routes, both automated end to end by
`tools/aws_cluster.py` (create → set up → run → fetch → release):

| | route 1: Soft-RoCE (`--mode rxe`) | route 2: EFA (`--mode efa`) |
|---|---|---|
| device | `rdma_rxe` makes the ENA NIC a RoCE v2 device (`rdma link add rxe0 type rxe netdev ens5`) | Elastic Fabric Adapter: AWS's RDMA NIC, SRD protocol |
| rocepool transport | `roce_transport.c` (RC QPs over rdma_cm) -- **unchanged** | `ofi_transport.c` (new): libfabric reliable datagrams, efa provider |
| who moves the bytes | the host CPU (rxe is software) | the NIC; hardware RDMA read on Nitro v4+ |
| what a run measures | the software and the multi-node flow; RDMA costs are CPU costs | real RDMA: a meaningful score |
| instance types | any (default `c6in.16xlarge`) | EFA-capable (default `c6in.32xlarge`: 128 vCPU, 200 Gb/s) |

```bash
python3 tools/aws_cluster.py run --mode efa --nodes 10            # valid 300 s IO500 on 10 EFA hosts
python3 tools/aws_cluster.py run --mode rxe --nodes 3 --quick     # Soft-RoCE, 30 s
python3 tools/aws_cluster.py list / down --run-id <id>
```

On hosts you set up yourself: `sudo ./run.sh --peers ...` recognises an
EFA device and builds the ofi backend; `sudo ./run.sh --soft-roce --peers ...`
creates rxe0 on every host that has no RDMA device.

## 1. The EFA backend (`roce/native/ofi_transport.c`)

The whole `roce_transport.h` API -- request/response, the envelope bulk
protocol, dedicated streams, registered memory, server-side reply buffers,
the chunk hooks (P2P), `roce_client_rdma_read` -- on libfabric `FI_EP_RDM`
endpoints, so the servers, the FUSE client, the interception library and
the KV API link against it unchanged.

What SRD changes, and how the backend answers it:

| SRD / EFA | the backend |
|---|---|
| no connections, no rdma_cm | a TCP socket per "connection" carries the handshake (endpoint names both ways) and is the liveness signal: a closed socket fails the waiting requests at once |
| datagrams, no per-connection QP | servers: K endpoints (`ROCE_OFI_SERVER_EPS`, default one per core), each with a pinned worker thread and 64 pre-posted receives; a connection is bound to one. Clients: a pool of endpoints per domain checked out per request (`ROCE_OFI_CLIENT_EPS`) |
| no ordering, no request/reply pairing | every request carries `reply_to` + a tag; the reply echoes the tag; late replies to timed-out requests are dropped |
| a client endpoint can go away while its request is queued | the server's address table has a per-index epoch (`reply_to = epoch << 32 \| index`); a worker admits a request only while its client is live in that epoch, and the AV entry is removed only when no admitted request uses it -- a reused index can never receive another client's reply or be read with stale keys |
| bulk | as RC: the server RDMA READs the request's registered sources (up to 4) into a pool buffer, placed so the data starts on a page boundary (O_DIRECT / SPDK straight from the buffer); replies go from registered memory (`roce_reply_alloc` leaves header room) |
| two buffers per large request | taken together (`pool_get_n`): at the pool cap, concurrent callers each holding one and waiting for the other cannot happen |
| provider without `FI_MR_VIRT_ADDR` (tcp) | remote addresses are offsets; `roce_server_mem_addr()` gives servers the right value to publish |

### One-sided reads on an unordered transport

RC executes a chain of RDMA READs in order; that is what made
`header, data, trailer` a seqlock read. SRD may execute them in any order,
so `roce_transport_reads_ordered()` tells the client which it has:

- **stripes** (`rp_ost_read_direct`): read the data, and only after it
  completed, the header. The lease's `seq` was observed by the OST before
  the data read; seqs are never reused (freeing leaves the seq odd); so an
  unchanged even header afterwards proves no writer touched the object in
  between. Two round trips instead of one chain.
- **lookups** (`mdt_lookup_direct`): bytes within one READ can arrive in
  any order, so a matching slot is confirmed by re-reading the slot after
  the probe completed, then its seq after that: seq(probe) = seq(last) and
  seqs never repeat ⇒ the re-read copy is consistent. Three round trips --
  more than one RPC -- so on unordered transports one-sided lookups are
  **off by default** (`ROCEPOOL_DIRECT_LOOKUP=1` forces them).
- `ROCE_READS_UNORDERED=1` makes the RC transport report itself unordered:
  `tests/test_direct_read.py` runs every one-sided test both ways on both
  backends.

### Robustness the tcp provider exposed

Testing the backend on libfabric's `tcp` provider (the same code, no EFA
needed) found two failure modes that would bite any provider:

- a reply to a client that closed its endpoint: the provider retried its
  connection on every `fi_send` attempt while holding the domain's
  progress lock, freezing every endpoint in the process. Posts now back off,
  give up when the peer is known gone (the worker's liveness hook), and are
  bounded by `ROCE_OFI_POST_TIMEOUT_MS`.
- the server reading from / replying to a removed AV entry crashed inside
  the provider: fixed by the epoch scheme above.

## 2. Build integration

- `roce/native/build.sh`: `ROCE_BACKEND=auto|verbs|ofi` (auto: ofi when
  `/sys/class/infiniband/efa*` exists). libfabric from `ROCE_LIBFABRIC`, the
  EFA installer's `/opt/amazon/efa`, a previous build, the system, the
  distro package, or built from source (v2.1.0). Same output names either
  way; `roce/native/.backend` records which. Validated here: both backends
  build with `-Werror`, the ofi one against a libfabric built **with the
  efa provider** (and rdma-core v59.2's libefa).
- `roce/native/build_lib.py` (tests, harnesses): `ROCEPOOL_BACKEND=ofi`
  builds everything on the ofi backend; `ROCE_OFI_PROVIDER` defaults to
  `efa` on an EFA host, else `tcp`.

## 3. Preflight and on-site

`tools/roce_preflight.py` tells three device kinds apart:

- **roce**: as before.
- **rxe**: the RoCE checks (GID, port, MTU, RoCE v2 mode) without the
  lossless one -- rxe retransmits in software, there is no PFC on AWS;
  exports `ROCE_ACK_TIMEOUT=14`. `--fix --soft-roce` creates rxe0 on the
  interface carrying `--ip` (else the default route's) and raises its MTU
  to 9001.
- **efa**: no GIDs or rdma_cm to check. Instead: the bootstrap IP, the
  libfabric efa provider answering `fi_info -p efa -t FI_EP_RDM`, and
  hardware RDMA read by instance family (EC2 metadata; Nitro v4+ PASS,
  others WARN -- libfabric then emulates reads with the target's CPU).
  Exports `ROCE_BACKEND=ofi ROCE_OFI_PROVIDER=efa`.

`tools/roce_onsite.py` builds the backend the preflight chose and passes
`--soft-roce` to every peer's preflight.

## 4. `tools/aws_cluster.py`

Resources: a key pair, a security group (all traffic among members in and
out -- EFA requires the self-referencing rules -- and SSH from this
machine's IP only), a cluster placement group, N instances of the Ubuntu
24.04 AMI (SSM parameter; arm64 for Graviton types) in an AZ that offers
the type, an EFA interface each in efa mode, 200 GiB gp3. Every instance
runs `shutdown -h +<max-hours>` from user data with shutdown behaviour
*terminate*: the machines go away even if the tool dies. All IDs are
written to `aws_runs/<run-id>/state.json` as they are created; `down`
releases them (also on any failure after launch, and on Ctrl-C).

Host setup: packages; rxe: `rdma_rxe` (`linux-modules-extra`), rxe0,
MTU 9001; efa: the AWS EFA installer, `fi_info` check; root SSH with the
run's key, and a host-to-host key generated on the head node. Then the tree
is shipped to the head node and `./run.sh --peers <private IPs> --ip
<head private IP>` runs there; its progress lines are relayed; the
reports come back to `aws_runs/<run-id>/`.

`tests/test_aws_cluster.py` (5) runs the tool against a fake `aws` CLI and
fake ssh/scp: the exact resources and their parameters for both modes, the
host scripts, the head-node command, report retrieval, release after a
failed setup, a refused non-EFA type creating nothing, `--keep` + `down`.

## 5. Validation

| | result |
|---|---|
| full suite, softverbs backend (RC) | **396 run, all pass** |
| full suite, ofi backend (libfabric tcp provider) | **396 run, all pass** (8 skipped: RC-QP semantics of the emulator) -- servers, FUSE client, interception library, KV API, NVMe tier, one-sided reads, NUMA pinning, the 3-node IO500 cluster |
| one-sided paths, unordered validation | `test_direct_read.py` (13) on both backends, ordered and unordered |
| 3-node IO500 simulation on the ofi backend (`stress_reports/44`) | runs end to end over libfabric: measured **3.64** (softverbs backend, same rig: 3.57). The probe chose RPC reads (3.05 vs 2.69 with one-sided): on the tcp provider an unordered one-sided read is two round trips through the same kernel sockets as an RPC -- the auto-selection doing its job |
| ofi backend against libfabric built with the efa provider | builds and links (`-Werror`) |
| on EFA hardware / on rxe | **not run here** (this sandbox has no RDMA-capable kernel and no AWS access): `tools/aws_cluster.py` is the first run |

What to watch on the first EFA run (all in `onsite_report.json` and the
server logs): the preflight's `libfabric` / `rdma_read` lines; the
transport step (pooling, stale-connection recovery, back-pressure over
EFA); the client probe's choice between one-sided and RPC reads (the
unordered stripe read costs two round trips, and an EFA round trip is longer
than a RoCE NIC's; the probe measures both and decides).
