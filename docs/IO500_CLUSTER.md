# N-node IO500: automated run, bottleneck analysis, projection

The **official IO500 app** (github.com/IO500/io500, unmodified) runs across N
nodes. Every node gets:

- one native MDT and one native OST (more per node with
  `--mdts-per-node` / `--osts-per-node`);
- a native C client mount;
- MPI ranks.

Every IO500 phase is scored: ior-easy/hard write/read, mdtest-easy/hard,
and find. Between phases the run snapshots the counters of every server
and every process. The report names each phase's bottleneck and projects
the result onto target hardware.

```bash
# on site: N real hosts (ssh key auth to each peer)
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 \
     --peers root@n2=192.168.50.2,root@n3=192.168.50.3,...   --bench full

# simulated N-node cluster on one Linux host (root)
sudo python3 tools/io500_run.py --nodes 10

# the REAL multi-host code path rehearsed on one host (fake sshd per node)
sudo python3 tests/fakes/cluster_rehearsal.py 3 report.json --stonewall 3
```

**What `roce_onsite.py --peers` automates** (step 7):

1. Ship the tree to the same path on every peer.
2. Run the preflight (`--fix`) and the build on each peer.
3. Check that the IO500 binary runs there.
4. Write `onsite_cluster.json`.
5. Run `tools/io500_run.py` against the cluster:
   - `--bench quick`: 30 s stonewall.
   - `--bench full`: 60 s stonewall, plus the untouched official 300 s
     procedure for the score itself.

Inside that run, `io500_run.py` also:

- **Refuses cloned hostnames.** MPI would treat two hosts with the same
  name as one machine.
- **Checks and repairs name resolution** (see below).
- **Projects onto the hosts' own profile** (`--project auto`): their cores,
  RAM and NIC speed.

## What is measured (per phase, from counter deltas)

- **Servers** (new `STATS` op, 50/51):
  - requests;
  - handler CPU time and wall time;
  - bytes in/out;
  - MDT lock wait and hold time. The MDT has one lock.
- **Processes** (`tools/procstat.py`): CPU per thread group:
  - `rt-*`: server transport and handlers;
  - `fuse-worker` / `fuse-io`: client;
  - IO500 ranks;
  - `sv-*`: the softverbs RDMA emulator.

  The emulator's CPU is reported but not carried into the projection,
  because a real NIC does that work.
- **Client**: per-opcode FUSE request counts. `rocepool-fuse` dumps them on
  SIGUSR1.
- **Operations**: each phase's real op count comes from IO500's own
  per-phase output (mdtest summary rate x time, the IOR result row, find's
  runtime x rate).

## Model (`tools/io500_model.py`)

Each phase's throughput on a hardware profile is the minimum over these
limits (operational laws). The binding one is that phase's bottleneck.

| limit | meaning |
|---|---|
| `cpu_cluster` | all cores / total CPU per op |
| `cpu_hot_node` | the busiest node: its share of server work plus its clients |
| `mdt_lock` | 1 / hold time per op on the hottest MDT |
| `client_threads` | FUSE worker threads per node |
| `fuse_mount` | kernel FUSE channel per mount (estimate: 150 k req/s, 4 GiB/s) |
| `shared_file_lock` | ior-hard: every rank on a node writes one inode |
| `nic` | link bandwidth |
| `latency` | ranks x round trips |
| `capacity` | a valid 300 s write phase must fit in the pool's RAM |

**Check on a simulated run.** Every node shares this host's cores there,
and the run is CPU-bound. So the host's cores divided by the measured
total CPU per op must reproduce each measured rate. On the 10-node run
this holds within **0.93-1.13x over all 12 phases**: the per-op costs
account for all the work. Every report includes this check
(`analysis.calibration`).

The profile values that are estimates rather than measurements are the
FUSE constants, the lock convoy factor, the RDMA per-message CPU and the
CPU speed. They are swept as a pessimistic/optimistic band. The on-site
run replaces all of them with measured values.

## Update: after striped directories, zero-copy bulk and FUSE over io_uring

`docs/SCALE_OUT_ROUND.md` has the rerun: the hot-MDT lock is gone as a
bottleneck, the projection is 98.2 on the classic FUSE channel (band
64-154) and 153.5 with io_uring's estimated per-mount rate, against WEKA
Afton's 105.94. `io500_run.py --fuse-uring auto` (default) probes both
channels on the hosts and uses the faster. The section below is the
analysis that motivated those changes, kept as the baseline.

## Update: interception client and the NVMe tier

`docs/KERNEL_BYPASS_ROUND.md`: `io500_run.py --client auto` (default) now
probes a third client, the LD_PRELOAD interception library in every rank
(no mount), against both FUSE channels; `--ost-tier auto` (default) runs
`tools/nvme_preflight.py` on every real host and starts each OST with a
write-through tier on its NVMe (and the drive's P2P memory when NIC, drive
and kernel allow it), so a valid 300 s write phase no longer has to fit in
RAM. Options: `--client fuse|intercept`, `--ost-tier none|file:PATH[:GIB]|
spdk:TRADDR`, `--ost-ram-cache-mib`, `--intercept-io-threads`.

## Result: 10 simulated nodes, projected onto 10 x (32 cores, 100 GbE, 512 GiB)

**Run.** `io500_run.py --nodes 10`: 20 native servers, 10 mounts and 20
ranks on 2 shared cores. Report: `stress_reports/28_io500_sim10.json`.

| | projected | WEKA Afton (10 nodes) |
|---|---|---|
| bandwidth | 13.3 GiB/s | 33.3 GiB/s |
| metadata | 336 kIOPS | 337 kIOPS |
| **score** | **66.9** (band 38-104) | 105.9 |

That is about **1.6x behind WEKA**: roughly 2.5x on bandwidth and even on
metadata.

Bottlenecks, per phase:

| phases | binding limit | projected |
|---|---|---|
| ior-easy-write, ior-hard-write | **RAM for a 300 s write phase** | 7.7 / 2.6 GiB/s |
| mdtest-hard write/stat/read/delete, find | **the hot MDT's single lock**: all of one directory's entries sit on ONE MDT (placement by parent directory) | 55 / 275 / 122 / 162 kIOPS |
| mdtest-easy, ior reads | **kernel FUSE channel per mount** | 374-1495 kIOPS, 40 GiB/s |
| ior-hard-write, right behind capacity | the shared file's inode lock per node | 2.8 GiB/s |

What each fix would be worth (cumulative):

| change | score |
|---|---|
| as is | 66.9 |
| + a directory's entries spread over all MDTs by name hash, finer MDT locks | 97.7 |
| + FUSE over io_uring (Linux 6.14+, per-core queues) | 154.8 |
| + an NVMe tier behind the memory pool (capacity for 300 s writes) | 255.9 |

RAM per node sets the bandwidth half of the score:

| RAM per node | score | bandwidth |
|---|---|---|
| 256 GiB | 56 | - |
| 512 GiB | 67 | 13.3 GiB/s |
| 1 TiB | 74 | - |
| 2 TiB | 81 | 19.3 GiB/s |

**Bigger machines barely help.** 10 x (64 cores, 200 GbE, 1 TiB) projects
**74**. The binding limits are serial ones per mount and per MDT, not core
counts.

## Found by the rehearsal (fixed; each covered by `tests/test_io500_cluster.py`)

- **MPICH finds ranks by host *name*.** Every host must resolve every
  host's hostname to a reachable address:
  - A name that does not resolve made `MPI_Init` **hang silently**.
  - A name that resolves to loopback (Debian's `127.0.1.1 <hostname>`)
    made connections fail.
  - Neither `MPIR_CVAR_CH3_INTERFACE_HOSTNAME` nor `-iface` avoids this
    (interface names differ per host).

  **Fix.** Before MPI starts, `io500_run.py` checks on every host. Only
  broken names get a marked block at the top of `/etc/hosts` (as root; a
  backup is kept at `/etc/hosts.rocepool.bak`). A name that already
  resolves to a real address is left alone. If a fix is impossible (not
  root), the run stops with the exact lines to add.
- **The model divided counter deltas by `rate x IO500 phase time`.** That
  time includes setup and syncs, so it overstated a short mdtest phase's
  work 4x. The metadata projection came out 4x optimistic: 135 instead of
  67. Op counts now come from each phase's own output.
- `python3 tools/netns_cluster.py up N` (the documented command) crashed
  on an import.

**Found by an independent review of the on-site path** (fixed before it
ever ran on hardware):

- **The peers never got MPI or IO500.** `--exclude=.bench_cache` also drops
  explicitly named tar members, so cluster-prep failed every peer. The
  builds now go in a second archive.
- **Every `--valid` (official 300 s) run was killed after 300 s.** Start was
  detected from pause-file lines, which IO500 prints only in pause mode.
  Start is now detected from `; START`, and in valid / `--no-pause` mode the
  counters are taken at IO500's `[phase]` headers. The rehearsal test runs
  this mode.
- **Hangs.**
  - A tar blocked on a full pipe when ssh died.
  - A first ssh to a host not yet in known_hosts failed.
  - A server that never printed READY blocked the run.
  - A stuck phase had no watchdog; there is now a no-progress watchdog.
  - A partially written result line was skipped for good.
  - A 6 h timeout SIGKILLed the run with no cleanup. The run now takes
    SIGTERM, and its `finally` unmounts and stops everything on every host.

  Every ssh now uses `StrictHostKeyChecking=accept-new` (a changed key
  still fails), `ConnectTimeout` and keepalives.
- **Cleanup** `pkill`ed every `hydra_pmi_proxy` on the hosts, including other
  MPI jobs'. It now matches only processes started from this tree. The
  rehearsal fails if anything is left behind, and its fake sshd, like real
  sshd without a tty, no longer kills commands on disconnect.
- **Name resolution.** A name is now broken if any host cannot resolve it,
  resolves it to loopback, or resolves it differently from another host
  (e.g. nss-myhostname giving only the host itself its docker0 address).
  Other fixes:
  - The block is rebuilt from the current cluster on every run.
  - `/etc/hosts` is written atomically (temp file, fsync, rename; in place
    for a bind-mounted file; `restorecon` for SELinux).
  - A non-UTF-8 file no longer passes as "not root".
  - `hostname` output is validated.
- **Counters.**
  - Processes were matched by PID alone across hosts, and PIDs repeat. They
    are now matched by (node, pid).
  - A failed peer snapshot became silent zeros or aborted the run. It is now
    recorded, and the report warns that those phases' projections are
    optimistic.
  - procstat now runs on Python < 3.9.
- **Environment.** Node 0's `ROCE_*` tuning overrode each peer's own
  `roce_env.sh`. Only cluster-wide settings are passed now.
- **Model.** Divisions by zero are guarded.

**Not binding, noted.** A stripe object that grows by 47 KB `WRITE_AT`s is
reallocated on each growth: 1.2 us/KB of OST handler CPU vs 0.5 on
ior-easy.

## Tests

`tests/test_io500_cluster.py`:

- the op-count parser;
- a simulated 3-node run: all scored phases, counters from every server
  and node, the calibration check, the hot MDT;
- the real multi-host path over fake sshds: cluster file, ssh, MPI by IP,
  name-resolution fix, per-host facts, auto profile, the valid-run
  (no-pause) code path, and nothing left behind on any node.

The rehearsal uses `tests/fakes/fake_sshd.py` + `tests/fakes/ssh` because
this environment has no sshd. They keep what the automation depends on:
a fresh login environment, separate stdout/stderr, exit status, and
no kill on disconnect (as real sshd without a tty). Still not rehearsed
here: separate filesystems per host (the tree and the builds are really
shipped), host keys, and `roce_onsite.py`'s own `cluster_step`. The first
on-site run covers those.
