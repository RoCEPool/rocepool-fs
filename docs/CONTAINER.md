# rocepool in a container

`ghcr.io/rocepool/rocepool-fs:<version>` (and `:latest`) is published with
every release. It holds the whole tree under `/opt/rocepool` and two builds:

- **For RoCE NICs (verbs backend).** The transport library, the native
  servers, the FUSE client, and the interception, KV and cuFile libraries.
  `ROCE_TRANSPORT_LIB` and `ROCEPOOL_KV_LIB` are already set for it.
- **For the software RDMA emulator** (`roce/native/softverbs/`), which needs
  no RDMA hardware. It runs the test suite and the examples.

The image is meant for:

- trying rocepool on any machine;
- running clients (FUSE, `LD_PRELOAD`, KV, GPU) next to applications that
  live in containers;
- running servers in a container on an RDMA host.

For a full unattended cluster run (preflight, IO500, report), the offline
package from the Releases page and `sudo ./run.sh` remain the way: they
tune the hosts themselves (MTU, PFC/ECN, power states, NVMe), and that
needs the host, not a container.

## Quick reference

```bash
# try it, no RDMA hardware: native servers + clients over the RDMA emulator
docker run --rm ghcr.io/rocepool/rocepool-fs \
  python3 -m unittest tests.test_kv_api tests.test_kv_gpu.KVGpuTests -v

# on an RDMA (RoCE) host: the NIC, host networking, an OST on this host's RoCE IP
docker run --rm -i --network host --device /dev/infiniband --cap-add IPC_LOCK --ulimit memlock=-1 \
  ghcr.io/rocepool/rocepool-fs python3 tools/crossover_node.py ost-native 192.168.10.19:19951
#   (-i: the server runs until its stdin closes; mdt-native for a metadata server)

# a KV client in a container (the OST list of your cluster)
docker run --rm --network host --device /dev/infiniband --cap-add IPC_LOCK --ulimit memlock=-1 \
  ghcr.io/rocepool/rocepool-fs python3 -c \
  "from roce.kv import KVClient; kv = KVClient(['192.168.10.19:19951']); kv.put('k', b'v'); print(kv.get('k'))"

# GPU clients (GPUDirect Storage, KV cache in GPU memory) need the NVIDIA runtime:
#   --gpus all, and /opt/rocepool/roce/native/cufile first in LD_LIBRARY_PATH (docs/GPUDIRECT_STORAGE.md)
```

The full documentation is in the repository:
https://github.com/RoCEPool/rocepool-fs
