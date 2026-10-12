# Architecture

## Layers, bottom to top

### 1. `storage_engine/` -- pluggable single-node storage

`storage_engine/base.py` defines `StorageEngine`: `put`/`get`/`delete`/
`stat`/`list_prefix`/`exists`/`total_bytes`, plus optimistic
compare-and-swap via `expected_version` on `put`. Every method must be
safe under concurrent calls from multiple threads in one process.

`storage_engine/memory_engine.py` is a real, working reference
implementation: a dict behind a lock. It is what every test in this
project actually runs against. A production engine (RapidsSE-backed, once
its licensing is confirmed independently -- see the main README -- or
anything else) implements the same interface; `tests/
test_storage_engine_contract.py` is written against the interface, not
`MemoryEngine`'s internals, specifically so it can be pointed at a
replacement engine unmodified.

### 2. `roce/protocol.py` -- wire format

A fixed 24-byte header (magic, opcode, request ID, key length, value
length) followed by the key and value bytes. Transport-agnostic on
purpose: both transports below carry the exact same bytes, so a test
passing against the loopback transport is exercising real protocol logic,
not a stand-in that happens to also pass.

### 3. `roce/transport_base.py` + two implementations

`Transport` is two methods: `send_request(peer, frame) -> frame` and
`serve(bind_address, handler) -> ServerHandle`.

- `loopback_transport.py`: real TCP sockets. Tested (12 tests: concurrent
  clients, large payloads, unreachable-peer handling). Exists so
  `pool_node.py` can be developed and CI-tested without RDMA hardware.
- `roce_transport.py` + `native/roce_transport.c`: real
  `rdma_cma`/`ibverbs` RC (Reliable Connected) queue pairs, two-sided
  SEND/RECV. Written against the documented API; compiled and run between
  two network nodes against softverbs (a strict userspace RC emulator,
  `native/softverbs/`) -- but never on a real RDMA NIC. See
  `docs/CROSSOVER_RDMA_TEST.md` and `docs/ROCE_DEPLOYMENT_NOTES.md`.

Both satisfy the identical interface, which is what makes
`roce/pool_node.py`'s actual distributed logic transport-independent: it
is written and tested once, against `Transport`, never against either
concrete implementation's internals.

### 4. `roce/pool_node.py` -- the actual multi-node memory pool

Given a local `StorageEngine` and a `Transport`, `PoolNode` makes a key
look like it lives in one flat namespace across every node in the
cluster:

- **Ownership**: `sha256(key) % len(nodes)` picks one owning node from a
  fixed, sorted node list agreed at construction time. Every node computes
  the same owner for the same key without needing to ask anyone (verified
  by `test_every_node_agrees_on_the_owner_of_a_given_key`).
- **Local vs. forwarded**: `put`/`get`/`delete`/`stat` check `is_local()`
  first; if the key belongs to this node, it's a direct local
  `StorageEngine` call with no network hop. Otherwise the request is
  forwarded, as an encoded `Frame`, to the owning peer, which runs the
  same local-engine call on its own copy of the data and replies.
- **`list_prefix`** cannot be routed to one owner -- a directory's
  children are scattered across the cluster by hash, not co-located -- so
  it fans out to every node and merges the results. This is the one pool
  operation whose cost scales with cluster size; noted, not hidden.
- **Errors cross the network as a small closed vocabulary**
  (`NOT_FOUND`, `VERSION_CONFLICT`, `STORAGE_ERROR`, ...) via
  `Op.ERROR` frames, re-raised locally as the matching Python exception
  type (`ObjectNotFound`, `VersionConflict`, `RemoteError` for anything
  else) -- never a raw, un-typed failure.

### 5. `fs/posix_api.py` -- POSIX-flavored file operations

Layered on top of anything satisfying a minimal `put`/`get`/`delete`/
`stat`/`list_prefix`/`exists` protocol -- a bare `StorageEngine` or a full
`PoolNode`, identically. Adds real path/directory semantics (`mkdir`,
`rmdir`, `listdir`, parent-must-exist-unless-`parents=True`,
`FileNotFoundInPool`/`IsADirectory`/`DirectoryNotEmpty`-shaped errors) on
top of the flat key/value namespace below, using `/`-joined keys and a
`<dir>/.dir` sentinel object to mark directories.

## Why the interfaces are drawn where they are

Every layer boundary above exists because it is also a *test* boundary:

- `StorageEngine` lets the engine be swapped (or run standalone) without
  touching pooling or file-semantics code.
- `Transport` lets RDMA be developed and tested against a TCP stand-in
  that carries the identical wire bytes, so pooling logic gets full test
  coverage on any machine, and only the transport itself needs real
  hardware to validate.
- The `_Backend` protocol in `fs/posix_api.py` (structural, not nominal --
  see `typing.Protocol`) lets the file layer sit on a bare engine or a
  full pool without caring which.

This is also why the bugs found during development (documented in the
project's delivery notes) were caught at all: each layer has its own
focused test suite that exercises it in isolation, over real transports,
rather than one end-to-end test that could pass by accident while masking
a bug in any one layer.
