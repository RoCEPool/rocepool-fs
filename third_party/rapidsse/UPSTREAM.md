# RapidsSE: the upstream files rocepool refers to

**Project**: RapidsSE, the Rapids Storage Engine. Copyright (C) 2026 Boray
Data. Apache License 2.0 (`LICENSE`, `COPYRIGHT`, `README.md` here are the
upstream files, unmodified).

**Version**: source archive `rapidsdataproject-rapidsse-90012081f5a9`
(repository `rapidsdataproject/rapidsse`, commit `90012081f5a9`), supplied by
the project owner.

**What is here**: only the upstream files that rocepool's code was written
against, copied byte for byte (no change of any kind), at their upstream
paths. They are reference material: rocepool does not compile or link them.

| upstream file(s) | used by | for |
|---|---|---|
| `src/wlp/thrift/messageDefinitions/compatibility/*.thrift`, `v5/*.thrift`, `compile.sh` | `wlp/messages.py`, `wlp/compact_protocol.py` | WLP v5 message type ids, every message struct's field ids, names and types |
| `src/wlp/thrift/supports/WlpHeader.hpp` | `wlp/framing.py` | the 8-byte message header (32-bit big-endian size, 8-bit type, 24 spare bits) |
| `src/wlp/thrift/supports/BufferTransport.hpp` | `wlp/compact_protocol.py` | how a message body is serialized (Compact Protocol, no Thrift message envelope) |
| `src/store/AdaptorWL.cpp` | `wlp/compact_protocol.py`, `wlp/wlp_client.py` | the server side of the protocol: `vSerialiseAndSendMsg` framing, the handshake / authentication / parse-bind-execute sequence |

`storage_engine/rapidsse_engine.py` (SQL for RapidsSE's storage model) sits on
`wlp/` and refers to no other upstream file.

`tests/test_wlp_against_rapidsse.py` checks `wlp/` against these files on every
test run: message type ids, struct field ids, names and primitive types, and
the header layout. Updating to a newer RapidsSE: replace these files from the
new source (same paths), update the version line above, and run that test.
