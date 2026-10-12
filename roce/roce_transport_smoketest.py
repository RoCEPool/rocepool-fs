#!/usr/bin/env python3
"""Two-process smoke test for RoCETransport, meant to be run on a real
RDMA-capable machine (or a Soft-RoCE loopback setup) AFTER building
roce/native/libroce_transport.so -- see roce/native/build.sh.

This is the first thing to run against real hardware, before trusting
RoCETransport with an actual PoolNode. It exercises exactly the same
Transport interface contract that tests/test_loopback_transport.py
verifies against the loopback transport, so a pass here is a meaningful,
comparable signal -- not a different, weaker test.

Usage:
    # terminal 1, on the server host:
    python3 roce_transport_smoketest.py server 0.0.0.0:19000

    # terminal 2, on the client host (or the same host, for a Soft-RoCE
    # local test):
    python3 roce_transport_smoketest.py client <server-ip>:19000
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # project root, so `roce` imports regardless of CWD

from roce.protocol import Frame, Op  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402


def run_server(bind_address: str) -> None:
    transport = RoCETransport()

    def handler(frame: Frame) -> Frame:
        print(f"[server] got op={frame.op} key={frame.key!r} value_len={len(frame.value)}")
        if frame.op == Op.PUT:
            return Frame(op=Op.PUT_OK, request_id=frame.request_id, key=frame.key, value=b"")
        if frame.op == Op.GET:
            return Frame(op=Op.GET_OK, request_id=frame.request_id, key=frame.key,
                          value=b"echo:" + frame.value)
        return Frame(op=Op.ERROR, request_id=frame.request_id, key=frame.key, value=b"UNKNOWN_OP")

    handle = transport.serve(bind_address, handler)
    print(f"[server] listening on {handle.local_address()} -- Ctrl-C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        handle.stop()
        print("[server] stopped")


def run_client(peer: str) -> None:
    transport = RoCETransport()

    print(f"[client] sending PUT to {peer} ...")
    resp = transport.send_request(peer, Frame(op=Op.PUT, request_id=1, key="smoketest", value=b"hello-roce"))
    assert resp.op == Op.PUT_OK, f"expected PUT_OK, got {resp.op}"
    print("[client] PUT_OK received")

    print(f"[client] sending GET to {peer} ...")
    resp = transport.send_request(peer, Frame(op=Op.GET, request_id=2, key="smoketest", value=b"payload-123"))
    assert resp.op == Op.GET_OK, f"expected GET_OK, got {resp.op}"
    assert resp.value == b"echo:payload-123", f"unexpected echoed value: {resp.value!r}"
    print("[client] GET_OK received, echoed value matched")

    print(f"[client] sending a larger (1MB) payload ...")
    big = bytes(range(256)) * 4096
    resp = transport.send_request(peer, Frame(op=Op.GET, request_id=3, key="big", value=big))
    assert resp.value == b"echo:" + big, "large payload roundtrip mismatch"
    print("[client] large payload roundtrip OK")

    print("[client] ALL CHECKS PASSED")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("server", "client"):
        print(__doc__)
        raise SystemExit(2)
    if sys.argv[1] == "server":
        run_server(sys.argv[2])
    else:
        run_client(sys.argv[2])
