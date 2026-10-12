"""Runs ONE RoCE scenario in its own process (ctypes caches the loaded
library per process, and each scenario may need a different build).
Library chosen via ROCE_TRANSPORT_LIB. Prints one line: OK <detail> or
ERR <message>. Used by tests/test_softverbs_rc_semantics.py.
"""
import sys
import threading
import time

sys.path.insert(0, sys.argv[1])

from roce.protocol import HEADER_SIZE, Frame, Op  # noqa: E402
from roce.roce_transport import RoCEStream, RoCETransport  # noqa: E402
from roce.transport_base import PeerUnreachable  # noqa: E402


def echo(frame: Frame) -> Frame:
    return Frame(op=Op.GET_OK, request_id=frame.request_id, key=frame.key, value=frame.value)


def slow_echo(frame: Frame) -> Frame:
    time.sleep(0.05)  # widens the window in which the server has no RECV posted
    return echo(frame)


def read_frame(s: RoCEStream) -> Frame:
    header = b""
    while len(header) < HEADER_SIZE:
        header += s.recv(HEADER_SIZE - len(header))
    n = Frame.peek_body_length(header)
    body = b""
    while len(body) < n:
        body += s.recv(n - len(body))
    return Frame.decode(header + body)


def main():
    scenario, port = sys.argv[2], int(sys.argv[3])
    addr = f"127.0.0.1:{port}"
    try:
        if scenario == "roundtrip":
            h = RoCETransport().serve(addr, echo)
            r = RoCETransport().send_request(addr, Frame(op=Op.GET, request_id=1, key="k", value=b"x" * 5000))
            h.stop()
            print("OK" if r.value == b"x" * 5000 else "ERR mismatch")
        elif scenario == "pipelined":
            h = RoCETransport().serve(addr, slow_echo)
            with RoCEStream.connect(addr) as s:
                for i in range(3):
                    s.sendall(Frame(op=Op.GET, request_id=i, key="k", value=bytes([i]) * 100).encode())
                got = [read_frame(s).value for _ in range(3)]
            h.stop()
            print("OK" if got == [bytes([i]) * 100 for i in range(3)] else f"ERR {got!r}")
        elif scenario == "oversize":
            # a raw SEND (stream) bigger than the peer's posted RECV buffer:
            # RC rejects it (request/response calls never do this -- their
            # bulk is RDMA READ by the server, and a frame over its limit
            # is refused at the envelope)
            h = RoCETransport(max_frame_bytes=64 * 1024).serve(addr, echo)
            with RoCEStream.connect(addr, max_frame_bytes=1 << 20) as s:
                s.sendall(Frame(op=Op.GET, request_id=1, key="k", value=b"y" * 100_000).encode())
                read_frame(s)
            h.stop()
            print("ERR oversized SEND was accepted")
        elif scenario == "bulk":
            # a request and a reply larger than a SEND: the bulk goes by RDMA
            # READ (server pulls the request body) and RDMA WRITE with
            # immediate (server pushes the reply), never through a SEND
            h = RoCETransport().serve(addr, echo)
            r = RoCETransport().send_request(addr, Frame(op=Op.GET, request_id=1, key="k", value=b"z" * 300_000))
            h.stop()
            print("OK" if r.value == b"z" * 300_000 else "ERR mismatch")
        elif scenario == "too-big-for-server":
            # request/response: the server refuses a frame over ITS limit
            # and the caller sees a failure, not a hang
            h = RoCETransport(max_frame_bytes=64 * 1024).serve(addr, echo)
            RoCETransport(max_frame_bytes=1 << 20).send_request(
                addr, Frame(op=Op.GET, request_id=1, key="k", value=b"y" * 100_000))
            h.stop()
            print("ERR oversized request was accepted")
        elif scenario == "no-listener":
            RoCETransport().send_request(addr, Frame(op=Op.GET, request_id=1, key="k", value=b""))
            print("ERR connected to nothing")
    except PeerUnreachable as exc:
        print(f"ERR {exc}")
    except Exception as exc:  # noqa: BLE001
        print(f"ERR {type(exc).__name__}: {exc}")
    sys.stdout.flush()


if __name__ == "__main__":
    t = threading.Timer(20, lambda: (print("ERR scenario timed out"), sys.stdout.flush(), __import__("os")._exit(3)))
    t.daemon = True
    t.start()
    main()
    __import__("os")._exit(0)
