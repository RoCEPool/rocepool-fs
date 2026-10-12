"""What CAN be tested about roce/roce_transport.py without real RDMA
hardware or a built libroce_transport.so (neither exists in this sandbox
-- see roce_transport.py's module docstring and this project's README):
that it fails the way it documents itself as failing, rather than
crashing some other way, when asked to do real RDMA work it can't do.
This covers both the pre-existing RoCETransport and the new RoCEStream
duck-typed adapter added for wlp_client.py.
"""
import unittest

from roce.roce_transport import PeerUnreachable, RoCENotAvailable, RoCEStream, RoCETransport, _load_library
from roce.protocol import Frame, Op


def _library_loads() -> bool:
    try:
        _load_library()
        return True
    except RoCENotAvailable:
        return False


class RoCETransportUnavailableTests(unittest.TestCase):
    def test_roce_transport_construction_raises_roce_not_available(self):
        if _library_loads():
            # A built library is present (roce/native/build.sh, e.g. after an
            # on-site build): construction then succeeds by design, and doing
            # RDMA work without a reachable RDMA peer/device must still fail
            # the documented way -- promptly, not hang.
            t = RoCETransport(timeout_ms=2000)
            with self.assertRaises((PeerUnreachable, RoCENotAvailable)):
                t.send_request("127.0.0.1:1", Frame(op=Op.GET, request_id=1, key="k", value=b""))
            return
        with self.assertRaises(RoCENotAvailable):
            RoCETransport()

    def test_roce_stream_connect_raises_roce_not_available(self):
        with self.assertRaises(RoCENotAvailable):
            RoCEStream.connect("127.0.0.1:19999")


if __name__ == "__main__":
    unittest.main()
