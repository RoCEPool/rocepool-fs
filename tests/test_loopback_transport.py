import threading
import unittest

from roce.loopback_transport import LoopbackTransport
from roce.protocol import Frame, Op
from roce.transport_base import PeerUnreachable


def echo_handler(req: Frame) -> Frame:
    return Frame(op=Op.GET_OK, request_id=req.request_id, key=req.key, value=req.value)


class LoopbackTransportTests(unittest.TestCase):
    def setUp(self):
        self.transport = LoopbackTransport()
        self.handle = self.transport.serve("127.0.0.1:0", echo_handler)
        self.addCleanup(self.handle.stop)

    def test_request_response_roundtrip_over_real_socket(self):
        resp = self.transport.send_request(
            self.handle.local_address(), Frame(op=Op.GET, request_id=7, key="k", value=b"payload"))
        self.assertEqual(resp.request_id, 7)
        self.assertEqual(resp.value, b"payload")

    def test_multiple_sequential_requests_same_connection_semantics(self):
        addr = self.handle.local_address()
        for i in range(20):
            resp = self.transport.send_request(addr, Frame(op=Op.GET, request_id=i, key="k", value=str(i).encode()))
            self.assertEqual(resp.request_id, i)
            self.assertEqual(resp.value, str(i).encode())

    def test_concurrent_clients_get_correctly_matched_responses(self):
        addr = self.handle.local_address()
        results = {}
        errors = []

        def worker(i):
            try:
                resp = self.transport.send_request(addr, Frame(op=Op.GET, request_id=i, key=f"k{i}", value=b"x" * i))
                results[i] = resp
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 32)
        for i, resp in results.items():
            self.assertEqual(resp.value, b"x" * i, f"request {i} got a mismatched response -- framing bug")

    def test_unreachable_peer_raises_peer_unreachable_not_a_raw_socket_error(self):
        with self.assertRaises(PeerUnreachable):
            self.transport.send_request("127.0.0.1:1", Frame(op=Op.GET, request_id=1, key="k"), timeout_s=1.0)

    def test_large_payload_roundtrips_correctly(self):
        payload = bytes(range(256)) * 20000  # 5.12MB, forces multiple recv() calls
        resp = self.transport.send_request(
            self.handle.local_address(), Frame(op=Op.GET, request_id=99, key="big", value=payload))
        self.assertEqual(resp.value, payload)


if __name__ == "__main__":
    unittest.main()
