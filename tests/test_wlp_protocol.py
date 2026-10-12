"""Round-trip tests for the actual WLP v5 message structs (messages.py)
and the 8-byte frame header (framing.py), including a real loopback TCP
socket test of send_frame/recv_frame so the framing code is exercised
against real recv() short-read behavior, not just in-memory buffers.
"""
import socket
import threading
import unittest

from wlp.compact_protocol import CompactReader, CompactWriter
from wlp.framing import HEADER_SIZE, pack_header, recv_frame, send_frame, unpack_header
from wlp.messages import (
    ColumnMetadata, ColumnValue, ProtocolCompatibleRequest, QueryExecuteRequest,
    Row, RowDataResponse, RowMetadataResponse, StatementReceipt,
)


def _round_trip(obj):
    writer = CompactWriter()
    obj.write(writer)
    return type(obj).read(CompactReader(writer.bytes()))


class MessageRoundTripTests(unittest.TestCase):
    def test_protocol_compatible_request(self):
        obj = ProtocolCompatibleRequest(clientProtocolVersion=5, clientDescription="test-client")
        got = _round_trip(obj)
        self.assertEqual(got.clientProtocolVersion, 5)
        self.assertEqual(got.clientDescription, "test-client")

    def test_query_execute_request_defaults_omitted(self):
        obj = QueryExecuteRequest(query="SELECT 1")
        got = _round_trip(obj)
        self.assertEqual(got.query, "SELECT 1")
        self.assertIsNone(got.continueBatchOnError)
        self.assertIsNone(got.maxRowsToReturn)

    def test_row_metadata_and_data(self):
        meta = RowMetadataResponse(numColumns=2, columns=[
            ColumnMetadata(columnName="object_key", columnType=24),
            ColumnMetadata(columnName="object_version", columnType=5),
        ], isBoundedResult=True)
        got_meta = _round_trip(meta)
        self.assertEqual([c.columnName for c in got_meta.columns], ["object_key", "object_version"])

        data = RowDataResponse(rows=[
            Row(columns=[ColumnValue(stringVal="a/b.bin"), ColumnValue(bigintVal=3)]),
            Row(columns=[ColumnValue(isNull=True), ColumnValue(bigintVal=0)]),
        ])
        got_data = _round_trip(data)
        self.assertEqual(got_data.rows[0].columns[0].stringVal, "a/b.bin")
        self.assertEqual(got_data.rows[0].columns[1].bigintVal, 3)
        self.assertEqual(got_data.rows[1].columns[0].isNull, True)

    def test_binary_column_value_round_trips_exact_bytes(self):
        payload = bytes(range(256)) * 4  # exercises multi-byte varint length too
        cv = ColumnValue(binaryVal=payload)
        got = _round_trip(cv)
        self.assertEqual(got.binaryVal, payload)

    def test_statement_receipt_list(self):
        from wlp.messages import QueryReceiptResponse
        obj = QueryReceiptResponse(statementReceipts=[
            StatementReceipt(statementString="CREATE TABLE t (x INT)", statementId="s1", statementKey=42),
        ])
        got = _round_trip(obj)
        self.assertEqual(got.statementReceipts[0].statementId, "s1")
        self.assertEqual(got.statementReceipts[0].statementKey, 42)


class FrameHeaderTests(unittest.TestCase):
    def test_pack_unpack_matches_wlpheader_layout(self):
        raw = pack_header(msg_type=51, body_size=300)
        self.assertEqual(len(raw), HEADER_SIZE)
        # 4-byte big-endian size, then 1-byte type, matching WlpHeader.hpp.
        self.assertEqual(raw[0:4], (300).to_bytes(4, "big"))
        self.assertEqual(raw[4], 51)
        msg_type, body_size = unpack_header(raw)
        self.assertEqual((msg_type, body_size), (51, 300))

    def test_send_recv_frame_over_real_loopback_socket(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        host, port = server_sock.getsockname()

        received = {}

        def _server():
            conn, _ = server_sock.accept()
            msg_type, body = recv_frame(conn)
            received["msg_type"] = msg_type
            received["body"] = body
            conn.close()

        thread = threading.Thread(target=_server, daemon=True)
        thread.start()

        client_sock = socket.create_connection((host, port), timeout=5)
        # A body bigger than a typical single TCP recv buffer chunk, to
        # exercise recv_frame's short-read accumulation loop for real.
        body = b"x" * 200_000
        send_frame(client_sock, 51, body)
        client_sock.close()
        thread.join(timeout=5)
        server_sock.close()

        self.assertEqual(received["msg_type"], 51)
        self.assertEqual(received["body"], body)


if __name__ == "__main__":
    unittest.main()
