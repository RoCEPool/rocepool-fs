"""A real WLP v5 server for testing wlp_client.py end-to-end over an
actual TCP socket and the actual Compact Protocol wire bytes.

This does NOT pretend to be RapidsSE's real C++ engine -- it is a test
double for the *protocol*, not for RapidsSE's SQL execution semantics.
SQL execution is delegated to real sqlite3 (the same "genuinely execute,
don't mock" approach used for tests/fakes/sqlite_dbapi_client.py earlier
in this project), so what this proves is: wlp_client.py's framing,
Compact Protocol encoding/decoding, and handshake/execute/bind message
sequencing round-trip correctly over a real socket against an
independent, from-spec implementation of the same wire format written on
the server side. It does NOT prove wire-compatibility with RapidsSE's
actual C++ server -- nothing in this sandbox can run that server (no
compiler toolchain exercised, no live cluster, no network egress to one).
"""
from __future__ import annotations

import sqlite3
import threading
import uuid

from wlp.compact_protocol import CompactReader, CompactWriter
from wlp.framing import recv_frame, send_frame
from wlp.messages import (
    MESSAGE_CLASSES, MSG_AUTHENTICATION_OK_RESPONSE, MSG_ERROR_RESPONSE,
    MSG_PARSE_STATEMENT_REQUEST, MSG_PARSE_STATEMENT_RESPONSE,
    MSG_PLAIN_AUTHENTICATION_REQUEST, MSG_PROTOCOL_COMPATIBLE_REQUEST,
    MSG_PROTOCOL_COMPATIBLE_RESPONSE, MSG_QUERY_EXECUTE_REQUEST, MSG_QUERY_EXECUTE_RESPONSE,
    MSG_QUERY_RECEIPT_RESPONSE, MSG_ROW_DATA_RESPONSE, MSG_ROW_METADATA_RESPONSE,
    MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST, MSG_STATEMENT_RESPONSE,
    AuthenticationOkResponse, ColumnMetadata, ColumnValue, ErrorResponse,
    ParseStatementResponse, ProtocolCompatibleResponse, QueryExecuteResponse,
    QueryReceiptResponse, Row, RowDataResponse, RowMetadataResponse, StatementReceipt,
    StatementResponse,
)

import socket
import select


def _py_to_column_value(v) -> ColumnValue:
    if v is None:
        return ColumnValue(isNull=True)
    if isinstance(v, bool):
        return ColumnValue(booleanVal=v)
    if isinstance(v, int):
        return ColumnValue(bigintVal=v)
    if isinstance(v, float):
        return ColumnValue(doubleVal=v)
    if isinstance(v, (bytes, bytearray)):
        return ColumnValue(binaryVal=bytes(v))
    return ColumnValue(stringVal=str(v))


def _column_value_to_py(cv: ColumnValue):
    if cv.isNull:
        return None
    for attr in ("booleanVal", "tinyIntVal", "smallIntVal", "integerVal",
                 "bigintVal", "doubleVal", "stringVal", "binaryVal"):
        val = getattr(cv, attr, None)
        if val is not None:
            return val
    return None


class WlpTestServer:
    """Threaded, real TCP WLP server backed by an in-memory sqlite3 db."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, port))
        self._listener.listen(8)
        self.host, self.port = self._listener.getsockname()
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        self._prepared: dict[str, str] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._listener.close()
        except OSError:
            pass
        self._thread.join(timeout=2)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            readable, _, _ = select.select([self._listener], [], [], 0.2)
            if not readable:
                continue
            try:
                conn, _addr = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve_conn, args=(conn,), daemon=True).start()

    def _send(self, conn, msg_type: int, struct_obj) -> None:
        writer = CompactWriter()
        struct_obj.write(writer)
        send_frame(conn, msg_type, writer.bytes())

    def _recv(self, conn):
        msg_type, body = recv_frame(conn)
        cls = MESSAGE_CLASSES[msg_type]
        obj = cls.read(CompactReader(body)) if body else cls()
        return msg_type, obj

    def _serve_conn(self, conn: socket.socket) -> None:
        try:
            self._handshake(conn)
            while True:
                msg_type, req = self._recv(conn)
                if msg_type == MSG_QUERY_EXECUTE_REQUEST:
                    self._handle_query_execute(conn, req)
                elif msg_type == MSG_PARSE_STATEMENT_REQUEST:
                    self._handle_parse(conn, req)
                elif msg_type == MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST:
                    self._handle_bind_execute(conn, req)
                else:
                    self._send(conn, MSG_ERROR_RESPONSE,
                               ErrorResponse(errorString=f"unsupported message type {msg_type}"))
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handshake(self, conn) -> None:
        msg_type, _req = self._recv(conn)
        assert msg_type == MSG_PROTOCOL_COMPATIBLE_REQUEST
        self._send(conn, MSG_PROTOCOL_COMPATIBLE_RESPONSE, ProtocolCompatibleResponse(
            protocolVersion=5, serverSoftwareVersion="rocepool-wlp-test-server",
            serverBuildInfo="test-fixture",
        ))
        msg_type, _req = self._recv(conn)
        assert msg_type == MSG_PLAIN_AUTHENTICATION_REQUEST
        self._send(conn, MSG_AUTHENTICATION_OK_RESPONSE, AuthenticationOkResponse())

    def _run_and_reply(self, conn, sql: str, params: tuple) -> None:
        try:
            cursor = self._db.cursor()
            cursor.execute(sql, params)
            if cursor.description is not None:
                columns = [d[0] for d in cursor.description]
                self._send(conn, MSG_ROW_METADATA_RESPONSE, RowMetadataResponse(
                    numColumns=len(columns),
                    columns=[ColumnMetadata(columnName=c, columnType=24) for c in columns],  # 24=VARCHAR
                    isBoundedResult=True,
                ))
                rows = cursor.fetchall()
                if rows:
                    self._send(conn, MSG_ROW_DATA_RESPONSE, RowDataResponse(rows=[
                        Row(columns=[_py_to_column_value(v) for v in row]) for row in rows
                    ]))
            self._db.commit()
            self._send(conn, MSG_STATEMENT_RESPONSE, StatementResponse(
                statementId=str(uuid.uuid4()), numMatchedRows=cursor.rowcount if cursor.rowcount >= 0 else 0,
                numAffectedRows=cursor.rowcount if cursor.rowcount >= 0 else 0, affectedVerb=4,
            ))
        except sqlite3.Error as exc:
            self._send(conn, MSG_ERROR_RESPONSE, ErrorResponse(errorString=str(exc)))
            raise _StatementFailed from exc

    def _handle_query_execute(self, conn, req) -> None:
        self._send(conn, MSG_QUERY_RECEIPT_RESPONSE, QueryReceiptResponse(statementReceipts=[
            StatementReceipt(statementString=req.query, statementId=str(uuid.uuid4()), statementKey=0),
        ]))
        try:
            self._run_and_reply(conn, req.query, ())
        except _StatementFailed:
            return
        self._send(conn, MSG_QUERY_EXECUTE_RESPONSE, QueryExecuteResponse(
            numStatementsParsed=1, numStatementsSucceeded=1, numStatementsFailed=0,
        ))

    def _handle_parse(self, conn, req) -> None:
        name = req.preparedStatementName or f"stmt_{uuid.uuid4().hex[:8]}"
        self._prepared[name] = req.sqlQuery
        self._send(conn, MSG_PARSE_STATEMENT_RESPONSE, ParseStatementResponse(preparedStatementName=name))

    def _handle_bind_execute(self, conn, req) -> None:
        sql = self._prepared.get(req.preparedStatementName)
        if sql is None:
            self._send(conn, MSG_ERROR_RESPONSE,
                       ErrorResponse(errorString=f"unknown prepared statement {req.preparedStatementName!r}"))
            return
        params = tuple(_column_value_to_py(v) for v in (req.bindableValues or []))
        try:
            self._run_and_reply(conn, sql, params)
        except _StatementFailed:
            return


class _StatementFailed(Exception):
    pass
