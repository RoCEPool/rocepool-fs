"""A real WLP v5 client: connects over TCP, performs the protocol
handshake, and exposes a DB-API2-shaped surface (execute/fetchone/
fetchall/commit) satisfying `storage_engine.rapidsse_engine.RapidsSEClient`
-- so `RapidsSEEngine` (unchanged) can be pointed at either this real
client or the earlier injected fake/sqlite client with zero code changes,
exactly matching the interface-first architecture requested for this
project.

Message flow, as RapidsSE's server implements it (AdaptorWL.cpp's state
machine, sen/OP_sendWL.sen), checked against a real RapidsSE node
(v1.2.0-dev2-171, tools/rapidsse_tpch.py runs its TPC-H regression plan
through this client with --client wlp):
  connect -> server sends the 8 bytes "RapidsSE" (welcome, unframed)
          -> ProtocolCompatibleRequest -> ProtocolCompatibleResponse
          -> ClearTextTunnelRequest -> TunnelSupportedResponse    (state TUNNELED)
          -> PlainAuthenticationRequest -> AuthenticationOkResponse
          -> SessionChangeRequest -> SessionChangeResponse         (state READY)
  no-params statement (DDL etc.) -> QueryExecuteRequest
          -> QueryReceiptResponse
          -> [RowMetadataResponse, RowDataResponse* ] (if it returns rows)
          -> StatementResponse, or ErrorResponse if the statement failed
          -> QueryExecuteResponse (batch dialog complete, also after an error)
  Every message: 8-byte header whose size counts the header itself
  (wlp/framing.py).

Not yet exercised against a real node: the parameterized path
(ParseStatementRequest -> ParseStatementResponse ->
StatementBindAndExecuteRequest -> [RowMetadataResponse, RowDataResponse*]
-> StatementResponse); it assumes no outer envelope, as
`tests/fakes/wlp_test_server.py` does.
"""
from __future__ import annotations

import datetime
import decimal
import socket
from typing import Any

from .compact_protocol import CompactReader, CompactWriter
from .framing import recv_exact, recv_frame, send_frame
from .messages import (
    MSG_AUTHENTICATION_OK_RESPONSE, MSG_CLEAR_TEXT_TUNNEL_REQUEST, MSG_ERROR_RESPONSE, MSG_PARSE_STATEMENT_REQUEST,
    MSG_PARSE_STATEMENT_RESPONSE, MSG_PLAIN_AUTHENTICATION_REQUEST,
    MSG_PROTOCOL_COMPATIBLE_REQUEST, MSG_PROTOCOL_COMPATIBLE_RESPONSE,
    MSG_PROTOCOL_INCOMPATIBLE_RESPONSE, MSG_QUERY_EXECUTE_REQUEST, MSG_QUERY_EXECUTE_RESPONSE,
    MSG_QUERY_RECEIPT_RESPONSE, MSG_ROW_DATA_RESPONSE, MSG_ROW_METADATA_RESPONSE, MSG_SESSION_CHANGE_REQUEST,
    MSG_SESSION_CHANGE_RESPONSE,
    MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST, MSG_STATEMENT_RESPONSE, MSG_TUNNEL_SUPPORTED_RESPONSE,
    ClearTextTunnelRequest, ColumnValue, ErrorResponse, ParseStatementRequest, ParseStatementResponse,
    PlainAuthenticationRequest, ProtocolCompatibleRequest, ProtocolCompatibleResponse,
    QueryExecuteRequest, SessionChangeRequest, StatementBindAndExecuteRequest,
)

PROTOCOL_VERSION = 5
WELCOME = b"RapidsSE"  # sent by the server on connect, before the first message


class WlpError(RuntimeError):
    def __init__(self, error: ErrorResponse):
        super().__init__(f"[{error.errorCode}] {error.errorString}")
        self.error = error


def _to_column_value(py_value: Any) -> ColumnValue:
    if py_value is None:
        return ColumnValue(isNull=True)
    if isinstance(py_value, bool):
        return ColumnValue(booleanVal=py_value)
    if isinstance(py_value, int):
        return ColumnValue(bigintVal=py_value)
    if isinstance(py_value, float):
        return ColumnValue(doubleVal=py_value)
    if isinstance(py_value, (bytes, bytearray)):
        return ColumnValue(binaryVal=bytes(py_value))
    if isinstance(py_value, str):
        return ColumnValue(stringVal=py_value)
    raise TypeError(f"cannot bind Python value of type {type(py_value)!r}")


def _tz(offset_hours, offset_minutes):
    if offset_hours is None and offset_minutes is None:
        return None
    return datetime.timezone(datetime.timedelta(hours=offset_hours or 0, minutes=offset_minutes or 0))


def _from_column_value(cv: ColumnValue) -> Any:
    """A received value as Python: DECIMAL -> decimal.Decimal (exact, as the
    server sends it as text), DATE -> datetime.date, TIME -> datetime.time,
    TIMESTAMP -> datetime.datetime (with its UTC offset when sent), INTERVAL
    -> datetime.timedelta (day-time) or a (years, months) tuple (year-month)."""
    if cv.isNull:
        return None
    for attr in ("booleanVal", "tinyIntVal", "smallIntVal", "integerVal",
                 "bigintVal", "doubleVal", "stringVal", "binaryVal"):
        v = getattr(cv, attr, None)
        if v is not None:
            return v
    if cv.bigDecimalVal is not None:
        return decimal.Decimal(cv.bigDecimalVal)
    if cv.dateVal is not None:
        d = cv.dateVal
        return datetime.date(d.year, d.month, d.day)
    if cv.timeVal is not None:
        t = cv.timeVal
        return datetime.time(t.hour, t.minute, t.second, t.nanoseconds // 1000,
                             tzinfo=_tz(t.offsetHours, t.offsetMinutes))
    if cv.timestampVal is not None:
        t = cv.timestampVal
        return datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second, t.nanoseconds // 1000,
                                 tzinfo=_tz(t.offsetHours, t.offsetMinutes))
    if cv.intervalVal is not None:
        iv = cv.intervalVal
        sign = -1 if iv.isNegative else 1
        if iv.year is not None or iv.month is not None:
            return (sign * (iv.year or 0), sign * (iv.month or 0))
        return sign * datetime.timedelta(days=iv.day or 0, hours=iv.hour or 0, minutes=iv.minute or 0,
                                         seconds=iv.second or 0, microseconds=(iv.nanoseconds or 0) // 1000)
    return None


class WlpClient:
    """One connection + one logical cursor (mirrors sqlite3's default
    connection-is-a-cursor convenience, matching how the fake DB-API2
    clients used elsewhere in this project behave).

    The wire I/O in `_send`/`_recv` only ever calls `self._sock.sendall(...)`
    and `self._sock.recv(n)` (via `wlp/framing.py`'s `send_frame`/
    `recv_frame`) -- it never touches anything TCP-specific. That means
    `self._sock` only needs to be *something* with those two methods, not
    literally a `socket.socket`. `__init__` keeps the original TCP-only
    convenience constructor unchanged (`WlpClient(host, port)` still opens
    a real `socket.create_connection`), and `from_stream` is the entry
    point for any other duck-typed stream -- in particular
    `roce.roce_transport.RoCEStream`, so this same client class and its
    handshake/execute/fetch logic run unmodified over RDMA instead of TCP.
    See `roce/roce_transport.py`'s `RoCEStream` docstring for how that
    adapter reconciles RDMA's message-boundary-preserving SEND/RECV with
    the byte-stream `.recv(n)` semantics this class (via `framing.py`)
    assumes.
    """

    def __init__(self, host: str, port: int, *, username: str = "sefs",
                 password: str = "", timeout: float = 10.0):
        sock = socket.create_connection((host, port), timeout=timeout)
        self._init_over_stream(sock, username, password)

    @classmethod
    def from_stream(cls, stream, *, username: str = "sefs", password: str = "") -> "WlpClient":
        """Build a WlpClient over any already-connected duck-typed stream
        exposing `sendall(bytes)` and `recv(nbytes) -> bytes` -- e.g. a
        `roce.roce_transport.RoCEStream` returned by `RoCEStream.connect(...)`,
        in place of the TCP socket the regular constructor opens. Ownership
        of `stream` passes to the returned WlpClient: `close()` closes it.
        """
        self = cls.__new__(cls)
        self._init_over_stream(stream, username, password)
        return self

    def _init_over_stream(self, stream, username: str, password: str) -> None:
        self._sock = stream
        self._last_rows: list[tuple] = []
        self._row_cursor = 0
        self._handshake(username, password)

    # -- low-level send/recv of one message --
    def _send(self, msg_type: int, struct_obj) -> None:
        writer = CompactWriter()
        struct_obj.write(writer)
        send_frame(self._sock, msg_type, writer.bytes())

    def _recv(self):
        msg_type, body = recv_frame(self._sock)
        from .messages import MESSAGE_CLASSES
        cls = MESSAGE_CLASSES.get(msg_type)
        if cls is None:
            raise WlpError(ErrorResponse(errorString=f"unknown message type {msg_type}"))
        obj = cls.read(CompactReader(body)) if body else cls()
        return msg_type, obj

    def _handshake(self, username: str, password: str) -> None:
        # RapidsSE greets every new connection with 8 bytes, "RapidsSE", before
        # any framed message (sen/OP_sendWL.sen, sendWL_StartMarker:
        # "RapidsSE" Socket[].sendStr). Found running against a real node: read
        # as a header, it made the first reply's length garbage.
        welcome = recv_exact(self._sock, len(WELCOME))
        if welcome != WELCOME:
            raise WlpError(ErrorResponse(errorString=f"not a RapidsSE server (greeting {welcome!r})"))
        self._send(MSG_PROTOCOL_COMPATIBLE_REQUEST, ProtocolCompatibleRequest(
            clientProtocolVersion=PROTOCOL_VERSION, clientDescription="rocepool-wlp-client",
        ))
        msg_type, resp = self._recv()
        if msg_type == MSG_PROTOCOL_INCOMPATIBLE_RESPONSE:
            raise WlpError(ErrorResponse(errorString="server rejected protocol version"))
        assert msg_type == MSG_PROTOCOL_COMPATIBLE_RESPONSE, resp

        # RapidsSE accepts authentication only in state TUNNELED (AdaptorWL.cpp
        # vProcessPlainAuthenticationRequest: bCheckState({TUNNELED})), reached
        # by a ClearTextTunnelRequest -> TunnelSupportedResponse exchange
        self._send(MSG_CLEAR_TEXT_TUNNEL_REQUEST, ClearTextTunnelRequest())
        msg_type, resp = self._recv()
        if msg_type == MSG_ERROR_RESPONSE:
            raise WlpError(resp)
        assert msg_type == MSG_TUNNEL_SUPPORTED_RESPONSE, resp

        self._send(MSG_PLAIN_AUTHENTICATION_REQUEST, PlainAuthenticationRequest(
            username=username, password=password,
        ))
        msg_type, resp = self._recv()
        if msg_type == MSG_ERROR_RESPONSE:
            raise WlpError(resp)
        assert msg_type == MSG_AUTHENTICATION_OK_RESPONSE, resp

        # ... and queries only in state READY, reached by a SessionChangeRequest
        # (vProcessSessionChangeRequest: AUTHENTICATED -> READY); no fields set:
        # RapidsSE ignores catalog and schema
        self._send(MSG_SESSION_CHANGE_REQUEST, SessionChangeRequest())
        msg_type, resp = self._recv()
        if msg_type == MSG_ERROR_RESPONSE:
            raise WlpError(resp)
        assert msg_type == MSG_SESSION_CHANGE_RESPONSE, resp
        self.session_id = resp.sessionId

    def close(self) -> None:
        self._sock.close()

    # -- the DB-API2-shaped surface RapidsSEEngine actually uses --
    def execute(self, sql: str, params: tuple = ()) -> None:
        rows: list[tuple] = []
        if params:
            self._send(MSG_PARSE_STATEMENT_REQUEST, ParseStatementRequest(sqlQuery=sql))
            msg_type, resp = self._recv()
            if msg_type == MSG_ERROR_RESPONSE:
                raise WlpError(resp)
            assert msg_type == MSG_PARSE_STATEMENT_RESPONSE, resp
            prepared_name = resp.preparedStatementName

            bound = [_to_column_value(p) for p in params]
            self._send(MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST, StatementBindAndExecuteRequest(
                preparedStatementName=prepared_name, bindableValues=bound,
            ))
            rows = self._drain_single_statement()
        else:
            self._send(MSG_QUERY_EXECUTE_REQUEST, QueryExecuteRequest(query=sql))
            msg_type, resp = self._recv()
            if msg_type == MSG_ERROR_RESPONSE:
                raise WlpError(resp)
            assert msg_type == MSG_QUERY_RECEIPT_RESPONSE, resp
            try:
                rows = self._drain_single_statement()
            except WlpError:
                # a failed statement: RapidsSE sends the ErrorResponse in place of
                # its StatementResponse and still ends the dialog with a
                # QueryExecuteResponse -- read it, or the server stays in
                # API_STATEMENT_SUBMITTED and refuses the next request
                self._drain_to(MSG_QUERY_EXECUTE_RESPONSE)
                raise
            msg_type, resp = self._recv()
            if msg_type == MSG_ERROR_RESPONSE:
                raise WlpError(resp)
            assert msg_type == MSG_QUERY_EXECUTE_RESPONSE, resp

        self._last_rows = rows
        self._row_cursor = 0

    def _drain_to(self, end_type: int) -> None:
        while self._recv()[0] != end_type:
            pass

    def _drain_single_statement(self) -> list[tuple]:
        rows: list[tuple] = []
        while True:
            msg_type, resp = self._recv()
            if msg_type == MSG_ERROR_RESPONSE:
                raise WlpError(resp)
            if msg_type == MSG_ROW_METADATA_RESPONSE:
                continue
            if msg_type == MSG_ROW_DATA_RESPONSE:
                for row in resp.rows:
                    rows.append(tuple(_from_column_value(c) for c in row.columns))
                continue
            if msg_type == MSG_STATEMENT_RESPONSE:
                return rows
            raise WlpError(ErrorResponse(errorString=f"unexpected message type {msg_type} mid-statement"))

    def fetchone(self) -> tuple | None:
        if self._row_cursor >= len(self._last_rows):
            return None
        row = self._last_rows[self._row_cursor]
        self._row_cursor += 1
        return row

    def fetchall(self) -> list[tuple]:
        rows = self._last_rows[self._row_cursor:]
        self._row_cursor = len(self._last_rows)
        return rows

    def commit(self) -> None:
        pass  # WLP v5's IDL exposes no explicit commit message in scope here; each
              # QueryExecuteRequest/StatementBindAndExecuteRequest is applied by the
              # server as it completes, per the DDL/DML statements observed above.
