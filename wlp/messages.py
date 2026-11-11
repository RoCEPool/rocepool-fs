"""WLP v5 message structs, field-for-field against the real .thrift IDL
files shipped in RapidsSE's source (kept in third_party/rapidsse/)
(`src/wlp/thrift/messageDefinitions/{compatibility,v5}/*.thrift`).

Only the messages this client actually needs for schema DDL and simple
parameterized CRUD are implemented (handshake, plain auth, query execute,
parse+bind+execute, and the response messages needed to read results back).
Message type numbers are copied verbatim from
`messageDefinitions/v5/MessageTypes.thrift`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from .compact_protocol import (
    T_BINARY, T_BOOL, T_DOUBLE, T_ENUM, T_I8, T_I16, T_I32, T_I64, T_LIST, T_STRING, T_STRUCT, ThriftStruct,
)

# --- message type ids, from MessageTypes.thrift ---
MSG_PROTOCOL_COMPATIBLE_REQUEST = 1
MSG_PROTOCOL_COMPATIBLE_RESPONSE = 2
MSG_PROTOCOL_INCOMPATIBLE_RESPONSE = 4
MSG_ERROR_RESPONSE = 6
MSG_CLEAR_TEXT_TUNNEL_REQUEST = 21
MSG_TUNNEL_SUPPORTED_RESPONSE = 22
MSG_PLAIN_AUTHENTICATION_REQUEST = 31
MSG_AUTHENTICATION_OK_RESPONSE = 38
MSG_SESSION_CHANGE_REQUEST = 41
MSG_SESSION_CHANGE_RESPONSE = 42
MSG_QUERY_EXECUTE_REQUEST = 51
MSG_QUERY_RECEIPT_RESPONSE = 52
MSG_ROW_METADATA_RESPONSE = 54
MSG_ROW_DATA_RESPONSE = 56
MSG_STATEMENT_RESPONSE = 58
MSG_QUERY_EXECUTE_RESPONSE = 60
MSG_PARSE_STATEMENT_REQUEST = 101
MSG_PARSE_STATEMENT_RESPONSE = 102
MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST = 103


@dataclass
class ProtocolCompatibleRequest(ThriftStruct):
    clientProtocolVersion: int = 0
    clientDescription: str | None = None
    _FIELDS: ClassVar = [
        (1, T_I32, "clientProtocolVersion", None),
        (2, T_STRING, "clientDescription", None),
    ]


@dataclass
class ProtocolCompatibleResponse(ThriftStruct):
    protocolVersion: int = 0
    serverSoftwareVersion: str = ""
    serverBuildInfo: str = ""
    _FIELDS: ClassVar = [
        (1, T_I32, "protocolVersion", None),
        (2, T_STRING, "serverSoftwareVersion", None),
        (3, T_STRING, "serverBuildInfo", None),
    ]


@dataclass
class PlainAuthenticationRequest(ThriftStruct):
    username: str = ""
    password: str = ""
    _FIELDS: ClassVar = [
        (1, T_STRING, "username", None),
        (2, T_STRING, "password", None),
    ]


@dataclass
class AuthenticationOkResponse(ThriftStruct):
    _FIELDS: ClassVar = []


@dataclass
class SessionChangeRequest(ThriftStruct):
    defaultFederation: str | None = None
    defaultCatalog: str | None = None
    defaultSchema: str | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "defaultFederation", None),
        (2, T_STRING, "defaultCatalog", None),
        (3, T_STRING, "defaultSchema", None),
    ]


@dataclass
class SessionChangeResponse(ThriftStruct):
    sessionId: str = ""
    sessionKey: int = 0
    _FIELDS: ClassVar = [
        (1, T_STRING, "sessionId", None),
        (2, T_I64, "sessionKey", None),
    ]


@dataclass
class ClearTextTunnelRequest(ThriftStruct):
    _FIELDS: ClassVar = []


@dataclass
class TunnelSupportedResponse(ThriftStruct):
    _FIELDS: ClassVar = []


@dataclass
class ErrorResponse(ThriftStruct):
    errorCode: int | None = None
    sqlStateCode: str | None = None
    errorString: str = ""
    stackTrace: str | None = None
    _FIELDS: ClassVar = [
        (1, T_I32, "errorCode", None),
        (2, T_STRING, "sqlStateCode", None),
        (3, T_STRING, "errorString", None),
        (4, T_STRING, "stackTrace", None),
    ]


@dataclass
class QueryExecuteRequest(ThriftStruct):
    query: str = ""
    continueBatchOnError: bool | None = None
    rowsPerRowDataResponse: int | None = None
    mustNotReturnRows: bool | None = None
    maxRowsToReturn: int | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "query", None),
        (2, T_BOOL, "continueBatchOnError", None),
        (3, T_I32, "rowsPerRowDataResponse", None),
        (4, T_BOOL, "mustNotReturnRows", None),
        (5, T_I64, "maxRowsToReturn", None),
    ]


@dataclass
class StatementReceipt(ThriftStruct):
    statementString: str | None = None
    statementId: str = ""
    statementKey: int = 0
    _FIELDS: ClassVar = [
        (1, T_STRING, "statementString", None),
        (2, T_STRING, "statementId", None),
        (3, T_I64, "statementKey", None),
    ]


@dataclass
class QueryReceiptResponse(ThriftStruct):
    statementReceipts: list = field(default_factory=list)
    _FIELDS: ClassVar = [
        (1, T_LIST, "statementReceipts", StatementReceipt),
    ]


@dataclass
class ColumnMetadata(ThriftStruct):
    columnName: str = ""
    columnType: int = 0  # DataType enum ordinal
    precision: int | None = None
    scale: int | None = None
    isNullable: bool | None = None
    charsetName: str | None = None
    collationName: str | None = None
    isAutoIncrement: bool | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "columnName", None),
        (2, "enum", "columnType", None),
        (3, T_I32, "precision", None),
        (4, T_I32, "scale", None),
        (5, T_BOOL, "isNullable", None),
        (6, T_STRING, "charsetName", None),
        (7, T_STRING, "collationName", None),
        (8, T_BOOL, "isAutoIncrement", None),
    ]


@dataclass
class RowMetadataResponse(ThriftStruct):
    numColumns: int = 0
    columns: list = field(default_factory=list)
    isBoundedResult: bool = True
    _FIELDS: ClassVar = [
        (1, T_I32, "numColumns", None),
        (2, T_LIST, "columns", ColumnMetadata),
        (3, T_BOOL, "isBoundedResult", None),
    ]


@dataclass
class Timestamp(ThriftStruct):
    year: int = 0
    month: int = 0
    day: int = 0
    hour: int = 0
    minute: int = 0
    second: int = 0
    nanoseconds: int = 0
    offsetHours: int | None = None
    offsetMinutes: int | None = None
    tzName: str | None = None
    _FIELDS: ClassVar = [
        (1, T_I32, "year", None), (2, T_I16, "month", None), (3, T_I16, "day", None),
        (4, T_I16, "hour", None), (5, T_I16, "minute", None), (6, T_I16, "second", None),
        (7, T_I32, "nanoseconds", None), (8, T_I8, "offsetHours", None), (9, T_I8, "offsetMinutes", None),
        (10, T_STRING, "tzName", None),
    ]


@dataclass
class Date(ThriftStruct):
    year: int = 0
    month: int = 0
    day: int = 0
    offsetHours: int | None = None
    offsetMinutes: int | None = None
    tzName: str | None = None
    _FIELDS: ClassVar = [
        (1, T_I32, "year", None), (2, T_I16, "month", None), (3, T_I16, "day", None),
        (4, T_I8, "offsetHours", None), (5, T_I8, "offsetMinutes", None), (6, T_STRING, "tzName", None),
    ]


@dataclass
class Time(ThriftStruct):
    hour: int = 0
    minute: int = 0
    second: int = 0
    nanoseconds: int = 0
    offsetHours: int | None = None
    offsetMinutes: int | None = None
    tzName: str | None = None
    _FIELDS: ClassVar = [
        (1, T_I16, "hour", None), (2, T_I16, "minute", None), (3, T_I16, "second", None),
        (4, T_I32, "nanoseconds", None), (5, T_I8, "offsetHours", None), (6, T_I8, "offsetMinutes", None),
        (7, T_STRING, "tzName", None),
    ]


INTERVAL_TYPES = ["YEAR", "MONTH", "YEAR_TO_MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DAY_TO_HOUR",
                  "DAY_TO_MINUTE", "DAY_TO_SECOND", "HOUR_TO_MINUTE", "HOUR_TO_SECOND", "MINUTE_TO_SECOND"]


@dataclass
class Interval(ThriftStruct):
    type: int = 0  # IntervalType, index into INTERVAL_TYPES
    isNegative: bool | None = None
    year: int | None = None
    month: int | None = None
    day: int | None = None
    hour: int | None = None
    minute: int | None = None
    second: int | None = None
    nanoseconds: int | None = None
    _FIELDS: ClassVar = [
        (1, T_ENUM, "type", None), (2, T_BOOL, "isNegative", None), (3, T_I64, "year", None),
        (4, T_I64, "month", None), (5, T_I64, "day", None), (6, T_I64, "hour", None),
        (7, T_I64, "minute", None), (8, T_I64, "second", None), (9, T_I64, "nanoseconds", None),
    ]


@dataclass
class ColumnValue(ThriftStruct):
    isNull: bool | None = None
    booleanVal: bool | None = None
    tinyIntVal: int | None = None
    smallIntVal: int | None = None
    integerVal: int | None = None
    bigintVal: int | None = None
    doubleVal: float | None = None
    bigDecimalVal: str | None = None
    timestampVal: Timestamp | None = None
    timeVal: Time | None = None
    dateVal: Date | None = None
    intervalVal: Interval | None = None
    stringVal: str | None = None
    binaryVal: bytes | None = None
    _FIELDS: ClassVar = [
        (1, T_BOOL, "isNull", None),
        (2, T_BOOL, "booleanVal", None),
        (3, T_I16, "tinyIntVal", None),
        (4, T_I16, "smallIntVal", None),
        (5, T_I32, "integerVal", None),
        (6, T_I64, "bigintVal", None),
        (7, T_DOUBLE, "doubleVal", None),
        (8, T_STRING, "bigDecimalVal", None),
        (9, T_STRUCT, "timestampVal", Timestamp),
        (10, T_STRUCT, "timeVal", Time),
        (11, T_STRUCT, "dateVal", Date),
        (12, T_STRUCT, "intervalVal", Interval),
        (13, T_STRING, "stringVal", None),
        (14, T_BINARY, "binaryVal", None),  # binary shares STRING wire encoding, decodes to raw bytes
    ]


@dataclass
class Row(ThriftStruct):
    columns: list = field(default_factory=list)
    _FIELDS: ClassVar = [
        (1, T_LIST, "columns", ColumnValue),
    ]


@dataclass
class RowDataResponse(ThriftStruct):
    rows: list = field(default_factory=list)
    _FIELDS: ClassVar = [
        (1, T_LIST, "rows", Row),
    ]


@dataclass
class StatementResponse(ThriftStruct):
    statementId: str = ""
    statementString: str | None = None
    numMatchedRows: int = 0
    numAffectedRows: int = 0
    affectedVerb: int = 4  # AffectedVerb.NONE
    statementName: str | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "statementId", None),
        (2, T_STRING, "statementString", None),
        (3, T_I64, "numMatchedRows", None),
        (4, T_I64, "numAffectedRows", None),
        (5, "enum", "affectedVerb", None),
        (6, T_STRING, "statementName", None),
    ]


@dataclass
class QueryExecuteResponse(ThriftStruct):
    numStatementsParsed: int = 0
    numStatementsSucceeded: int = 0
    numStatementsFailed: int = 0
    executionTime: float | None = None
    _FIELDS: ClassVar = [
        (1, T_I64, "numStatementsParsed", None),
        (2, T_I64, "numStatementsSucceeded", None),
        (3, T_I64, "numStatementsFailed", None),
        (4, T_DOUBLE, "executionTime", None),
    ]


@dataclass
class ParseStatementRequest(ThriftStruct):
    sqlQuery: str = ""
    preparedStatementName: str | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "sqlQuery", None),
        (2, T_STRING, "preparedStatementName", None),
    ]


@dataclass
class ParseStatementResponse(ThriftStruct):
    preparedStatementName: str = ""
    _FIELDS: ClassVar = [
        (1, T_STRING, "preparedStatementName", None),
    ]


@dataclass
class StatementBindAndExecuteRequest(ThriftStruct):
    preparedStatementName: str = ""
    bindableValues: list | None = None
    _FIELDS: ClassVar = [
        (1, T_STRING, "preparedStatementName", None),
        (2, T_LIST, "bindableValues", ColumnValue),
    ]


# message type id -> struct class, used by both client and test-server code
# to decode an incoming frame generically.
MESSAGE_CLASSES = {
    MSG_PROTOCOL_COMPATIBLE_REQUEST: ProtocolCompatibleRequest,
    MSG_PROTOCOL_COMPATIBLE_RESPONSE: ProtocolCompatibleResponse,
    MSG_ERROR_RESPONSE: ErrorResponse,
    MSG_CLEAR_TEXT_TUNNEL_REQUEST: ClearTextTunnelRequest,
    MSG_TUNNEL_SUPPORTED_RESPONSE: TunnelSupportedResponse,
    MSG_PLAIN_AUTHENTICATION_REQUEST: PlainAuthenticationRequest,
    MSG_AUTHENTICATION_OK_RESPONSE: AuthenticationOkResponse,
    MSG_SESSION_CHANGE_REQUEST: SessionChangeRequest,
    MSG_SESSION_CHANGE_RESPONSE: SessionChangeResponse,
    MSG_QUERY_EXECUTE_REQUEST: QueryExecuteRequest,
    MSG_QUERY_RECEIPT_RESPONSE: QueryReceiptResponse,
    MSG_ROW_METADATA_RESPONSE: RowMetadataResponse,
    MSG_ROW_DATA_RESPONSE: RowDataResponse,
    MSG_STATEMENT_RESPONSE: StatementResponse,
    MSG_QUERY_EXECUTE_RESPONSE: QueryExecuteResponse,
    MSG_PARSE_STATEMENT_REQUEST: ParseStatementRequest,
    MSG_PARSE_STATEMENT_RESPONSE: ParseStatementResponse,
    MSG_STATEMENT_BIND_AND_EXECUTE_REQUEST: StatementBindAndExecuteRequest,
}
