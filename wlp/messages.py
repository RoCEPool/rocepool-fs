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
    T_BINARY, T_BOOL, T_DOUBLE, T_I16, T_I32, T_I64, T_LIST, T_STRING, T_STRUCT, ThriftStruct,
)

# --- message type ids, from MessageTypes.thrift ---
MSG_PROTOCOL_COMPATIBLE_REQUEST = 1
MSG_PROTOCOL_COMPATIBLE_RESPONSE = 2
MSG_PROTOCOL_INCOMPATIBLE_RESPONSE = 4
MSG_ERROR_RESPONSE = 6
MSG_PLAIN_AUTHENTICATION_REQUEST = 31
MSG_AUTHENTICATION_OK_RESPONSE = 38
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
class ColumnValue(ThriftStruct):
    isNull: bool | None = None
    booleanVal: bool | None = None
    tinyIntVal: int | None = None
    smallIntVal: int | None = None
    integerVal: int | None = None
    bigintVal: int | None = None
    doubleVal: float | None = None
    bigDecimalVal: str | None = None
    # timestampVal / timeVal / dateVal / intervalVal deliberately omitted --
    # not needed for the byte/int/string/double values this storage adapter
    # ever binds (object_key, object_value, object_version, timestamps as
    # doubles, is_dir as bool). Adding them later is a pure extension of
    # _FIELDS, nothing else here would change.
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
    MSG_PLAIN_AUTHENTICATION_REQUEST: PlainAuthenticationRequest,
    MSG_AUTHENTICATION_OK_RESPONSE: AuthenticationOkResponse,
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
