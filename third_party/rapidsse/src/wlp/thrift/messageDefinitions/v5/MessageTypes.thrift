
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// All messages from client to server have an odd numbered type.
// All messages from server to client have an even numbered type.
// Any messages that can be sent from both client and server have an even numbered type.
enum MessageTypes {

  // These values must remain fixed for all protocol versions.
  // This is copied here from the ../compatibility/MessageTypes.thrift file for convenience only.
  ProtocolCompatibleRequest       = 1,
  ProtocolCompatibleResponse      = 2,
  ProtocolIncompatibleResponse    = 4

  // The values below can change for each version of the protocol.
  ErrorResponse                   = 6,
  WarningResponse                 = 8,
  ShutdownNotification            = 10,
  DisconnectNotification          = 12,
  DisconnectRequest               = 13,
  PingRequest                     = 15,
  PingResponse                    = 16,

  ClearTextTunnelRequest          = 21,
  TunnelSupportedResponse         = 22,

  PlainAuthenticationRequest      = 31,
  KerberosAuthenticationRequest   = 33,
  GssNegotiationMsg               = 34
  AuthenticationOkResponse        = 38,

  SessionChangeRequest            = 41,
  SessionChangeResponse           = 42,

  QueryExecuteRequest             = 51,
  QueryReceiptResponse            = 52,
  RowMetadataResponse             = 54,
  RowDataResponse                 = 56,
  StatementResponse               = 58,
  QueryExecuteResponse            = 60,

  QueryProgressRequest            = 81,
  QueryProgressResponse           = 82,

  SessionMonitoringRequest        = 91,
  SessionProtocolVersionResponse  = 92,
  SessionDataResponse             = 94,
  SessionDroppedMessagesResponse  = 96,
  SessionMonitoringResponse       = 98,

  ParseStatementRequest           = 101,
  ParseStatementResponse          = 102,
  StatementBindAndExecuteRequest  = 103,
  ClosePreparedStatementRequest   = 105,
  ClosePreparedStatementResponse  = 106
}
