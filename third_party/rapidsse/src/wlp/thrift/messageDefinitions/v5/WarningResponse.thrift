
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that a warning occurred in the current request.
//
// Direction: server --> client

struct WarningResponse {
  // The RapidsDB warning code, if available.
  1: optional i32 warningCode;

  // The ANSI SQL-State warning code, if available.
  2: optional string sqlStateCode;

  // A message from the server describing the warning.
  3: required string warningString;

  // A string containing a stack trace of the warning, if applicable.
  4: optional string stackTrace;
}
