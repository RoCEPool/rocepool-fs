
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that an error occurred in the current request.
//
// Direction: server --> client

struct ErrorResponse {
  // The RapidsDB error code, if available.
  1: optional i32 errorCode;

  // The ANSI SQL-State error code, if available.
  2: optional string sqlStateCode;

  // A message from the server describing the error.
  3: required string errorString;

  // A string containing a stack trace of the error.
  4: optional string stackTrace;
}
