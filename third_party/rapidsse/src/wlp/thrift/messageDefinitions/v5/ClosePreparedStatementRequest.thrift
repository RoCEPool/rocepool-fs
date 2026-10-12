
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to close a previously prepared statement
// and release any plans and resources associated with it.
//
// Direction: client --> server

struct ClosePreparedStatementRequest {

  // The name of the previously prepared statement.
  1: required string preparedStatementName;

}
