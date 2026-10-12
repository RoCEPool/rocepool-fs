
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5


// Sent from the server to the client to acknowledge that a previously prepared
// statement has been closed.
//
// Direction: server --> client

struct ClosePreparedStatementResponse {
  // The name of the previously prepared statement.
  1: required string preparedStatementName;
}
