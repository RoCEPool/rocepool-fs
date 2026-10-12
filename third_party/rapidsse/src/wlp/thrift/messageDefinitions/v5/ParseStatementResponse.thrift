
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5


// Sent from the server to the client to acknowledge a sql query that has been planned and
// saved as a prepared statement.
//
// Direction: server --> client

struct ParseStatementResponse {
  // The name of the prepared statement that this query is saved under for the current session.
  1: required string preparedStatementName;
}
