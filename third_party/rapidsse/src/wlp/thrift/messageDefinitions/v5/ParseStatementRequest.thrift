
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to parse and plan a sql statement and
// turn it into prepared statement on the server.
//
// Direction: client --> server

struct ParseStatementRequest {
  // The SQL query to execute. Could be a batch of SQL statements separated by semicolons
  // (when supported).
  1: required string sqlQuery;

  // The name to use on the server to store the prepared statement under. If specified,
  // this must be unique to the session. If not specified then the server will come up
  // with a name instead.
  2: optional string preparedStatementName;

}
