
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to execute a single SQL statement or
// a batch of SQL statements delimited by semicolons.
//
// Direction: client --> server

struct QueryExecuteRequest {
  // The query string to execute. Could be a batch of SQL statements separated by semicolons
  // (when supported).
  1: required string query;

  // Whether to continue executing the rest of the batched SQL statements if one statement
  // produces an error. Default is false.
  2: optional bool continueBatchOnError = false;

  // The preferred maximum number of rows in each RowDataResponse message returned from
  // the server to the client. 0 means unlimited. Default is 10,000 rows per message, though
  // messages cannot be bigger than 16MB since there are only 24bits allocated to the message
  // size in the message header.
  3: optional i32 rowsPerRowDataResponse = 10000;

  // An optional flag that can be set to true to indicate to the SQL planner that the commands
  // within must not return rows. If the SQL planner recognises that a command *could* return
  // rows then it will fail and return an error. This is useful for certain calling contexts
  // such as executeUpdate() in JDBC that expect the result not to return any rows.
  4: optional bool mustNotReturnRows = false;

  // A mechanism for limiting the number of rows returned from the server.
  // If this value is >= 0 then the server will not return any more rows for any individual
  // statement in this request. If this limit is hit then the server will send a StatementResponse
  // as if there were genuinely no more rows to be returned.
  // Setting this value to -1 means that the server should return all rows from queries.
  5: optional i64 maxRowsToReturn = -1;

  // Additional session fields can go in here.
}
