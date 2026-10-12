
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5


// Sent from the server to the client to indicate that the QueryExecute dialog is now complete.
// This is distinct from the StatementResponse because the original QueryExecuteRequest message
// may have contained multiple batched statements, to which a StatementResponse would have been
// returned for each of them. The whole batched dialog is now indicating that it has completed
// with this message being sent.
//
// Direction: server --> client

struct QueryExecuteResponse {
  // The number of individual statements in the query (as the query could be a batch).
  1: required i64 numStatementsParsed

  // The number of statements from the QueryExecuteRequest that have successfully completed.
  2: required i64 numStatementsSucceeded

  // The number of statements from the QueryExecuteRequest that have returned with an error.
  3: required i64 numStatementsFailed

  // The time it took to execute the query on the server, in seconds.
  4: optional double executionTime
}
