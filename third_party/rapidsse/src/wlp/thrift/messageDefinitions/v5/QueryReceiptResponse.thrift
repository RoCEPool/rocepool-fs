
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Receipt of a singular executable statement within RapidsDB.
// It contains a unique number as well as a unique key, which are used to retrieve the
// statement for side-band operations.
// This object is used below.
struct StatementReceipt {
  // The statement to be executed, echoed back to the user for debugging/logging purposes.
  1: optional string statementString;

  // A unique identifier for this statement in the entire cluster.
  2: required string statementId;

  // The unique and secret statement key, used to set up side-band commands against this statement
  // from a different connection, such as stat cancellation and query progress monitoring.
  3: required i64 statementKey;
}


// Sent from the server to the client to acknowledge that a query was received by the server
// and will be executed. It contains a list of statement receipts, which may contain more than
// one StatementReceipt if there was more than one executable statement batched in the query.
//
// Direction: server --> client

struct QueryReceiptResponse {

  1: required list<StatementReceipt> statementReceipts;
}
