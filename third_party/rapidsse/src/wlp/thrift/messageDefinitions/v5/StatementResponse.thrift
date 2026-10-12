
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5


// An enum that describes a range of verbs for how rows were affected as a result of a query.
// This is used below.

enum AffectedVerb {
  SELECTED,
  INSERTED,
  UPDATED,
  DELETED,
  NONE      // e.g. for DDL
}

// Sent from the server to the client to indicate that an individual statement in the
// QueryExecuteRequest has completed. One of these will be sent for each batched statement
// found in the QueryExecuteRequest message.
//
// Direction: server --> client

struct StatementResponse {
  // The cluster-wide ID of the statement that was executed.
  1: required string statementId;

  // The optional statement string that was executed.
  2: optional string statementString;

  // The number of rows that were matched by this statement.
  3: required i64 numMatchedRows;

  // The number of rows that were affected by this statement, if applicable.
  4: required i64 numAffectedRows;

  // How those rows were affected, if applicable.
  5: required AffectedVerb affectedVerb;

  // The aliased name of the statement, if applicable.
  6: optional string statementName;

}
