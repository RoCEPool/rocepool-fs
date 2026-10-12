
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

include "ColumnValue.thrift"

// This message is sent by a client to the server to bind values to a previously prepared
// statement and then execute it.
//
// Direction: client --> server

struct StatementBindAndExecuteRequest {
  // The name of the previously prepared statement on the server.
  1: required string preparedStatementName;

  // A list of values to bind into the prepared statement.
  2: optional list<ColumnValue.ColumnValue> bindableValues;

}
