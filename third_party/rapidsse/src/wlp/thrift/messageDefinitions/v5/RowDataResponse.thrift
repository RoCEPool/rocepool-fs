
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

include "ColumnValue.thrift"

// Definition of a single row, used below.
struct Row {
  1: required list<ColumnValue.ColumnValue> columns
}


// A message that contains row data. Sent from the server to the client following a
// SELECT query with the result data for the query.
//
// Direction: server --> client
struct RowDataResponse {

  // A list of rows
  1: required list<Row> rows;
}
