
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

include "ColumnMetadata.thrift"

// Sent from the server to the client following a SELECT query with the metadata for the
// results that will follow.
//
// Direction: server --> client

struct RowMetadataResponse {
  // The number of columns in each row returned.
  1: required i32 numColumns;

  // A description of each column in the result set.
  // Note: included resources must be named <fileName>.<resourceName>
  // hence the ColumnMetadata.ColumnMetadata.
  2: required list<ColumnMetadata.ColumnMetadata> columns;

  // True if the result is definitely bounded (i.e., not continuous).
  // If it is false then the boundedness of the result may be unknown or it
  // is genuinely continuous.
  // Note that a result may be bounded, but reaching that bound may take considerable
  // amount of time. e.g., a query could be bounded by a realtime timestamp value that
  // is set 3 weeks into the future. The query is still bounded, but at what point do
  // we deem the query to be considered unbounded in this instance?
  3: required bool isBoundedResult;
}
