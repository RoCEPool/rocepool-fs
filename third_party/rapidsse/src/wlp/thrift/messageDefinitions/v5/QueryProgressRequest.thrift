
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to tell it to try to provide a progress
// indication of the execution of a statement.
//
// Direction: client --> server

struct QueryProgressRequest {
  // A unique identifier for this query so it can be referred to numerically.
  1: required i64 queryId;

  // The unique and secret query key, used to set up side-band commands against this query
  // from a different connection, such as query cancellation and query progress monitoring.
  2: required i64 queryKey;

  // Additional session fields can go in here.
}
