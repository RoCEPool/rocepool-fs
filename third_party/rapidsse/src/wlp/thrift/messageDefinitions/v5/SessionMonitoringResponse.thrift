
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that there are no more messages to follow
// for the session being monitored, likely because the session has been closed.
//
// Direction: server --> client

struct SessionMonitoringResponse {

  // The unique identifier for the session that was monitored.
  1: required i64 sessionId;

}
