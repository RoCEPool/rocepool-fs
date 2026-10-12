
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to tell it to monitor and sent it all the
// input and output of a given session. This is useful for debugging and logging purposes.
//
// Direction: client --> server

struct SessionMonitoringRequest {
  // A unique identifier for this session.
  1: required i64 sessionId;

  // The unique and secret session key, used to authenticate the above session ID.
  2: required i64 sessionKey;

  // Additional session fields can go in here.
}
