
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that a session change was accepted by the server.
//
// Direction: server --> client

struct SessionChangeResponse {
  // A unique identifier for this session.
  1: required string sessionId;

  // The unique and secret session key, used to set up side-band commands against this session
  // from a different connection, such as session monitoring.
  2: required i64 sessionKey;

}
