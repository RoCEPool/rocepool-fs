
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that the server is about to
// disconnect this particular client.
//
// Direction: server --> client

struct DisconnectNotification {

  // An optional reason why the client is being disconnected.
  1: optional string reason

}
