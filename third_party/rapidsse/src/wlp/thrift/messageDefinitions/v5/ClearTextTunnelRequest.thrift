
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the client to server to indicate that it is requesting a clear text (unencrypted)
// tunnel for messages in this session.
//
// Direction: client --> server

struct ClearTextTunnelRequest {
  // No content fields yet.
}
