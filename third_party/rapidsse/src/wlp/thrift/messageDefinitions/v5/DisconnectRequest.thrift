
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the client to the server to indicate that the server may terminate the
// client's connection and clean up any resources held.
//
// Direction: client --> server

struct DisconnectRequest {
  // No fields

}
