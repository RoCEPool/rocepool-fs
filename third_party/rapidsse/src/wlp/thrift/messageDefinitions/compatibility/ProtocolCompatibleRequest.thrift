
namespace java com.rapidsdata.wirelineprotocol.messages.compatibility
namespace cpp  com.rapidsdata.wirelineprotocol.messages.compatibility

// This message is sent by a connecting client to tell the server which version of the
// wire line protocol it supports. The server will then respond with a
// ProtocolIncompatibleResponse or a ProtocolCompatibleResponse message.
//
// Direction: client --> server

struct ProtocolCompatibleRequest {
  // The version of the wire line protocol that the client supports.
  1: required i32 clientProtocolVersion

  // A string that the server can use in logging/debugging to identify the client that is connecting.
  2: optional string clientDescription
}
