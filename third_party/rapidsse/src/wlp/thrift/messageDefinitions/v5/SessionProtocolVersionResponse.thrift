
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to acknowledge that session monitoring is about to begin
// and to tell the client the version number of the protocol for the session being monitored
// so that it knows how to decode the serialized packets.
//
// Direction: server --> client

struct SessionProtocolVersionResponse {
  // The version number of the wire line protocol of the session being monitored.
  1: required i32 protocolVersion;

}
