
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to encapsulate a message sent in/out of the session
// being monitored.
//
// Direction: server --> client

struct SessionDataResponse {
  // An encapsulated message that was sent or received by the server for the session being
  // monitored. This message is a regular thrift message with the standard length and type
  // header prepended to it.
  1: required binary message;

}
