
namespace java com.rapidsdata.wirelineprotocol.messages.compatibility
namespace cpp  com.rapidsdata.wirelineprotocol.messages.compatibility

// All messages from client to server have an odd numbered type.
// All messages from server to client have an even numbered type.
// Any messages that can be sent from both client and server have an even numbered type.
// Refer to the MessageTypes.thrift file located in each protocol version package for
// the remaining message type definitions.
enum MessageTypes {
  ProtocolCompatibleRequest    = 1,
  ProtocolCompatibleResponse   = 2,
  ProtocolIncompatibleResponse = 4
}
