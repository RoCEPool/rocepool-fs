
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5


// Information about an individual message that was not sent to the monitoring session because
// it couldn't keep up. Used below.
struct DroppedMessageInfo {
  // The size of the message that was dropped.
  1: required i32 messageSize

  // The type of the message that was dropped.
  2: required i32 messageType
}

// Sent from the server to the client to give a rough indication on the number and type
// of messages from the monitored session were not sent to this client because the client
// (or server) were not keeping up.
//
// Direction: server --> client

struct SessionDroppedMessagesResponse {

  // Information about the session monitoring messages that were dropped.
  1: optional list<DroppedMessageInfo> droppedMessages

  // Were there any messages that we dropped where we were unable to record the
  // messages size and type? If true then the droppedMessages field above is not
  // a complete and accurate record of the dropped session monitoring messages.
  2: required bool missingDroppedMessages
}
