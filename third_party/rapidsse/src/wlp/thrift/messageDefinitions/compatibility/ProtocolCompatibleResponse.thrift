
namespace java com.rapidsdata.wirelineprotocol.messages.compatibility
namespace cpp  com.rapidsdata.wirelineprotocol.messages.compatibility

// Sent from the server to the client to indicate that the server can support the version of
// the wire line protocol that the client is requesting.
//
// Direction: server --> client

struct ProtocolCompatibleResponse {
  // The version of the wire line protocol that has been agreed to between the client and server.
  1: required i32 protocolVersion

  // A string describing the release version of the RapidsDB node.
  2: required string serverSoftwareVersion

  // A string describing the build information of the RapidsDB node (branch, build date, etc).
  3: required string serverBuildInfo
}
