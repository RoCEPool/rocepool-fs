
namespace java com.rapidsdata.wirelineprotocol.messages.compatibility
namespace cpp  com.rapidsdata.wirelineprotocol.messages.compatibility

// Sent from the server to the client to indicate that the server does not support the
// wire line protocol version that the client is requesting.
//
// Direction: server --> client

struct ProtocolIncompatibleResponse {
  // The version of the wire line protocol that the client supports.
  1: required i32 clientProtocolVersion

  // The minimum version of the wire line protocol that the server supports.
  2: required i32 minimumProtocolVersion

  // The maximum (and preferred) version of the wire line protocol that the server supports.
  3: required i32 maximumProtocolVersion

  // A string describing the release version of the RapidsDB node.
  4: required string serverSoftwareVersion

  // A string describing the build information of the RapidsDB node (branch, build date, etc).
  5: required string serverBuildInfo

  // An error message from the server.
  6: required string errorMessage
}
