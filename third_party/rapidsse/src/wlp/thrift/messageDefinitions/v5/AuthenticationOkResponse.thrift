
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the server to the client to indicate that the authentication process has succeeded.
// For early versions of RapidsDB, this could be sent without the client being asked for
// authentication information (such as a password) in order to short-cut the authentication
// process when user authentication is not yet supported.
//
// Direction: server --> client

struct AuthenticationOkResponse {
  // No fields required.

}
