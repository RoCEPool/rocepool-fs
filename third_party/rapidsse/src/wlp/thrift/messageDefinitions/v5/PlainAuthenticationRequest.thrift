
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the client to the server to indicate that it wishes to begin an authentication process
// using a username and password. Also see KerberosAuthenticationRequest.
//
// Direction: client --> server

struct PlainAuthenticationRequest {
  // The username to authenticate with.
  1: required string username;

  // The password to authenticate with.
  2: required string password;

}
