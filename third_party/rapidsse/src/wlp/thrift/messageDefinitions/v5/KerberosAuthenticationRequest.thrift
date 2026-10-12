
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Sent from the client to the server to indicate that it wishes to begin an authentication process
// using a Kerberos tickets. Also see PlainAuthenticationRequest.
//
// Direction: client --> server

struct KerberosAuthenticationRequest {
  // The Kerberos principal of the client authenticating with RDP.
  1: required string clientPrincipal;

  // The initial opaque GSS-API token
  2: required binary token;
}
