
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// This message is sent by a client to the server to setup or change the client's
// session settings. This can include things like the current location or any other
// session parameters. All fields are interpreted and set. A null value for
// defaultFederation means "use the default federation on the server". A null
// value for defaultCatalog or defaultSchema means that there the session will have
// no current catalog or schema set, so table resolution will search all catalogs
// and schemas.
//
// Direction: client --> server

struct SessionChangeRequest {

  // The default federation to query.
  1: optional string defaultFederation;

  // The default catalog name to use when a query does not specify a catalog name.
  2: optional string defaultCatalog;

  // The default schema name to use when a query does not specify a catalog name.
  3: optional string defaultSchema;

  // Additional session fields can go in here.
}
