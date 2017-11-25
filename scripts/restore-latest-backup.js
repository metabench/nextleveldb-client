
// Finds latest backup on disk
//  Could have marking of complete backups

// Loads .be (Binary Encoded) files

// Will restore this backup 

// 


// Restore it to a server given in the config.

// Could restore every record in a directory.
//  Possibility of changing table reference key prefix?
//   Confirming that the records are going back into the correct table.
//    That could possibly be done through record validation, where it checks types and structure.

// Will be able to upload a few weeks worth of bittrex in a few minutes.

// Run another server, with collecter. Keep that running.
//  Keep stable.

// Keep the first version running on data1 for a while. It appears stable, but does not index the records.
//  Many records, such as the snapshot records, won't be indexed apart from the PK.
//  PK lookup is fine for many things.


// Run another. That can be one that we keep changing and upgrading.

// Then continue local development. See about having that local server sync with at least one of the others.

// Or, get backup done again, get other server running.
// Get third server running, and use that to make the changes.
//  Have redundency
// Maybe make another server or two with auto-restart

// Could have a client that connects to multiple databases at once. Having the db use this client will enable sharding and replication, as the server will use the client to
//  connect to other servers.

// 1. Keep current server running
// 2. Start server with current code
// 3. Start another
// 4. Start another, but then keep updating the code here.

// Streaming data between servers will do a lot to 




