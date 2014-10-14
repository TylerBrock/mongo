//
// Check that write commands set and clear last error appropriately
//

var collname = "write_command_gle";
var result;

//
// Insert
//

db[collname].drop();

// Insert a document, check for no error
db.runCommand({ insert: collname, documents: [{_id:1}] });
result = db.runCommand({getlasterror: 1});
assert(result.ok == 1);
assert(result.err == null);

// Insert a document with the same _id, check that there are erros
db.runCommand({ insert: collname, documents: [{_id:1}] });
result = db.runCommand({getlasterror: 1});
assert(result.ok == 1);
assert(result.err != null);

// Check that there is still an error if we check again
result = db.runCommand({getlasterror: 1});
assert(result.ok == 1);
assert(result.err != null);

// Insert a document with a new _id, should clear the error
db.runCommand({ insert: collname, documents: [{_id:2}] });
result = db.runCommand({getlasterror: 1});
assert(result.ok == 1);
assert(result.err == null);

//
// Update
//

db[collname].drop();
db[collname].insert({a: 1});
db[collname].insert({a: 2});
db[collname].ensureIndex({a: 1}, {unique: true});

// Check for error
db.runCommand({update: collname, updates: [{q: {a:2}, u: {a:1}}]});
result = db.runCommand({getLastError: 1});
assert(result.ok == 1);
assert(result.err != null);

// Check that a good update clears the error
db.runCommand({update: collname, updates: [{q: {a:2}, u: {a:3}}]});
result = db.runCommand({getLastError: 1});
assert(result.ok == 1);
assert(result.err == null);

//
// Delete
//

db[collname].drop();

// This does not produce an error (neither does legacy)
db.runCommand({"delete": "does_not_exist", "deletes": [{badlynamed: {/*nada*/}}]});
result = db.runCommand({getLastError: 1});
assert(result.ok == 1);
assert(result.err == null);
