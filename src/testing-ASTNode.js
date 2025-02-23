const ASTNode = require('./core/ASTNode');

// Create SELECT node
const selectNode = new ASTNode('SELECT', 'SELECT user.name AS uname');

// Create FROM node and add as child
const fromNode = new ASTNode('FROM', 'FROM GleambookUsers user');
selectNode.addChild(fromNode);

// Serialize the query
const sqlQuery = selectNode.serialize();
console.log(sqlQuery);
// Expected output: "SELECT user.name AS uname FROM GleambookUsers user"
