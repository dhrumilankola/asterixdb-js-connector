const Connector = require('../core/Connector')
const QueryBuilder = require('../core/QueryBuilder');

// Query 2-A - Equijoin
// USE TinySocial;
// SELECT user.name AS uname, msg.message AS message
// FROM GleambookUsers user, GleambookMessages msg
// WHERE msg.authorId = user.id;
async function executeQuery2A() {
    const connector = new Connector();
    const query = await new QueryBuilder()
      .use('TinySocial')
      // We can specify the entire SELECT clause as a single string.
      .select(['user.name AS uname, msg.message AS message'])
      // For multi-table FROM, simply pass both datasets separated by a comma.
      .from('GleambookUsers user, GleambookMessages msg')
      .where('msg.authorId = user.id')
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 2-A (Equijoin) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 2-A Error:', error.message);
    }
  }
  
  // Query 2-B - Index join
  // USE TinySocial;
  // SELECT user.name AS uname, msg.message AS message
  // FROM GleambookUsers user, GleambookMessages msg
  // WHERE msg.authorId /*+ indexnl */ = user.id;
  async function executeQuery2B() {
    const connector = new Connector();
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select(['user.name AS uname, msg.message AS message'])
      .from('GleambookUsers user, GleambookMessages msg')
      // Include the index hint in the WHERE clause.
      .where('msg.authorId /*+ indexnl */ = user.id')
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 2-B (Index join) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 2-B Error:', error.message);
    }
  }

(async() => {
    await executeQuery2A();
    await executeQuery2B();    
})();