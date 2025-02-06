// Query 8 - Simple Aggregation
// USE TinySocial;
// SELECT COUNT(gbu) AS numUsers FROM GleambookUsers gbu;
async function executeQuery8() {
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
    
    const query = new QueryBuilder()
      .use('TinySocial')
      .select('COUNT(gbu) AS numUsers')
      .from('GleambookUsers gbu')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 8 (Simple Aggregation) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 8 Error:', error.message);
    }
  }

executeQuery8();