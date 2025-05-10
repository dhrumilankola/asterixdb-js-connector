// Query 10 - Grouping and Limits
// USE TinySocial;
// SELECT uid AS user, c AS count
// FROM ChirpMessages cm
// GROUP BY cm.user.screenName AS uid WITH c AS count(cm)
// ORDER BY c DESC
// LIMIT 3;
async function executeQuery10() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
    
    // We incorporate the computed alias for count using a standard GROUP BY approach.
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('uid AS user, COUNT(cm) AS count')
      .from('ChirpMessages cm')
      .groupBy('cm.user.screenName AS uid')
      .orderBy('count DESC')
      .limit(3)
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 10 (Grouping and Limits) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 10 Error:', error.message);
    }
  }

executeQuery10();