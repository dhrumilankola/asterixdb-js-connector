const Connector = require('../src/core/Connector')
const QueryBuilder = require('../src/core/QueryBuilder');

// Query 9-A - Grouping and Aggregation
// USE TinySocial;
// SELECT uid AS user, COUNT(cm) AS count
// FROM ChirpMessages cm
// GROUP BY cm.user.screenName AS uid;
async function executeQuery9A() {
    const connector = new Connector();
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('uid AS user, COUNT(cm) AS count')
      .from('ChirpMessages cm')
      .groupBy('cm.user.screenName AS uid')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 9-A (Grouping and Aggregation) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 9-A Error:', error.message);
    }
  }

// Query 9-B - (Hash-Based) Grouping and Aggregation
// USE TinySocial;
// SELECT uid AS user, COUNT(cm) AS count
// FROM ChirpMessages cm
//  /*+ hash */
// GROUP BY cm.user.screenName AS uid;
async function executeQuery9B() {
    const connector = new Connector();
    // Since our builder does not have a dedicated hint method for grouped aggregation,
    // we inject the hint manually into the query string.
    const baseQuery = await new QueryBuilder()
      .use('TinySocial')
      .select('uid AS user, COUNT(cm) AS count')
      .from('ChirpMessages')
      .groupBy('cm.user.screenName AS uid')
      .build();
    // Inject the hash hint after the FROM clause.
    const query = baseQuery.replace('FROM ChirpMessages cm', 'FROM ChirpMessages cm /*+ hash */');
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 9-B (Hash-Based Grouping and Aggregation) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 9-B Error:', error.message);
    }
  }
  
(async() => {
    await executeQuery9A();
    await executeQuery9B();
})();