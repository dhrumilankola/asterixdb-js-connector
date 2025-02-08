const Connector = require('../core/Connector')
const QueryBuilder = require('../core/QueryBuilder');

// Query 1 - Other Query Filters
// USE TinySocial;
// SELECT VALUE user
// FROM GleambookUsers user
// WHERE user.userSince >= datetime('2010-07-22T00:00:00')
//   AND user.userSince <= datetime('2012-07-29T23:59:59');
async function executeQuery1() {
    const connector = new Connector();
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE user'])
      .from('GleambookUsers user')
      .where("user.userSince >= datetime('2010-07-22T00:00:00') AND user.userSince <= datetime('2012-07-29T23:59:59')")
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 1 (Other Query Filters) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 1 Error:', error.message);
    }
  }

(async() => {
    await executeQuery1();
})();