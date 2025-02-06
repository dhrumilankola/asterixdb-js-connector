const Connector = require('../core/Connector')
const QueryBuilder = require('../core/QueryBuilder');

// Query 0-A - Exact-Match Lookup
// USE TinySocial;
// SELECT VALUE user
// FROM GleambookUsers user
// WHERE user.id = 8;
async function executeQuery0A() {
    const connector = new Connector();
    const query = new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE user'])
      .from('GleambookUsers user')
      .where('user.id = 8')
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 0-A (Exact-Match Lookup) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 0-A Error:', error.message);
    }
  }

// Query 0-B - Range Scan
// USE TinySocial;
// SELECT VALUE user
// FROM GleambookUsers user
// WHERE user.id >= 2 AND user.id <= 4;
async function executeQuery0B() {
    const connector = new Connector();
    const query = new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE user'])
      .from('GleambookUsers user')
      .where('user.id >= 2 AND user.id <= 4')
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 0-B (Range Scan) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 0-B Error:', error.message);
    }
  }

(async() => {
    await executeQuery0A();
    await executeQuery0B();
})();