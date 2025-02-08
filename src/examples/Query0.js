const Connector = require('../core/Connector');
const QueryBuilder = require('../core/QueryBuilder');

// Query 0-A - Exact-Match Lookup
async function executeQuery0A() {
  const connector = new Connector();
  try {
    const query =  await new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE user'])
      .from('GleambookUsers user')
      .where('user.id = 8')
      .build();

    const result = await connector.executeQuery(query);
    console.log('Query 0-A (Exact-Match Lookup) Result:');
    console.dir(result, { depth: null, colors: true });
  } catch (error) {
    console.error('Query 0-A Error:', error.message);
  }
}

// Query 0-B - Range Scan
async function executeQuery0B() {
  const connector = new Connector();
  try {
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE user']) 
      .from('GleambookUsers user')
      .where('user.id >= 2 AND user.id <= 4')
      .build();

    const result = await connector.executeQuery(query);
    console.log('Query 0-B (Range Scan) Result:');
    console.dir(result, { depth: null, colors: true });
  } catch (error) {
    console.error('Query 0-B Error:', error.message);
  }
}

(async () => {
  await executeQuery0A();
  await executeQuery0B();
})();
