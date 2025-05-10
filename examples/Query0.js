const Connector = require('../src/core/Connector');
const QueryBuilder = require('../src/core/QueryBuilder');

// Query 0-A - Exact-Match Lookup
async function executeQuery0A() {
  const connector = new Connector();
  try {
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select(['VALUE users'])
      .from('GleambookUsers users')
      .where('users.id = 8')
      .build();

    console.log('Generated Query 0-A:', query);
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

    console.log('Generated Query 0-B:', query);
    const result = await connector.executeQuery(query);
    console.log('Query 0-B (Range Scan) Result:');
    console.dir(result, { depth: null, colors: true });
  } catch (error) {
    console.error('Query 0-B Error:', error.message);
  }
}

// Query 0-C - Nested Subquery Test
async function executeQuery0C() {
  const connector = new Connector();
  try {
    // Outer query: set use, select, and from so that _outerAliases is populated.
    const qb = new QueryBuilder()
      .use('TinySocial')
      .select(['user.name AS uname'])
      .from('GleambookUsers user');
    
    // Now add the nested subquery.
    await qb.selectSubQuery('messages', async nestedQB => {
      nestedQB.use('TinySocial');
      nestedQB.select(['VALUE msg.message'])
        .from('GleambookMessages msg')
        .where('msg.authorId = user.id');
    });
    
    const query = await qb.build();
    console.log('Generated Query 0-C:', query);
    const result = await connector.executeQuery(query);
    console.log('Query 0-C (Nested Subquery) Result:');
    console.dir(result, { depth: null, colors: true });
  } catch (error) {
    console.error('Query 0-C Error:', error.message);
  }
}


(async () => {
  await executeQuery0A();
  await executeQuery0B();
  await executeQuery0C();
})();
