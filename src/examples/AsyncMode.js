const Connector = require('../core/Connector');
const QueryBuilder = require('../core/QueryBuilder');

// Executing an asynchronous query.
async function executeAsyncQuery() {
  const connector = new Connector();
  const query = new QueryBuilder()
    .use('TinySocial')
    .select(['*'])
    .from('ChirpUsers')
    .build();

  try {
    const result = await connector.executeQueryAsync(query);
    console.log('Asynchronous Query Result:');
    console.dir(result, { depth: null, colors: true });
  } catch (error) {
    console.error('Asynchronous Query Error:', error.message);
  }
}

// Test 2: Another async query with aggregation
// USE TinySocial;
// SELECT 
//     u.employment[0].organizationName as org,
//     COUNT(*) as emp_count
// FROM GleambookUsers u
// GROUP BY u.employment[0].organizationName
// ORDER BY emp_count DESC;
async function executeAsyncQueryTest2() {
    const connector = new Connector();
  
    const query = `
      USE TinySocial;
      SELECT 
        u.employment[0].organizationName as org,
        COUNT(*) as emp_count
      FROM GleambookUsers u
      GROUP BY u.employment[0].organizationName
      ORDER BY emp_count DESC;
    `;
  
    try {
      const result = await connector.executeQueryAsync(query);
      console.log("Test 2: Another async query with aggregation result:");
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error("Test 2: Async query error:", error.message);
    }
  }
    

(async()=>{
    await executeAsyncQuery();
    await executeAsyncQueryTest2();
})();

