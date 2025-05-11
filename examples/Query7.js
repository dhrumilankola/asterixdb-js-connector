// Query 7 - Universal Quantification
// USE TinySocial;
// SELECT VALUE gbu
// FROM GleambookUsers gbu
// WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);
async function executeQuery7() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
    const fromClauseString = `GleambookUsers gbu`; // Using a template literal
    
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('VALUE gbu')
      .from(fromClauseString) // Pass the variable
      .where('(EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN)')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      // console.log('Query 7 (Universal Quantification) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 7 Error:', error.message);
      if (error.stack) console.error(error.stack);
    }
  }

executeQuery7();