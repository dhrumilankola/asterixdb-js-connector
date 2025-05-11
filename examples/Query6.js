// Query 6 - Existential Quantification
// USE TinySocial;
// SELECT VALUE gbu
// FROM GleambookUsers gbu
// WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);
async function executeQuery6() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
    const fromClauseString = `GleambookUsers gbu`; // Using a template literal
    
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('VALUE gbu')
      .from(fromClauseString) // Pass the variable
      .where('(SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN)')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      // console.log('Query 6 (Existential Quantification) Result:');
      console.dir(result, { depth: null, colors: true });
      // console.log('Query 6 (Existential Quantification) executed. Result length:', result && result.results ? result.results.length : 'N/A (no results array)');
    } catch (error) {
      console.error('Query 6 Error:', error.message);
      if (error.stack) console.error(error.stack);
    }
  }

executeQuery6();