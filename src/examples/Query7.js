// Query 7 - Universal Quantification
// USE TinySocial;
// SELECT VALUE gbu
// FROM GleambookUsers gbu
// WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);
async function executeQuery7() {
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
    
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('VALUE gbu')
      .from('GleambookUsers')
      .where('(EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN)')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 7 (Universal Quantification) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 7 Error:', error.message);
    }
  }

executeQuery7();