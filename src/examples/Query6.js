// Query 6 - Existential Quantification
// USE TinySocial;
// SELECT VALUE gbu
// FROM GleambookUsers gbu
// WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);
async function executeQuery6() {
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
    
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select('VALUE gbu')
      .from('GleambookUsers')
      .where('(SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN)')
      .build();
    
    try {
      const result = await connector.executeQuery(query);
      console.log('Query 6 (Existential Quantification) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 6 Error:', error.message);
    }
  }

executeQuery6();