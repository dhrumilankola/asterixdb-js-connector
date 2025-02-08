// File: /src/index.js

const Connector = require('../core/Connector');
const QueryBuilder = require('../core/QueryBuilder');

// Option 1: Direct execution of a SQL++ query provided by the developer.
async function executeDirectQuery() {
    const connector = new Connector();
    const rawQuery = 'USE TinySocial; SELECT * FROM ChirpUsers LIMIT 2;';
    try {
      const result = await connector.executeQuery(rawQuery);
      console.log('Direct Query Result:');
      // console.log('Direct Query Result:', JSON.stringify(result, null, 2));
      console.dir(result, {depth: null, colors: true});
    } catch (error) {
      console.error('Direct Query Error:', error.message);
    }
  }
  
// Option 2: Building a SQL++ query programmatically using QueryBuilder.
async function executeBuiltQuery() {
    const connector = new Connector();
    const query = await new QueryBuilder()
      .use('TinySocial')
      .select(['*'])
      .from('ChirpUsers')
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Built Query Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Built Query Error:', error.message);
    }
  }


(async () => {
    await executeDirectQuery();
    await executeBuiltQuery();
})();