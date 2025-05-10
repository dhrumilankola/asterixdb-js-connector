// File: /src/index.js

const Connector = require('../src/core/Connector');
const QueryBuilder = require('../src/core/QueryBuilder');

// Direct execution of a SQL++ query provided by the developer.
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
  

executeDirectQuery();

