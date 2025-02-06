// File: /src/index.js

const Connector = require('../core/Connector');
const QueryBuilder = require('../core/QueryBuilder');

async function executeBuiltQuery() {
    const connector = new Connector();
    const query = new QueryBuilder()
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

// Building and executing an INSERT query programmatically using QueryBuilder.
async function executeInsertQuery() {
    const connector = new Connector();
    const sampleData = [
      {
        screenName: "danko001",
        lang: "en",
        friendsCount: 1250,
        statusesCount: 50,
        name: "Dhrumil Ankola",
        followersCount: 4500
      }
    ];
  
    const query = new QueryBuilder()
      .use('TinySocial')
      .insertInto('ChirpUsers')
      .values(sampleData)
      .build();
  
    try {
      const result = await connector.executeQuery(query);
      console.log('Insert Query Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Insert Query Error:', error.message);
    }
  }


(async () => {
    await executeInsertQuery();
    await executeBuiltQuery();
})();