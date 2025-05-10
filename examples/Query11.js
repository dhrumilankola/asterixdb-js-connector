// Query 11 - Left Outer Fuzzy Join
// USE TinySocial;
// SET simfunction "jaccard";
// SET simthreshold "0.3";
// SELECT cm1 AS chirp,
//        (SELECT VALUE cm2.chirpId
//         FROM ChirpMessages cm2
//         WHERE cm2.referredTopics ~= cm1.referredTopics
//           AND cm2.chirpId > cm1.chirpId) AS similarChirps
// FROM ChirpMessages cm1;
async function executeQuery11() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
    
    const mainQueryString = await new QueryBuilder()
      .set('simfunction', 'jaccard')
      .set('simthreshold', '0.3')
      .use('TinySocial')
      .select('cm1 AS chirp') // Select the first part
      .selectSubQuery('similarChirps', qb => // Use selectSubQuery for the correlated subquery
        qb.select('VALUE cm2.chirpId')
          .from('ChirpMessages cm2')
          .where('cm2.referredTopics ~= cm1.referredTopics AND cm2.chirpId > cm1.chirpId')
          // .use('TinySocial') // Not needed here, will inherit from parent or be set by parent builder context
      )
      .from('ChirpMessages cm1')
      .build();
    
    try {
      const result = await connector.executeQuery(mainQueryString);
      console.log('Query 11 (Left Outer Fuzzy Join) executed. Result length:', result.length);
    } catch (error) {
      console.error('Query 11 Error:', error.message);
      if (error.stack) console.error(error.stack);
    }
  }

executeQuery11();