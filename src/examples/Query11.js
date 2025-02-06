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
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
    
    // Build the nested subquery.
    let subquery = new QueryBuilder()
      .select('VALUE cm2.chirpId')
      .from('ChirpMessages cm2')
      .where('cm2.referredTopics ~= cm1.referredTopics AND cm2.chirpId > cm1.chirpId')
      .build();
    // Remove trailing semicolon.
    if (subquery.endsWith(';')) {
      subquery = subquery.slice(0, -1);
    }
    
    // Build the main query.
    const mainQuery = new QueryBuilder()
      .set('simfunction', 'jaccard')
      .set('simthreshold', '0.3')
      .use('TinySocial')
      .select(`cm1 AS chirp, (${subquery}) AS similarChirps`)
      .from('ChirpMessages cm1')
      .build();
    
    try {
      const result = await connector.executeQuery(mainQuery);
      console.log('Query 11 (Left Outer Fuzzy Join) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 11 Error:', error.message);
    }
  }

executeQuery11();