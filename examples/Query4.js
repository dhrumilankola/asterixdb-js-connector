// Query 4 - Theta Join
// USE TinySocial;
// SELECT cm1.messageText AS message,
//        (SELECT VALUE cm2.messageText
//         FROM ChirpMessages cm2
//         WHERE `spatial-distance`(cm1.senderLocation, cm2.senderLocation) <= 1
//           AND cm2.chirpId < cm1.chirpId) AS nearbyMessages
// FROM ChirpMessages cm1;
async function executeQuery4() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
  
    const mainQueryString = await new QueryBuilder()
      .use('TinySocial')
      .select("cm1.messageText AS message")
      .selectSubQuery("nearbyMessages", qb =>
        qb.select("VALUE cm2.messageText")
          .from("ChirpMessages cm2")
          .where("`spatial-distance`(cm1.senderLocation, cm2.senderLocation) <= 1 AND cm2.chirpId < cm1.chirpId"))
      .from("ChirpMessages cm1")
      .build();
  
    try {
      const result = await connector.executeQuery(mainQueryString);
      // console.log('Query 4 (Theta Join) Result:');
      // console.dir(result, { depth: null, colors: true });
      console.log('Query 4 (Theta Join) executed. Result length:', result && result.results ? result.results.length : 'N/A (no results array)');
    } catch (error) {
      console.error('Query 4 Error:', error.message);
      if (error.stack) console.error(error.stack);
    }
  }
  

executeQuery4();
  