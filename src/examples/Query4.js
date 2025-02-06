// Query 4 - Theta Join
// USE TinySocial;
// SELECT cm1.messageText AS message,
//        (SELECT VALUE cm2.messageText
//         FROM ChirpMessages cm2
//         WHERE `spatial-distance`(cm1.senderLocation, cm2.senderLocation) <= 1
//           AND cm2.chirpId < cm1.chirpId) AS nearbyMessages
// FROM ChirpMessages cm1;
async function executeQuery4() {
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
  
    const mainQuery = new QueryBuilder()
      .use('TinySocial')
      .select("cm1.messageText AS message")
      .selectSubQuery("nearbyMessages", qb =>
        qb.select("VALUE cm2.messageText")
          .from("ChirpMessages cm2")
          .where("`spatial-distance`(cm1.senderLocation, cm2.senderLocation) <= 1 AND cm2.chirpId < cm1.chirpId"))
      .from("ChirpMessages cm1")
      .build();
  
    try {
      const result = await connector.executeQuery(mainQuery);
      console.log('Query 4 (Theta Join) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 4 Error:', error.message);
    }
  }
  

executeQuery4();
  