// Query 5 - Fuzzy Join
// USE TinySocial;
// SET simfunction "edit-distance";
// SET simthreshold "3";
// SELECT gbu.id AS id, gbu.name AS name,
//        (SELECT cm.user.screenName AS chirpScreenname, cm.user.name AS chirpName
//         FROM ChirpMessages cm
//         WHERE cm.user.name ~= gbu.name) AS similarUsers
// FROM GleambookUsers gbu;
async function executeQuery5() {
    const Connector = require('../src/core/Connector');
    const QueryBuilder = require('../src/core/QueryBuilder');
    const connector = new Connector();
  
    const mainQueryString = await new QueryBuilder()
      .set('simfunction', 'edit-distance')
      .set('simthreshold', '3')
      .use('TinySocial')
      .select("gbu.id AS id, gbu.name AS name")
      .selectSubQuery("similarUsers", qb =>
        qb.select("cm.user.screenName AS chirpScreenname, cm.user.name AS chirpName")
          .from("ChirpMessages cm")
          .where("cm.user.name ~= gbu.name"))
      .from("GleambookUsers gbu")
      .build();
  
    try {
      const result = await connector.executeQuery(mainQueryString);
      // console.log('Query 5 (Fuzzy Join) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 5 Error:', error.message);
      if (error.stack) console.error(error.stack);
    }
  }
  

executeQuery5();