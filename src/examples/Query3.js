// Query 3 - Nested Outer Join
// USE TinySocial;
// SELECT user.name AS uname,
//        (SELECT VALUE msg.message
//         FROM GleambookMessages msg
//         WHERE msg.authorId = user.id) AS messages
// FROM GleambookUsers user;
async function executeQuery3() {
    const Connector = require('../core/Connector');
    const QueryBuilder = require('../core/QueryBuilder');
    const connector = new Connector();
  
    const mainQuery = new QueryBuilder()
      .use('TinySocial')
      .select("user.name AS uname")
      .selectSubQuery("messages", qb =>
        qb.select("VALUE msg.message")
          .from("GleambookMessages msg")
          .where("msg.authorId = user.id"))
      .from("GleambookUsers user")
      .build();
  
    try {
      const result = await connector.executeQuery(mainQuery);
      console.log('Query 3 (Nested Outer Join) Result:');
      console.dir(result, { depth: null, colors: true });
    } catch (error) {
      console.error('Query 3 Error:', error.message);
    }
  }  
  

(async() => {
    await executeQuery3();
})();