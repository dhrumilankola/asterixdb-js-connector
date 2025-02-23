const QueryBuilder = require('./core/QueryBuilder');

// Test a basic SELECT query.
const query1 = new QueryBuilder()
  .use('TinySocial')
  .select('user.name AS uname')
  .from('GleambookUsers user')
  .where('user.age > 21')
  .build();

console.log(query1);

// Test a nested subquery.
const query2 = new QueryBuilder()
  .use('TinySocial')
  .select('user.name AS uname')
  .selectSubQuery('messages', qb => 
    qb.select('VALUE msg.message')
      .from('GleambookMessages msg')
      .where('msg.authorId = user.id'))
  .from('GleambookUsers user')
  .build();

console.log(query2);
