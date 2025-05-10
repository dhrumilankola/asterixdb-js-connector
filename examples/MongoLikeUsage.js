const { connect } = require('..');

/**
 * This example demonstrates how to use the MongoDB-like API for AsterixDB.
 * It shows how to perform common operations like find, insert, update, and delete
 * with a syntax similar to the MongoDB Node.js driver.
 */
async function example() {
  try {
    // Connect to AsterixDB (default URL: http://localhost:19002)
    const client = connect();
    
    // Get a database reference
    // In AsterixDB, this is a "dataverse"
    const db = client.db('TinySocial');
    
    // Get a collection reference
    // In AsterixDB, this is a "dataset"
    const users = db.collection('ChirpUsers');
    
    // Example: Find all documents
    const allUsers = await users.find({}, { limit: 3 });
    console.log('Find all (limit 3):', allUsers);
    
    // Example: Find with a filter
    const filteredUsers = await users.find({ screenName: 'NathanGiesen@211' });
    console.log('Find by screenName (NathanGiesen@211):', filteredUsers);
    
    // Example: Find one document
    const oneUser = await users.findOne({ screenName: 'ColineGeyer@63' });
    console.log('Find one (ColineGeyer@63):', oneUser);
    
    // Example: Count documents
    const userCount = await users.countDocuments({ friendsCount: { $gt: 2 } });
    console.log(`Users with more than 2 friends: ${userCount}`);
    
    // Example: Insert one document
    let insertedScreenName; // To store the screenName of the inserted user for later use
    try {
      const newUser = {
        screenName: `NewUser@${Math.floor(Math.random() * 1000)}`,
        lang: 'en',
        friendsCount: 1,
        statusesCount: 0,
        name: `A New User ${Math.floor(Math.random() * 1000)}`,
        followersCount: 0,
      };
      insertedScreenName = newUser.screenName; // Store for later use

      const insertResult = await users.insertOne(newUser);
      console.log('Insert result:', insertResult);

      // Example: Find the inserted document by its screenName (Primary Key)
      const insertedUser = await users.findOne({ screenName: insertedScreenName });
      console.log('Found inserted user:', insertedUser);

      // Example: Update a document (e.g., update the statusesCount of the inserted user)
      const updateResult = await users.updateOne(
        { screenName: insertedScreenName }, 
        { $set: { statusesCount: 1, lang: "fr" } }
      );
      console.log('Update result:', updateResult);
      const updatedUser = await users.findOne({ screenName: insertedScreenName });
      console.log('Updated user:', updatedUser);

      // Example: Delete the document
      const deleteResult = await users.deleteOne({ screenName: insertedScreenName });
      console.log('Delete result:', deleteResult);
      const deletedUser = await users.findOne({ screenName: insertedScreenName });
      console.log('Attempted find after delete (should be null/empty):', deletedUser);

    } catch (error) {
      console.error('Error during insert/update/delete operations:', error);
    }
    
    // Example: Using more complex queries
    const complexQuery = await users.find({
      $or: [
        { screenName: { $regex: '^Nathan' } },
        { followersCount: { $gt: 20 } }
      ]
    }, { limit: 3 });
    console.log('Complex query result:', complexQuery);
    
    // Close the client connection
    await client.close();
  } catch (error) {
    console.error('Error in MongoLikeUsage example:', error.message);
  }
}

// Run the example
example(); 