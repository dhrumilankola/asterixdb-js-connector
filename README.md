# AsterixDB JavaScript Connector

A JavaScript library for connecting to AsterixDB, offering a MongoDB-like API and offline capabilities, primarily tested and demonstrated in a Node.js environment.

## Features

- **MongoDB-like API**: Provides a familiar interface for common database operations, simplifying the learning curve for developers accustomed to MongoDB.
- **SQL++ Query Execution**: Supports direct execution of SQL++ statements for complex queries and operations not covered by the Mongo-like API. Includes a `QueryBuilder` for programmatic query construction.
- **Offline Capabilities (Node.js focused)**:
    - **Caching**: Utilizes `localforage` (with `localforage-driver-memory` for Node.js) to cache query results, enabling faster subsequent access and offline data retrieval.
    - **Operation Queuing**: Queues DML operations (inserts, updates, deletes) when offline, with the intention for them to be processed when connectivity is restored (manual/direct queue interaction demonstrated).
- **Modular Design**: Built with distinct components like `Connector`, `OfflineEnabledConnector`, `AsterixClient`, `AsterixDatabase`, `AsterixCollection`, `LocalStorageAdapter`, and `SyncManager`.

## Installation

To install the connector, use npm:
```bash
npm install ./
```
(Assuming you are in the root directory of the project. If publishing to npm, the command would be `npm install asterixdb-js-connector`)

## Basic Usage

```javascript
const { connect } = require('asterixdb-js-connector'); // Or your relative path if not installed as a module

async function main() {
  // Connect to AsterixDB
  // For offline features, see the 'Offline Capabilities' section.
  const client = connect({ astxUrl: 'http://localhost:19002' });

  try {
    // Get a database reference (AsterixDB dataverse)
    const db = client.db('TinySocial');

    // Get a collection reference (AsterixDB dataset)
    const users = db.collection('ChirpUsers');

    // Example: Insert a document
    const newUser = {
      screenName: `User-${Math.random().toString(36).substring(2, 9)}`,
      lang: 'en',
      friendsCount: Math.floor(Math.random() * 100),
      statusesCount: Math.floor(Math.random() * 200),
      name: 'A New User',
      followersCount: Math.floor(Math.random() * 1000)
    };
    const insertedUser = await users.insertOne(newUser);
    console.log('Inserted User:', insertedUser.screenName);

    // Example: Find a document
    const foundUser = await users.findOne({ screenName: insertedUser.screenName });
    console.log('Found User:', foundUser);

    // Example: Update a document
    await users.updateOne(
      { screenName: insertedUser.screenName },
      { $set: { friendsCount: insertedUser.friendsCount + 5 } }
    );
    console.log('Updated user:', insertedUser.screenName);

    // Example: Delete a document
    await users.deleteOne({ screenName: insertedUser.screenName });
    console.log('Deleted user:', insertedUser.screenName);

  } catch (error) {
    console.error('An error occurred:', error);
  } finally {
    // Close the connection
    await client.close();
    console.log('Connection closed.');
  }
}

main();
```

## MongoDB-Compatible API

The connector provides the following MongoDB-compatible methods:

### Client

- `connect(url, options)` - Connect to AsterixDB
- `client.db(name)` - Get a database reference
- `client.isConnected()` - Check if connected to the server
- `client.close()` - Close the connection

### Database

- `db.collection(name)` - Get a collection reference
- `db.listCollections()` - List all collections in the database
- `db.createCollection(name, options)` - Create a new collection
- `db.dropCollection(name)` - Drop a collection

### Collection

- `collection.find(query, options)` - Find documents
- `collection.findOne(query, options)` - Find a single document
- `collection.countDocuments(query, options)` - Count documents
- `collection.insertOne(doc)` - Insert a document
- `collection.insertMany(docs)` - Insert multiple documents
- `collection.updateOne(filter, update, options)` - Update a document
- `collection.updateMany(filter, update, options)` - Update multiple documents
- `collection.deleteOne(filter)` - Delete a document
- `collection.deleteMany(filter)` - Delete multiple documents
- `collection.distinct(field, filter)` - Get distinct values for a field

## Direct SQL++ Execution

For advanced use cases or operations not directly mapped in the Mongo-like API, you can execute SQL++ queries. The `Connector` class is used for this.
```javascript
const { Connector } = require('asterixdb-js-connector'); // Adjust path as needed

async function directSql() {
  const connector = new Connector({ astxUrl: 'http://localhost:19002' });
  try {
    const result = await connector.executeQuery("""
      USE TinySocial;
      SELECT u.screenName, u.friendsCount
      FROM ChirpUsers u
      WHERE u.friendsCount > 100
      ORDER BY u.friendsCount DESC
      LIMIT 3;
    """);
    console.log('SQL++ Query Result:', result.results || result);
  } catch (error) {
    console.error('SQL++ execution error:', error);
  }
  // Note: The basic Connector used here does not have a .close() method in this example context.
}

directSql();
```

## Offline Capabilities and Configuration

The connector supports offline caching and operation queuing, primarily demonstrated for Node.js environments using an in-memory driver.

To enable offline features, configure the client upon connection:
```javascript
const { connect } = require('asterixdb-js-connector'); // Adjust path as needed

const client = connect({
  astxUrl: 'http://localhost:19002',
  localStorage: { // Configure localStorage features
    offlineEnabled: true,    // Master switch for offline features
    cacheTTL: 5 * 60 * 1000, // Cache Time-To-Live in milliseconds (e.g., 5 minutes)
    debug: true,             // Enable debug logging for the adapter
    enableOfflineQueue: true // Enable the queue for operations made while "offline"
  }
});

// ... use the client as usual ...

// When offlineEnabled is true:
// - SELECT queries (find, findOne) will first check the cache.
//   If data is found and not expired, it's returned from cache.
//   Otherwise, data is fetched from the server and then cached.
// - DML operations (insertOne, updateOne, deleteOne) can be queued if the SyncManager
//   determines the client is "offline" (this logic is part of SyncManager and may involve
//   simulated or actual network checks). The PowerhouseExample.js shows direct queue interaction.

// The LocalStorageAdapter uses 'localforage'. In Node.js, it attempts to use
// 'localforage-driver-memory'. Ensure this driver is available if not bundling.
// If `localforage-driver-memory` is not found, localforage might default to behaviors
// unsuitable for Node.js, potentially leading to errors.

// For a comprehensive demonstration of these features, including direct interaction
// with the offline queue and cache statistics, refer to:
// src/examples/PowerhouseExample.js
```

_Note: The `forceSynchronization()` method mentioned in previous versions was part of an earlier conceptual design. Actual synchronization triggering is handled by the `SyncManager` based on its internal logic (e.g., network status changes, periodic checks), or by direct interaction with the `LocalStorageAdapter`'s queue as shown in `PowerhouseExample.js`._

## Examples

The `examples/` directory contains scripts demonstrating various features:
- `MongoLikeUsage.js`: Basic DML operations using the MongoDB-like API.
- `DemoUsage.js`: A comprehensive script showcasing DDL (commented out by default), DML, caching, and offline queueing simulation.

### Running the Examples

Navigate to the `src/examples/` directory and run the scripts using Node.js:
```bash
cd src/examples
node MongoLikeUsage.js
node DemoUsage.js
```
Ensure your AsterixDB instance is running and accessible at `http://localhost:19002` (or update the URL in the example scripts).

## Development & Building

If you need to bundle the connector for browser usage or specific Node.js environments, you can use webpack:
```bash
npm run build
```
This will generate a bundled file in the `dist/` directory (configuration in `webpack.config.js`).

## License

MIT 