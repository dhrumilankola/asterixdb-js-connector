# AsterixDB JavaScript Connector

[![NPM version](https://img.shields.io/npm/v/asterixdb-js-connector.svg?style=flat)](https://www.npmjs.com/package/asterixdb-js-connector)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
<!-- Add other badges if you have them, e.g., build status, code coverage -->

A comprehensive JavaScript library for connecting to and interacting with Apache AsterixDB. It offers a developer-friendly, MongoDB-like API for common database operations, alongside the ability to execute raw SQL++ queries and leverage a powerful QueryBuilder. The connector also features robust offline capabilities, including data caching and operation queuing, primarily designed and tested for Node.js environments.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Client](#client)
  - [Database (Dataverse)](#database-dataverse)
  - [Collection (Dataset)](#collection-dataset)
- [API Usage](#api-usage)
  - [Connecting to AsterixDB](#connecting-to-asterixdb)
  - [MongoDB-like Operations](#mongodb-like-operations)
    - [Inserting Documents](#inserting-documents)
    - [Finding Documents](#finding-documents)
    - [Updating Documents](#updating-documents)
    - [Deleting Documents](#deleting-documents)
    - [Counting Documents](#counting-documents)
    - [Distinct Values](#distinct-values)
  - [Direct SQL++ Execution](#direct-sql-execution)
  - [QueryBuilder](#querybuilder)
- [Offline Capabilities](#offline-capabilities)
  - [Enabling Offline Features](#enabling-offline-features)
  - [Caching](#caching)
  - [Operation Queuing](#operation-queuing)
  - [Synchronization](#synchronization)
- [Running Examples](#running-examples)
- [API Documentation](#api-documentation)
- [Building for the Browser (Optional)](#building-for-the-browser-optional)
- [Contributing](#contributing)
- [License](#license)

## Features

- **MongoDB-like API**: Familiar and intuitive interface (`find`, `insertOne`, `updateOne`, etc.) for interacting with AsterixDB datasets.
- **Full SQL++ Power**: Execute any SQL++ query directly for complex operations.
- **Fluent QueryBuilder**: Programmatically construct complex SQL++ queries with a chained, easy-to-use API.
- **Offline First (Node.js)**:
    - **Automatic Caching**: Transparently caches query results using `localforage` (`localforage-driver-memory` for Node.js) to improve performance and enable offline data access.
    - **Operation Queuing**: Automatically queues DML operations (inserts, updates, deletes) when offline.
    - **Automatic Synchronization**: Manages synchronization of queued operations when network connectivity is restored.
- **Event-Driven**: Emits events for online/offline status changes and synchronization progress.
- **Modular and Extensible**: Designed with clear separation of concerns (Client, Connector, Database, Collection, Offline Adapters).

## Installation

```bash
npm install asterixdb-js-connector
```

## Quick Start

```javascript
const { connect } = require('asterixdb-js-connector');

async function main() {
  // Connect to AsterixDB (defaults to http://localhost:19002)
  // To enable offline features, see the "Offline Capabilities" section.
  const client = connect({ astxUrl: 'http://localhost:19002' });

  try {
    // Get a database (Dataverse) reference
    const db = client.db('TinySocial'); // Replace 'TinySocial' with your Dataverse name

    // Get a collection (Dataset) reference
    const users = db.collection('ChirpUsers'); // Replace 'ChirpUsers' with your Dataset name

    // Insert a document
    const newUser = {
      screenName: `User-${Date.now()}`,
      name: 'Awesome Dev',
      lang: 'en',
      friendsCount: 50,
      statusesCount: 100,
      followersCount: 200
    };
    const insertedUser = await users.insertOne(newUser);
    console.log('Inserted User ScreenName:', insertedUser.screenName);

    // Find a document
    const foundUser = await users.findOne({ screenName: insertedUser.screenName });
    console.log('Found User:', foundUser);

    // Update a document
    const updateResult = await users.updateOne(
      { screenName: insertedUser.screenName },
      { $set: { friendsCount: (foundUser.friendsCount || 0) + 10 } }
    );
    console.log('Update Result:', updateResult);

    // Find the updated document
    const updatedUser = await users.findOne({ screenName: insertedUser.screenName });
    console.log('Updated User friendsCount:', updatedUser.friendsCount);
    
    // Delete the document
    const deleteResult = await users.deleteOne({ screenName: insertedUser.screenName });
    console.log('Delete Result:', deleteResult);

  } catch (error) {
    console.error('An error occurred:', error.message);
    if (error.stack) console.error(error.stack);
  } finally {
    // Close the connection
    await client.close();
    console.log('Connection closed.');
  }
}

main();
```

## Core Concepts

### Client
The `AsterixClient` is the main entry point to interact with AsterixDB. It's obtained via the `connect()` function.

### Database (Dataverse)
An `AsterixDatabase` instance represents an AsterixDB Dataverse. You obtain it using `client.db('YourDataverseName')`.

### Collection (Dataset)
An `AsterixCollection` instance represents an AsterixDB Dataset within a Dataverse. You obtain it using `db.collection('YourDatasetName')`. It provides the MongoDB-like methods for CRUD operations.

## API Usage

### Connecting to AsterixDB
```javascript
const { connect } = require('asterixdb-js-connector');

// Default connection (http://localhost:19002, no offline features)
const client = connect();

// Connection with a specific URL
const clientWithUrl = connect({ astxUrl: 'http://your-asterixdb-url:19002' });

// Connection with offline features enabled (see "Offline Capabilities" section for details)
const offlineClient = connect({
  astxUrl: 'http://localhost:19002',
  localStorage: {
    offlineEnabled: true,
    cacheTTL: 10 * 60 * 1000, // 10 minutes
    enableOfflineQueue: true
  }
});

// Check connection status
async function checkStatus() {
  if (await offlineClient.isConnected()) {
    console.log('Successfully connected to AsterixDB!');
  } else {
    console.log('Failed to connect or server is not reachable.');
  }
  await offlineClient.close();
}
// checkStatus();
```

### MongoDB-like Operations
All MongoDB-like operations are `async` and return Promises.

#### Inserting Documents
- `collection.insertOne(doc)`: Inserts a single document.
- `collection.insertMany(docsArray)`: Inserts an array of documents. (Note: `insertMany` in the current implementation might insert one by one or require specific AsterixDB SQL++ for bulk, verify its bulk nature.)

```javascript
const userDoc = { screenName: 'dev123', name: 'Developer One', lang: 'js' };
const inserted = await users.insertOne(userDoc);
console.log('Inserted ID (screenName):', inserted.screenName); // Or inserted._id if you use it
```

#### Finding Documents
- `collection.find(query, options)`: Finds multiple documents. `options` can include `projection`, `sort`, `limit`, `skip`.
- `collection.findOne(query, options)`: Finds a single document.

```javascript
// Find all users with more than 100 followers
const popularUsers = await users.find({ followersCount: { $gt: 100 } }, { limit: 10 });
console.log(`Found ${popularUsers.length} popular users.`);

// Find one specific user
const specificUser = await users.findOne({ screenName: 'dev123' });
if (specificUser) {
  console.log('Specific user name:', specificUser.name);
}
```

#### Updating Documents
- `collection.updateOne(filter, update, options)`: Updates a single document. `options` can include `upsert: true`.
- `collection.updateMany(filter, update, options)`: Updates multiple documents.

```javascript
// Increment statusesCount for 'dev123'
const updateResult = await users.updateOne(
  { screenName: 'dev123' },
  { $inc: { statusesCount: 1 } }
);
console.log('Matched:', updateResult.matchedCount, 'Modified:', updateResult.modifiedCount);
```

#### Deleting Documents
- `collection.deleteOne(filter)`: Deletes a single document.
- `collection.deleteMany(filter)`: Deletes multiple documents.

```javascript
const deleteResult = await users.deleteOne({ screenName: 'oldUser' });
console.log('Deleted count:', deleteResult.deletedCount);
```

#### Counting Documents
- `collection.countDocuments(query, options)`: Counts documents matching the query.

```javascript
const englishUsersCount = await users.countDocuments({ lang: 'en' });
console.log('Number of English-speaking users:', englishUsersCount);
```

#### Distinct Values
- `collection.distinct(field, filter)`: Gets distinct values for a specified field.

```javascript
const distinctLangs = await users.distinct('lang');
console.log('Distinct languages used:', distinctLangs);
```

### Direct SQL++ Execution
For operations not covered by the MongoDB-like API or for maximum control, execute SQL++ queries directly.

```javascript
const { Connector } = require('asterixdb-js-connector'); // Or from client._connector if using an existing client

async function directQuery() {
  // If you are NOT using the main client's offline features for this specific query:
  const directConnector = new Connector({ astxUrl: 'http://localhost:19002' });
  
  // Or, if you want to use the same connector instance as your main client (respecting its offline settings):
  // const client = connect(...);
  // const directConnector = client._connector; // Access the underlying connector

  try {
    const query = `
      USE TinySocial;
      SELECT u.screenName, u.friendsCount
      FROM ChirpUsers u
      WHERE u.friendsCount > 50
      ORDER BY u.friendsCount DESC
      LIMIT 5;
    `;
    const response = await directConnector.executeQuery(query);
    console.log('SQL++ Query Results:', response.results);
  } catch (error) {
    console.error('SQL++ execution error:', error.message);
  }
  // If using a separately created Connector, it does not have a .close() method.
  // Client connections should be closed via client.close().
}

// directQuery();
```

### QueryBuilder
Construct SQL++ queries programmatically using the fluent `QueryBuilder`.

```javascript
const { QueryBuilder } = require('asterixdb-js-connector'); // Or from client._connector.QueryBuilder if using client

async function builtQuery() {
  const client = connect(); // Assuming you want to use client for execution
  const db = client.db('TinySocial');
  const usersCollection = db.collection('ChirpUsers'); // For context, not directly used by this QB example

  const qb = new QueryBuilder();
  const queryString = await qb
    .use('TinySocial')
    .select(['u.screenName', 'u.name', 'u.followersCount'])
    .from('ChirpUsers u')
    .where('u.followersCount > 100 AND u.lang = "en"')
    .orderBy('u.followersCount DESC')
    .limit(5)
    .build();

  console.log('Built Query:', queryString);

  try {
    // Execute using any connector instance
    const response = await client._connector.executeQuery(queryString);
    console.log('QueryBuilder Results:', response.results);
  } catch (error) {
    console.error('QueryBuilder execution error:', error.message);
  } finally {
    await client.close();
  }
}
// builtQuery();
```

## Offline Capabilities

The connector provides robust offline support, primarily for Node.js environments, through caching and operation queuing.

### Enabling Offline Features
To use offline features, you must enable them when creating the client:

```javascript
const client = connect({
  astxUrl: 'http://your-asterixdb-url:19002',
  localStorage: { // This object enables and configures offline features
    offlineEnabled: true,       // Master switch. Default: false
    cacheTTL: 30 * 60 * 1000,   // Cache Time-To-Live in ms (e.g., 30 minutes). Default: 1 hour
    enableOfflineQueue: true,   // Enable DML operation queuing. Default: false
    debug: false                // Enable verbose debug logging for the offline adapter. Default: false
  }
});
```

### Caching
- When `offlineEnabled: true`, results of read queries (e.g., `find`, `findOne`) are automatically cached.
- If the client is determined to be offline, or if a subsequent identical query is made within the `cacheTTL`, results are served from the cache.
- The cache uses `localforage`. In Node.js, it automatically tries to use `localforage-driver-memory`. Ensure this driver is installed (`npm install localforage-driver-memory`) if it's not bundled with your application.

### Operation Queuing
- When `offlineEnabled: true` and `enableOfflineQueue: true`, Data Modification Language (DML) operations (e.g., `insertOne`, `updateOne`, `deleteOne`) performed while the `SyncManager` detects an offline state are automatically queued.
- These queued operations are stored locally.

### Synchronization
- The `SyncManager` component (used internally by `OfflineEnabledConnector`) monitors network status (in browser environments or simulated in Node.js based on connectivity checks).
- When connectivity is restored, the `SyncManager` attempts to synchronize queued DML operations with the AsterixDB server.
- Events are emitted during the sync process:
  - `online`, `offline`: Network status changes.
  - `syncStart`: Synchronization process begins.
  - `syncProgress`: Progress update during synchronization.
  - `syncComplete`: Synchronization finished successfully.
  - `syncError`: An error occurred during the overall sync process.
  - `syncConflict`: A specific operation in the queue failed to sync (e.g., due to a conflict or server-side error). The operation remains in the queue for potential manual resolution.
  - `operationQueued`: An operation has been added to the offline queue.

```javascript
// Listening to sync events
client.on('syncStart', () => console.log('Sync process started...'));
client.on('syncComplete', ({ operationsSynced }) => console.log(`Sync completed. ${operationsSynced} operations processed.`));
client.on('syncConflict', ({ operationId, error }) => console.warn(`Operation ${operationId} failed to sync: ${error}`));
```
For detailed examples of interacting with offline features, including cache statistics and manual queue inspection/management, see `examples/PowerhouseExample.js`.

## Running Examples
The `examples/` directory contains various scripts demonstrating the connector's features.
1. Ensure your AsterixDB instance is running and accessible (default: `http://localhost:19002`).
2. Navigate to the project root directory.
3. Run examples using Node.js:
   ```bash
   node examples/MongoLikeUsage.js
   node examples/PowerhouseExample.js
   # Run other QueryX.js examples as needed
   node examples/Query3.js 
   ```
   Modify connection URLs within example scripts if your AsterixDB setup differs.

## API Documentation
Detailed API documentation can be generated using JSDoc:
```bash
npm run docs
```
This will create documentation in the `docs/` directory. Open `docs/index.html` in your browser.

## Building for the Browser (Optional)
If you intend to use this library in a browser environment and require a single bundled file, you can use the provided Webpack configuration:
```bash
npm run build
```
This command generates a bundled file (e.g., UMD module) in the `dist/` directory. Note that browser-based offline storage (IndexedDB, WebSQL, localStorage) provided by `localforage` will be used instead of `localforage-driver-memory`. Ensure your application handles permissions and browser compatibility for these storage mechanisms.

## Contributing
Contributions are welcome! Please feel free to submit issues, fork the repository, and create pull requests.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.