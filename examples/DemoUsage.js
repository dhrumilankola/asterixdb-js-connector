// PowerhouseExample.js
// This script demonstrates the comprehensive capabilities of the AsterixDB JavaScript Connector,
// including DDL operations, Mongo-like DML, and LocalStorage-based caching/offline features.

const { connect, Connector } = require('../src'); // Adjust path based on your project structure

// --- Configuration ---
const ASTERIXDB_URL = 'http://localhost:19002'; // Your AsterixDB instance URL
const DATAVERSE_NAME = 'TinySocial'; // Use the existing TinySocial dataverse
const LOCAL_STORAGE_TTL = 5 * 60 * 1000; // 5 minutes for cache TTL

// Helper function to log sections
function logSection(title) {
  console.log(`\n--- ${title.toUpperCase()} ---`);
}

// Helper function to generate random data
const getRandomId = () => Math.random().toString(36).substring(2, 10);

async function powerhouseDemo() {
  let client;
  // const rawConnector = new Connector({ astxUrl: ASTERIXDB_URL }); // For DDL - DDL is commented out

  try {
    // --- 1. INITIAL SETUP & DDL OPERATIONS (Commented out due to read-only mode issues) ---
    // logSection(`Setting up Dataverse: ${DATAVERSE_NAME}`);
    // console.log(`Dataverse ${DATAVERSE_NAME} creation/use steps are commented out.`);


    // logSection('Creating Types and Datasets (Commented out)');
    // console.log('Type and Dataset creation steps are commented out.');

    // logSection('Creating Indexes (Commented out)');
    // console.log('Index creation steps are commented out.');

    logSection(`Using existing Dataverse: ${DATAVERSE_NAME}`);

    // --- 2. CONNECT WITH LOCAL STORAGE ENABLED ---
    logSection('Connecting client with LocalStorage features');
    client = connect({
      astxUrl: ASTERIXDB_URL,
      localStorage: { 
        offlineEnabled: true,
        cacheTTL: LOCAL_STORAGE_TTL,
        // debug: true, // Keep debug off for cleaner example output unless specifically needed
        enableOfflineQueue: true 
      }
    });
    // console.log('Client connected with LocalStorage configured.');
    const db = client.db(DATAVERSE_NAME);
    const chirpUsers = db.collection('ChirpUsers');
    // console.log('[PowerhouseExample] chirpUsers object type:', typeof chirpUsers);
    // console.log('[PowerhouseExample] chirpUsers instance direct properties:', chirpUsers ? Object.getOwnPropertyNames(chirpUsers) : 'chirpUsers is null/undefined');
    // console.log('[PowerhouseExample] chirpUsers.insertOne is func (from PowerhouseExample scope)?', typeof chirpUsers.insertOne === 'function');
    // console.log('[PowerhouseExample] chirpUsers.find is func (from PowerhouseExample scope)?', typeof chirpUsers.find === 'function');

    if (chirpUsers && typeof chirpUsers.insertOne !== 'function') {
      console.error('[PowerhouseExample] FATAL: chirpUsers.insertOne is not a function. Aborting example.');
      return; // Exit early if critical method is missing
    }
    // const gleambookUsers = db.collection('GleambookUsers'); // Commented out as DDL is skipped

    // --- 3. DML OPERATIONS WITH MONGO-LIKE API (on ChirpUsers) ---
    logSection('Inserting ChirpUsers (Online)');
    const chirpUser1 = {
      screenName: `powerhouse-chirp-${getRandomId()}`,
      lang: 'en',
      friendsCount: Math.floor(Math.random() * 200),
      statusesCount: Math.floor(Math.random() * 500),
      name: 'Powerhouse User One',
      followersCount: Math.floor(Math.random() * 10000)
    };
    // console.log('Attempting to insert ChirpUser1:', chirpUser1);
    try {
      await chirpUsers.insertOne(chirpUser1);
      console.log('Successfully inserted ChirpUser1:', chirpUser1.screenName);
    } catch (insertError) {
      console.error('[PowerhouseExample] ERROR during chirpUsers.insertOne(chirpUser1):', insertError.message);
      // console.error('[PowerhouseExample] InsertError name:', insertError.name);
      // console.error('[PowerhouseExample] InsertError stack:', insertError.stack);
      // if (insertError.cause) console.error('[PowerhouseExample] InsertError cause:', insertError.cause);
      throw insertError; 
    }

    const chirpUser2 = {
      screenName: `powerhouse-chirp-${getRandomId()}`,
      lang: 'es',
      friendsCount: Math.floor(Math.random() * 200),
      statusesCount: Math.floor(Math.random() * 500),
      name: 'Usuario Powerhouse Dos',
      followersCount: Math.floor(Math.random() * 10000)
    };
    // console.log('Attempting to insert ChirpUser2:', chirpUser2);
    try {
      await chirpUsers.insertOne(chirpUser2);
      console.log('Successfully inserted ChirpUser2:', chirpUser2.screenName);
    } catch (insertError2) {
      console.error('[PowerhouseExample] ERROR during chirpUsers.insertOne(chirpUser2):', insertError2.message);
      // console.error('[PowerhouseExample] InsertError2 name:', insertError2.name);
      // console.error('[PowerhouseExample] InsertError2 stack:', insertError2.stack);
      // if (insertError2.cause) console.error('[PowerhouseExample] InsertError2 cause:', insertError2.cause);
      throw insertError2;
    }

    logSection('Finding a ChirpUser');
    let foundUser = await chirpUsers.findOne({ screenName: chirpUser1.screenName });
    console.log('Found ChirpUser1 (after insert):', foundUser ? foundUser.screenName : 'Not found');
    
    logSection('Finding multiple ChirpUsers with filter');
    const englishChirpUsers = await chirpUsers.find({ lang: 'en', name: {$regex: '^Powerhouse User'} });
    console.log(`English Powerhouse ChirpUsers found: ${englishChirpUsers.length}`);

    logSection('Counting ChirpUsers');
    const count = await chirpUsers.countDocuments({ name: {$regex: '^Powerhouse User'} });
    console.log(`Total Powerhouse ChirpUsers found by name: ${count}`);

    // --- 4. LOCAL STORAGE - OFFLINE QUEUEING SIMULATION (Re-enabled) ---
    logSection('Simulating Offline Insert for a new ChirpUser');
    
    let localStorageFeature = null;
    if (client._connector && client._connector.syncManager && client._connector.syncManager.localStorage) {
      localStorageFeature = client._connector.syncManager.localStorage;
      // console.log('[PowerhouseExample] Found localStorageAdapter.');
    } else {
      console.log('[PowerhouseExample] Could not find localStorageAdapter.');
    }
        
    if (localStorageFeature && typeof localStorageFeature.queueOperation === 'function') { 
      // console.log('Client has localStorage features with queueOperation method available.');
      const offlineChirpUser = {
        screenName: `offline-chirp-${getRandomId()}`,
        lang: 'fr',
        friendsCount: 10,
        statusesCount: 1,
        name: 'Offline Chirp Powerhouse',
        followersCount: 5,
        _id: `offline-${getRandomId()}` 
      };
      const operationId = `chirp-insert-${offlineChirpUser.screenName}`;
      
      const conceptualQuery = `INSERT INTO ${DATAVERSE_NAME}.${chirpUsers.name} VALUE ${JSON.stringify(offlineChirpUser)};`;

      const operation = {
        type: 'insertOne', 
        query: conceptualQuery, 
        collectionName: chirpUsers.name, 
        dataverseName: DATAVERSE_NAME, 
        document: offlineChirpUser,    
        priority: 2
      };
      
      // console.log('Simulating direct queueing of operation:');
      // console.log('Queued operation object:', operation); 
      await localStorageFeature.queueOperation(operationId, operation);
      console.log(`Manually queued ChirpUser insert operation: ${operationId} for ${offlineChirpUser.screenName}`);
      
      const pendingOps = await localStorageFeature.getPendingOperations();
      console.log('Pending operations in queue after manual add:', pendingOps.length);

      // console.log('NOTE: True offline queue processing by SyncManager depends on it detecting network status changes and being active.');
      // console.log('This part of the demo shows direct interaction with the queueing mechanism of the storage adapter.');
      
      if (localStorageFeature.syncManager && typeof localStorageFeature.syncManager.sync === 'function') {
        // console.log('Attempting to manually trigger sync process...');
        // await localStorageFeature.syncManager.sync(); // This might be too aggressive for a simple demo log
        // console.log('Manual sync process triggered (if it ran, check logs or database).');
        // const pendingOpsAfterSync = await localStorageFeature.getPendingOperations();
        // console.log('Pending operations after manual sync trigger:', pendingOpsAfterSync.length);
      } else {
        // console.log('Manual sync trigger not available or SyncManager not directly accessible.');
      }

    } else {
      console.log('Offline queueing simulation skipped: localStorage features or queueOperation not available as expected.');
    }

    // --- 5. CLEANUP (Optional: Deleting a user) ---
    logSection('Deleting ChirpUser2');
    if (chirpUser2 && chirpUser2.screenName) {
      const deleteResult = await chirpUsers.deleteOne({ screenName: chirpUser2.screenName });
      console.log(`Attempted to delete ChirpUser2 (${chirpUser2.screenName}): ${deleteResult.deletedCount > 0 ? 'Success' : 'Failed/Not Found'}`);
      // const deletedVerify = await chirpUsers.findOne({ screenName: chirpUser2.screenName });
      // console.log(`Verification for ChirpUser2 (should be null if deleted): ${deletedVerify ? JSON.stringify(deletedVerify) : 'Not Found'}`);
    }

    logSection('Deleting ChirpUser1');
    if (chirpUser1 && chirpUser1.screenName) {
      const deleteResult1 = await chirpUsers.deleteOne({ screenName: chirpUser1.screenName });
      console.log(`Attempted to delete ChirpUser1 (${chirpUser1.screenName}): ${deleteResult1.deletedCount > 0 ? 'Success' : 'Failed/Not Found'}`);
    }

    // --- 6. CACHE STATS --- (Re-enabled)
    logSection('Cache Statistics');
    if (localStorageFeature && typeof localStorageFeature.getCacheStats === 'function') {
        const stats = await localStorageFeature.getCacheStats();
        console.log('Current Cache Stats:', stats);
    } else {
        console.log('getCacheStats not available on localStorageFeature.');
    }

  } catch (error) {
    console.error('POWERHOUSE DEMO FAILED WITH ERROR:');
    console.error('Error Name:', error.name);
    console.error('Error Message:', error.message);
    console.error('Error Stack:', error.stack);
    if (error.response && error.response.data) { 
        console.error('Axios Error Response Data:', JSON.stringify(error.response.data, null, 2));
    }
    if (error.cause) { // If Node.js version supports error.cause
        console.error('Error Cause:', error.cause);
    }
  } finally {
    if (client) {
      logSection('Closing client connection');
      await client.close();
      console.log('Client connection closed.');
    }
    // rawConnector does not have a close method in this example, it's stateless for executeQuery
    logSection('Demo Finished');
  }
}

powerhouseDemo(); 