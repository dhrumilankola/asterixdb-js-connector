const Connector = require('./core/Connector');
const QueryBuilder = require('./core/QueryBuilder');
const OfflineEnabledConnector = require('./offline/OfflineEnabledConnector');
const LocalStorageAdapter = require('./offline/LocalStorageAdapter');
const SyncManager = require('./offline/SyncManager');
const AsterixCollection = require('./mongo/AsterixCollection');
const AsterixDatabase = require('./mongo/AsterixDatabase');
const AsterixClient = require('./core/AsterixClient');

// Export all components
module.exports = {
  // Core components
  Connector,
  QueryBuilder,
  OfflineEnabledConnector,
  LocalStorageAdapter,
  SyncManager,
  
  // MongoDB-like interface
  AsterixCollection,
  AsterixDatabase,
  AsterixClient,
  
  // Main client constructor for MongoDB-like usage
  connect: (config) => new AsterixClient(config)
};
