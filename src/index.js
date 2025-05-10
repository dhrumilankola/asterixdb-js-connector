const Connector = require('./core/Connector');
const QueryBuilder = require('./core/QueryBuilder');
const OfflineEnabledConnector = require('./core/OfflineEnabledConnector');
const LocalStorageAdapter = require('./core/LocalStorageAdapter');
const SyncManager = require('./core/SyncManager');
const AsterixCollection = require('./core/AsterixCollection');
const AsterixDatabase = require('./core/AsterixDatabase');
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
