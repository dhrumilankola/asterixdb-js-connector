const Connector = require('./Connector');
const OfflineEnabledConnector = require('../offline/OfflineEnabledConnector');
const AsterixDatabase = require('../mongo/AsterixDatabase');

/**
 * AsterixClient provides a MongoDB-like interface for connecting to AsterixDB.
 * 
 * Example usage:
 * ```
 * const client = new AsterixClient('http://localhost:19002');
 * const db = client.db('TinySocial');
 * const users = db.collection('ChirpUsers');
 * 
 * // Find documents
 * const results = await users.find({ screenName: 'NathanGiesen@211' });
 * ```
 */
class AsterixClient {
  /**
   * Creates a new AsterixClient.
   * 
   * @param {string} config - The URL of the AsterixDB HTTP API endpoint or configuration object
   */
  constructor(config = {}) {
    const DEFAULT_URL = 'http://localhost:19002';
    let actualUrl;
    let clientProvidedOptions = {};

    if (typeof config === 'string') {
      actualUrl = config;
    } else if (typeof config === 'object' && config !== null) {
      actualUrl = config.astxUrl || DEFAULT_URL;
      clientProvidedOptions = { ...config };
      delete clientProvidedOptions.astxUrl;
    } else {
      actualUrl = DEFAULT_URL;
    }

    this.url = actualUrl;

    // Start with all defaults
    this.options = {
      autoConnect: true,
      offlineEnabled: false,
      cacheTTL: 3600000, // 1 hour
      debug: false,
      enableOfflineQueue: false,
    };

    if (clientProvidedOptions.autoConnect !== undefined) {
      this.options.autoConnect = clientProvidedOptions.autoConnect;
    }


    if (clientProvidedOptions.localStorage && typeof clientProvidedOptions.localStorage === 'object') {
      const lsOpts = clientProvidedOptions.localStorage;
      if (lsOpts.offlineEnabled !== undefined) this.options.offlineEnabled = lsOpts.offlineEnabled;
      if (lsOpts.cacheTTL !== undefined) this.options.cacheTTL = lsOpts.cacheTTL;
      if (lsOpts.debug !== undefined) this.options.debug = lsOpts.debug;
      if (lsOpts.enableOfflineQueue !== undefined) this.options.enableOfflineQueue = lsOpts.enableOfflineQueue;
    } else {
      if (clientProvidedOptions.offlineEnabled !== undefined) this.options.offlineEnabled = clientProvidedOptions.offlineEnabled;
      if (clientProvidedOptions.cacheTTL !== undefined) this.options.cacheTTL = clientProvidedOptions.cacheTTL;
      if (clientProvidedOptions.debug !== undefined) this.options.debug = clientProvidedOptions.debug;
      if (clientProvidedOptions.enableOfflineQueue !== undefined) this.options.enableOfflineQueue = clientProvidedOptions.enableOfflineQueue;
    }
    
    this._connector = null;
    this._databases = {};

    if (this.options.autoConnect) {
      this.connect();
    }
  }
  
  /**
   * Connects to the AsterixDB server.
   * 
   * @returns {AsterixClient} - The client instance for chaining
   */
  connect() {
    if (this._connector) {
      return this;
    }
    
    if (this.options.offlineEnabled === true) {
      this._connector = new OfflineEnabledConnector({
        astxUrl: this.url,
        cacheTTL: this.options.cacheTTL,
        debug: this.options.debug,
        enableOfflineQueue: this.options.enableOfflineQueue
      });
    } else {
      this._connector = new Connector({ astxUrl: this.url });
    }
    return this;
  }
  
  /**
   * Gets a database instance.
   * 
   * @param {string} name - The name of the database (dataverse in AsterixDB)
   * @returns {AsterixDatabase} - The database instance
   */
  db(name) {
    if (!this._connector) {
      this.connect(); 
      if (!this._connector) {
        throw new Error("Connection failed. Cannot get database reference.");
      }
    }
    
    if (!this._databases[name]) {
      this._databases[name] = new AsterixDatabase(name, this._connector);
    }
    
    return this._databases[name];
  }
  
  /**
   * Checks if the server is reachable.
   * 
   * @returns {Promise<boolean>} - True if the server is reachable
   */
  async isConnected() {
    try {
      if (!this._connector) {
        this.connect();
        if (!this._connector) return false;
      }
      await this._connector.executeQuery('SELECT 1;');
      return true;
    } catch (error) {
      return false;
    }
  }
  
  /**
   * Closes the client connection.
   * 
   * @returns {Promise<void>}
   */
  async close() {
    if (this.options.offlineEnabled && this._connector && this._connector.syncManager) {
      this._connector.syncManager.destroy();
    }
    this._connector = null;
    this._databases = {};
  }
}

module.exports = AsterixClient; 