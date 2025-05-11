// src/core/OfflineEnabledConnector.js
const Connector = require('../core/Connector');
const SyncManager = require('./SyncManager');
const crypto = require('crypto');

/**
 * Extension of the AsterixDB Connector that adds offline capabilities.
 * Provides transparent caching of query results and queuing of operations when offline.
 */
class OfflineEnabledConnector {
  constructor(options = {}) {
    this.connector = new Connector();
    this.syncManager = new SyncManager();
    
    // Configure caching behavior
    this.options = {
      cacheEnabled: true,
      cacheTTL: 3600000, // 1 hour in milliseconds
      ...options
    };
    
    // Set up event proxying
    this.setupEventProxying();
  }
  
  /**
   * Proxies events from the SyncManager to consumers of this class.
   */
  setupEventProxying() {
    // List of events to proxy
    const eventsToProxy = [
      'online', 'offline', 'syncStart', 'syncProgress', 
      'syncComplete', 'syncError', 'syncSkipped', 
      'syncConflict', 'operationQueued'
    ];
    
    // Set up proxying for each event
    eventsToProxy.forEach(eventName => {
      this.syncManager.on(eventName, (...args) => {
        // Proxy the event to any listeners registered on this connector
        this.emit(eventName, ...args);
      });
    });
  }
  
  /**
   * Registers an event listener for connector events.
   * @param {string} eventName - Name of the event to listen for.
   * @param {Function} listener - Function to call when event occurs.
   */
  on(eventName, listener) {
    if (!this._eventListeners) {
      this._eventListeners = {};
    }
    
    if (!this._eventListeners[eventName]) {
      this._eventListeners[eventName] = [];
    }
    
    this._eventListeners[eventName].push(listener);
  }
  
  /**
   * Emits an event to registered listeners.
   * @param {string} eventName - Name of the event to emit.
   * @param {...any} args - Arguments to pass to listeners.
   */
  emit(eventName, ...args) {
    if (!this._eventListeners || !this._eventListeners[eventName]) {
      return;
    }
    
    this._eventListeners[eventName].forEach(listener => {
      try {
        listener(...args);
      } catch (error) {
        console.error(`Error in event listener for ${eventName}:`, error);
      }
    });
  }
  
  /**
   * Generates a cache key for a query.
   * @param {string} query - The SQL++ query to execute.
   * @param {Object} params - Query parameters.
   * @returns {string} A unique cache key.
   */
  generateCacheKey(query, params = {}) {
    const input = JSON.stringify({ query, params });
    return crypto.createHash('md5').update(input).digest('hex');
  }

  /**
   * Executes a SQL++ query with offline support.
   * @param {string} query - The SQL++ query to execute.
   * @param {Object} options - Query options.
   * @returns {Promise<Object>} The query result.
   */
  async executeQuery(query, options = {}) {
    const isReadOnly = this.isReadOnlyQuery(query);
    const combinedOptions = { ...this.options, ...options };
    
    // For read-only queries, try to serve from cache if we're offline
    if (isReadOnly && combinedOptions.cacheEnabled) {
      const cacheKey = this.generateCacheKey(query, options);
      
      try {
        // Try to execute online first
        if (this.syncManager.isOnline) {
          const result = await this.connector.executeQuery(query, options);
          
          // Cache the successful result
          await this.syncManager.localStorage.setCache(cacheKey, result, {
            query,
            options,
            timestamp: Date.now(),
            ttl: combinedOptions.cacheTTL
          });
          
          return result;
        } else {
          // We're offline, try to serve from cache
          // console.debug('Offline mode: attempting to serve from cache');
          const cachedData = await this.syncManager.localStorage.getCache(cacheKey);
          
          if (cachedData) {
            const { data, metadata } = cachedData;
            const now = Date.now();
            
            // Check if cache is still valid
            if (metadata.timestamp + (metadata.ttl || combinedOptions.cacheTTL) > now) {
              // console.debug('Serving from cache for query:', query);
              return data;
            } else {
              // console.debug('Cache expired for query:', query);
            }
          }
          
          // No valid cache, throw offline error
          throw new Error('Cannot execute query: offline and no valid cache exists');
        }
      } catch (error) {
        if (this.syncManager.isOnline) {
          // If we're online but the query failed, propagate the error
          throw error;
        } else {
          // We're offline, try one more time with cache
          const cachedData = await this.syncManager.localStorage.getCache(cacheKey);
          
          if (cachedData) {
            // console.debug('Offline fallback: serving expired cache for query:', query);
            return {
              ...cachedData.data,
              _fromCache: true,
              _cacheTimestamp: cachedData.metadata.timestamp
            };
          }
          
          // No cache at all, propagate the offline error
          throw error;
        }
      }
    } 
    // For write operations, queue them when offline
    else if (!isReadOnly) {
      if (this.syncManager.isOnline) {
        // If online, execute directly
        return this.connector.executeQuery(query, options);
      } else {
        // If offline, queue the operation for later
        const operationId = crypto.randomUUID ? 
          crypto.randomUUID() : 
          `op_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
          
        await this.syncManager.queueOperation(operationId, {
          type: this.getOperationType(query),
          query,
          options,
          data: options.data
        });
        
        return {
          status: 'queued',
          message: 'Operation queued for execution when online',
          operationId
        };
      }
    } 
    // Non-cacheable read-only query while offline
    else if (isReadOnly && !combinedOptions.cacheEnabled && !this.syncManager.isOnline) {
      throw new Error('Cannot execute non-cacheable query while offline');
    } 
    // Default fallback - direct execution
    else {
      return this.connector.executeQuery(query, options);
    }
  }
  
  /**
   * Determines if a query is read-only based on its SQL++ content.
   * @param {string} query - The SQL++ query.
   * @returns {boolean} True if the query is read-only.
   */
  isReadOnlyQuery(query) {
    if (!query || typeof query !== 'string') {
      return false;
    }
    
    const normalized = query.trim().toUpperCase();
    
    // Check if the query starts with any write operation keywords
    const writeOperations = [
      'INSERT', 'UPSERT', 'DELETE', 'UPDATE', 'CREATE', 
      'DROP', 'LOAD', 'SET '
    ];
    
    return !writeOperations.some(op => normalized.startsWith(op));
  }
  
  /**
   * Determines the operation type from the query.
   * @param {string} query - The SQL++ query.
   * @returns {string} The operation type (INSERT, UPDATE, DELETE, etc.).
   */
  getOperationType(query) {
    if (!query || typeof query !== 'string') {
      return 'UNKNOWN';
    }
    
    const normalized = query.trim().toUpperCase();
    
    if (normalized.startsWith('INSERT')) return 'INSERT';
    if (normalized.startsWith('UPSERT')) return 'UPSERT';
    if (normalized.startsWith('DELETE')) return 'DELETE';
    if (normalized.startsWith('UPDATE')) return 'UPDATE';
    if (normalized.startsWith('CREATE')) return 'CREATE';
    if (normalized.startsWith('DROP')) return 'DROP';
    if (normalized.startsWith('LOAD')) return 'LOAD';
    
    return 'UNKNOWN';
  }
  
  /**
   * Clears the cache for a specific query or all cached queries.
   * @param {string} [query] - Optional query to clear cache for.
   * @param {Object} [options] - Optional query parameters.
   * @returns {Promise<void>}
   */
  async clearQueryCache(query = null, options = null) {
    if (query) {
      const cacheKey = this.generateCacheKey(query, options);
      await this.syncManager.localStorage.removeCache(cacheKey);
    } else {
      await this.syncManager.localStorage.clearCache();
    }
  }
  
  /**
   * Forces an immediate synchronization attempt for queued operations.
   * @returns {Promise<void>}
   */
  async forceSynchronization() {
    return this.syncManager.sync();
  }
  
  /**
   * Gets the current online/offline status.
   * @returns {boolean} True if online, false if offline.
   */
  isOnline() {
    return this.syncManager.isOnline;
  }
  
  /**
   * Gets the count of pending operations that need to be synced.
   * @returns {Promise<number>} The number of pending operations.
   */
  async getPendingOperationCount() {
    const operations = await this.syncManager.localStorage.getPendingOperations();
    return operations.length;
  }
  
  /**
   * Cleans up resources when the connector is no longer needed.
   */
  destroy() {
    this.syncManager.destroy();
    this._eventListeners = {};
  }
}

module.exports = OfflineEnabledConnector;