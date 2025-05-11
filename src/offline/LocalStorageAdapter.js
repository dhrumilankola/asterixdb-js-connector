// src/core/LocalStorageAdapter.js
const localforage = require('localforage');
const crypto = require('crypto');

let localforageDriverPromise = null;
let nodeJsMemoryDriverName = null; // To store the registered driver name

if (typeof window === 'undefined') { 
  try {
    const nodeJsMemoryDriverModule = require('localforage-driver-memory');
    if (nodeJsMemoryDriverModule) {
      localforageDriverPromise = localforage.defineDriver(nodeJsMemoryDriverModule)
        .then(() => {
          nodeJsMemoryDriverName = nodeJsMemoryDriverModule._driver; 
          if (!nodeJsMemoryDriverName) {
            const errMsg = 'localforage-driver-memory did not provide a _driver name.';
            throw new Error(errMsg);
          }
        })
        .catch(err => {
          localforageDriverPromise = Promise.reject(err); 
        });
    } else {
      localforageDriverPromise = Promise.resolve(); 
    }
  } catch (e) {
    localforageDriverPromise = Promise.resolve(); 
  }
} else {
  localforageDriverPromise = Promise.resolve();
}

/**
 * Handles local storage operations for offline mode using localforage.
 * Provides caching and operation queueing capabilities.
 */
class LocalStorageAdapter {
  /**
   * Creates a new LocalStorageAdapter.
   * 
   * @param {Object} options - Configuration options
   * @param {number} options.cacheTTL - Cache time-to-live in milliseconds (default: 3600000 = 1 hour)
   * @param {number} options.maxRetries - Maximum retries for failed operations (default: 3)
   * @param {boolean} options.debug - Enable debug logging (default: false)
   */
  constructor(options = {}) {
    this.options = {
      cacheTTL: 3600000, // 1 hour
      maxRetries: 3,
      debug: false,
      ...options
    };
    // Stores will be initialized after driver setup is attempted.
    // Public methods should await this._readyPromise.
    this._readyPromise = localforageDriverPromise.finally(() => {
        this._initializeStores();
        if (typeof window !== 'undefined') { 
            this._initCacheCleanup();
        }
    });
  }
  
  _initializeStores() {
    let driverConfig = undefined;
    if (typeof window === 'undefined') {
      if (nodeJsMemoryDriverName) {
        this._log('debug', `Node.js: Using explicitly defined driver '${nodeJsMemoryDriverName}' for localforage instances.`);
        driverConfig = nodeJsMemoryDriverName; // Pass the registered name
      } else {
        this._log('warn', 'Node.js: Memory driver name not available. localforage will use defaults (likely fail).');
        // driverConfig remains undefined, localforage will try its sequence.
      }
    } else {
      this._log('debug', 'Browser: Letting localforage pick default drivers (IndexedDB, WebSQL, localStorage).');
      // Let localforage decide in browser, or specify: [localforage.INDEXEDDB, localforage.WEBSQL, localforage.LOCALSTORAGE]
    }

    this.store = localforage.createInstance({
      name: 'AsterixDBDriverCache',
      storeName: 'offlineCache',
      description: 'Local storage for offline AsterixDB query cache',
      driver: driverConfig
    });
    this.queueStore = localforage.createInstance({
      name: 'AsterixDBDriverQueue',
      storeName: 'pendingQueue',
      description: 'Queue for offline operations',
      driver: driverConfig
    });
    this.metaStore = localforage.createInstance({
      name: 'AsterixDBDriverMeta',
      storeName: 'metadata',
      description: 'Metadata for AsterixDB connector',
      driver: driverConfig
    });
  }

  /**
   * Generates a hash key for a query.
   * 
   * @param {string} query - The SQL++ query or MongoDB-style query object as JSON string
   * @param {string} [collection] - Optional collection/dataset name for context
   * @returns {string} - Hash key for the query
   */
  generateQueryKey(query, collection = '') {
    if (!query) {
      throw new Error('Query is required to generate a key');
    }
    
    const queryString = typeof query === 'string' ? query : JSON.stringify(query);
    const data = `${collection}:${queryString}`;
    
    // Use simple hash for query key
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * Stores data in the local cache with a key and optional metadata.
   * 
   * @param {string} key - Unique key for the data (e.g., query hash or dataset ID).
   * @param {any} data - Data to store (e.g., query results or document).
   * @param {Object} [metadata] - Optional metadata (e.g., timestamp, version).
   * @returns {Promise<void>}
   */
  async setCache(key, data, metadata = {}) {
    await this._readyPromise;
    if (!this.store) {
        this._log('error', 'Store not initialized in setCache. LocalStorageAdapter not ready.');
        throw new Error('LocalStorageAdapter not ready: store is not initialized.');
    }
    if (!key || typeof key !== 'string') {
      throw new Error('Invalid cache key: must be a non-empty string');
    }
    const entry = {
      data,
      metadata: {
        timestamp: Date.now(),
        expiresAt: Date.now() + this.options.cacheTTL,
        ...metadata,
      },
    };
    try {
      await this.store.setItem(key, entry);
      await this._addToCacheRegistry(key, entry.metadata.expiresAt);
      this._log('debug', `Cached data for key: ${key}`);
    } catch (error) {
      this._log('error', `Cache error for key ${key}:`, error);
      throw new Error(`Failed to cache data for key ${key}: ${error.message}`);
    }
  }

  /**
   * Retrieves cached data by key.
   * 
   * @param {string} key - The key to look up.
   * @returns {Promise<{ data: any, metadata: Object } | null>}
   */
  async getCache(key) {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready (store)");
    if (!key || typeof key !== 'string') {
      throw new Error('Invalid cache key: must be a non-empty string');
    }

    try {
      const entry = await this.store.getItem(key);
      if (!entry) {
        this._log('debug', `No cached data found for key: ${key}`);
        return null;
      }
      
      // Check if cache entry has expired
      if (entry.metadata.expiresAt && entry.metadata.expiresAt < Date.now()) {
        this._log('debug', `Cache entry expired for key: ${key}`);
        await this.removeCache(key);
        return null;
      }
      
      // Update access time for LRU strategy
      entry.metadata.lastAccessed = Date.now();
      await this.store.setItem(key, entry);
      
      return entry;
    } catch (error) {
      this._log('error', `Cache retrieval error for key ${key}:`, error);
      throw new Error(`Failed to retrieve cache for key ${key}: ${error.message}`);
    }
  }

  /**
   * Removes cached data by key.
   * 
   * @param {string} key - The key to remove.
   * @returns {Promise<void>}
   */
  async removeCache(key) {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready (store)");
    if (!key || typeof key !== 'string') {
      throw new Error('Invalid cache key: must be a non-empty string');
    }

    try {
      await this.store.removeItem(key);
      await this._removeFromCacheRegistry(key);
      this._log('debug', `Removed cache entry for key: ${key}`);
    } catch (error) {
      this._log('error', `Cache removal error for key ${key}:`, error);
      throw new Error(`Failed to remove cache for key ${key}: ${error.message}`);
    }
  }
  
  /**
   * Updates the TTL for a cached item.
   * 
   * @param {string} key - The key to update.
   * @param {number} newTTL - New TTL in milliseconds.
   * @returns {Promise<void>}
   */
  async updateCacheTTL(key, newTTL) {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready (store)");
    try {
      const entry = await this.store.getItem(key);
      if (!entry) {
        throw new Error(`Cache entry ${key} not found`);
      }
      
      const newExpiresAt = Date.now() + newTTL;
      entry.metadata.expiresAt = newExpiresAt;
      
      await this.store.setItem(key, entry);
      await this._updateCacheRegistryExpiry(key, newExpiresAt);
      
      this._log('debug', `Updated TTL for cache key: ${key}`);
    } catch (error) {
      this._log('error', `Error updating TTL for ${key}:`, error);
      throw new Error(`Failed to update TTL: ${error.message}`);
    }
  }

  /**
   * Queues an operation (e.g., INSERT, UPDATE) for later sync.
   * 
   * @param {string} operationId - Unique ID for the operation.
   * @param {Object} operation - Operation details (e.g., query, data, type).
   * @returns {Promise<void>}
   */
  async queueOperation(operationId, operation) {
    await this._readyPromise;
    if (!this.queueStore) throw new Error("Adapter not ready (queueStore)");
    if (!operationId || typeof operationId !== 'string') {
      throw new Error('Invalid operation ID: must be a non-empty string');
    }

    if (!operation || typeof operation !== 'object') {
      throw new Error('Invalid operation: must be an object');
    }

    if (!operation.type || !operation.query) {
      throw new Error('Invalid operation: must have type and query properties');
    }

    const entry = {
      operation,
      metadata: {
        timestamp: Date.now(),
        status: 'pending',
        retryCount: 0,
        priority: operation.priority || 1 // Default priority
      },
    };

    try {
      await this.queueStore.setItem(operationId, entry);
      this._log('debug', `Queued operation: ${operationId}`);
    } catch (error) {
      this._log('error', `Operation queue error for ID ${operationId}:`, error);
      throw new Error(`Failed to queue operation ${operationId}: ${error.message}`);
    }
  }

  /**
   * Retrieves all pending operations from the queue.
   * 
   * @param {Object} [options] - Filter options
   * @param {string} [options.status] - Filter by status (e.g., 'pending', 'failed')
   * @param {boolean} [options.sortByPriority] - Sort by priority (default: true)
   * @returns {Promise<Array<{ operationId: string, operation: Object, metadata: Object }>>}
   */
  async getPendingOperations(options = {}) {
    await this._readyPromise;
    if (!this.queueStore) throw new Error("Adapter not ready");
    const { status, sortByPriority = true } = options;
    const operations = [];
    
    try {
      await this.queueStore.iterate((value, key) => {
        // Filter by status if specified
        if (status && value.metadata.status !== status) {
          return;
        }
        
        operations.push({ operationId: key, ...value });
      });
      
      // Sort operations by priority and timestamp
      if (sortByPriority) {
        operations.sort((a, b) => {
          // Higher priority first
          if (a.metadata.priority !== b.metadata.priority) {
            return b.metadata.priority - a.metadata.priority;
          }
          // Then oldest first
          return a.metadata.timestamp - b.metadata.timestamp;
        });
      }
      
      return operations;
    } catch (error) {
      this._log('error', 'Error retrieving pending operations:', error);
      throw new Error(`Failed to retrieve pending operations: ${error.message}`);
    }
  }

  /**
   * Updates the metadata for an operation (e.g., to mark retry attempts).
   * 
   * @param {string} operationId - The ID of the operation to update.
   * @param {Object} newMetadata - The new metadata to merge with existing.
   * @returns {Promise<void>}
   */
  async updateOperationMetadata(operationId, newMetadata) {
    await this._readyPromise;
    if (!this.queueStore) throw new Error("Adapter not ready");
    try {
      const entry = await this.queueStore.getItem(operationId);
      if (!entry) {
        throw new Error(`Operation ${operationId} not found in queue`);
      }

      entry.metadata = { ...entry.metadata, ...newMetadata };
      await this.queueStore.setItem(operationId, entry);
      this._log('debug', `Updated metadata for operation: ${operationId}`);
    } catch (error) {
      this._log('error', `Error updating operation metadata for ${operationId}:`, error);
      throw new Error(`Failed to update operation metadata: ${error.message}`);
    }
  }

  /**
   * Removes an operation from the queue after successful sync.
   * 
   * @param {string} operationId - The ID of the operation to remove.
   * @returns {Promise<void>}
   */
  async removeOperation(operationId) {
    await this._readyPromise;
    if (!this.queueStore) throw new Error("Adapter not ready");
    if (!operationId || typeof operationId !== 'string') {
      throw new Error('Invalid operation ID: must be a non-empty string');
    }

    try {
      await this.queueStore.removeItem(operationId);
      this._log('debug', `Removed operation from queue: ${operationId}`);
    } catch (error) {
      this._log('error', `Operation removal error for ID ${operationId}:`, error);
      throw new Error(`Failed to remove operation ${operationId}: ${error.message}`);
    }
  }

  /**
   * Clears the entire cache (for debugging or reset).
   * 
   * @returns {Promise<void>}
   */
  async clearCache() {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready");
    try {
      await this.store.clear();
      await this.metaStore.removeItem('cacheRegistry');
      this._log('debug', 'Cleared local cache');
    } catch (error) {
      this._log('error', 'Error clearing cache:', error);
      throw new Error(`Failed to clear cache: ${error.message}`);
    }
  }

  /**
   * Clears the entire operation queue (for debugging or reset).
   * 
   * @returns {Promise<void>}
   */
  async clearQueue() {
    await this._readyPromise;
    if (!this.queueStore) throw new Error("Adapter not ready");
    try {
      await this.queueStore.clear();
      this._log('debug', 'Cleared operation queue');
    } catch (error) {
      this._log('error', 'Error clearing operation queue:', error);
      throw new Error(`Failed to clear queue: ${error.message}`);
    }
  }
  
  /**
   * Removes expired cache entries.
   * 
   * @returns {Promise<number>} - Number of removed entries
   */
  async cleanupExpiredCache() {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready");
    try {
      const registry = await this._getCacheRegistry();
      const now = Date.now();
      let removedCount = 0;
      
      for (const [key, expiresAt] of Object.entries(registry)) {
        if (expiresAt < now) {
          await this.store.removeItem(key);
          delete registry[key];
          removedCount++;
        }
      }
      
      if (removedCount > 0) {
        await this.metaStore.setItem('cacheRegistry', registry);
        this._log('debug', `Removed ${removedCount} expired cache entries`);
      }
      
      return removedCount;
    } catch (error) {
      this._log('error', 'Error cleaning up expired cache:', error);
      return 0;
    }
  }
  
  /**
   * Gets cache statistics.
   * 
   * @returns {Promise<Object>} - Cache statistics
   */
  async getCacheStats() {
    await this._readyPromise;
    if (!this.store) throw new Error("Adapter not ready");
    try {
      const registry = await this._getCacheRegistry();
      const cacheCount = Object.keys(registry).length;
      
      let sizeEstimate = 0;
      let expiredCount = 0;
      const now = Date.now();
      
      for (const [key, expiresAt] of Object.entries(registry)) {
        if (expiresAt < now) {
          expiredCount++;
        }
        
        try {
          const entry = await this.store.getItem(key);
          if (entry) {
            // Estimate size in bytes
            sizeEstimate += JSON.stringify(entry).length * 2; // Rough estimate
          }
        } catch (e) {
          // Ignore errors in size estimation
        }
      }
      
      const queueOperations = await this.getPendingOperations();
      
      return {
        cacheEntries: cacheCount,
        expiredEntries: expiredCount,
        estimatedSize: sizeEstimate,
        queuedOperations: queueOperations.length
      };
    } catch (error) {
      this._log('error', 'Error getting cache stats:', error);
      throw new Error(`Failed to get cache stats: ${error.message}`);
    }
  }
  
  // Private methods
  
  /**
   * Initializes the cache cleanup interval.
   * 
   * @private
   */
  _initCacheCleanup() {
    // Run cleanup every hour
    const cleanupInterval = setInterval(() => {
      this.cleanupExpiredCache().catch(err => {
        this._log('error', 'Error in cache cleanup:', err);
      });
    }, 3600000); // 1 hour
    
    // Clean up interval on node process exit
    if (typeof process !== 'undefined') {
      process.on('exit', () => {
        clearInterval(cleanupInterval);
      });
    }
  }
  
  /**
   * Gets the cache registry, which tracks all cache keys and their expiration.
   * 
   * @private
   * @returns {Promise<Object>} - Cache registry
   */
  async _getCacheRegistry() {
    await this._readyPromise;
    if (!this.metaStore) throw new Error("Adapter not ready (metaStore)");
    const registry = await this.metaStore.getItem('cacheRegistry');
    return registry || {};
  }
  
  /**
   * Adds a key to the cache registry.
   * 
   * @private
   * @param {string} key - Cache key
   * @param {number} expiresAt - Expiration timestamp
   * @returns {Promise<void>}
   */
  async _addToCacheRegistry(key, expiresAt) {
    await this._readyPromise;
    if (!this.metaStore) throw new Error("Adapter not ready (metaStore)");
    const registry = await this._getCacheRegistry();
    registry[key] = expiresAt;
    await this.metaStore.setItem('cacheRegistry', registry);
  }
  
  /**
   * Removes a key from the cache registry.
   * 
   * @private
   * @param {string} key - Cache key
   * @returns {Promise<void>}
   */
  async _removeFromCacheRegistry(key) {
    await this._readyPromise;
    if (!this.metaStore) throw new Error("Adapter not ready (metaStore)");
    const registry = await this._getCacheRegistry();
    if (registry[key]) {
      delete registry[key];
      await this.metaStore.setItem('cacheRegistry', registry);
    }
  }
  
  /**
   * Updates a key's expiration in the cache registry.
   * 
   * @private
   * @param {string} key - Cache key
   * @param {number} expiresAt - New expiration timestamp
   * @returns {Promise<void>}
   */
  async _updateCacheRegistryExpiry(key, expiresAt) {
    await this._readyPromise;
    if (!this.metaStore) throw new Error("Adapter not ready (metaStore)");
    const registry = await this._getCacheRegistry();
    if (registry[key]) {
      registry[key] = expiresAt;
      await this.metaStore.setItem('cacheRegistry', registry);
    }
  }
  
  /**
   * Internal logging method.
   * 
   * @private
   * @param {string} level - Log level
   * @param {string} message - Log message
   * @param {*} [data] - Additional data
   */
  _log(level, message, data) {
    if (level === 'error' || level === 'warn' || this.options.debug) {
      const logMessage = `[LocalStorageAdapter] ${level.toUpperCase()}: ${message}`;
      if (data) {
        if (data instanceof Error) {
          console[level] ? console[level](logMessage, data.message, data.stack) : console.log(logMessage, data.message, data.stack);
        } else {
          console[level] ? console[level](logMessage, data) : console.log(logMessage, data);
        }
      } else {
        console[level] ? console[level](logMessage) : console.log(logMessage);
      }
    }
  }
}

module.exports = LocalStorageAdapter;