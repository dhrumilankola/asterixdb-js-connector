// src/core/SyncManager.js
const EventEmitter = require('events');
const LocalStorageAdapter = require('./LocalStorageAdapter');
const Connector = require('../core/Connector');

/**
 * SyncManager handles synchronization between local storage and AsterixDB.
 * It processes queued operations when connectivity is restored and emits events.
 */
class SyncManager extends EventEmitter {
  constructor() {
    super();
    this.localStorage = new LocalStorageAdapter();
    this.connector = new Connector();
    this.isOnline = typeof navigator !== 'undefined' ? navigator.onLine : true;
    this.syncInterval = null;
    this.isSyncing = false;

    // Bind methods
    this.handleOnline = this.handleOnline.bind(this);
    this.handleOffline = this.handleOffline.bind(this);
    this.sync = this.sync.bind(this);

    // Set up connectivity listeners if in browser
    if (typeof window !== 'undefined') {
      this.setupConnectivityListeners();
    }
  }

  /**
   * Sets up listeners for online/offline events.
   */
  setupConnectivityListeners() {
    window.addEventListener('online', this.handleOnline);
    window.addEventListener('offline', this.handleOffline);
    
    // Emit initial state
    if (this.isOnline) {
      setTimeout(() => this.emit('online'), 0);
    } else {
      setTimeout(() => this.emit('offline'), 0);
    }
  }

  /**
   * Handles the browser going online.
   */
  handleOnline() {
    this.isOnline = true;
    this.emit('online');
    this.startSync();
  }

  /**
   * Handles the browser going offline.
   */
  handleOffline() {
    this.isOnline = false;
    this.emit('offline');
    this.stopSync();
  }

  /**
   * Starts periodic sync attempts when online.
   * @param {number} [interval=5000] - Sync check interval in milliseconds.
   */
  startSync(interval = 5000) {
    if (this.syncInterval) {
      this.stopSync(); // Clear any existing interval first
    }
    
    this.syncInterval = setInterval(() => {
      if (this.isOnline && !this.isSyncing) {
        this.sync().catch(error => {
          this.emit('syncError', { error: error.message });
        });
      }
    }, interval);
  }

  /**
   * Stops periodic sync attempts.
   */
  stopSync() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
  }

  /**
   * Manually triggers a sync attempt.
   * @returns {Promise<void>}
   */
  async sync() {
    if (!this.isOnline) {
      this.emit('syncSkipped', { reason: 'offline' });
      return;
    }

    if (this.isSyncing) {
      return;
    }

    this.isSyncing = true;
    this.emit('syncStart');

    try {
      const pendingOperations = await this.localStorage.getPendingOperations();
      
      if (pendingOperations.length === 0) {
        this.emit('syncComplete', { operationsSynced: 0 });
        this.isSyncing = false;
        return;
      }

      let completedCount = 0;
      this.emit('syncProgress', {
        total: pendingOperations.length,
        completed: completedCount,
      });

      for (const { operationId, operation, metadata } of pendingOperations) {
        try {
          if (!operation || !operation.type || !operation.query) {
            throw new Error(`Invalid operation format for ${operationId}`);
          }

          let result;
          if (operation.type === 'INSERT' || operation.type === 'UPDATE' || operation.type === 'DELETE') {
            result = await this.connector.executeQuery(operation.query);
          } else {
            throw new Error(`Unsupported operation type: ${operation.type}`);
          }

          if (result && result.status === 'success') {
            await this.localStorage.removeOperation(operationId);
            completedCount++;
            
            this.emit('syncProgress', {
              total: pendingOperations.length,
              completed: completedCount,
              operationId,
            });
          } else {
            throw new Error(`Server rejected operation ${operationId}: ${JSON.stringify(result)}`);
          }
        } catch (error) {
          this.emit('syncConflict', {
            operationId,
            operation,
            metadata,
            error: error.message,
          });
        }
      }

      this.emit('syncComplete', { operationsSynced: completedCount });
    } catch (error) {
      this.emit('syncError', { error: error.message });
    } finally {
      this.isSyncing = false;
    }
  }

  /**
   * Queues an operation for later sync when offline.
   * @param {string} operationId - Unique ID for the operation.
   * @param {Object} operation - Operation details (type, query, data).
   * @returns {Promise<void>}
   */
  async queueOperation(operationId, operation) {
    await this.localStorage.queueOperation(operationId, operation);
    this.emit('operationQueued', { operationId, operation });
    
    if (this.isOnline) {
      // Attempt immediate sync if online
      try {
        await this.sync();
      } catch (error) {
      }
    }
  }

  /**
   * Cleans up resources when the SyncManager is no longer needed.
   */
  destroy() {
    this.stopSync();
    
    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.handleOnline);
      window.removeEventListener('offline', this.handleOffline);
    }
  }
}

module.exports = SyncManager;