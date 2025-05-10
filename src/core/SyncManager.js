// src/core/SyncManager.js
const EventEmitter = require('events');
const LocalStorageAdapter = require('./LocalStorageAdapter');
const Connector = require('./Connector');

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
    console.debug('Connectivity restored');
    this.isOnline = true;
    this.emit('online');
    this.startSync();
  }

  /**
   * Handles the browser going offline.
   */
  handleOffline() {
    console.debug('Lost connectivity');
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
          console.error('Error during scheduled sync:', error);
          this.emit('syncError', { error: error.message });
        });
      }
    }, interval);
    
    console.debug(`Started sync interval every ${interval}ms`);
  }

  /**
   * Stops periodic sync attempts.
   */
  stopSync() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
      console.debug('Stopped sync interval');
    }
  }

  /**
   * Manually triggers a sync attempt.
   * @returns {Promise<void>}
   */
  async sync() {
    if (!this.isOnline) {
      console.debug('Cannot sync: offline');
      this.emit('syncSkipped', { reason: 'offline' });
      return;
    }

    if (this.isSyncing) {
      console.debug('Sync already in progress');
      return;
    }

    this.isSyncing = true;
    this.emit('syncStart');

    try {
      const pendingOperations = await this.localStorage.getPendingOperations();
      
      if (pendingOperations.length === 0) {
        console.debug('No pending operations to sync');
        this.emit('syncComplete', { operationsSynced: 0 });
        this.isSyncing = false;
        return;
      }

      console.debug(`Found ${pendingOperations.length} pending operations`);
      
      // Initialize progress tracking
      let completedCount = 0;
      this.emit('syncProgress', {
        total: pendingOperations.length,
        completed: completedCount,
      });

      // Process each operation
      for (const { operationId, operation, metadata } of pendingOperations) {
        try {
          console.debug(`Processing operation: ${operationId}`);
          
          // Validate operation
          if (!operation || !operation.type || !operation.query) {
            throw new Error(`Invalid operation format for ${operationId}`);
          }

          // Execute the operation against AsterixDB
          let result;
          if (operation.type === 'INSERT' || operation.type === 'UPDATE' || operation.type === 'DELETE') {
            result = await this.connector.executeQuery(operation.query);
          } else {
            throw new Error(`Unsupported operation type: ${operation.type}`);
          }

          // Check result and handle success/failure
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
          console.error(`Failed to sync operation ${operationId}: ${error.message}`);
          
          this.emit('syncConflict', {
            operationId,
            operation,
            metadata,
            error: error.message,
          });
          
          // Leave operation in queue for manual resolution
        }
      }

      console.debug('Sync completed successfully');
      this.emit('syncComplete', { operationsSynced: completedCount });
    } catch (error) {
      console.error(`Sync failed: ${error.message}`);
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
        console.error(`Error during immediate sync after queuing: ${error.message}`);
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
    
    console.debug('SyncManager destroyed');
  }
}

module.exports = SyncManager;