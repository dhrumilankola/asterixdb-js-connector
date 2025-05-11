const AsterixCollection = require('./AsterixCollection');

/**
 * AsterixDatabase provides a MongoDB-like interface for interacting with an AsterixDB dataverse.
 */
class AsterixDatabase {
  /**
   * Creates a new AsterixDatabase instance.
   * 
   * @param {string} name - The name of the database (dataverse in AsterixDB)
   * @param {Connector|OfflineEnabledConnector} connector - The connector instance
   */
  constructor(name, connector) {
    this.name = name;
    this._connector = connector;
    this._collections = {};
  }
  
  /**
   * Gets a collection from this database.
   * 
   * @param {string} name - The name of the collection (dataset in AsterixDB)
   * @returns {AsterixCollection} - The collection instance
   */
  collection(name) {
    if (!this._collections[name]) {
      this._collections[name] = new AsterixCollection(name, this, this._connector);
    }
    return this._collections[name];
  }
  
  /**
   * Lists all collections (datasets) in this database (dataverse).
   * 
   * @returns {Promise<Array<string>>} - Array of collection names
   */
  async listCollections() {
    try {
      // In AsterixDB, to list datasets in a dataverse, we query Metadata.Dataset
      const query = `
        SELECT d.DatasetName as name 
        FROM Metadata.Dataset d 
        WHERE d.DataverseName = '${this.name}'
      `;
      
      const result = await this._connector.executeQuery(query);
      
      if (result && result.results) {
        return result.results.map(dataset => dataset.name);
      }
      
      return [];
    } catch (error) {
      throw new Error(`Failed to list collections in database '${this.name}': ${error.message}`);
    }
  }
  
  /**
   * Creates a new collection in the database.
   * 
   * @param {string} name - The name of the collection to create
   * @param {Object} options - Collection creation options
   * @param {Object} options.schema - The schema definition for the collection
   * @returns {Promise<AsterixCollection>} - The newly created collection
   */
  async createCollection(name, options = {}) {
    try {
      // Construct the CREATE statement based on the provided schema
      let createStatement = `CREATE DATASET ${this.name}.${name}`;
      
      if (options.schema) {
        createStatement += ` (${this._buildTypeDeclaration(options.schema)})`;
      }
      
      createStatement += ';';
      
      // Execute the CREATE statement
      await this._connector.executeQuery(`USE ${this.name}; ${createStatement}`);
      
      // Return the collection instance
      return this.collection(name);
    } catch (error) {
      throw new Error(`Failed to create collection '${name}': ${error.message}`);
    }
  }
  
  /**
   * Drops a collection from the database.
   * 
   * @param {string} name - The name of the collection to drop
   * @returns {Promise<boolean>} - True if the collection was dropped
   */
  async dropCollection(name) {
    try {
      await this._connector.executeQuery(`USE ${this.name}; DROP DATASET ${name};`);
      
      // Remove the collection from the cache
      if (this._collections[name]) {
        delete this._collections[name];
      }
      
      return true;
    } catch (error) {
      throw new Error(`Failed to drop collection '${name}': ${error.message}`);
    }
  }
  
  /**
   * Helper method to build an AsterixDB type declaration from a schema object.
   * 
   * @private
   * @param {Object} schema - The schema definition
   * @returns {string} - The type declaration string
   */
  _buildTypeDeclaration(schema) {
    if (typeof schema === 'string') {
      return schema; // Already a type declaration string
    }
    
    // Convert schema object to AsterixDB type declaration
    const fields = [];
    
    for (const [key, value] of Object.entries(schema)) {
      let type;
      
      if (typeof value === 'string') {
        type = value;
      } else if (value.type) {
        type = value.type;
      } else {
        // Nested object
        type = `{ ${this._buildTypeDeclaration(value)} }`;
      }
      
      fields.push(`${key}: ${type}`);
    }
    
    return fields.join(', ');
  }
}

module.exports = AsterixDatabase; 