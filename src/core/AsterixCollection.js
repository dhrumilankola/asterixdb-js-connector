const QueryBuilder = require('./QueryBuilder');
const QueryTranslator = require('./QueryTranslator');

/**
 * AsterixCollection provides a MongoDB-like interface for interacting with AsterixDB datasets.
 */
class AsterixCollection {
  /**
   * Creates a new AsterixCollection instance.
   * 
   * @param {string} name - The name of the collection (dataset in AsterixDB)
   * @param {AsterixDatabase} database - The database this collection belongs to
   * @param {Connector|OfflineEnabledConnector} connector - The connector instance
   */
  constructor(name, database, connector) {
    this.name = name;
    this.database = database;
    this._connector = connector;
    this._queryTranslator = new QueryTranslator();
  }
  
  /**
   * Finds documents in the collection.
   * 
   * @param {Object} query - The query filter
   * @param {Object} options - Query options
   * @param {Object} options.projection - Fields to include/exclude
   * @param {Object} options.sort - Sort specification
   * @param {number} options.limit - Maximum number of documents to return
   * @param {number} options.skip - Number of documents to skip
   * @returns {Promise<Array>} - Array of matching documents
   */
  async find(query = {}, options = {}) {
    try {
      // Build the SQL++ query using QueryBuilder and our query translator
      const sqlppQuery = await this._buildFindQuery(query, options);
      
      // Execute the query
      const result = await this._connector.executeQuery(sqlppQuery);
      
      // Extract and return the results
      if (result && result.results) {
        return result.results;
      }
      
      return [];
    } catch (error) {
      throw new Error(`Find operation failed: ${error.message}`);
    }
  }
  
  /**
   * Finds a single document in the collection.
   * 
   * @param {Object} query - The query filter
   * @param {Object} options - Query options
   * @param {Object} options.projection - Fields to include/exclude
   * @param {Object} options.sort - Sort specification
   * @returns {Promise<Object|null>} - The matching document or null
   */
  async findOne(query = {}, options = {}) {
    try {
      // Set limit to 1 and reuse find implementation
      const limitedOptions = { ...options, limit: 1 };
      const results = await this.find(query, limitedOptions);
      
      return results.length > 0 ? results[0] : null;
    } catch (error) {
      throw new Error(`FindOne operation failed: ${error.message}`);
    }
  }
  
  /**
   * Counts documents in the collection.
   * 
   * @param {Object} query - The query filter
   * @param {Object} options - Count options
   * @returns {Promise<number>} - The count of matching documents
   */
  async countDocuments(query = {}, options = {}) {
    try {
      // Translate the MongoDB-style query to SQL++ WHERE clause
      const whereClause = this._queryTranslator.toSQLPP(query);
      
      // Build a count query
      const builder = new QueryBuilder()
        .use(this.database.name)
        .select(['COUNT(*) as count'])
        .from(this.name);
      
      if (whereClause && whereClause !== '{}') {
        builder.where(whereClause);
      }
      
      const sqlppQuery = await builder.build();
      
      // Execute the query
      const result = await this._connector.executeQuery(sqlppQuery);
      
      // Extract and return the count
      if (result && result.results && result.results.length > 0) {
        return result.results[0].count;
      }
      
      return 0;
    } catch (error) {
      throw new Error(`CountDocuments operation failed: ${error.message}`);
    }
  }
  
  /**
   * Inserts a single document into the collection.
   * 
   * @param {Object} doc - The document to insert
   * @returns {Promise<Object>} - The inserted document with _id
   */
  async insertOne(doc) {
    try {
      // Ensure the document has an _id field
      const docToInsert = { ...doc };
      if (!docToInsert._id) {
        docToInsert._id = this._generateId();
        console.log(`[AsterixCollection.insertOne] Generated _id '${docToInsert._id}' for doc with screenName: '${docToInsert.screenName || 'N/A'}'`);
      }
      
      // Build the SQL++ INSERT query
      console.log(`[AsterixCollection.insertOne] About to build INSERT query for doc with screenName: '${docToInsert.screenName || 'N/A'}'`);
      const builder = new QueryBuilder()
        .use(this.database.name)
        .insertInto(this.name)
        .values([docToInsert]);
      
      // The builder.build() method itself contains a console.log for the generated query.
      const sqlppQuery = await builder.build();
      
      console.log(`[AsterixCollection.insertOne] About to execute INSERT query for doc with screenName: '${docToInsert.screenName || 'N/A'}'`);
      await this._connector.executeQuery(sqlppQuery);
      
      console.log(`[AsterixCollection.insertOne] Successfully executed INSERT for doc with screenName: '${docToInsert.screenName || 'N/A'}'`);
      return docToInsert;
    } catch (error) {
      console.error(`[AsterixCollection.insertOne] ERROR for doc screenName '${doc.screenName || 'N/A'}: ${error.message}`);
      console.error(`[AsterixCollection.insertOne] Error stack: ${error.stack}`);
      if (error.response && error.response.data) {
        console.error(`[AsterixCollection.insertOne] Axios Error Response Data: ${JSON.stringify(error.response.data, null, 2)}`);
      }
      throw new Error(`InsertOne operation failed for ${doc.screenName || 'document'}: ${error.message}`);
    }
  }
  
  /**
   * Inserts multiple documents into the collection.
   * 
   * @param {Array<Object>} docs - The documents to insert
   * @returns {Promise<Object>} - Result with insertedCount and insertedIds
   */
  async insertMany(docs) {
    try {
      // Ensure each document has an _id field
      const docsToInsert = docs.map(doc => {
        const docToInsert = { ...doc };
        if (!docToInsert._id) {
          docToInsert._id = this._generateId();
        }
        return docToInsert;
      });
      
      // Build the SQL++ INSERT query
      const builder = new QueryBuilder()
        .use(this.database.name)
        .insertInto(this.name)
        .values(docsToInsert);
      
      const sqlppQuery = await builder.build();
      
      // Execute the query
      await this._connector.executeQuery(sqlppQuery);
      
      // Construct the result
      const insertedIds = {};
      docsToInsert.forEach((doc, index) => {
        insertedIds[index] = doc._id;
      });
      
      return {
        acknowledged: true,
        insertedCount: docsToInsert.length,
        insertedIds
      };
    } catch (error) {
      throw new Error(`InsertMany operation failed: ${error.message}`);
    }
  }
  
  /**
   * Updates a single document in the collection.
   * 
   * @param {Object} filter - The filter to select the document
   * @param {Object} update - The update operations to apply
   * @param {Object} options - Update options
   * @param {boolean} options.upsert - Insert if no documents match
   * @returns {Promise<Object>} - The update result
   */
  async updateOne(filter, update, options = {}) {
    try {
      const originalDoc = await this.findOne(filter);

      if (!originalDoc) {
        if (options.upsert) {
          // Document doesn't exist, and upsert is true: insert a new document.
          // Create the new document by applying update operators to the filter fields (or an empty object).
          // This attempts to construct the document as it would be if it matched the filter and then was updated.
          let docToInsert = this._applyUpdateOperatorsToDocument({ ...filter }, update);
          
          // Ensure primary key from filter is preserved if not explicitly set by update operators
          // (especially if filter itself contains the PK, e.g. { screenName: "xyz" })
          for (const key in filter) {
            if (filter.hasOwnProperty(key) && !docToInsert.hasOwnProperty(key) && !key.startsWith('$')) {
              docToInsert[key] = filter[key];
            }
          }

          // Remove any MongoDB operators from the document to be inserted
          for (const key in docToInsert) {
            if (key.startsWith('$')) {
              delete docToInsert[key];
            }
          }
          
          const inserted = await this.insertOne(docToInsert);
          return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0, // Or 1 if we consider the upsert a modification
            upsertedCount: 1,
            upsertedId: inserted._id || inserted[this.database.getPrimaryKeyForCollection(this.name)] // Assuming _id or actual PK
          };
        } else {
          // Document doesn't exist, and upsert is false.
          return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
          };
        }
      }

      // Document exists, proceed with delete-then-insert.
      const modifiedDoc = this._applyUpdateOperatorsToDocument(originalDoc[this.name] || originalDoc, update); // originalDoc might be { CollectionName: { doc } }

      // Ensure the primary key of the original document is part of the modified document
      // This is crucial so the "new" document replaces the old one with the same identity.
      let primaryKeyField;
      if (this.name === 'ChirpUsers') { // Specific for MongoLikeUsage.js example
        primaryKeyField = 'screenName';
      } else {
        // Fallback or attempt to get from a (not-yet-implemented) database schema method
        // primaryKeyField = this.database.getPrimaryKeyForCollection(this.name) || Object.keys(filter)[0];
        // For now, defaulting to the first key in the filter if not ChirpUsers. This might be risky.
        primaryKeyField = Object.keys(filter)[0]; 
        if (!primaryKeyField) {
            throw new Error('Could not determine primary key for update operation and filter is empty.');
        }
        console.warn(`Update operation for collection '${this.name}' is guessing primary key as '${primaryKeyField}'. Consider implementing a generic PK retrieval mechanism.`);
      }

      if (originalDoc[this.name] && originalDoc[this.name][primaryKeyField] && !modifiedDoc[primaryKeyField]) {
         modifiedDoc[primaryKeyField] = originalDoc[this.name][primaryKeyField];
      } else if (originalDoc[primaryKeyField] && !modifiedDoc[primaryKeyField]) {
         modifiedDoc[primaryKeyField] = originalDoc[primaryKeyField];
      }

      // 1. Delete the original document
      // Construct WHERE clause for delete. It should be specific enough, ideally using the PK.
      const deleteFilter = {};
      // Attempt to use the actual primary key from the original document for precise deletion
      const pkValueFromOriginal = originalDoc[this.name] ? originalDoc[this.name][primaryKeyField] : originalDoc[primaryKeyField];
      if (pkValueFromOriginal !== undefined) {
        deleteFilter[primaryKeyField] = pkValueFromOriginal;
      } else {
        // Fallback to using the provided filter if PK can't be reliably determined from originalDoc
        // This might be less precise if the filter isn't on a unique key.
        Object.assign(deleteFilter, filter); 
      }

      const deleteWhereClause = this._queryTranslator.toSQLPP(deleteFilter);
      if (!deleteWhereClause || deleteWhereClause === '{}') {
        throw new Error('Cannot perform delete step of update: Invalid or empty filter for delete.');
      }

      const deleteQuery = `USE ${this.database.name}; DELETE FROM ${this.name} WHERE ${deleteWhereClause};`;
      // We expect this to delete one document. SQL++ DELETE doesn't return count easily without subqueries.
      await this._connector.executeQuery(deleteQuery);

      // 2. Insert the modified document
      // The `insertOne` method handles _id generation if not present, but for updates,
      // we want to ensure the PK is from the modifiedDoc (which should carry it from originalDoc)
      // Remove _id if it was auto-generated by findOne and not part of the actual data structure, 
      // relying on the true PK for the dataset.
      // For ChirpUsers, screenName is PK. If modifiedDoc contains screenName, that's good.
      // If _id was on originalDoc but is NOT the PK and not in schema, it should not persist if not set by user.
      const pkIsId = primaryKeyField === '_id';
      if (!pkIsId && modifiedDoc._id && !(originalDoc[this.name] ? originalDoc[this.name].hasOwnProperty('_id') : originalDoc.hasOwnProperty('_id'))) {
          // If _id was potentially added by our system and it is NOT the PK, and was not in original data, remove it before insert.
          // This assumes the schema's PK is what defines uniqueness.
          //delete modifiedDoc._id; 
          // Let's be cautious: if _id is there and not PK, it might be intentional. But for ChirpUsers, screenName is key.
          // The insertOne method itself might add an _id if not present and if it's the default behavior.
          // We trust that `modifiedDoc` correctly represents the state to be inserted, including its correct PK.
      }
      
      // If an _id was part of the original document (and potentially the PK), ensure it's in modifiedDoc.
      // Or if not PK, but was user-defined, it should persist.
      const originalDocActual = originalDoc[this.name] || originalDoc;
      if (originalDocActual.hasOwnProperty('_id') && !modifiedDoc.hasOwnProperty('_id')) {
        modifiedDoc._id = originalDocActual._id;
      }      

      await this.insertOne(modifiedDoc); // insertOne will use its own QueryBuilder logic

      return {
        acknowledged: true,
        matchedCount: 1,
        modifiedCount: 1, // Assumes the document was indeed changed and re-inserted
        upsertedCount: 0,
        upsertedId: null
      };

    } catch (error) {
      // Log the full error for better diagnostics
      console.error(`UpdateOne operation failed for filter: ${JSON.stringify(filter)}, update: ${JSON.stringify(update)}`, error);
      throw new Error(`UpdateOne operation failed: ${error.message}`);
    }
  }
  
  /**
   * Updates multiple documents in the collection.
   * 
   * @param {Object} filter - The filter to select documents
   * @param {Object} update - The update operations to apply
   * @param {Object} options - Update options
   * @param {boolean} options.upsert - Insert if no documents match
   * @returns {Promise<Object>} - The update result
   */
  async updateMany(filter, update, options = {}) {
    try {
      // First, check if any documents exist
      const count = await this.countDocuments(filter);
      
      if (count === 0 && options.upsert) {
        // If no documents match and upsert is true, perform an insert
        const newDoc = this._mergeFilterAndUpdate(filter, update);
        const result = await this.insertOne(newDoc);
        
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result._id
        };
      } else if (count === 0) {
        // No documents match and upsert is false
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 0
        };
      }
      
      // Documents exist, perform an update
      // Translate the MongoDB-style update to SQL++ SET clause
      const setClause = this._queryTranslator.updateToSQLPP(update);
      const whereClause = this._queryTranslator.toSQLPP(filter);
      
      // Build and execute the SQL++ UPDATE query
      const sqlppQuery = `
        USE ${this.database.name};
        UPDATE ${this.name}
        SET ${setClause}
        WHERE ${whereClause};
      `;
      
      await this._connector.executeQuery(sqlppQuery);
      
      return {
        acknowledged: true,
        matchedCount: count,
        modifiedCount: count,
        upsertedCount: 0
      };
    } catch (error) {
      throw new Error(`UpdateMany operation failed: ${error.message}`);
    }
  }
  
  /**
   * Deletes a single document from the collection.
   * 
   * @param {Object} filter - The filter to select the document
   * @returns {Promise<Object>} - The delete result
   */
  async deleteOne(filter) {
    try {
      // Translate the MongoDB-style filter to SQL++ WHERE clause
      const whereClause = this._queryTranslator.toSQLPP(filter);
      
      // Build and execute the SQL++ DELETE query
      // LIMIT 1 is not standard SQL++ for DELETE and will cause an error.
      // If the whereClause correctly targets a unique document (e.g., by primary key),
      // it will inherently delete only one.
      const sqlppQuery = `
        USE ${this.database.name};
        DELETE FROM ${this.name}
        WHERE ${whereClause};
      `;
      
      const result = await this._connector.executeQuery(sqlppQuery);
      
      // Extract the delete count from the result
      let deletedCount = 0;
      if (result && result.metrics && result.metrics.mutationCount !== undefined) {
        deletedCount = result.metrics.mutationCount;
      }
      
      return {
        acknowledged: true,
        deletedCount
      };
    } catch (error) {
      throw new Error(`DeleteOne operation failed: ${error.message}`);
    }
  }
  
  /**
   * Deletes multiple documents from the collection.
   * 
   * @param {Object} filter - The filter to select documents
   * @returns {Promise<Object>} - The delete result
   */
  async deleteMany(filter) {
    try {
      // Translate the MongoDB-style filter to SQL++ WHERE clause
      const whereClause = this._queryTranslator.toSQLPP(filter);
      
      // Build and execute the SQL++ DELETE query
      const sqlppQuery = `
        USE ${this.database.name};
        DELETE FROM ${this.name}
        WHERE ${whereClause};
      `;
      
      const result = await this._connector.executeQuery(sqlppQuery);
      
      // Extract the delete count from the result
      let deletedCount = 0;
      if (result && result.metrics && result.metrics.mutationCount !== undefined) {
        deletedCount = result.metrics.mutationCount;
      }
      
      return {
        acknowledged: true,
        deletedCount
      };
    } catch (error) {
      throw new Error(`DeleteMany operation failed: ${error.message}`);
    }
  }
  
  /**
   * Returns distinct values for a field.
   * 
   * @param {string} field - The field to find distinct values for
   * @param {Object} filter - The filter to apply before finding distinct values
   * @returns {Promise<Array>} - Array of distinct values
   */
  async distinct(field, filter = {}) {
    try {
      // Translate the MongoDB-style filter to SQL++ WHERE clause
      const whereClause = this._queryTranslator.toSQLPP(filter);
      
      // Build the SQL++ query to get distinct values
      const builder = new QueryBuilder()
        .use(this.database.name)
        .select([`DISTINCT ${field} as value`])
        .from(this.name);
      
      if (whereClause && whereClause !== '{}') {
        builder.where(whereClause);
      }
      
      const sqlppQuery = await builder.build();
      
      // Execute the query
      const result = await this._connector.executeQuery(sqlppQuery);
      
      // Extract and return the distinct values
      if (result && result.results) {
        return result.results.map(item => item.value);
      }
      
      return [];
    } catch (error) {
      throw new Error(`Distinct operation failed: ${error.message}`);
    }
  }
  
  /**
   * Builds a SQL++ find query using QueryBuilder and QueryTranslator.
   * 
   * @private
   * @param {Object} query - The MongoDB-style query filter
   * @param {Object} options - Query options
   * @returns {Promise<string>} - The SQL++ query
   */
  async _buildFindQuery(query, options) {
    // Create a new QueryBuilder
    const builder = new QueryBuilder()
      .use(this.database.name);
    
    // Handle projection
    if (options.projection) {
      const fields = this._buildProjection(options.projection);
      builder.select(fields);
    } else {
      builder.select(['*']);
    }
    
    // Add FROM clause
    builder.from(this.name);
    
    // Handle query filter
    if (query && Object.keys(query).length > 0) {
      const whereClause = this._queryTranslator.toSQLPP(query);
      if (whereClause && whereClause !== '{}') {
        builder.where(whereClause);
      }
    }
    
    // Handle sort
    if (options.sort) {
      const sortClause = this._buildSortClause(options.sort);
      if (sortClause) {
        builder.orderBy(sortClause);
      }
    }
    
    // Handle limit
    if (options.limit) {
      builder.limit(options.limit);
    }
    
    // Build the final query
    return builder.build();
  }
  
  /**
   * Builds a projection clause for SQL++.
   * 
   * @private
   * @param {Object} projection - MongoDB-style projection
   * @returns {Array<string>} - Array of field specifications
   */
  _buildProjection(projection) {
    const fields = [];
    const inclusion = Object.values(projection).some(value => value === 1);
    
    for (const [field, value] of Object.entries(projection)) {
      if (field === '_id' && value === 0) {
        // Always handle _id explicitly
        continue;
      }
      
      if (inclusion) {
        // Inclusion projection
        if (value === 1) {
          fields.push(field);
        }
      } else {
        // Exclusion projection
        if (value === 0) {
          // Can't easily do exclusion in SQL++, so we'll include all fields except excluded
          // This is simplified and would need to be enhanced for a real implementation
        }
      }
    }
    
    // If using inclusion and _id wasn't explicitly excluded, include it
    if (inclusion && projection._id !== 0) {
      if (!fields.includes('_id')) {
        fields.push('_id');
      }
    }
    
    return fields.length > 0 ? fields : ['*'];
  }
  
  /**
   * Builds a sort clause for SQL++.
   * 
   * @private
   * @param {Object} sort - MongoDB-style sort specification
   * @returns {string} - ORDER BY clause
   */
  _buildSortClause(sort) {
    const sortParts = [];
    
    for (const [field, direction] of Object.entries(sort)) {
      const dir = direction === 1 ? 'ASC' : 'DESC';
      sortParts.push(`${field} ${dir}`);
    }
    
    return sortParts.join(', ');
  }
  
  /**
   * Merges a filter and update document for upsert operations.
   * 
   * @private
   * @param {Object} filter - The filter document
   * @param {Object} update - The update document
   * @returns {Object} - The merged document
   */
  _mergeFilterAndUpdate(filter, update) {
    // Handle simple filter fields
    const newDoc = { ...filter };
    
    // Handle $set operation in update
    if (update.$set) {
      Object.assign(newDoc, update.$set);
    }
    
    // Handle direct field updates
    for (const [key, value] of Object.entries(update)) {
      if (!key.startsWith('$')) {
        newDoc[key] = value;
      }
    }
    
    return newDoc;
  }
  
  /**
   * Generates a simple ID for documents.
   * 
   * @private
   * @returns {string} - A generated ID
   */
  _generateId() {
    return Math.random().toString(36).substring(2, 15) + 
           Math.random().toString(36).substring(2, 15);
  }
  
  /**
   * Helper function to apply MongoDB-style update operators to a document.
   *
   * @private
   * @param {Object} doc - The original document.
   * @param {Object} update - The MongoDB update object (e.g., { $set: { ... }, $inc: { ... } }).
   * @returns {Object} - The modified document.
   */
  _applyUpdateOperatorsToDocument(doc, update) {
    const modifiedDoc = { ...doc }; // Create a shallow copy

    for (const [operator, fields] of Object.entries(update)) {
      if (typeof fields !== 'object' || fields === null) {
        // Handle direct field assignment if no operator is used (e.g. update({field: value}))
        // This is not standard MongoDB behavior for updateOne but can be a fallback
        if (!operator.startsWith('$')) {
          modifiedDoc[operator] = fields;
        }
        continue;
      }
      switch (operator) {
        case '$set':
          for (const [field, value] of Object.entries(fields)) {
            modifiedDoc[field] = value;
          }
          break;
        case '$inc':
          for (const [field, value] of Object.entries(fields)) {
            if (typeof modifiedDoc[field] === 'number' && typeof value === 'number') {
              modifiedDoc[field] += value;
            } else if (typeof value === 'number') { // Field might not exist, initialize it
              modifiedDoc[field] = value;
            }
            // Consider logging a warning if types are incompatible
          }
          break;
        case '$unset':
          for (const field of Object.keys(fields)) {
            delete modifiedDoc[field];
          }
          break;
        // Add other operators as needed (e.g., $push, $pull, $rename)
        // For $rename:
        // case '$rename':
        //   for (const [oldName, newName] of Object.entries(fields)) {
        //     if (modifiedDoc.hasOwnProperty(oldName)) {
        //       modifiedDoc[newName] = modifiedDoc[oldName];
        //       delete modifiedDoc[oldName];
        //     }
        //   }
        //   break;
        default:
          console.warn(`Unsupported update operator: ${operator}. It will be ignored.`);
          // Or throw an error:
          // throw new Error(`Unsupported update operator: ${operator}`);
      }
    }
    return modifiedDoc;
  }
}

module.exports = AsterixCollection; 