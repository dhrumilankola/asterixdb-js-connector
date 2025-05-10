/**
 * Translates MongoDB-style query expressions to SQL++ syntax for AsterixDB.
 */
class QueryTranslator {
  /**
   * Translates a MongoDB-style query to SQL++ WHERE clause.
   * 
   * @param {Object} query - MongoDB-style query
   * @returns {string} - SQL++ WHERE clause
   */
  toSQLPP(query) {
    if (!query || Object.keys(query).length === 0) {
      return '{}'; // Empty query
    }
    
    const conditions = [];
    
    for (const [field, value] of Object.entries(query)) {
      // Handle special operators like $and, $or, etc.
      if (field.startsWith('$')) {
        switch (field) {
          case '$and':
            conditions.push(this._handleLogicalAnd(value));
            break;
          case '$or':
            conditions.push(this._handleLogicalOr(value));
            break;
          case '$not':
            conditions.push(this._handleLogicalNot(value));
            break;
          default:
            throw new Error(`Unsupported logical operator: ${field}`);
        }
      } else {
        // Handle field comparisons
        const condition = this._handleFieldComparison(field, value);
        if (condition) {
          conditions.push(condition);
        }
      }
    }
    
    return conditions.length > 0 ? conditions.join(' AND ') : '{}';
  }
  
  /**
   * Translates a MongoDB-style update document to SQL++ SET clause.
   * 
   * @param {Object} update - MongoDB-style update document
   * @returns {string} - SQL++ SET clause
   */
  updateToSQLPP(update) {
    if (!update || Object.keys(update).length === 0) {
      throw new Error('Empty update document');
    }
    
    const setExpressions = [];
    
    for (const [op, fields] of Object.entries(update)) {
      switch (op) {
        case '$set':
          for (const [field, value] of Object.entries(fields)) {
            setExpressions.push(`${field} = ${this._valueToSQLPP(value)}`);
          }
          break;
        case '$inc':
          for (const [field, value] of Object.entries(fields)) {
            setExpressions.push(`${field} = ${field} + ${this._valueToSQLPP(value)}`);
          }
          break;
        case '$push':
          for (const [field, value] of Object.entries(fields)) {
            setExpressions.push(`${field} = ARRAY_CONCAT(${field}, [${this._valueToSQLPP(value)}])`);
          }
          break;
        case '$unset':
          for (const field of Object.keys(fields)) {
            // In SQL++, we can remove a field by setting it to MISSING
            setExpressions.push(`${field} = MISSING`);
          }
          break;
        default:
          // For direct field updates (not using operators)
          if (!op.startsWith('$')) {
            setExpressions.push(`${op} = ${this._valueToSQLPP(fields)}`);
          } else {
            throw new Error(`Unsupported update operator: ${op}`);
          }
      }
    }
    
    return setExpressions.join(', ');
  }
  
  /**
   * Handles logical AND operator.
   * 
   * @private
   * @param {Array} conditions - Array of conditions to AND
   * @returns {string} - SQL++ AND expression
   */
  _handleLogicalAnd(conditions) {
    if (!Array.isArray(conditions) || conditions.length === 0) {
      throw new Error('$and requires a non-empty array');
    }
    
    const sqlConditions = conditions.map(cond => `(${this.toSQLPP(cond)})`);
    return sqlConditions.join(' AND ');
  }
  
  /**
   * Handles logical OR operator.
   * 
   * @private
   * @param {Array} conditions - Array of conditions to OR
   * @returns {string} - SQL++ OR expression
   */
  _handleLogicalOr(conditions) {
    if (!Array.isArray(conditions) || conditions.length === 0) {
      throw new Error('$or requires a non-empty array');
    }
    
    const sqlConditions = conditions.map(cond => `(${this.toSQLPP(cond)})`);
    return sqlConditions.join(' OR ');
  }
  
  /**
   * Handles logical NOT operator.
   * 
   * @private
   * @param {Object} condition - Condition to negate
   * @returns {string} - SQL++ NOT expression
   */
  _handleLogicalNot(condition) {
    return `NOT (${this.toSQLPP(condition)})`;
  }
  
  /**
   * Handles a field comparison.
   * 
   * @private
   * @param {string} field - Field name
   * @param {*} value - Field value or comparison
   * @returns {string} - SQL++ comparison expression
   */
  _handleFieldComparison(field, value) {
    // If value is a simple scalar, do an equality comparison
    if (this._isScalar(value)) {
      return `${field} = ${this._valueToSQLPP(value)}`;
    }
    
    // If value is an object, it may contain comparison operators
    if (typeof value === 'object' && value !== null) {
      const conditions = [];
      
      for (const [op, opValue] of Object.entries(value)) {
        switch (op) {
          case '$eq':
            conditions.push(`${field} = ${this._valueToSQLPP(opValue)}`);
            break;
          case '$ne':
            conditions.push(`${field} != ${this._valueToSQLPP(opValue)}`);
            break;
          case '$gt':
            conditions.push(`${field} > ${this._valueToSQLPP(opValue)}`);
            break;
          case '$gte':
            conditions.push(`${field} >= ${this._valueToSQLPP(opValue)}`);
            break;
          case '$lt':
            conditions.push(`${field} < ${this._valueToSQLPP(opValue)}`);
            break;
          case '$lte':
            conditions.push(`${field} <= ${this._valueToSQLPP(opValue)}`);
            break;
          case '$in':
            conditions.push(`${field} IN ${this._valueToSQLPP(opValue)}`);
            break;
          case '$nin':
            conditions.push(`${field} NOT IN ${this._valueToSQLPP(opValue)}`);
            break;
          case '$exists':
            if (opValue) {
              conditions.push(`${field} IS NOT MISSING`);
            } else {
              conditions.push(`${field} IS MISSING`);
            }
            break;
          case '$regex':
            // SQL++ uses LIKE for pattern matching
            let pattern = opValue;
            if (pattern.startsWith('^')) {
              pattern = pattern.substring(1);
            } else {
              pattern = '%' + pattern;
            }
            if (pattern.endsWith('$')) {
              pattern = pattern.substring(0, pattern.length - 1);
            } else {
              pattern = pattern + '%';
            }
            // Replace regex pattern with SQL LIKE pattern
            pattern = pattern.replace(/\.\*/g, '%');
            conditions.push(`${field} LIKE "${pattern}"`);
            break;
          default:
            throw new Error(`Unsupported comparison operator: ${op}`);
        }
      }
      
      return conditions.join(' AND ');
    }
    
    return null;
  }
  
  /**
   * Converts a JavaScript value to SQL++ literal syntax.
   * 
   * @private
   * @param {*} value - The value to convert
   * @returns {string} - SQL++ literal representation
   */
  _valueToSQLPP(value) {
    if (value === null) {
      return 'NULL';
    }
    
    if (value === undefined) {
      return 'MISSING';
    }
    
    if (typeof value === 'string') {
      // Escape quotes in strings
      return `"${value.replace(/"/g, '\\"')}"`;
    }
    
    if (typeof value === 'number' || typeof value === 'boolean') {
      return value.toString();
    }
    
    if (value instanceof Date) {
      // Format date as ISO string and wrap in datetime constructor
      return `datetime("${value.toISOString()}")`;
    }
    
    if (Array.isArray(value)) {
      const elements = value.map(element => this._valueToSQLPP(element));
      return `[${elements.join(', ')}]`;
    }
    
    if (typeof value === 'object') {
      const fields = [];
      
      for (const [key, val] of Object.entries(value)) {
        fields.push(`"${key}": ${this._valueToSQLPP(val)}`);
      }
      
      return `{ ${fields.join(', ')} }`;
    }
    
    throw new Error(`Unsupported value type: ${typeof value}`);
  }
  
  /**
   * Checks if a value is a scalar (string, number, boolean).
   * 
   * @private
   * @param {*} value - The value to check
   * @returns {boolean} - True if the value is a scalar
   */
  _isScalar(value) {
    return typeof value === 'string' || 
           typeof value === 'number' || 
           typeof value === 'boolean' ||
           value === null ||
           value === undefined;
  }
}

module.exports = QueryTranslator; 