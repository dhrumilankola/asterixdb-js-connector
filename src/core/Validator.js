const Connector = require('./Connector');

class Validator {
  constructor() {
    this.connector = new Connector();
  }

  async validateFromClause(dataverseName, fromClause) {
    // Split datasets and handle aliases
    const datasets = fromClause.split(',').map(ds => {
      const parts = ds.trim().split(/\s+/);
      return {
        name: parts[0],
        alias: parts[1] || parts[0]
      };
    });

    // Validate each dataset in the FROM clause
    for (const dataset of datasets) {
      const exists = await this.validateDataset(dataverseName, dataset.name);
      if (!exists) {
        throw new Error(`Dataset ${dataset.name} not found in dataverse ${dataverseName}`);
      }
    }
    
    return true;
  }

  async validateSelectQuery() {
    if (!this._from || this._from.length === 0) {
      throw new Error("SELECT queries require a FROM clause.");
    }

    // Build the FROM clause string and validate local datasets.
    const fromClauseStr = this._from
      .map(ds => ds.alias === ds.name ? ds.name : `${ds.name} ${ds.alias}`)
      .join(', ');
    await this._validator.validateFromClause(this._dataverse, fromClauseStr);

    // Validate columns in the SELECT clause.
    for (const selectPart of this._selectParts) {
      // Remove any aliasing from the SELECT part (e.g., "user.name AS uname").
      const parts = selectPart.split(/\s+AS\s+/i);
      const columnExpr = parts[0].trim();
      if (columnExpr.indexOf('.') !== -1) {
        const [alias, columnName] = columnExpr.split('.');
        // Try to find the alias in the local FROM clause.
        let ds = this._from.find(ds => ds.alias === alias);
        if (!ds) {
          // If not found locally, check in outer query context.
          if (!this._outerAliases.hasOwnProperty(alias)) {
            throw new Error(
              `Alias "${alias}" in SELECT clause does not match any dataset in FROM clause or outer query context`
            );
          }
          // Otherwise, assume the column is valid (correlated subquery).
        } else {
          // Validate that the column exists in the local dataset.
          await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
        }
      } else {
        // For unqualified columns:
        if (this._from.length === 1) {
          await this._validator.validateColumns(this._dataverse, this._from[0].name, [columnExpr], false);
        } else {
          throw new Error(
            `Ambiguous column "${columnExpr}" in SELECT clause for multi-dataset query. Please qualify the column with an alias.`
          );
        }
      }
    }

    // Validate the WHERE clause similarly.
    if (this._where) {
      const whereColumns = this.extractColumnsFromCondition(this._where);
      for (const fullColumn of whereColumns) {
        if (fullColumn.indexOf('.') !== -1) {
          const [alias, columnName] = fullColumn.split('.');
          let ds = this._from.find(ds => ds.alias === alias);
          if (!ds) {
            if (!this._outerAliases.hasOwnProperty(alias)) {
              throw new Error(
                `Alias "${alias}" in WHERE clause does not match any dataset in FROM clause or outer query context`
              );
            }
          } else {
            await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
          }
        } else {
          if (this._from.length === 1) {
            await this._validator.validateColumns(this._dataverse, this._from[0].name, [fullColumn], false);
          } else {
            throw new Error(
              `Ambiguous column "${fullColumn}" in WHERE clause for multi-dataset query. Please qualify the column with an alias.`
            );
          }
        }
      }
    }

    return true;
  }

  async validateDataset(dataverseName, datasetName) {
    const query = `
      USE Metadata;
      SELECT VALUE ds
      FROM \`Dataset\` ds
      WHERE ds.DataverseName = "${dataverseName}" 
      AND ds.DatasetName = "${datasetName}";
    `;
  
    try {
      const result = await this.connector.executeQuery(query);
      return result.results && result.results.length > 0;
    } catch (error) {
      console.error('Error validating dataset:', error);
      throw new Error(`Failed to validate dataset ${datasetName} in dataverse ${dataverseName}`);
    }
  }

  async validateColumns(dataverseName, datasetName, columns, isValueSelect = false) {
    // For VALUE queries, we only need to validate the dataset exists
    if (isValueSelect) {
      return true;
    }

    try {
      const datasetQuery = `
        USE Metadata;
        SELECT VALUE ds
        FROM \`Dataset\` ds
        WHERE ds.DataverseName = "${dataverseName}" 
        AND ds.DatasetName = "${datasetName}";
      `;
      const datasetResult = await this.connector.executeQuery(datasetQuery);
      
      if (!datasetResult.results || !datasetResult.results[0]) {
        throw new Error(`Dataset ${datasetName} not found`);
      }

      const datatypeName = datasetResult.results[0].DatatypeName;

      const typeQuery = `
        USE Metadata;
        SELECT VALUE dt
        FROM \`Datatype\` dt
        WHERE dt.DataverseName = "${dataverseName}" 
        AND dt.DatatypeName = "${datatypeName}";
      `;
      const typeResult = await this.connector.executeQuery(typeQuery);

      if (!typeResult.results || !typeResult.results[0]) {
        throw new Error(`Datatype ${datatypeName} not found`);
      }

      const schemaFields = Object.keys(typeResult.results[0].Derived.Record.Fields);
      return true;
    } catch (error) {
      console.error('Error in validateColumns:', error);
      throw error;
    }
  }

  async validateColumnTypes(dataverseName, datasetName, columnValues) {
    try {
      const schema = await this.getDatasetSchema(dataverseName, datasetName);
      
      for (const [column, value] of columnValues.entries()) {
        const fieldInfo = schema[column];
        if (!fieldInfo) {
          throw new Error(`Column ${column} not found in schema`);
        }

        if (!this.isTypeValid(fieldInfo.TypeName, value)) {
          throw new Error(
            `Type mismatch for column ${column}: expected ${fieldInfo.TypeName}, ` +
            `got ${typeof value}`
          );
        }
      }

      return true;
    } catch (error) {
      console.error('Error in validateColumnTypes:', error);
      throw error;
    }
  }

  async getDatasetSchema(dataverseName, datasetName) {
    const query = `
      USE Metadata;
      SELECT dt.Derived.Record.Fields as schema
      FROM Dataset ds, Datatype dt
      WHERE ds.DataverseName = "${dataverseName}"
      AND ds.DatasetName = "${datasetName}"
      AND dt.DataverseName = ds.DataverseName
      AND dt.DatatypeName = ds.DatatypeName;
    `;

    const result = await this.connector.executeQuery(query);
    if (!result.results || !result.results[0]) {
      throw new Error(`Schema not found for ${datasetName}`);
    }

    return result.results[0];
  }

  isTypeValid(expectedType, value) {
    switch (expectedType.toLowerCase()) {
      case 'string':
        return typeof value === 'string';
      case 'int32':
      case 'int64':
      case 'int':
      case 'tinyint':
      case 'smallint':
      case 'bigint':
        return Number.isInteger(value);
      case 'float':
      case 'double':
        return typeof value === 'number';
      case 'boolean':
        return typeof value === 'boolean';
      case 'array':
        return Array.isArray(value);
      case 'object':
        return typeof value === 'object' && value !== null;
      default:
        return true;
    }
  }
}

module.exports = Validator;