// QueryBuilder.js
const Validator = require('./Validator');

class QueryBuilder {
  constructor(outerAliases = {}) {
    this._validator = new Validator();
    this._setCommands = [];
    this._dataverse = null;
    this._selectParts = [];
    this._isValueSelect = false;
    this._from = [];
    this._where = null;
    this._groupBy = null;
    this._orderBy = null;
    this._limit = null;
    this._insertInto = null;
    this._values = null;
    this._queryType = null;
    this._outerAliases = outerAliases; // this object holds alias mapping from the parent query.
  }

  use(dataverse) {
    this._dataverse = dataverse;
    return this;
  }

  set(command, value) {
    this._setCommands.push(`SET ${command} "${value}";`);
    return this;
  }

  select(columns) {
    if (this._queryType && this._queryType !== 'SELECT') {
      throw new Error("Cannot mix SELECT with INSERT query parts.");
    }
    this._queryType = 'SELECT';
    
    if (!Array.isArray(columns)) {
      columns = [columns];
    }
    
    // For VALUE queries, extract the alias to use in FROM clause
    const valuePattern = /^VALUE\s+(\w+)$/i;
    for (const col of columns) {
      const match = col.match(valuePattern);
      if (match) {
        // Store the alias
        // (This is used if there is a single dataset; for multi-dataset queries we’ll rely on FROM aliases.)
        this._fromAlias = match[1];  
        break;
      }
    }
    
    this._selectParts = columns;
    return this;
  }

  /**
   * Modified from() method:
   * - If a string is provided, split on commas.
   * - For each dataset, check if an alias is provided; if not, default alias to the dataset name.
   */
  from(source) {
    if (typeof source === 'string') {
      this._from = source.split(',').map(item => {
        const parts = item.trim().split(/\s+/);
        return {
          name: parts[0],
          alias: parts[1] || parts[0]
        };
      });
    } else if (Array.isArray(source)) {
      // Assume source is an array of objects with {name, alias}
      this._from = source;
    } else {
      throw new Error("FROM clause must be a string or an array");
    }
    return this;
  }

  where(condition) {
    this._where = condition;
    return this;
  }

  groupBy(expression) {
    this._groupBy = expression;
    return this;
  }

  orderBy(expression) {
    this._orderBy = expression;
    return this;
  }

  limit(n) {
    this._limit = n;
    return this;
  }

  insertInto(table) {
    if (this._queryType && this._queryType !== 'INSERT') {
      throw new Error("Cannot mix INSERT with SELECT query parts.");
    }
    this._queryType = 'INSERT';
    this._insertInto = table;
    return this;
  }

  values(data) {
    if (this._queryType !== 'INSERT') {
      throw new Error("VALUES clause is valid only for INSERT queries.");
    }
    this._values = JSON.stringify(data, null, 2);
    return this;
  }

  /**
   * Helper method: Returns a mapping of local alias → dataset name.
   */
  _getLocalAliasMapping() {
    let mapping = {};
    if (this._from && Array.isArray(this._from)) {
      for (const ds of this._from) {
        mapping[ds.alias] = ds.name;
      }
    }
    return mapping;
  }

    /**
     * Build a subquery as part of the SELECT clause.
     * Note: Since build() is async, this method should be async as well.
     *
     * @param {string} alias - The alias for the subquery column.
     * @param {function(QueryBuilder): Promise<void> | void} builderFn - A function
     *   that receives a nested QueryBuilder instance to define the subquery.
     */
    async selectSubQuery(alias, builderFn) {
        // Create a nested builder, passing the outer context (the current FROM mapping)
        const nestedBuilder = new QueryBuilder(this._getLocalAliasMapping());
        // Let the developer build the nested query.
        await builderFn(nestedBuilder);
        let subquery = await nestedBuilder.build();
        // Remove trailing semicolon if present (for embedding within the parent query)
        if (subquery.trim().endsWith(";")) {
            subquery = subquery.trim().slice(0, -1);
        }
        // Append the subquery to the SELECT parts
        this._selectParts.push(`(${subquery}) AS ${alias}`);
        return this;
        }

  async validateQuery() {
    if (!this._dataverse) {
      throw new Error("Dataverse must be specified using the use() method.");
    }

    let validationSuccess = false;
    try {
      if (this._queryType === 'SELECT') {
        if (this._selectParts.length === 0) {
          throw new Error("SELECT clause is required for a SELECT query.");
        }
        validationSuccess = await this.validateSelectQuery();
      } else if (this._queryType === 'INSERT') {
        if (!this._insertInto || !this._values) {
          throw new Error("Both INSERT INTO and VALUES clauses are required for an INSERT query.");
        }
        validationSuccess = await this.validateInsertQuery();
      }
    } catch (error) {
      console.error('Validation error:', error);
      throw error;
    }

    if (!validationSuccess) {
      throw new Error("Query validation failed.");
    }

    return true;
  }

  /**
   * Modified validateSelectQuery:
   * - Build a proper FROM clause string using the parsed _from array.
   * - Validate each dataset using the validator’s validateFromClause.
   * - (Optionally, you can also validate that SELECT/WHERE columns match the correct dataset using the alias.)
   */
  async validateSelectQuery() {
    if (!this._from || this._from.length === 0) {
      throw new Error("SELECT queries require a FROM clause.");
    }

    // Build the FROM clause string that includes aliases (if any)
    const fromClauseStr = this._from
      .map(ds => ds.alias === ds.name ? ds.name : `${ds.name} ${ds.alias}`)
      .join(', ');
    // Validate each dataset in the FROM clause
    await this._validator.validateFromClause(this._dataverse, fromClauseStr);

    // Example column validation:
    // For each SELECT part that is a qualified column (alias.column), validate that column exists.
    // (This logic assumes that in multi-dataset queries, columns are always prefixed by an alias.)
    for (const selectPart of this._selectParts) {
      // Remove any aliasing in the SELECT part (e.g. "user.name AS uname")
      const parts = selectPart.split(/\s+AS\s+/i);
      const columnExpr = parts[0].trim();
      if (columnExpr.indexOf('.') !== -1) {
        const [alias, columnName] = columnExpr.split('.');
        const ds = this._from.find(ds => ds.alias === alias);
        if (!ds) {
          throw new Error(`Alias ${alias} in SELECT clause does not match any dataset in FROM clause`);
        }
        // Validate that the column exists in the corresponding dataset.
        await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
      } else {
        // If unqualified and there is only one dataset, validate against it.
        if (this._from.length === 1) {
          await this._validator.validateColumns(this._dataverse, this._from[0].name, [columnExpr], false);
        } else {
          throw new Error(`Ambiguous column "${columnExpr}" in SELECT clause for multi-dataset query. Please qualify the column with an alias.`);
        }
      }
    }

    // Similar approach for WHERE clause column validation.
    if (this._where) {
      const whereColumns = this.extractColumnsFromCondition(this._where);
      for (const fullColumn of whereColumns) {
        if (fullColumn.indexOf('.') !== -1) {
          const [alias, columnName] = fullColumn.split('.');
          const ds = this._from.find(ds => ds.alias === alias);
          if (!ds) {
            throw new Error(`Alias ${alias} in WHERE clause does not match any dataset in FROM clause`);
          }
          await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
        } else {
          if (this._from.length === 1) {
            await this._validator.validateColumns(this._dataverse, this._from[0].name, [fullColumn], false);
          } else {
            throw new Error(`Ambiguous column "${fullColumn}" in WHERE clause for multi-dataset query. Please qualify the column with an alias.`);
          }
        }
      }
    }

    return true;
  }

  async validateInsertQuery() {
    const datasetName = this._insertInto;
    // Validate dataset existence
    const datasetExists = await this._validator.validateDataset(this._dataverse, datasetName);
    if (!datasetExists) {
      throw new Error(`Dataset ${datasetName} not found in dataverse ${this._dataverse}.`);
    }
    const values = JSON.parse(this._values);
    const columnValues = new Map(Object.entries(values));
    await this._validator.validateColumnTypes(this._dataverse, datasetName, columnValues);
    return true;
  }

  async build() {
    // Validate the query before building
    await this.validateQuery();
    const query = this.constructQueryString();
    console.log("Generated query:", query);  // Debug: show generated query
    return query;
  }

  /**
   * Modified constructQueryString:
   * - Use the parsed _from array to build a correct FROM clause.
   */
  constructQueryString() {
    let queryStr = "";

    if (this._setCommands.length > 0) {
      queryStr += this._setCommands.join('\n') + "\n";
    }

    if (this._dataverse) {
      queryStr += `USE ${this._dataverse};\n`;
    }

    if (this._queryType === 'SELECT') {
      const selectClause = this._selectParts.join(', ');
      queryStr += `SELECT ${selectClause}`;

      if (this._from && this._from.length > 0) {
        const fromClauseStr = this._from
          .map(ds => ds.alias === ds.name ? ds.name : `${ds.name} ${ds.alias}`)
          .join(', ');
        queryStr += ` FROM ${fromClauseStr}`;
      }

      if (this._where) {
        queryStr += ` WHERE ${this._where}`;
      }

      if (this._groupBy) {
        queryStr += ` GROUP BY ${this._groupBy}`;
      }

      if (this._orderBy) {
        queryStr += ` ORDER BY ${this._orderBy}`;
      }

      if (this._limit !== null && this._limit !== undefined) {
        queryStr += ` LIMIT ${this._limit}`;
      }

      queryStr += ";";

    } else {
      throw new Error("No query type specified. Use select() or insertInto() to start building a query.");
    }

    return queryStr;
  }

  extractColumnsFromCondition(condition) {
    if (!condition) return [];
    
    // Remove block comments (e.g. /*+ indexnl */)
    const cleanCondition = condition.replace(/\/\*[\s\S]*?\*\//g, '');
    
    // Use regex to find qualified column names
    const pathRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\b/g;
    const matches = [...cleanCondition.matchAll(pathRegex)];
    
    // SQL++ keywords that should be ignored
    const sqlppKeywords = new Set([
      'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 
      'NULL', 'MISSING', 'UNKNOWN', 'VALUE', 'ELEMENT'
    ]);
    
    return matches
      .map(match => match[1])
      .filter(column => {
        const parts = column.split('.');
        // Filter out tokens that are SQL++ keywords
        return !parts.some(part => sqlppKeywords.has(part.toUpperCase()));
      });
  }
  
}

module.exports = QueryBuilder;
