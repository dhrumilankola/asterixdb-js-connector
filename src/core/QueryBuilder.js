// QueryBuilder.js
const Validator = require('./Validator');
const ASTNode = require('./ASTNode');

class QueryBuilder {
  constructor(outerAliases = {}) {
    this._outerAliases = outerAliases;
    this._setCommands = []; // We can keep this as simple strings or convert them to AST nodes as needed.
    this._queryType = null;
    this._isValueSelect = false;
    
    // AST nodes for each clause.
    this.astUse = null;
    this.astSelect = null;  // This will be an ASTNode with children for each select item.
    this.astFrom = null;
    this.astWhere = null;
    this.astGroupBy = null;
    this.astOrderBy = null;
    this.astLimit = null;
    
    // For INSERT queries
    this.astInsertInto = null;
    this.astValues = null;
    
    this.validator = new Validator();

  }

  // Extract dataverse name from the USE node, e.g. from "USE TinySocial;"
  _getDataverseName() {
    if (!this.astUse || !this.astUse.value) {
      throw new Error("Dataverse is not specified. Use the use() method.");
    }
    return this.astUse.value.replace(/^USE\s+/i, '').replace(/;$/, '').trim();
  }
  
  // Extract FROM clause string, removing the "FROM" keyword.
  _getFromClause() {
    if (!this.astFrom || !this.astFrom.value) {
      throw new Error("FROM clause is missing.");
    }
    return this.astFrom.value.replace(/^FROM\s+/i, '').trim();
  }
  
  // Extract SELECT clause string: join all child nodes of the SELECT AST.
  _getSelectClause() {
    if (!this.astSelect || this.astSelect.children.length === 0) {
      throw new Error("SELECT clause is missing.");
    }
    // For our purposes, we simply join the values.
    return this.astSelect.children.map(child => child.value).join(', ');
  }
  
  // Extract WHERE clause string, if any.
  _getWhereClause() {
    if (!this.astWhere || !this.astWhere.value) {
      return null;
    }
    return this.astWhere.value.replace(/^WHERE\s+/i, '').trim();
  }

  async validateQuery() {
    const dataverseName = this._getDataverseName();
    const fromClauseStr = this._getFromClause();
    
    // Validate the FROM clause.
    await this.validator.validateFromClause(dataverseName, fromClauseStr);
    
    // Parse the FROM clause into an array of dataset objects.
    const datasets = fromClauseStr.split(/\s*,\s*/).map(ds => {
      const parts = ds.trim().split(/\s+/);
      return { name: parts[0], alias: parts[1] || parts[0] };
    });
    
    // Validate SELECT clause only if not a VALUE query.
    if (!this._isValueSelect) {
      // Iterate directly over the SELECT AST children.
      for (const child of this.astSelect.children) {
        // Skip validation for subqueries.
        if (child.type.toUpperCase() === 'SUBQUERY') continue;
        
        const colExpr = child.value.trim();
        // Remove any aliasing by splitting on "AS".
        const parts = colExpr.split(/\s+AS\s+/i);
        const columnExpr = parts[0].trim();
        
        if (columnExpr.indexOf('.') !== -1) {
          const [alias, columnName] = columnExpr.split('.');
          let ds = datasets.find(ds => ds.alias === alias);
          if (!ds) {
            // If not found locally, check the outer aliases.
            if (!this._outerAliases || !this._outerAliases[alias]) {
              throw new Error(`Alias "${alias}" in SELECT clause does not match any dataset in FROM clause or outer query context`);
            }
            // If found in outerAliases, assume the column is valid.
          } else {
            await this.validator.validateColumns(dataverseName, ds.name, [columnName], false);
          }
        } else {
          // Unqualified column.
          if (datasets.length === 1) {
            await this.validator.validateColumns(dataverseName, datasets[0].name, [columnExpr], false);
          } else {
            throw new Error(`Ambiguous column "${columnExpr}" in SELECT clause. Please qualify the column with an alias.`);
          }
        }
      }
    }
    
    // Validate WHERE clause if present.
    const whereClause = this._getWhereClause();
    if (whereClause) {
      const whereColumns = whereClause.match(/\b[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?\b/g) || [];
      const sqlppKeywords = new Set([
        'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 
        'NULL', 'MISSING', 'UNKNOWN', 'VALUE', 'ELEMENT'
      ]);
      for (const token of whereColumns) {
        if (sqlppKeywords.has(token.toUpperCase())) continue;
        if (token.indexOf('.') !== -1) {
          const [alias, columnName] = token.split('.');
          let ds = datasets.find(ds => ds.alias === alias);
          if (!ds) {
            if (!this._outerAliases || !this._outerAliases[alias]) {
              throw new Error(`Alias "${alias}" in WHERE clause does not match any dataset in FROM clause or outer query context`);
            }
          } else {
            await this.validator.validateColumns(dataverseName, ds.name, [columnName], false);
          }
        }
      }
    }
    
    return true;
  }
  
  

  use(dataverse) {
    this.astUse = new ASTNode('USE', `USE ${dataverse};`);
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
    
    const valuePattern = /^VALUE\s+(.+)$/i;
    for (const col of columns) {
      if (valuePattern.test(col.trim())) {  // Ensure we trim the column.
        this._isValueSelect = true;
        break;
      }
    }
    
    if (!this.astSelect) {
      this.astSelect = new ASTNode('SELECT');
    }
    
    columns.forEach(col => {
      this.astSelect.addChild(new ASTNode('COLUMN', col));
    });
    return this;
  }
  
  
  

  async selectSubQuery(alias, builderFn) {
    // Create a nested QueryBuilder, passing a copy of the outer alias mapping.
    const nestedBuilder = new QueryBuilder({ ...this._outerAliases });
    
    // Execute the callback so the nested builder can build its own AST.
    await builderFn(nestedBuilder);
    
    // Build the subquery string (awaiting the asynchronous build).
    let subquery = await nestedBuilder.build();
    
    // Remove any trailing semicolon.
    if (subquery.trim().endsWith(";")) {
      subquery = subquery.trim().slice(0, -1);
    }
    
    // **NEW**: Remove any leading USE clause from the subquery.
    // This regex removes a leading "USE <dataverse>;" (with optional spaces).
    subquery = subquery.replace(/^USE\s+[a-zA-Z0-9_]+\s*;?\s*/i, '');
    
    // Create a subquery node with the alias.
    const subqueryNode = new ASTNode('SUBQUERY', `(${subquery}) AS ${alias}`);
    
    // Ensure the main SELECT node exists.
    if (!this.astSelect) {
      this.astSelect = new ASTNode('SELECT');
    }
    // Add the subquery node as a child of the SELECT node.
    this.astSelect.addChild(subqueryNode);
    
    return this;
  }
  
  
  
  

  /**
   * Modified from() method:
   * - If a string is provided, split on commas.
   * - For each dataset, check if an alias is provided; if not, default alias to the dataset name.
   */
  from(source) {
    if (this._queryType !== 'SELECT') {
      throw new Error("FROM clause is valid only for SELECT queries.");
    }
    // Create a FROM node.
    this.astFrom = new ASTNode('FROM', `FROM ${source}`);
    
    // Parse the source string into dataset objects.
    const datasets = source.split(/\s*,\s*/).map(ds => {
      const parts = ds.trim().split(/\s+/);
      return { name: parts[0], alias: parts[1] || parts[0] };
    });
    // Update the outer aliases mapping.
    for (const ds of datasets) {
      this._outerAliases[ds.alias] = ds.name;
    }
    return this;
  }
  
  

  where(condition) {
    this.astWhere = new ASTNode('WHERE', `WHERE ${condition}`);
    return this;
  }
  
  groupBy(expression) {
    this.astGroupBy = new ASTNode('GROUP BY', `GROUP BY ${expression}`);
    return this;
  }
  
  orderBy(expression) {
    this.astOrderBy = new ASTNode('ORDER BY', `ORDER BY ${expression}`);
    return this;
  }
  
  limit(n) {
    this.astLimit = new ASTNode('LIMIT', `LIMIT ${n}`);
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

//   /**
//    * Helper method: Returns a mapping of local alias → dataset name.
//    */
//   _getLocalAliasMapping() {
//     let mapping = {};
//     if (this._from && Array.isArray(this._from)) {
//       for (const ds of this._from) {
//         mapping[ds.alias] = ds.name;
//       }
//     }
//     return mapping;
//   }

//     /**
//      * Build a subquery as part of the SELECT clause.
//      * Note: Since build() is async, this method should be async as well.
//      *
//      * @param {string} alias - The alias for the subquery column.
//      * @param {function(QueryBuilder): Promise<void> | void} builderFn - A function
//      *   that receives a nested QueryBuilder instance to define the subquery.
//      */
//     async selectSubQuery(alias, builderFn) {
//         // Create a nested builder, passing the outer context (the current FROM mapping)
//         const nestedBuilder = new QueryBuilder(this._getLocalAliasMapping());
//         // Let the developer build the nested query.
//         await builderFn(nestedBuilder);
//         let subquery = await nestedBuilder.build();
//         // Remove trailing semicolon if present (for embedding within the parent query)
//         if (subquery.trim().endsWith(";")) {
//             subquery = subquery.trim().slice(0, -1);
//         }
//         // Append the subquery to the SELECT parts
//         this._selectParts.push(`(${subquery}) AS ${alias}`);
//         return this;
//         }

//   async validateQuery() {
//     if (!this._dataverse) {
//       throw new Error("Dataverse must be specified using the use() method.");
//     }

//     let validationSuccess = false;
//     try {
//       if (this._queryType === 'SELECT') {
//         if (this._selectParts.length === 0) {
//           throw new Error("SELECT clause is required for a SELECT query.");
//         }
//         validationSuccess = await this.validateSelectQuery();
//       } else if (this._queryType === 'INSERT') {
//         if (!this._insertInto || !this._values) {
//           throw new Error("Both INSERT INTO and VALUES clauses are required for an INSERT query.");
//         }
//         validationSuccess = await this.validateInsertQuery();
//       }
//     } catch (error) {
//       console.error('Validation error:', error);
//       throw error;
//     }

//     if (!validationSuccess) {
//       throw new Error("Query validation failed.");
//     }

//     return true;
//   }

//   /**
//    * Modified validateSelectQuery:
//    * - Build a proper FROM clause string using the parsed _from array.
//    * - Validate each dataset using the validator’s validateFromClause.
//    * - (Optionally, you can also validate that SELECT/WHERE columns match the correct dataset using the alias.)
//    */
//   async validateSelectQuery() {
//     if (!this._from || this._from.length === 0) {
//       throw new Error("SELECT queries require a FROM clause.");
//     }

//     // Build the FROM clause string that includes aliases (if any)
//     const fromClauseStr = this._from
//       .map(ds => ds.alias === ds.name ? ds.name : `${ds.name} ${ds.alias}`)
//       .join(', ');
//     // Validate each dataset in the FROM clause
//     await this._validator.validateFromClause(this._dataverse, fromClauseStr);

//     // Example column validation:
//     // For each SELECT part that is a qualified column (alias.column), validate that column exists.
//     // (This logic assumes that in multi-dataset queries, columns are always prefixed by an alias.)
//     for (const selectPart of this._selectParts) {
//       // Remove any aliasing in the SELECT part (e.g. "user.name AS uname")
//       const parts = selectPart.split(/\s+AS\s+/i);
//       const columnExpr = parts[0].trim();
//       if (columnExpr.indexOf('.') !== -1) {
//         const [alias, columnName] = columnExpr.split('.');
//         const ds = this._from.find(ds => ds.alias === alias);
//         if (!ds) {
//           throw new Error(`Alias ${alias} in SELECT clause does not match any dataset in FROM clause`);
//         }
//         // Validate that the column exists in the corresponding dataset.
//         await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
//       } else {
//         // If unqualified and there is only one dataset, validate against it.
//         if (this._from.length === 1) {
//           await this._validator.validateColumns(this._dataverse, this._from[0].name, [columnExpr], false);
//         } else {
//           throw new Error(`Ambiguous column "${columnExpr}" in SELECT clause for multi-dataset query. Please qualify the column with an alias.`);
//         }
//       }
//     }

//     // Similar approach for WHERE clause column validation.
//     if (this._where) {
//       const whereColumns = this.extractColumnsFromCondition(this._where);
//       for (const fullColumn of whereColumns) {
//         if (fullColumn.indexOf('.') !== -1) {
//           const [alias, columnName] = fullColumn.split('.');
//           const ds = this._from.find(ds => ds.alias === alias);
//           if (!ds) {
//             throw new Error(`Alias ${alias} in WHERE clause does not match any dataset in FROM clause`);
//           }
//           await this._validator.validateColumns(this._dataverse, ds.name, [columnName], false);
//         } else {
//           if (this._from.length === 1) {
//             await this._validator.validateColumns(this._dataverse, this._from[0].name, [fullColumn], false);
//           } else {
//             throw new Error(`Ambiguous column "${fullColumn}" in WHERE clause for multi-dataset query. Please qualify the column with an alias.`);
//           }
//         }
//       }
//     }

//     return true;
//   }

//   async validateInsertQuery() {
//     const datasetName = this._insertInto;
//     // Validate dataset existence
//     const datasetExists = await this._validator.validateDataset(this._dataverse, datasetName);
//     if (!datasetExists) {
//       throw new Error(`Dataset ${datasetName} not found in dataverse ${this._dataverse}.`);
//     }
//     const values = JSON.parse(this._values);
//     const columnValues = new Map(Object.entries(values));
//     await this._validator.validateColumnTypes(this._dataverse, datasetName, columnValues);
//     return true;
//   }

async build() {
    // First, run validations.
    await this.validateQuery();
    
    // Create the root query node with an empty value.
    const root = new ASTNode('QUERY', ' ');
    
    if (this._setCommands.length > 0) {
      const setNode = new ASTNode('SET', this._setCommands.join('\n'));
      root.addChild(setNode);
    }
    
    if (this.astUse) {
      root.addChild(this.astUse);
    }
    
    if (this._queryType === 'SELECT') {
      if (!this.astSelect) {
        throw new Error("SELECT clause is required for a SELECT query.");
      }
      root.addChild(this.astSelect);
      if (this.astFrom) {
        root.addChild(this.astFrom);
      }
      if (this.astWhere) {
        root.addChild(this.astWhere);
      }
      if (this.astGroupBy) {
        root.addChild(this.astGroupBy);
      }
      if (this.astOrderBy) {
        root.addChild(this.astOrderBy);
      }
      if (this.astLimit) {
        root.addChild(this.astLimit);
      }
    } else if (this._queryType === 'INSERT') {
      if (!this.astInsertInto || !this.astValues) {
        throw new Error("Both INSERT INTO and VALUES clauses are required for an INSERT query.");
      }
      root.addChild(this.astInsertInto);
      root.addChild(this.astValues);
    } else {
      throw new Error("No query type specified. Use select() or insertInto() to start building a query.");
    }
    
    // Serialize the AST.
    let queryStr = root.serialize().trim();
    if (!queryStr.endsWith(';')) {
      queryStr += ';';
    }
    
    console.log("Generated query:", queryStr);
    return queryStr;
  }
  
  
  
  
  

//   /**
//    * Modified constructQueryString:
//    * - Use the parsed _from array to build a correct FROM clause.
//    */
//   constructQueryString() {
//     let queryStr = "";

//     if (this._setCommands.length > 0) {
//       queryStr += this._setCommands.join('\n') + "\n";
//     }

//     if (this._dataverse) {
//       queryStr += `USE ${this._dataverse};\n`;
//     }

//     if (this._queryType === 'SELECT') {
//       const selectClause = this._selectParts.join(', ');
//       queryStr += `SELECT ${selectClause}`;

//       if (this._from && this._from.length > 0) {
//         const fromClauseStr = this._from
//           .map(ds => ds.alias === ds.name ? ds.name : `${ds.name} ${ds.alias}`)
//           .join(', ');
//         queryStr += ` FROM ${fromClauseStr}`;
//       }

//       if (this._where) {
//         queryStr += ` WHERE ${this._where}`;
//       }

//       if (this._groupBy) {
//         queryStr += ` GROUP BY ${this._groupBy}`;
//       }

//       if (this._orderBy) {
//         queryStr += ` ORDER BY ${this._orderBy}`;
//       }

//       if (this._limit !== null && this._limit !== undefined) {
//         queryStr += ` LIMIT ${this._limit}`;
//       }

//       queryStr += ";";

//     } else {
//       throw new Error("No query type specified. Use select() or insertInto() to start building a query.");
//     }

//     return queryStr;
//   }

//   extractColumnsFromCondition(condition) {
//     if (!condition) return [];
    
//     // Remove block comments (e.g. /*+ indexnl */)
//     const cleanCondition = condition.replace(/\/\*[\s\S]*?\*\//g, '');
    
//     // Use regex to find qualified column names
//     const pathRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\b/g;
//     const matches = [...cleanCondition.matchAll(pathRegex)];
    
//     // SQL++ keywords that should be ignored
//     const sqlppKeywords = new Set([
//       'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 
//       'NULL', 'MISSING', 'UNKNOWN', 'VALUE', 'ELEMENT'
//     ]);
    
//     return matches
//       .map(match => match[1])
//       .filter(column => {
//         const parts = column.split('.');
//         // Filter out tokens that are SQL++ keywords
//         return !parts.some(part => sqlppKeywords.has(part.toUpperCase()));
//       });
//   }
  
}

module.exports = QueryBuilder;
