const Validator = require('./Validator');
const ASTNode = require('./ASTNode');

class QueryBuilder {
  constructor(options = {}) {
    this._outerAliases = options.outerAliases || {};
    this._dataverseNodeFromParent = options.dataverseNodeFromParent; // For subqueries
    this._pendingSubQueries = []; // To store definitions for deferred subquery builds

    this._setCommands = [];
    this._queryType = null;
    this._isValueSelect = false;
    
    // AST nodes for each clause.
    this.astUse = null;
    this.astSelect = null;
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
    const useNode = this.astUse || this._dataverseNodeFromParent; 
    if (!useNode || !useNode.value) {
      throw new Error("Dataverse is not specified. Use the use() method or ensure it's provided from a parent query context.");
    }
    return useNode.value.replace(/^USE\s+/i, '').replace(/;$/, '').trim();
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
    return this.astSelect.children.map(child => child.value).join(', ');
  }
  
  _getWhereClause() {
    if (!this.astWhere || !this.astWhere.value) {
      return null;
    }
    return this.astWhere.value.replace(/^WHERE\s+/i, '').trim();
  }

  async validateQuery() {
    if (this._queryType === 'INSERT') {
      if (!this.astInsertInto || !this.astValues) {
        throw new Error("Both INSERT INTO and VALUES clauses are required for an INSERT query.");
      }
      return true;
    }
    
    const dataverseName = this._getDataverseName();
    const fromClauseStr = this._getFromClause();
    
    await this.validator.validateFromClause(dataverseName, fromClauseStr);
    
    const datasets = fromClauseStr.split(/\s*,\s*/).map(ds => {
      const parts = ds.trim().split(/\s+/);
      const name = parts[0];
      const alias = parts.length > 1 && parts[1] ? parts[1] : parts[0];
      return { name, alias };
    });
    
    if (!this._isValueSelect && this.astSelect) {
      for (const child of this.astSelect.children) {
        const colExpr = child.value.trim();

        if (colExpr.startsWith('(') && /\)\s+AS\s+\w+/i.test(colExpr)) {
          continue;
        }

        const parts = colExpr.split(/\s+AS\s+/i);
        const columnExpr = parts[0].trim();

        if (columnExpr.startsWith('(')) {
          continue;
        }

        if (columnExpr.indexOf('.') !== -1) {
          const [alias, ...restOfCol] = columnExpr.split('.');
          const columnName = restOfCol.join('.');
          let ds = datasets.find(d => d.alias === alias);
          if (!ds) {
            if (!this._outerAliases || !this._outerAliases[alias]) {
              throw new Error(`Alias "${alias}" in SELECT clause (from column expression "${columnExpr}") does not match any dataset in FROM clause or outer query context`);
            }
          } else {
            await this.validator.validateColumns(dataverseName, ds.name, [columnName.split('.')[0]], false);
          }
        } else {
          if (columnExpr === '*') continue;
          const isKnownAlias = datasets.some(d => d.alias === columnExpr) || (this._outerAliases && this._outerAliases[columnExpr]);
          const isFunctionCall = /\w+\(.*\)/.test(columnExpr);
          const isLiteralOrKeywordLike = /^([A-Z_]+|\d+|\"[^\\\"]*\"|\'[^\\\']*\')/.test(columnExpr);

          if (!isKnownAlias && !isFunctionCall && !isLiteralOrKeywordLike && datasets.length === 1) {
            await this.validator.validateColumns(dataverseName, datasets[0].name, [columnExpr], false);
          } else if (!isKnownAlias && !isFunctionCall && !isLiteralOrKeywordLike && datasets.length > 1) {
          }
        }
      }
    }
    
    const whereClause = this._getWhereClause();
    if (whereClause) {
      const potentialIdentifiers = whereClause.match(/\b[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*\b/g) || [];
      const sqlppKeywords = new Set([
        'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'MISSING', 'UNKNOWN',
        'VALUE', 'ELEMENT', 'FROM', 'WHERE', 'SELECT', 'GROUP', 'ORDER', 'BY', 'LIMIT',
        'USE', 'SET', 'INSERT', 'INTO', 'VALUES', 'SOME', 'EVERY', 'SATISFIES', 'AS',
        'ASC', 'DESC', 'JOIN', 'ON', 'LET', 'WITH', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
      ]);

      const hasIterationPattern = /(SOME|EVERY|FOR)\s+\w+\s+IN/i.test(whereClause);

      for (const token of potentialIdentifiers) {
        if (sqlppKeywords.has(token.toUpperCase())) continue;
        if (/^\".*\"$/.test(token) || /^\'.*\'$/.test(token) || !isNaN(parseFloat(token))) continue;

        if (token.indexOf('.') !== -1) {
          const [alias, ...rest] = token.split('.');
          const columnName = rest.join('.');

          let ds = datasets.find(d => d.alias === alias);
          if (!ds) { 
            if (!this._outerAliases || !this._outerAliases[alias]) {
              const rangeVarPattern = new RegExp(`\\b(SOME|EVERY|FOR)\\s+${alias}\\s+IN\\b`, 'i');
              if (hasIterationPattern && rangeVarPattern.test(whereClause)) {
              } else {
                throw new Error(`Alias "${alias}" in WHERE clause (from token "${token}") does not match any dataset in FROM clause or outer query context, and does not appear to be a local range variable.`);
              }
            }
          } else { 
            await this.validator.validateColumns(dataverseName, ds.name, [columnName.split('.')[0]], false);
          }
        } else { 
          const isFromAlias = datasets.some(d => d.alias === token);
          const isOuterAlias = this._outerAliases && this._outerAliases[token];
          
          if (!isFromAlias && !isOuterAlias && datasets.length === 1) {
            if (!/\w+\(.*\)/.test(token) && !(/^([A-Z_]+|\d+|\"[^\"]*\"|\'[^\\\']*\')/.test(token))) {
                await this.validator.validateColumns(dataverseName, datasets[0].name, [token], false);
            }
          } else if (!isFromAlias && !isOuterAlias && hasIterationPattern) {
            const rangeVarPatternDirect = new RegExp(`\\b(SOME|EVERY|FOR)\\s+${token}\\s+IN\\b`, 'i');
            if (rangeVarPatternDirect.test(whereClause)) {
            } else {
            }
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
      if (valuePattern.test(col.trim())) {  
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
  
  
  

  // selectSubQuery is now synchronous
  selectSubQuery(alias, builderFn) {
    if (this._queryType && this._queryType !== 'SELECT') {
      throw new Error("Cannot mix SELECT with INSERT query parts.");
    }
    this._queryType = 'SELECT';

    if (!this.astSelect) {
      this.astSelect = new ASTNode('SELECT');
    }

    const subQueryDefinition = { alias, builderFn, type: 'SUBQUERY_PLACEHOLDER' };
    this._pendingSubQueries.push(subQueryDefinition);

    this.astSelect.addChild(new ASTNode('SUBQUERY_PLACEHOLDER', alias));
    return this;
  }
  
  
  
  

  /**
   * from() method:
   * - If a string is provided, split on commas.
   * - For each dataset, check if an alias is provided; if not, default alias to the dataset name.
   */
  from(source) {
    if (this._queryType !== 'SELECT') {
      throw new Error("FROM clause is valid only for SELECT queries.");
    }
    this.astFrom = new ASTNode('FROM', `FROM ${source}`);

    const localDatasets = source.split(/\s*,\s*/).map(ds_str => {
      const trimmed_ds_str = ds_str.trim();
      const parts = trimmed_ds_str.split(/\s+/);
      
      const name = parts[0];
      const alias = parts.length > 1 && parts[1] ? parts[1] : parts[0];
      
      return { name, alias };
    });

    for (const ds_obj of localDatasets) {
      this._outerAliases[ds_obj.alias] = ds_obj.name;
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
    // Create proper ASTNode for INSERT clause
    this.astInsertInto = new ASTNode('INSERT', `INSERT INTO ${table}`);
    return this;
  }

  values(data) {
    if (this._queryType !== 'INSERT') {
      throw new Error("VALUES clause is valid only for INSERT queries.");
    }
    
    // Convert data to SQL++ format
    let dataExpressionString;
    if (Array.isArray(data)) {

      if (data.length > 1) {

        throw new Error("Inserting multiple records in a single .values() call with automatic formatting is not yet supported for SQL++. Please insert one object at a time or provide a pre-formatted collection string.");
      } else if (data.length === 1) {
        dataExpressionString = `(${this._formatValueObject(data[0])})`;
      } else {
        throw new Error("Cannot insert empty data.");
      }
    } else {
      dataExpressionString = `(${this._formatValueObject(data)})`;
    }

    this.astValues = new ASTNode('INSERT_DATA', dataExpressionString);
    return this;
  }
  
  _formatValueObject(obj) {
    const parts = [];
    
    for (const [key, value] of Object.entries(obj)) {
      let formattedValue;
      
      if (value === null) {
        formattedValue = 'null'; // SQL++ null literal
      } else if (value === undefined) {

        formattedValue = 'null'; 
      } else if (typeof value === 'string') {
        // Escape double quotes in strings and ensure the string is double-quoted
        formattedValue = `"${value.replace(/"/g, '\\"')}"`;
      } else if (typeof value === 'number' || typeof value === 'boolean') {
        formattedValue = value.toString();
      } else if (value instanceof Date) {
        // Format date as ISO string for datetime constructor
        formattedValue = `datetime("${value.toISOString()}")`;
      } else if (Array.isArray(value)) {
        // Format array: [val1, val2, ...]
        const elements = value.map(elem => {

          if (typeof elem === 'string') {
            return `"${elem.replace(/"/g, '\\"')}"`;
          } else if (typeof elem === 'number' || typeof elem === 'boolean') {
            return elem.toString();
          } else if (elem === null) {
            return 'null';
          }
          // For nested objects/arrays within arrays, JSON.stringify might be a shortcut,
          // but a proper SQL++ formatter would be ideal.
          return JSON.stringify(elem); // Fallback for complex array elements
        });
        formattedValue = `[${elements.join(', ')}]`;
      } else if (typeof value === 'object') {
        formattedValue = JSON.stringify(value); 
      } else {

        throw new Error(`Unsupported data type for field '${key}': ${typeof value}`);
      }
      
      parts.push(`"${key}": ${formattedValue}`); // Keys must be double-quoted strings
    }
    
    return `{${parts.join(', ')}}`; // Return a SQL++ object constructor string: { "k1": v1, "k2": v2 }
  }

  async build() {
    if (this.astSelect && this._pendingSubQueries.length > 0) {
      const newSelectChildren = [];
      let pendingIndex = 0;
      for (const childNode of this.astSelect.children) {
        if (childNode.type === 'SUBQUERY_PLACEHOLDER' && pendingIndex < this._pendingSubQueries.length) {
          const subQueryDef = this._pendingSubQueries[pendingIndex++];
          const { alias, builderFn } = subQueryDef;

          const nestedBuilder = new QueryBuilder({
            outerAliases: { ...this._outerAliases },
            dataverseNodeFromParent: this.astUse 
          });

          await builderFn(nestedBuilder); 
          let subquerySql = await nestedBuilder.build();

          // Clean up the generated subquery string
          subquerySql = subquerySql.replace(/^USE\s+[a-zA-Z0-9_]+\s*;?\s*/i, '');
          if (subquerySql.trim().endsWith(";")) {
            subquerySql = subquerySql.trim().slice(0, -1);
          }
          newSelectChildren.push(new ASTNode('COLUMN', `(${subquerySql}) AS ${alias}`));
        } else {
          newSelectChildren.push(childNode);
        }
      }
      this.astSelect.children = newSelectChildren;
      this._pendingSubQueries = []; 
    }

    await this.validateQuery();

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
      if (!this.astUse && this._setCommands.length === 0) {
         throw new Error("No query type specified. Use select() or insertInto() to start building a query, or ensure USE/SET commands are present.");
      } else if (this.astUse && !this.astSelect && !this.astInsertInto && this.astFrom) {
      } else if (!this.astUse && !this.astSelect && !this.astInsertInto && this._setCommands.length > 0){
      } else if (!this.astUse && !this.astSelect && !this.astInsertInto && this._setCommands.length === 0 && !this.astFrom) {
         throw new Error("Query is empty or incomplete.");
      }
    }
    
    let queryStr = root.serialize().trim();
    if (!queryStr.endsWith(';') && (this._queryType === 'SELECT' || this._queryType === 'INSERT' || this._setCommands.length > 0)) {
      if(queryStr) queryStr += ';';
    } else if (!queryStr && (this._queryType === 'SELECT' || this._queryType === 'INSERT')) {
      throw new Error("Trying to build an empty SELECT/INSERT query.");
    }
    
    return queryStr;
  }
}

module.exports = QueryBuilder;
