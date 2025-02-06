// File: /src/core/QueryBuilder.js

/**
 * A robust QueryBuilder for SQL++ targeting AsterixDB.
 *
 * Supports:
 *  - Dataverse selection (USE clause)
 *  - SELECT queries with support for nested subqueries, existential/universal quantification,
 *    aggregation, grouping, ordering, and limits.
 *  - INSERT queries (with VALUES)
 *  - SET commands (for hints and other session settings)
 *
 * Nested subqueries can be embedded implicitly by using the new selectSubQuery() method.
 */
class QueryBuilder {
    constructor() {
      this._setCommands = [];
      this._dataverse = null;
      this._selectParts = []; // Array of strings for SELECT clause parts.
      this._from = null;
      this._where = null;
      this._groupBy = null;
      this._orderBy = null;
      this._limit = null;
      this._insertInto = null;
      this._values = null;
      this._queryType = null;
    }
  
    // Specify a dataverse (USE clause)
    use(dataverse) {
      this._dataverse = dataverse;
      return this;
    }
  
    // Specify SET commands.
    set(command, value) {
      this._setCommands.push(`SET ${command} "${value}";`);
      return this;
    }
  
    // Begin a SELECT query by specifying the SELECT clause.
    // Accepts a string or an array of strings.
    select(columns) {
      if (this._queryType && this._queryType !== 'SELECT') {
        throw new Error("Cannot mix SELECT with INSERT query parts.");
      }
      this._queryType = 'SELECT';
      if (!Array.isArray(columns)) {
        columns = [columns];
      }
      // Append each column to the select parts.
      this._selectParts = this._selectParts.concat(columns);
      return this;
    }
  
    /**
     * New method: selectSubQuery(alias, builderFn)
     * Allows embedding a nested subquery in the SELECT clause implicitly.
     *
     * @param {string} alias - The alias for the nested subquery.
     * @param {function(QueryBuilder): void} builderFn - A callback that receives a new QueryBuilder to build the nested query.
     * @returns {QueryBuilder} The current instance for chaining.
     */
    selectSubQuery(alias, builderFn) {
      const nestedBuilder = new QueryBuilder();
      builderFn(nestedBuilder);
      let subquery = nestedBuilder.build();
      if (subquery.endsWith(";")) {
        subquery = subquery.slice(0, -1);
      }
      // Append the nested subquery in parentheses with its alias.
      this._selectParts.push(`(${subquery}) AS ${alias}`);
      return this;
    }
  
    // Specify the FROM clause.
    from(source) {
      if (this._queryType !== 'SELECT') {
        throw new Error("FROM clause is valid only for SELECT queries.");
      }
      this._from = source;
      return this;
    }
  
    // Specify the WHERE clause.
    where(condition) {
      this._where = condition;
      return this;
    }
  
    // Specify GROUP BY clause.
    groupBy(expression) {
      this._groupBy = expression;
      return this;
    }
  
    // Specify ORDER BY clause.
    orderBy(expression) {
      this._orderBy = expression;
      return this;
    }
  
    // Specify LIMIT clause.
    limit(n) {
      this._limit = n;
      return this;
    }
  
    // Begin an INSERT query.
    insertInto(table) {
      if (this._queryType && this._queryType !== 'INSERT') {
        throw new Error("Cannot mix INSERT with SELECT query parts.");
      }
      this._queryType = 'INSERT';
      this._insertInto = table;
      return this;
    }
  
    // Specify VALUES for an INSERT query.
    values(data) {
      if (this._queryType !== 'INSERT') {
        throw new Error("VALUES clause is valid only for INSERT queries.");
      }
      this._values = JSON.stringify(data, null, 2);
      return this;
    }
  
    // Build the final SQL++ query string.
    build() {
      let queryStr = "";
      if (this._setCommands.length > 0) {
        queryStr += this._setCommands.join('\n') + "\n";
      }
      if (this._dataverse) {
        queryStr += `USE ${this._dataverse};\n`;
      }
      if (this._queryType === 'INSERT') {
        if (!this._insertInto || !this._values) {
          throw new Error("Both INSERT INTO and VALUES clauses are required for an INSERT query.");
        }
        queryStr += `INSERT INTO ${this._insertInto}\n(${this._values});`;
      } else if (this._queryType === 'SELECT') {
        if (this._selectParts.length === 0) {
          throw new Error("SELECT clause is required for a SELECT query.");
        }
        const selectClause = this._selectParts.join(', ');
        queryStr += `SELECT ${selectClause}`;
        if (this._from) {
          queryStr += ` FROM ${this._from}`;
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
  }
  
  module.exports = QueryBuilder;
  