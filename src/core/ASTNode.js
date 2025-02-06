// File: /src/core/ASTNode.js

/**
 * Represents a node in the Abstract Syntax Tree (AST) for SQL++ queries.
 * Each node corresponds to a part of a SQL++ query (e.g., SELECT, FROM, WHERE).
 * The AST is used to construct and serialize SQL++ queries that adhere to AsterixDB's SQL++ syntax.
 */
class ASTNode {
    /**
     * Creates an instance of ASTNode.
     * @param {string} type - The type of the AST node (e.g., 'SELECT', 'FROM', 'WHERE').
     * @param {string|null} value - The SQL fragment or literal value associated with this node.
     */
    constructor(type, value = null) {
      this.type = type;
      this.value = value;
      this.children = [];
    }
  
    /**
     * Adds a child node to this node.
     * This allows the building of a tree structure that represents a full SQL++ query.
     * @param {ASTNode} node - The child AST node to add.
     */
    addChild(node) {
      this.children.push(node);
    }
  
    /**
     * Serializes the AST node (and its children) into a SQL++ string.
     * This method constructs the SQL++ fragment by concatenating this node's value (or type)
     * with the serialized forms of its children, separated by spaces.
     *
     * @returns {string} The serialized SQL++ query fragment.
     */
    serialize() {
      // Use the provided value if available, otherwise fall back to the node's type.
      let sqlFragment = this.value ? this.value : this.type;
  
      if (this.children.length > 0) {
        // Serialize each child and join them with a space.
        const childrenSql = this.children.map(child => child.serialize()).join(' ');
        sqlFragment += ' ' + childrenSql;
      }
  
      return sqlFragment;
    }
  }
  
  module.exports = ASTNode;
  