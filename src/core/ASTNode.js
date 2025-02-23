/**
 * Represents a node in the Abstract Syntax Tree (AST) for SQL++ queries.
 */
class ASTNode {
    /**
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
     * @param {ASTNode} node - The child AST node to add.
     */
    addChild(node) {
        this.children.push(node);
    }

    /**
     * Serializes the AST node (and its children) into a SQL++ string.
     * @returns {string} The serialized SQL++ query fragment.
     */
    serialize() {
        // Use the provided value if available; otherwise fall back to the node's type.
        let sqlFragment = this.value ? this.value : this.type;
      
        if (this.children.length > 0) {
          // Determine the separator based on the node type.
          let separator = ' '; // default separator
          if (this.type.toUpperCase() === 'SELECT') {
            // For SELECT nodes, join children with a comma and a space.
            separator = ', ';
          }
          const childrenSql = this.children.map(child => child.serialize()).join(separator);
          sqlFragment += ' ' + childrenSql;
        }
      
        return sqlFragment;
    }
}

module.exports = ASTNode;
