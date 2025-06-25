package org.cjgratacos.jdbc;

/**
 * Data class to hold extracted table and index names from queries. DynamoDB PartiQL supports
 * querying specific indexes using the syntax: SELECT * FROM "table"."index"
 */
public class TableIndexInfo {
  private final String tableName;
  private final String indexName;

  /**
   * Creates a new TableIndexInfo instance.
   *
   * @param tableName the table name (required)
   * @param indexName the index name (optional, null for primary table queries)
   */
  public TableIndexInfo(String tableName, String indexName) {
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be null or empty");
    }
    this.tableName = tableName;
    this.indexName = indexName;
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the index name.
   *
   * @return the index name, or null if querying the primary table
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Checks if this query targets a specific index.
   *
   * @return true if an index is specified, false otherwise
   */
  public boolean hasIndex() {
    return indexName != null && !indexName.isEmpty();
  }

  /**
   * Checks if this query targets the primary index. In DynamoDB, "PRIMARY" is a special index name
   * representing the main table.
   *
   * @return true if targeting the PRIMARY index explicitly
   */
  public boolean isPrimaryIndex() {
    return "PRIMARY".equalsIgnoreCase(indexName);
  }

  /**
   * Returns the full qualified name for use in PartiQL queries.
   *
   * @return "table"."index" if index is specified, otherwise just "table"
   */
  public String getQualifiedName() {
    if (hasIndex() && !isPrimaryIndex()) {
      return "\"" + tableName + "\".\"" + indexName + "\"";
    } else {
      return "\"" + tableName + "\"";
    }
  }

  @Override
  public String toString() {
    return "TableIndexInfo{"
        + "tableName='"
        + tableName
        + '\''
        + ", indexName='"
        + indexName
        + '\''
        + '}';
  }
}
