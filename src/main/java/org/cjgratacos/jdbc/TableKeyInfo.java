package org.cjgratacos.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Holds information about table keys (primary and secondary) for column ordering. */
public class TableKeyInfo {
  private final String tableName;
  private final List<String> primaryKeyColumns;
  private final List<String> secondaryKeyColumns;

  /**
   * Creates a new TableKeyInfo instance.
   *
   * @param tableName the name of the table
   * @param primaryKeyColumns the primary key columns (partition key, sort key)
   * @param secondaryKeyColumns the secondary index key columns
   */
  public TableKeyInfo(
      String tableName, List<String> primaryKeyColumns, List<String> secondaryKeyColumns) {
    this.tableName = tableName;
    this.primaryKeyColumns =
        new ArrayList<>(primaryKeyColumns != null ? primaryKeyColumns : Collections.emptyList());
    this.secondaryKeyColumns =
        new ArrayList<>(
            secondaryKeyColumns != null ? secondaryKeyColumns : Collections.emptyList());
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
   * Gets the primary key columns.
   *
   * @return unmodifiable list of primary key columns
   */
  public List<String> getPrimaryKeyColumns() {
    return Collections.unmodifiableList(primaryKeyColumns);
  }

  /**
   * Gets the secondary key columns.
   *
   * @return unmodifiable list of secondary key columns
   */
  public List<String> getSecondaryKeyColumns() {
    return Collections.unmodifiableList(secondaryKeyColumns);
  }

  /**
   * Checks if a column is a primary key column.
   *
   * @param columnName the column name to check
   * @return true if the column is a primary key column
   */
  public boolean isPrimaryKey(String columnName) {
    return primaryKeyColumns.contains(columnName);
  }

  /**
   * Checks if a column is a secondary key column.
   *
   * @param columnName the column name to check
   * @return true if the column is a secondary key column
   */
  public boolean isSecondaryKey(String columnName) {
    return secondaryKeyColumns.contains(columnName);
  }

  /**
   * Checks if a column is any type of key column.
   *
   * @param columnName the column name to check
   * @return true if the column is either a primary or secondary key column
   */
  public boolean isKeyColumn(String columnName) {
    return isPrimaryKey(columnName) || isSecondaryKey(columnName);
  }
}
