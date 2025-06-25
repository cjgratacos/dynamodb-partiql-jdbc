package org.cjgratacos.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * ResultSetMetaData implementation for DynamoDB PartiQL query results.
 *
 * <p>This class provides metadata about the columns in a DynamoDB ResultSet, including column
 * names, types, and other properties. Since DynamoDB is a NoSQL database with flexible schemas, the
 * metadata is inferred from the actual data in the result set.
 *
 * <h2>Type Mapping:</h2>
 *
 * <ul>
 *   <li><strong>DynamoDB S (String)</strong>: VARCHAR
 *   <li><strong>DynamoDB N (Number)</strong>: NUMERIC
 *   <li><strong>DynamoDB BOOL</strong>: BOOLEAN
 *   <li><strong>DynamoDB B (Binary)</strong>: VARBINARY
 *   <li><strong>DynamoDB SS, NS, L, M</strong>: OTHER (complex types)
 *   <li><strong>DynamoDB NULL</strong>: NULL
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class DynamoDbResultSetMetaData implements ResultSetMetaData {

  private final List<ColumnInfo> columns;

  /**
   * Creates a new ResultSetMetaData from the data in a result set.
   *
   * @param items the result set items to analyze for metadata
   */
  public DynamoDbResultSetMetaData(final List<Map<String, AttributeValue>> items) {
    this(items, null);
  }

  /**
   * Creates a new ResultSetMetaData from the data in a result set with key information.
   *
   * @param items the result set items to analyze for metadata
   * @param keyInfo table key information for column ordering (optional)
   */
  public DynamoDbResultSetMetaData(
      final List<Map<String, AttributeValue>> items, final TableKeyInfo keyInfo) {
    this.columns = this.analyzeColumns(items, keyInfo);
  }

  /**
   * Analyzes the result set items to determine column metadata.
   *
   * @param items the result set items
   * @param keyInfo table key information for column ordering (optional)
   * @return list of column information
   */
  private List<ColumnInfo> analyzeColumns(
      final List<Map<String, AttributeValue>> items, final TableKeyInfo keyInfo) {
    final List<ColumnInfo> columnList = new ArrayList<>();

    if (items == null || items.isEmpty()) {
      return columnList;
    }

    // Collect all unique column names from all rows
    final Set<String> allColumnNames = new java.util.LinkedHashSet<>();
    for (final Map<String, AttributeValue> item : items) {
      if (item != null) {
        allColumnNames.addAll(item.keySet());
      }
    }

    // Separate columns into key columns and non-key columns if key info is provided
    if (keyInfo != null) {
      final List<String> orderedColumns = new ArrayList<>();
      final List<String> nonKeyColumns = new ArrayList<>();

      // First, add primary key columns in order
      for (final String primaryKey : keyInfo.getPrimaryKeyColumns()) {
        if (allColumnNames.contains(primaryKey)) {
          orderedColumns.add(primaryKey);
        }
      }

      // Then, add secondary key columns that aren't already added
      for (final String secondaryKey : keyInfo.getSecondaryKeyColumns()) {
        if (allColumnNames.contains(secondaryKey) && !orderedColumns.contains(secondaryKey)) {
          orderedColumns.add(secondaryKey);
        }
      }

      // Finally, add all other columns
      for (final String columnName : allColumnNames) {
        if (!orderedColumns.contains(columnName)) {
          nonKeyColumns.add(columnName);
        }
      }

      // Combine all columns in the desired order
      orderedColumns.addAll(nonKeyColumns);

      // Create column info for each column in the ordered list
      int columnIndex = 1;
      for (final String columnName : orderedColumns) {
        final int sqlType = this.determineSqlType(columnName, items);
        final String typeName = this.getTypeName(sqlType);
        final ColumnInfo column = new ColumnInfo(columnIndex++, columnName, sqlType, typeName);
        columnList.add(column);
      }
    } else {
      // No key info provided, use original ordering
      int columnIndex = 1;
      for (final String columnName : allColumnNames) {
        final int sqlType = this.determineSqlType(columnName, items);
        final String typeName = this.getTypeName(sqlType);
        final ColumnInfo column = new ColumnInfo(columnIndex++, columnName, sqlType, typeName);
        columnList.add(column);
      }
    }

    return columnList;
  }

  /** Determines the SQL type for a column by examining its values across all rows. */
  private int determineSqlType(
      final String columnName, final List<Map<String, AttributeValue>> items) {
    for (final Map<String, AttributeValue> item : items) {
      final AttributeValue value = item.get(columnName);
      if (value != null && !Boolean.TRUE.equals(value.nul())) {
        return this.mapAttributeValueToSqlType(value);
      }
    }
    return Types.OTHER; // Default for unknown/null columns
  }

  /** Maps a DynamoDB AttributeValue to the corresponding SQL type. */
  private int mapAttributeValueToSqlType(final AttributeValue value) {
    if (value.s() != null) {
      return Types.VARCHAR;
    }

    if (value.n() != null) {
      return Types.NUMERIC;
    }

    if (value.bool() != null) {
      return Types.BOOLEAN;
    }

    if (value.b() != null) {
      return Types.VARBINARY;
    }

    if (Boolean.TRUE.equals(value.nul())) {
      return Types.NULL;
    }

    // Complex types (SS, NS, L, M) or unknown types
    return Types.OTHER;
  }

  /** Gets the type name for a SQL type. */
  private String getTypeName(final int sqlType) {
    return switch (sqlType) {
      case Types.VARCHAR -> "VARCHAR";
      case Types.NUMERIC -> "NUMERIC";
      case Types.BOOLEAN -> "BOOLEAN";
      case Types.VARBINARY -> "VARBINARY";
      case Types.NULL -> "NULL";
      default -> "OTHER";
    };
  }

  @Override
  public int getColumnCount() throws SQLException {
    return this.columns.size();
  }

  @Override
  public String getColumnName(final int column) throws SQLException {
    this.validateColumnIndex(column);
    return this.columns.get(column - 1).name;
  }

  @Override
  public String getColumnLabel(final int column) throws SQLException {
    return this.getColumnName(column);
  }

  @Override
  public int getColumnType(final int column) throws SQLException {
    this.validateColumnIndex(column);
    return this.columns.get(column - 1).sqlType;
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException {
    this.validateColumnIndex(column);
    return this.columns.get(column - 1).typeName;
  }

  @Override
  public String getColumnClassName(final int column) throws SQLException {
    this.validateColumnIndex(column);
    final int sqlType = this.columns.get(column - 1).sqlType;
    return switch (sqlType) {
      case Types.VARCHAR -> "java.lang.String";
      case Types.NUMERIC -> "java.math.BigDecimal";
      case Types.BOOLEAN -> "java.lang.Boolean";
      case Types.VARBINARY -> "byte[]";
      case Types.NULL -> "java.lang.Object";
      default -> "java.lang.Object";
    };
  }

  @Override
  public boolean isAutoIncrement(final int column) throws SQLException {
    return false; // DynamoDB doesn't have auto-increment
  }

  @Override
  public boolean isCaseSensitive(final int column) throws SQLException {
    this.validateColumnIndex(column);
    return this.columns.get(column - 1).sqlType == Types.VARCHAR;
  }

  @Override
  public boolean isSearchable(final int column) throws SQLException {
    return true; // All columns are searchable in DynamoDB
  }

  @Override
  public boolean isCurrency(final int column) throws SQLException {
    return false; // DynamoDB doesn't have currency type
  }

  @Override
  public int isNullable(final int column) throws SQLException {
    return ResultSetMetaData.columnNullable; // DynamoDB columns can be null
  }

  @Override
  public boolean isSigned(final int column) throws SQLException {
    this.validateColumnIndex(column);
    return this.columns.get(column - 1).sqlType == Types.NUMERIC;
  }

  @Override
  public int getColumnDisplaySize(final int column) throws SQLException {
    this.validateColumnIndex(column);
    final int sqlType = this.columns.get(column - 1).sqlType;
    return switch (sqlType) {
      case Types.VARCHAR -> 255; // Default display size for strings
      case Types.NUMERIC -> 20; // Default display size for numbers
      case Types.BOOLEAN -> 5; // "true" or "false"
      default -> 50;
    };
  }

  @Override
  public int getPrecision(final int column) throws SQLException {
    this.validateColumnIndex(column);
    final int sqlType = this.columns.get(column - 1).sqlType;
    return switch (sqlType) {
      case Types.NUMERIC -> 38; // DynamoDB supports up to 38 digits
      default -> 0;
    };
  }

  @Override
  public int getScale(final int column) throws SQLException {
    this.validateColumnIndex(column);
    final int sqlType = this.columns.get(column - 1).sqlType;
    return switch (sqlType) {
      case Types.NUMERIC -> 10; // Default scale for numeric
      default -> 0;
    };
  }

  @Override
  public String getTableName(final int column) throws SQLException {
    return ""; // Table name not available in result set context
  }

  @Override
  public String getCatalogName(final int column) throws SQLException {
    return ""; // DynamoDB doesn't have catalogs
  }

  @Override
  public String getSchemaName(final int column) throws SQLException {
    return ""; // DynamoDB doesn't have schemas
  }

  @Override
  public boolean isReadOnly(final int column) throws SQLException {
    return true; // Result sets are read-only
  }

  @Override
  public boolean isWritable(final int column) throws SQLException {
    return false; // Result sets are read-only
  }

  @Override
  public boolean isDefinitelyWritable(final int column) throws SQLException {
    return false; // Result sets are read-only
  }

  /** Validates that the column index is within valid range. */
  private void validateColumnIndex(final int column) throws SQLException {
    if (column < 1 || column > this.columns.size()) {
      throw new SQLException("Column index out of range: " + column);
    }
  }

  /** Simple data class to hold column information. */
  private static class ColumnInfo {
    final int index;
    final String name;
    final int sqlType;
    final String typeName;

    ColumnInfo(final int index, final String name, final int sqlType, final String typeName) {
      this.index = index;
      this.name = name;
      this.sqlType = sqlType;
      this.typeName = typeName;
    }
  }

  // Unsupported methods for JDBC metadata
  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    throw new SQLException("Unwrap not supported");
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }
}
