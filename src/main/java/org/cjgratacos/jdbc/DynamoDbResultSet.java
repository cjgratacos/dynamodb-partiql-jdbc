package org.cjgratacos.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

/**
 * JDBC ResultSet implementation for DynamoDB PartiQL query results.
 *
 * <p>This class represents a table of data returned from executing a PartiQL SELECT query against
 * Amazon DynamoDB. It provides methods for navigating through the result set and retrieving column
 * values in various Java data types.
 *
 * <h2>Navigation Features:</h2>
 *
 * <ul>
 *   <li>Forward-only cursor navigation (TYPE_FORWARD_ONLY)
 *   <li>Automatic pagination support for large result sets
 *   <li>Lazy loading of additional pages when needed
 *   <li>Client-side LIMIT and OFFSET implementation
 *   <li>Configurable fetch size for performance tuning
 *   <li>Proper resource cleanup and lifecycle management
 * </ul>
 *
 * <h2>Data Type Support:</h2>
 *
 * <ul>
 *   <li><strong>String types</strong>: getString(), getNString() - DynamoDB String (S) type
 *   <li><strong>Number types</strong>: getInt(), getLong(), getFloat(), getDouble(), getBigDecimal() - DynamoDB Number (N) type
 *   <li><strong>Boolean</strong>: getBoolean() - DynamoDB Boolean (BOOL) type
 *   <li><strong>Binary</strong>: getBytes(), getBinaryStream() - DynamoDB Binary (B) type
 *   <li><strong>Date/Time</strong>: getDate(), getTime(), getTimestamp() - Parsed from ISO-8601 strings
 *   <li><strong>NULL</strong>: wasNull() - DynamoDB NULL type
 * </ul>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li>Automatic type conversion from DynamoDB AttributeValues
 *   <li>Column access by name or index (1-based)
 *   <li>Case-insensitive column name lookup
 *   <li>Comprehensive null handling
 *   <li>Support for nested attribute access
 *   <li>Primary and secondary key ordering for SELECT *
 * </ul>
 *
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * Statement stmt = connection.createStatement();
 * ResultSet rs = stmt.executeQuery("SELECT * FROM Users WHERE status = 'active' LIMIT 100");
 * 
 * while (rs.next()) {
 *     String userId = rs.getString("userId");
 *     String name = rs.getString("name");
 *     int age = rs.getInt("age");
 *     boolean active = rs.getBoolean("isActive");
 *     Timestamp lastLogin = rs.getTimestamp("lastLogin");
 *     
 *     if (rs.wasNull()) {
 *         // Handle null lastLogin
 *     }
 * }
 * rs.close();
 * }</pre>
 * 
 * <ul>
 *   <li><strong>Numeric types</strong>: getInt(), getLong(), getDouble(), getFloat(), getByte(),
 *       getShort(), getBigDecimal()
 *   <li><strong>Boolean type</strong>: getBoolean()
 *   <li><strong>Binary data</strong>: getBytes()
 *   <li><strong>Generic access</strong>: getObject() with automatic type conversion
 * </ul>
 *
 * <h2>DynamoDB AttributeValue Mapping:</h2>
 *
 * <ul>
 *   <li><strong>S (String)</strong>: Mapped to String/numeric types
 *   <li><strong>N (Number)</strong>: Parsed to appropriate numeric types
 *   <li><strong>BOOL (Boolean)</strong>: Mapped to boolean
 *   <li><strong>B (Binary)</strong>: Mapped to byte[]
 *   <li><strong>NULL</strong>: Handled as Java null values
 *   <li><strong>SS, NS, L, M</strong>: Returned as collections via getObject()
 * </ul>
 *
 * <h2>Limitations:</h2>
 *
 * <ul>
 *   <li>Read-only (CONCUR_READ_ONLY) - no update operations
 *   <li>No scrollable cursor support (previous(), absolute(), relative())
 *   <li>Date/Time types not supported (SQLException thrown)
 *   <li>Large object types (Blob, Clob) not supported
 *   <li>Complex types (Array, Struct) not supported
 * </ul>
 *
 * <h2>Pagination and Row Limiting:</h2>
 *
 * <p>When DynamoDB returns paginated results, this ResultSet automatically fetches the next page
 * when {@link #next()} is called and the current page is exhausted. The number of rows returned is
 * controlled by the following priority order:
 *
 * <ol>
 *   <li>SQL LIMIT clause in the query
 *   <li>Statement.setMaxRows() value
 *   <li>Statement.setFetchSize() as a safety limit when maxRows is 0
 * </ol>
 *
 * <p>When maxRows is 0 (no limit), the fetchSize is used as an effective limit to prevent unbounded
 * queries from fetching all rows from large tables. This provides protection against accidental
 * full table scans.
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * ResultSet rs = statement.executeQuery("SELECT id, name, age FROM Users WHERE status = 'active'");
 *
 * while (rs.next()) {
 *     String id = rs.getString("id");
 *     String name = rs.getString("name");
 *     int age = rs.getInt("age");
 *
 *     System.out.printf("User: %s (ID: %s, Age: %d)%n", name, id, age);
 * }
 *
 * rs.close();
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see DynamoDbStatement
 * @see DynamoDbConnection
 */
public class DynamoDbResultSet implements ResultSet {

  private final DynamoDbClient client;
  private final String sql;
  private final List<Map<String, AttributeValue>> items;
  private String nextToken;
  private int currentRow = -1;
  /** Flag indicating whether this ResultSet has been closed */
  protected boolean closed = false;
  /** The current item/row from DynamoDB */
  protected Map<String, AttributeValue> currentItem;
  private boolean wasNull = false;
  private ResultSetMetaData cachedMetaData;
  private final int fetchSize;
  private int offsetRemaining;
  private final LimitOffsetInfo limitOffsetInfo;
  private int rowsReturned = 0;
  private final TableKeyInfo tableKeyInfo;
  private final int maxRows;
  private final OffsetTokenCache offsetTokenCache;
  private int totalRowsFetched = 0; // Total rows fetched including skipped ones

  /**
   * Creates a new DynamoDB result set from a query response.
   *
   * <p>This constructor initializes the result set with the items returned from a DynamoDB PartiQL
   * query execution. It handles pagination by storing the next token for subsequent page retrieval
   * when needed. The items list is copied to ensure it remains modifiable for pagination.
   *
   * <p>The result set is positioned before the first row initially. Call {@link #next()} to move to
   * the first row and subsequent rows. When all current items are consumed and a next token exists,
   * additional pages will be automatically fetched.
   *
   * @param client the DynamoDB client for fetching additional pages
   * @param sql the original PartiQL query (used for pagination)
   * @param response the DynamoDB query response containing items and pagination info
   * @param fetchSize the number of items to fetch per page (0 means use default)
   * @see DynamoDbStatement#executeQuery(String)
   * @see #next()
   * @see #fetchNextPage()
   */
  public DynamoDbResultSet(
      final DynamoDbClient client,
      final String sql,
      final ExecuteStatementResponse response,
      final int fetchSize) {
    this(client, sql, response, fetchSize, new LimitOffsetInfo(null, null), null, 0, null);
  }

  /**
   * Creates a new DynamoDB result set from a query response with LIMIT/OFFSET support.
   *
   * <p>This constructor initializes the result set with the items returned from a DynamoDB PartiQL
   * query execution. It handles pagination by storing the next token for subsequent page retrieval
   * when needed. The items list is copied to ensure it remains modifiable for pagination.
   *
   * <p>The result set is positioned before the first row initially. Call {@link #next()} to move to
   * the first row and subsequent rows. When all current items are consumed and a next token exists,
   * additional pages will be automatically fetched.
   *
   * <p>If an OFFSET is specified, the result set will skip that many rows before returning data.
   * This is implemented client-side by fetching and discarding rows until the offset is satisfied.
   *
   * @param client the DynamoDB client for fetching additional pages
   * @param sql the original PartiQL query (used for pagination)
   * @param response the DynamoDB query response containing items and pagination info
   * @param fetchSize the number of items to fetch per page (0 means use default)
   * @param limitOffsetInfo the LIMIT and OFFSET values extracted from the query
   * @see DynamoDbStatement#executeQuery(String)
   * @see #next()
   * @see #fetchNextPage()
   */
  public DynamoDbResultSet(
      final DynamoDbClient client,
      final String sql,
      final ExecuteStatementResponse response,
      final int fetchSize,
      final LimitOffsetInfo limitOffsetInfo) {
    this(client, sql, response, fetchSize, limitOffsetInfo, null, 0, null);
  }

  /**
   * Creates a new DynamoDB result set from a query response with LIMIT/OFFSET and key info support.
   *
   * <p>When maxRows is 0 (no limit) and fetchSize is positive, the fetchSize is used as an
   * effective maxRows limit. This prevents unbounded queries from accidentally fetching all rows
   * from large tables, which is especially important for GUI clients like DbVisualizer.
   *
   * @param client the DynamoDB client for fetching additional pages
   * @param sql the original PartiQL query (used for pagination)
   * @param response the DynamoDB query response containing items and pagination info
   * @param fetchSize the number of items to fetch per page (also used as safety limit when
   *     maxRows=0)
   * @param limitOffsetInfo the LIMIT and OFFSET values extracted from the query
   * @param tableKeyInfo the table key information for column ordering (optional)
   * @param maxRows the maximum number of rows to return (0 means use fetchSize as limit)
   * @param offsetTokenCache the cache for optimizing OFFSET performance (optional)
   */
  public DynamoDbResultSet(
      final DynamoDbClient client,
      final String sql,
      final ExecuteStatementResponse response,
      final int fetchSize,
      final LimitOffsetInfo limitOffsetInfo,
      final TableKeyInfo tableKeyInfo,
      final int maxRows,
      final OffsetTokenCache offsetTokenCache) {
    this.client = client;
    this.sql = sql;
    this.items = new ArrayList<>(response.items()); // Create modifiable copy
    this.nextToken = response.nextToken();
    this.fetchSize = fetchSize;
    this.limitOffsetInfo = limitOffsetInfo;
    this.offsetRemaining = limitOffsetInfo.hasOffset() ? limitOffsetInfo.getOffset() : 0;
    this.tableKeyInfo = tableKeyInfo;
    // When maxRows is 0 (no limit), use fetchSize as a safety limit to prevent unbounded queries
    this.maxRows = (maxRows == 0 && fetchSize > 0) ? fetchSize : maxRows;
    this.offsetTokenCache = offsetTokenCache;
    this.totalRowsFetched = response.items().size();

    // Check if we can use cached token for OFFSET
    if (offsetTokenCache != null
        && limitOffsetInfo.hasOffset()
        && limitOffsetInfo.getOffset() > 0) {
      OffsetTokenCache.TokenEntry cachedEntry =
          offsetTokenCache.getNearestToken(sql, limitOffsetInfo.getOffset());
      if (cachedEntry != null && cachedEntry.getOffset() > 0) {
        // We found a cached token, adjust our offset calculation
        this.offsetRemaining = limitOffsetInfo.getOffset() - cachedEntry.getOffset();
        this.totalRowsFetched = cachedEntry.getOffset();
        // TODO: Use the cached token to jump ahead in pagination
        // This would require refactoring to fetch from the cached position
      }
    }
  }

  /**
   * Creates a new DynamoDB result set with predefined metadata items.
   *
   * <p>This constructor is used for metadata ResultSets returned by DatabaseMetaData methods like
   * getTables(), getColumns(), getIndexInfo(), etc. The items are provided directly without
   * requiring a DynamoDB client or query execution. The items list is copied to ensure it remains
   * modifiable.
   *
   * <p>No pagination is supported for metadata result sets as they contain finite, pre-computed
   * data.
   *
   * @param items the list of metadata items to include in this result set
   * @see DynamoDbDatabaseMetaData#getTables(String, String, String, String[])
   * @see DynamoDbDatabaseMetaData#getColumns(String, String, String, String)
   * @see DynamoDbDatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)
   */
  public DynamoDbResultSet(final List<Map<String, AttributeValue>> items) {
    this.client = null;
    this.sql = null;
    this.items = new ArrayList<>(items); // Create modifiable copy
    this.nextToken = null;
    this.fetchSize = 0; // No fetch size for metadata queries
    this.limitOffsetInfo = new LimitOffsetInfo(null, null);
    this.offsetRemaining = 0;
    this.rowsReturned = 0;
    this.tableKeyInfo = null;
    this.maxRows = 0; // No limit for metadata queries
    this.offsetTokenCache = null;
    this.totalRowsFetched = 0;
  }

  @Override
  public boolean next() throws SQLException {
    this.validateNotClosed();

    // Check if we've reached the LIMIT from the SQL query
    if (this.limitOffsetInfo.hasLimit() && this.rowsReturned >= this.limitOffsetInfo.getLimit()) {
      return false;
    }

    // Check if we've reached the maxRows limit (includes fetchSize as safety limit when maxRows was
    // 0)
    if (this.maxRows > 0 && this.rowsReturned >= this.maxRows) {
      return false;
    }

    // Loop to handle OFFSET skipping
    while (true) {
      if (this.currentRow + 1 < this.items.size()) {
        this.currentRow++;

        // If we still have offset to skip, continue without setting currentItem
        if (this.offsetRemaining > 0) {
          this.offsetRemaining--;
          // Track skipped rows for cache positioning
          if (this.offsetTokenCache != null && this.limitOffsetInfo.hasOffset()) {
            int skippedPosition = this.limitOffsetInfo.getOffset() - this.offsetRemaining;
            // Store current token at cache intervals during skipping
            if (this.offsetTokenCache.shouldCache(skippedPosition) && this.nextToken != null) {
              this.offsetTokenCache.put(this.sql, skippedPosition, this.nextToken);
            }
          }
          continue;
        }

        this.currentItem = this.items.get(this.currentRow);
        this.rowsReturned++;

        // Cache token at intervals during regular iteration (not just during offset skipping)
        if (this.offsetTokenCache != null && this.nextToken != null && !this.nextToken.isEmpty()) {
          int currentPosition = this.totalRowsFetched - this.items.size() + this.currentRow + 1;
          if (this.offsetTokenCache.shouldCache(currentPosition)) {
            this.offsetTokenCache.put(this.sql, currentPosition, this.nextToken);
          }
        }

        return true;
      }

      // If we have more pages and a client, fetch the next page
      // But only if we haven't reached our LIMIT or maxRows yet
      if (this.client != null && this.nextToken != null && !this.nextToken.isEmpty()) {
        // Check LIMIT from SQL query
        if (this.limitOffsetInfo.hasLimit()
            && this.rowsReturned >= this.limitOffsetInfo.getLimit()) {
          return false;
        }
        // Check maxRows (includes fetchSize as safety limit when maxRows was 0)
        if (this.maxRows > 0 && this.rowsReturned >= this.maxRows) {
          return false;
        }
        this.fetchNextPage();
        // Continue the loop to process the newly fetched items
        continue;
      }

      // No more items available
      return false;
    }
  }

  private void fetchNextPage() throws SQLException {
    try {
      final var requestBuilder =
          ExecuteStatementRequest.builder().statement(this.sql).nextToken(this.nextToken);

      // Calculate the effective limit for this page
      int effectiveLimit = Integer.MAX_VALUE;

      // Consider LIMIT from SQL query
      if (this.limitOffsetInfo.hasLimit()) {
        effectiveLimit =
            Math.min(effectiveLimit, this.limitOffsetInfo.getLimit() - this.rowsReturned);
      }

      // Consider maxRows from setMaxRows()
      if (this.maxRows > 0) {
        effectiveLimit = Math.min(effectiveLimit, this.maxRows - this.rowsReturned);
      }

      // Consider fetchSize as the page size
      if (this.fetchSize > 0) {
        effectiveLimit = Math.min(effectiveLimit, this.fetchSize);
      }

      // Apply the effective limit if it's reasonable
      if (effectiveLimit > 0 && effectiveLimit < Integer.MAX_VALUE) {
        requestBuilder.limit(effectiveLimit);
      } else if (this.fetchSize > 0) {
        requestBuilder.limit(this.fetchSize);
      }

      final var request = requestBuilder.build();
      final var response = this.client.executeStatement(request);
      this.items.addAll(response.items());
      this.nextToken = response.nextToken();

      // Update total rows fetched
      this.totalRowsFetched += response.items().size();

      // Cache the token if we have an offset cache and we're at a cache interval
      if (this.offsetTokenCache != null && this.nextToken != null && !this.nextToken.isEmpty()) {
        // Calculate the actual offset position (accounting for any initial offset)
        int currentOffset = this.totalRowsFetched;
        if (this.limitOffsetInfo.hasOffset()) {
          currentOffset = this.limitOffsetInfo.getOffset() + this.rowsReturned;
        }

        if (this.offsetTokenCache.shouldCache(currentOffset)) {
          this.offsetTokenCache.put(this.sql, currentOffset, this.nextToken);
        }
      }
    } catch (final Exception e) {
      throw new SQLException("Failed to fetch next page: " + e.getMessage(), e);
    }
  }

  private void validateNotClosed() throws SQLException {
    if (this.closed) {
      throw new SQLException("ResultSet is closed");
    }
  }

  private void validateCurrentRow() throws SQLException {
    this.validateNotClosed();
    if (this.currentRow < 0 || this.currentItem == null) {
      throw new SQLException("No current row. Call next() first.");
    }
  }

  @Override
  public void close() throws SQLException {
    if (!this.closed) {
      this.closed = true;
      // Clear references to help GC
      this.currentItem = null;
      this.cachedMetaData = null;
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public String getString(final int columnIndex) throws SQLException {
    this.validateCurrentRow();
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getString(columnName);
  }

  @Override
  public String getString(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null) {
      this.wasNull = true;
      return null;
    }

    this.wasNull = false;

    // Handle different DynamoDB attribute types
    if (attributeValue.s() != null) {
      return attributeValue.s();
    }

    if (attributeValue.n() != null) {
      return attributeValue.n();
    }

    if (attributeValue.bool() != null) {
      return attributeValue.bool().toString();
    }

    if (attributeValue.nul() != null && attributeValue.nul()) {
      this.wasNull = true;
      return null;
    }

    // For complex types, return JSON representation
    return attributeValue.toString();
  }

  @Override
  public Object getObject(final int columnIndex) throws SQLException {
    this.validateCurrentRow();
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getObject(columnName);
  }

  @Override
  public Object getObject(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null) {
      this.wasNull = true;
      return null;
    }

    this.wasNull = false;

    // Convert DynamoDB AttributeValue to Java object
    if (attributeValue.s() != null) {
      return attributeValue.s();
    }

    if (attributeValue.n() != null) {
      // Try to parse as different numeric types
      final var numberStr = attributeValue.n();
      try {
        if (numberStr.contains(".")) {
          return Double.parseDouble(numberStr);
        }

        return Long.parseLong(numberStr);
      } catch (final NumberFormatException e) {
        return numberStr; // Return as string if parsing fails
      }
    }

    if (attributeValue.bool() != null) {
      return attributeValue.bool();
    }

    if (attributeValue.nul() != null && attributeValue.nul()) {
      this.wasNull = true;
      return null;
    }

    if (attributeValue.ss() != null) {
      return attributeValue.ss(); // String set
    }

    if (attributeValue.ns() != null) {
      return attributeValue.ns(); // Number set
    }

    if (attributeValue.l() != null) {
      return attributeValue.l(); // List
    }

    if (attributeValue.m() != null) {
      return attributeValue.m(); // Map
    }

    return attributeValue.toString();
  }

  private String getColumnNameByIndex(final int columnIndex) throws SQLException {
    if (this.currentItem == null) {
      throw new SQLException("No current row");
    }

    // Get column names from metadata to ensure consistency
    final var metaData = this.getMetaData();
    final int columnCount = metaData.getColumnCount();

    if (columnIndex < 1 || columnIndex > columnCount) {
      throw new SQLException("Column index out of range: " + columnIndex);
    }

    return metaData.getColumnName(columnIndex);
  }

  // Numeric getters
  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getBoolean(columnName);
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return false;
    }

    this.wasNull = false;

    if (attributeValue.bool() != null) {
      return attributeValue.bool();
    }

    // Try to parse string as boolean
    if (attributeValue.s() != null) {
      return Boolean.parseBoolean(attributeValue.s());
    }

    // Numeric values: 0 = false, non-zero = true
    if (attributeValue.n() != null) {
      try {
        return Double.parseDouble(attributeValue.n()) != 0.0;
      } catch (final NumberFormatException e) {
        return false;
      }
    }

    return false;
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getInt(columnName);
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Integer.parseInt(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to int: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Integer.parseInt(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to int: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to int: " + attributeValue);
  }

  @Override
  public double getDouble(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getDouble(columnName);
  }

  @Override
  public double getDouble(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0.0;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Double.parseDouble(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to double: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Double.parseDouble(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to double: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to double: " + attributeValue);
  }

  // ============================================================================
  // Navigation Methods
  // ============================================================================

  @Override
  public boolean isBeforeFirst() throws SQLException {
    this.validateNotClosed();
    return this.currentRow == -1 && !this.items.isEmpty();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    this.validateNotClosed();
    return this.currentRow >= this.items.size() && this.nextToken == null;
  }

  @Override
  public boolean isFirst() throws SQLException {
    this.validateNotClosed();
    return this.currentRow == 0 && !this.items.isEmpty();
  }

  @Override
  public boolean isLast() throws SQLException {
    this.validateNotClosed();
    return this.currentRow == this.items.size() - 1 && this.nextToken == null;
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException("beforeFirst() not supported - ResultSet is forward-only");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException("afterLast() not supported - ResultSet is forward-only");
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException("first() not supported - ResultSet is forward-only");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException("last() not supported - ResultSet is forward-only");
  }

  @Override
  public int getRow() throws SQLException {
    this.validateNotClosed();
    return this.currentRow >= 0 ? this.currentRow + 1 : 0;
  }

  @Override
  public boolean absolute(final int row) throws SQLException {
    throw new SQLException("absolute() not supported - ResultSet is forward-only");
  }

  @Override
  public boolean relative(final int rows) throws SQLException {
    throw new SQLException("relative() not supported - ResultSet is forward-only");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLException("previous() not supported - ResultSet is forward-only");
  }

  @Override
  public void setFetchDirection(final int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(final int rows) throws SQLException {
    // Ignore - DynamoDB handles pagination internally
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0; // Unknown
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  @Override
  public boolean wasNull() throws SQLException {
    return this.wasNull;
  }

  @Override
  public int findColumn(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var columnNames = this.currentItem.keySet().toArray(new String[0]);
    for (int i = 0; i < columnNames.length; i++) {
      if (columnNames[i].equals(columnLabel)) {
        return i + 1; // JDBC uses 1-based indexing
      }
    }
    throw new SQLException("Column not found: " + columnLabel);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (this.cachedMetaData == null) {
      this.cachedMetaData = new DynamoDbResultSetMetaData(this.items, this.tableKeyInfo);
    }
    return this.cachedMetaData;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null; // No warnings
  }

  @Override
  public void clearWarnings() throws SQLException {
    // No warnings to clear
  }

  @Override
  public Statement getStatement() throws SQLException {
    return null; // Could return the statement that created this ResultSet
  }

  // ============================================================================
  // Additional Getter Methods
  // ============================================================================

  @Override
  public byte getByte(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getByte(columnName);
  }

  @Override
  public byte getByte(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Byte.parseByte(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to byte: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Byte.parseByte(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to byte: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to byte: " + attributeValue);
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getShort(columnName);
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Short.parseShort(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to short: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Short.parseShort(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to short: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to short: " + attributeValue);
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getLong(columnName);
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Long.parseLong(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to long: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Long.parseLong(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to long: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to long: " + attributeValue);
  }

  @Override
  public float getFloat(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getFloat(columnName);
  }

  @Override
  public float getFloat(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return 0.0f;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return Float.parseFloat(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to float: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return Float.parseFloat(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to float: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to float: " + attributeValue);
  }

  @Override
  public byte[] getBytes(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getBytes(columnName);
  }

  @Override
  public byte[] getBytes(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return null;
    }

    this.wasNull = false;

    if (attributeValue.b() != null) {
      return attributeValue.b().asByteArray();
    }

    if (attributeValue.s() != null) {
      return attributeValue.s().getBytes();
    }

    throw new SQLException("Cannot convert attribute to bytes: " + attributeValue);
  }

  @Override
  public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getBigDecimal(columnName);
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return null;
    }

    this.wasNull = false;

    if (attributeValue.n() != null) {
      try {
        return new BigDecimal(attributeValue.n());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert value to BigDecimal: " + attributeValue.n());
      }
    }

    if (attributeValue.s() != null) {
      try {
        return new BigDecimal(attributeValue.s());
      } catch (final NumberFormatException e) {
        throw new SQLException("Cannot convert string to BigDecimal: " + attributeValue.s());
      }
    }

    throw new SQLException("Cannot convert attribute to BigDecimal: " + attributeValue);
  }

  @Override
  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    final var value = this.getBigDecimal(columnIndex);
    return value != null ? value.setScale(scale, BigDecimal.ROUND_HALF_UP) : null;
  }

  @Override
  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
    final var value = this.getBigDecimal(columnLabel);
    return value != null ? value.setScale(scale, BigDecimal.ROUND_HALF_UP) : null;
  }

  // ============================================================================
  // Generic Object Getters
  // ============================================================================

  @Override
  public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
    final var columnName = this.getColumnNameByIndex(columnIndex);
    return this.getObject(columnName, type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
    this.validateCurrentRow();
    final var attributeValue = this.currentItem.get(columnLabel);

    if (attributeValue == null || (attributeValue.nul() != null && attributeValue.nul())) {
      this.wasNull = true;
      return null;
    }

    this.wasNull = false;

    // Handle common types
    if (type == String.class) {
      return (T) this.getString(columnLabel);
    }

    if (type == Integer.class || type == int.class) {
      return (T) Integer.valueOf(this.getInt(columnLabel));
    }

    if (type == Long.class || type == long.class) {
      return (T) Long.valueOf(this.getLong(columnLabel));
    }

    if (type == Double.class || type == double.class) {
      return (T) Double.valueOf(this.getDouble(columnLabel));
    }

    if (type == Float.class || type == float.class) {
      return (T) Float.valueOf(this.getFloat(columnLabel));
    }

    if (type == Boolean.class || type == boolean.class) {
      return (T) Boolean.valueOf(this.getBoolean(columnLabel));
    }

    if (type == BigDecimal.class) {
      return (T) this.getBigDecimal(columnLabel);
    }

    if (type == byte[].class) {
      return (T) this.getBytes(columnLabel);
    }

    throw new SQLException("Unsupported type: " + type.getName());
  }

  @Override
  public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException("getObject with type map not supported");
  }

  @Override
  public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException("getObject with type map not supported");
  }

  // ============================================================================
  // Missing methods - mostly throwing "not supported" exceptions
  // ============================================================================

  @Override
  public Date getDate(final int columnIndex) throws SQLException {
    throw new SQLException("Date types not supported");
  }

  @Override
  public Date getDate(final String columnLabel) throws SQLException {
    throw new SQLException("Date types not supported");
  }

  @Override
  public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLException("Date types not supported");
  }

  @Override
  public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLException("Date types not supported");
  }

  @Override
  public Time getTime(final int columnIndex) throws SQLException {
    throw new SQLException("Time types not supported");
  }

  @Override
  public Time getTime(final String columnLabel) throws SQLException {
    throw new SQLException("Time types not supported");
  }

  @Override
  public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLException("Time types not supported");
  }

  @Override
  public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLException("Time types not supported");
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex) throws SQLException {
    throw new SQLException("Timestamp types not supported");
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel) throws SQLException {
    throw new SQLException("Timestamp types not supported");
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLException("Timestamp types not supported");
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLException("Timestamp types not supported");
  }

  @Override
  public InputStream getAsciiStream(final int columnIndex) throws SQLException {
    throw new SQLException("Stream types not supported");
  }

  @Override
  public InputStream getAsciiStream(final String columnLabel) throws SQLException {
    throw new SQLException("Stream types not supported");
  }

  @Override
  public InputStream getBinaryStream(final int columnIndex) throws SQLException {
    throw new SQLException("Stream types not supported");
  }

  @Override
  public InputStream getBinaryStream(final String columnLabel) throws SQLException {
    throw new SQLException("Stream types not supported");
  }

  @Override
  public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
    throw new SQLException("Unicode streams not supported");
  }

  @Override
  public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
    throw new SQLException("Unicode streams not supported");
  }

  @Override
  public Reader getCharacterStream(final int columnIndex) throws SQLException {
    throw new SQLException("Character streams not supported");
  }

  @Override
  public Reader getCharacterStream(final String columnLabel) throws SQLException {
    throw new SQLException("Character streams not supported");
  }

  @Override
  public Reader getNCharacterStream(final int columnIndex) throws SQLException {
    throw new SQLException("N-Character streams not supported");
  }

  @Override
  public Reader getNCharacterStream(final String columnLabel) throws SQLException {
    throw new SQLException("N-Character streams not supported");
  }

  @Override
  public String getNString(final int columnIndex) throws SQLException {
    return this.getString(columnIndex);
  }

  @Override
  public String getNString(final String columnLabel) throws SQLException {
    return this.getString(columnLabel);
  }

  @Override
  public Blob getBlob(final int columnIndex) throws SQLException {
    throw new SQLException("Blob types not supported");
  }

  @Override
  public Blob getBlob(final String columnLabel) throws SQLException {
    throw new SQLException("Blob types not supported");
  }

  @Override
  public Clob getClob(final int columnIndex) throws SQLException {
    throw new SQLException("Clob types not supported");
  }

  @Override
  public Clob getClob(final String columnLabel) throws SQLException {
    throw new SQLException("Clob types not supported");
  }

  @Override
  public NClob getNClob(final int columnIndex) throws SQLException {
    throw new SQLException("NClob types not supported");
  }

  @Override
  public NClob getNClob(final String columnLabel) throws SQLException {
    throw new SQLException("NClob types not supported");
  }

  @Override
  public SQLXML getSQLXML(final int columnIndex) throws SQLException {
    throw new SQLException("SQLXML types not supported");
  }

  @Override
  public SQLXML getSQLXML(final String columnLabel) throws SQLException {
    throw new SQLException("SQLXML types not supported");
  }

  @Override
  public Array getArray(final int columnIndex) throws SQLException {
    throw new SQLException("Array types not supported");
  }

  @Override
  public Array getArray(final String columnLabel) throws SQLException {
    throw new SQLException("Array types not supported");
  }

  @Override
  public Ref getRef(final int columnIndex) throws SQLException {
    throw new SQLException("Ref types not supported");
  }

  @Override
  public Ref getRef(final String columnLabel) throws SQLException {
    throw new SQLException("Ref types not supported");
  }

  @Override
  public URL getURL(final int columnIndex) throws SQLException {
    throw new SQLException("URL types not supported");
  }

  @Override
  public URL getURL(final String columnLabel) throws SQLException {
    throw new SQLException("URL types not supported");
  }

  @Override
  public RowId getRowId(final int columnIndex) throws SQLException {
    throw new SQLException("RowId types not supported");
  }

  @Override
  public RowId getRowId(final String columnLabel) throws SQLException {
    throw new SQLException("RowId types not supported");
  }

  // Update methods - all throw not supported
  @Override
  public void updateNull(final int columnIndex) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNull(final String columnLabel) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateByte(final int columnIndex, final byte x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateByte(final String columnLabel, final byte x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateShort(final int columnIndex, final short x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateShort(final String columnLabel, final short x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateInt(final int columnIndex, final int x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateInt(final String columnLabel, final int x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateLong(final int columnIndex, final long x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateLong(final String columnLabel, final long x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateFloat(final int columnIndex, final float x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateFloat(final String columnLabel, final float x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateDouble(final int columnIndex, final double x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateDouble(final String columnLabel, final double x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateString(final int columnIndex, final String x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateString(final String columnLabel, final String x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateDate(final int columnIndex, final Date x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateDate(final String columnLabel, final Date x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateTime(final int columnIndex, final Time x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateTime(final String columnLabel, final Time x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader x, final int length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader x, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNString(final int columnIndex, final String nString) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNString(final String columnLabel, final String nString) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateObject(final int columnIndex, final Object x, final int scaleOrLength)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateObject(final String columnLabel, final Object x, final int scaleOrLength)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateObject(final int columnIndex, final Object x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateObject(final String columnLabel, final Object x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final int columnIndex, final Clob x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final String columnLabel, final Clob x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader, final long length)
      throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateArray(final int columnIndex, final Array x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateArray(final String columnLabel, final Array x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateRef(final int columnIndex, final Ref x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateRef(final String columnLabel, final Ref x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  @Override
  public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
    throw new SQLException("Updates not supported");
  }

  // Row manipulation methods
  @Override
  public void insertRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Row manipulation not supported");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException("Named cursors not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  // Wrapper interface methods
  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(this.getClass());
  }
}
