package org.cjgratacos.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

/**
 * JDBC Statement implementation for executing PartiQL queries against Amazon DynamoDB.
 *
 * <p>This class provides the ability to execute PartiQL (SQL-compatible query language) statements
 * against DynamoDB tables. It supports both query operations (SELECT) and data modification
 * operations (INSERT, UPDATE, DELETE, UPSERT, REPLACE).
 *
 * <h2>Supported PartiQL Operations:</h2>
 *
 * <ul>
 *   <li><strong>SELECT</strong>: Query operations that return data
 *   <li><strong>INSERT</strong>: Insert new items into a table
 *   <li><strong>UPDATE</strong>: Modify existing items in a table
 *   <li><strong>DELETE</strong>: Remove items from a table
 *   <li><strong>UPSERT</strong>: Insert or update items (DynamoDB-specific)
 *   <li><strong>REPLACE</strong>: Replace existing items (DynamoDB-specific)
 * </ul>
 *
 * <h2>Statement Types:</h2>
 *
 * <ul>
 *   <li>{@link #executeQuery(String)} - For SELECT statements that return ResultSet
 *   <li>{@link #executeUpdate(String)} - For DML statements (INSERT, UPDATE, DELETE, etc.)
 *   <li>{@link #execute(String)} - Generic execution method that returns boolean
 * </ul>
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Automatic PartiQL syntax validation
 *   <li>Proper distinction between query and update operations
 *   <li>Forward-only result set navigation
 *   <li>Query timeout support (configurable)
 *   <li>Automatic resource cleanup
 *   <li>LIMIT and OFFSET support with client-side implementation
 *   <li>Automatic row limiting to prevent unbounded queries
 * </ul>
 *
 * <h2>Row Limiting Behavior:</h2>
 *
 * <p>The driver applies row limits in the following priority order to prevent fetching all rows:
 *
 * <ol>
 *   <li><strong>SQL LIMIT clause</strong>: Highest priority, extracted from the SQL query
 *   <li><strong>maxRows</strong>: Set via {@link #setMaxRows(int)}
 *   <li><strong>fetchSize</strong>: Used as a safety limit when no other limits are set (default:
 *       100)
 * </ol>
 *
 * <p>This ensures that queries never accidentally fetch all rows from large tables, even when GUI
 * clients like DbVisualizer don't set explicit limits. The fetchSize acts as a safety net to
 * prevent performance issues and excessive memory usage.
 *
 * <h2>Limitations:</h2>
 *
 * <ul>
 *   <li>Batch operations are not supported
 *   <li>Named cursors are not supported
 *   <li>Query cancellation is not supported (DynamoDB limitation)
 *   <li>Generated keys are not supported
 * </ul>
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * try (Statement stmt = connection.createStatement()) {
 *     // Set custom fetch size for pagination
 *     stmt.setFetchSize(50);
 *
 *     // Query operation with automatic limiting
 *     ResultSet rs = stmt.executeQuery("SELECT * FROM MyTable WHERE id = '123'");
 *     while (rs.next()) {
 *         System.out.println(rs.getString("name"));
 *     }
 *
 *     // Query with explicit LIMIT
 *     rs = stmt.executeQuery("SELECT * FROM MyTable LIMIT 10 OFFSET 20");
 *
 *     // Update operation
 *     int rowsAffected = stmt.executeUpdate("UPDATE MyTable SET name = 'John' WHERE id = '123'");
 *     System.out.println("Rows affected: " + rowsAffected);
 * }
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see DynamoDbConnection
 * @see DynamoDbResultSet
 */
public class DynamoDbStatement implements Statement {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbStatement.class);

  private final DynamoDbClient client;
  private final DynamoDbConnection connection;
  private final RetryHandler retryHandler;
  private final QueryMetrics queryMetrics;
  private boolean closed = false;
  private int maxRows = 0;
  private int queryTimeout = 0;
  private int fetchSize = 100; // Default to 100 rows for better GUI client performance
  private ResultSet currentResultSet;
  private SQLWarning warningChain;
  private final Object lock = new Object();
  private final int offsetWarningThreshold = 1000; // Default threshold
  private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
  
  // Batch operation support
  private final List<String> batchCommands = new ArrayList<>();
  private final PartiQLToTransactionConverter transactionConverter = new PartiQLToTransactionConverter();

  /**
   * Creates a new DynamoDB statement for the given connection.
   *
   * <p>This constructor initializes the statement with the provided connection and DynamoDB client.
   * The statement can then be used to execute PartiQL queries and updates against DynamoDB tables.
   *
   * @param connection the DynamoDB connection that created this statement
   * @param client the AWS DynamoDB client for executing operations
   * @param retryHandler the retry handler for managing throttling and transient errors
   * @see DynamoDbConnection#createStatement()
   */
  public DynamoDbStatement(
      final DynamoDbConnection connection,
      final DynamoDbClient client,
      final RetryHandler retryHandler) {
    this.connection = connection;
    this.client = client;
    this.retryHandler = retryHandler;
    this.queryMetrics = retryHandler.getQueryMetrics();
    if (DynamoDbStatement.logger.isDebugEnabled()) {
      DynamoDbStatement.logger.debug(
          "Created DynamoDbStatement with fetchSize={}, maxRows={}", this.fetchSize, this.maxRows);
    }
  }

  @Override
  public ResultSet executeQuery(final String sql) throws SQLException {
    CorrelationContext.newOperation("executeQuery");
    try {
      synchronized (this.lock) {
        this.validateNotClosed();

        // Handle information_schema queries by redirecting to JDBC metadata API
        final ResultSet informationSchemaResult = this.handleInformationSchemaQuery(sql);
        if (informationSchemaResult != null) {
          this.currentResultSet = informationSchemaResult;
          return informationSchemaResult;
        }

        // Normalize index syntax from "table.index" to "table"."index"
        final String processedSql = SqlQueryParser.normalizeIndexSyntax(sql);

        // Extract LIMIT and OFFSET from SQL query
        final LimitOffsetInfo limitOffsetInfo = SqlQueryParser.extractLimitOffset(processedSql);
        SqlQueryParser.validateLimitOffset(limitOffsetInfo);

        // Add warning for large OFFSET values
        if (limitOffsetInfo.hasOffset()) {
          final SQLWarning offsetWarning =
              SqlQueryParser.createOffsetWarning(
                  limitOffsetInfo.getOffset(), this.offsetWarningThreshold);
          if (offsetWarning != null) {
            this.addWarning(offsetWarning);
            DynamoDbStatement.logger.warn("Large OFFSET detected: {}", limitOffsetInfo.getOffset());
          }
        }

        // Remove LIMIT and OFFSET from SQL before sending to DynamoDB
        final String cleanedSql = SqlQueryParser.removeLimitOffset(processedSql);

        this.validatePartiQLSyntax(cleanedSql);
        this.analyzeQueryAndAddWarnings(cleanedSql);

        final var startTime = Instant.now();
        final var truncatedSql = cleanedSql.substring(0, Math.min(cleanedSql.length(), 50));

        if (DynamoDbStatement.logger.isDebugEnabled()) {
          DynamoDbStatement.logger.debug(
              "Executing query: {} (statement fetchSize={}, maxRows={})",
              truncatedSql,
              this.fetchSize,
              this.maxRows);
          if (limitOffsetInfo.hasLimit() || limitOffsetInfo.hasOffset()) {
            DynamoDbStatement.logger.debug(
                "LIMIT: {}, OFFSET: {}", limitOffsetInfo.getLimit(), limitOffsetInfo.getOffset());
          }
        }

        // Close previous result set if it exists
        if (this.currentResultSet != null) {
          try {
            this.currentResultSet.close();
          } catch (final SQLException e) {
            DynamoDbStatement.logger.warn("Error closing previous result set", e);
          }
          this.currentResultSet = null;
        }

        final var requestBuilder = ExecuteStatementRequest.builder().statement(cleanedSql);

        // Calculate the effective limit for the initial request
        int effectiveLimit = Integer.MAX_VALUE;

        // Priority 1: LIMIT from SQL query
        if (limitOffsetInfo.hasLimit()) {
          effectiveLimit = limitOffsetInfo.getLimit();
          if (DynamoDbStatement.logger.isDebugEnabled()) {
            DynamoDbStatement.logger.debug("Using LIMIT from SQL query: {}", effectiveLimit);
          }
        }

        // Priority 2: maxRows from setMaxRows()
        if (this.maxRows > 0 && this.maxRows < effectiveLimit) {
          effectiveLimit = this.maxRows;
          if (DynamoDbStatement.logger.isDebugEnabled()) {
            DynamoDbStatement.logger.debug("Using maxRows limit: {}", effectiveLimit);
          }
        }

        // Priority 3: fetchSize as default limit when no other limits are set
        // This prevents fetching all rows when DBVisualizer doesn't set maxRows
        if (this.fetchSize > 0 && effectiveLimit == Integer.MAX_VALUE) {
          effectiveLimit = this.fetchSize;
          if (DynamoDbStatement.logger.isDebugEnabled()) {
            DynamoDbStatement.logger.debug(
                "Using fetchSize as default limit to prevent fetching all rows: {}",
                effectiveLimit);
          }
        }

        // Apply the effective limit if it's reasonable
        if (effectiveLimit > 0 && effectiveLimit < Integer.MAX_VALUE) {
          requestBuilder.limit(effectiveLimit);
          if (DynamoDbStatement.logger.isDebugEnabled()) {
            DynamoDbStatement.logger.debug(
                "Final effective limit applied to DynamoDB: {}", effectiveLimit);
          }
        } else {
          // If we still have no limit and fetchSize is set, use it as a safety measure
          if (this.fetchSize > 0) {
            requestBuilder.limit(this.fetchSize);
            if (DynamoDbStatement.logger.isDebugEnabled()) {
              DynamoDbStatement.logger.debug(
                  "Applying fetchSize {} as safety limit to prevent unbounded queries",
                  this.fetchSize);
            }
          } else {
            DynamoDbStatement.logger.warn(
                "No limit applied to DynamoDB query - this may fetch all rows! (maxRows={},"
                    + " fetchSize={})",
                this.maxRows,
                this.fetchSize);
          }
        }

        final var request = requestBuilder.build();

        final ExecuteStatementResponse response;
        try {
          response =
              this.retryHandler.executeWithRetry(
                  () -> this.client.executeStatement(request), "executeQuery: " + truncatedSql);
        } catch (final SQLException e) {
          this.queryMetrics.recordError("QUERY_EXECUTION", e);
          throw e;
        }

        final var executionTime = Duration.between(startTime, Instant.now());
        this.recordQueryMetrics("SELECT", executionTime, response);

        // Extract table name to get key information only for SELECT * queries
        TableKeyInfo tableKeyInfo = null;
        String tableName = null;
        if (SqlQueryParser.isSelectAllQuery(processedSql)) {
          tableName = SqlQueryParser.extractTableName(processedSql);
          tableKeyInfo = (tableName != null) ? this.getTableKeyInfo(tableName) : null;
        } else {
          // Also extract table name for non-SELECT * queries to check if updatable
          tableName = SqlQueryParser.extractTableName(processedSql);
        }

        final OffsetTokenCache cache = this.connection.getOffsetTokenCache();

        // Check if this is an updatable query (simple single-table SELECT)
        boolean isUpdatable = false;
        Map<String, String> primaryKeyColumns = null;
        if (tableName != null && isSimpleQuery(processedSql) && resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
          // Get primary key information
          primaryKeyColumns = getPrimaryKeyColumns(tableName);
          isUpdatable = !primaryKeyColumns.isEmpty();
        }

        if (isUpdatable) {
          this.currentResultSet =
              new UpdatableResultSet(
                  this.client,
                  cleanedSql,
                  response,
                  this.fetchSize,
                  limitOffsetInfo,
                  tableKeyInfo,
                  this.maxRows,
                  cache,
                  this,
                  tableName,
                  primaryKeyColumns);
        } else {
          this.currentResultSet =
              new DynamoDbResultSet(
                  this.client,
                  cleanedSql,
                  response,
                  this.fetchSize,
                  limitOffsetInfo,
                  tableKeyInfo,
                  this.maxRows,
                  cache);
        }

        if (DynamoDbStatement.logger.isInfoEnabled()) {
          DynamoDbStatement.logger.info(
              "Query executed successfully: {} items returned in {}ms",
              response.items().size(),
              executionTime.toMillis());
        }

        return this.currentResultSet;
      }
    } catch (final SQLException e) {
      this.queryMetrics.recordError("QUERY_FAILED", e);
      throw e;
    } finally {
      CorrelationContext.clear();
    }
  }

  @Override
  public int executeUpdate(final String sql) throws SQLException {
    CorrelationContext.newOperation("executeUpdate");
    try {
      synchronized (this.lock) {
        this.validateNotClosed();

        if (!this.isDMLStatement(sql)) {
          throw new SQLException(
              "executeUpdate can only be used with DML statements (INSERT, UPDATE, DELETE, UPSERT,"
                  + " REPLACE)");
        }

        // Check if we're in a transaction first, before validating syntax
        TransactionManager transactionManager = this.connection.getTransactionManager();
        if (!this.connection.getAutoCommit() && transactionManager.isInTransaction()) {
          // Try to parse the DML statement for transaction support
          PartiQLToTransactionConverter.DMLOperation dmlOp = transactionConverter.parseDMLStatement(sql);
          
          if (dmlOp != null) {
            // Add to transaction instead of executing immediately
            switch (dmlOp.getType()) {
              case INSERT:
                transactionManager.addPut(dmlOp.getTableName(), dmlOp.getItem());
                break;
              case UPDATE:
                transactionManager.addUpdate(
                    dmlOp.getTableName(),
                    dmlOp.getKey(),
                    dmlOp.getUpdateExpression(),
                    dmlOp.getExpressionAttributeNames(),
                    dmlOp.getExpressionAttributeValues()
                );
                break;
              case DELETE:
                transactionManager.addDelete(dmlOp.getTableName(), dmlOp.getKey());
                break;
            }
            
            if (DynamoDbStatement.logger.isDebugEnabled()) {
              DynamoDbStatement.logger.debug("Added {} operation to transaction for table {}", 
                  dmlOp.getType(), dmlOp.getTableName());
            }
            
            // Return 0 for transaction operations (actual row count unknown until commit)
            return 0;
          }
          // If we can't parse it for transactions, fall through to normal execution
          // This handles UPSERT, REPLACE, and complex statements
        }

        // Now validate syntax for direct execution
        this.validatePartiQLSyntax(sql);
        this.analyzeQueryAndAddWarnings(sql);

        final var startTime = Instant.now();
        final var truncatedSql = sql.substring(0, Math.min(sql.length(), 50));
        final var queryType = this.extractQueryType(sql);

        if (DynamoDbStatement.logger.isDebugEnabled()) {
          DynamoDbStatement.logger.debug("Executing update: {} (type={})", truncatedSql, queryType);
        }

        // Close current result set if it exists (shouldn't happen for updates, but safety)
        if (this.currentResultSet != null) {
          try {
            this.currentResultSet.close();
          } catch (final SQLException e) {
            DynamoDbStatement.logger.warn("Error closing previous result set", e);
          }
          this.currentResultSet = null;
        }

        final var request = ExecuteStatementRequest.builder().statement(sql).build();

        final ExecuteStatementResponse response;
        try {
          response =
              this.retryHandler.executeWithRetry(
                  () -> this.client.executeStatement(request), "executeUpdate: " + truncatedSql);
        } catch (final SQLException e) {
          this.queryMetrics.recordError("UPDATE_EXECUTION", e);
          throw e;
        }

        final var executionTime = Duration.between(startTime, Instant.now());
        this.recordQueryMetrics(queryType, executionTime, response);

        // DynamoDB doesn't return affected row count, so we return 1 for successful execution
        final var affectedRows = response.items().isEmpty() ? 0 : 1;

        if (DynamoDbStatement.logger.isInfoEnabled()) {
          DynamoDbStatement.logger.info(
              "Update executed successfully: {} rows affected in {}ms (type={})",
              affectedRows,
              executionTime.toMillis(),
              queryType);
        }

        return affectedRows;
      }
    } catch (final SQLException e) {
      this.queryMetrics.recordError("UPDATE_FAILED", e);
      throw e;
    } finally {
      CorrelationContext.clear();
    }
  }

  @Override
  public boolean execute(final String sql) throws SQLException {
    CorrelationContext.newOperation("execute");
    try {
      synchronized (this.lock) {
        this.validateNotClosed();
        this.validatePartiQLSyntax(sql);

        final var queryType = this.extractQueryType(sql);

        if (DynamoDbStatement.logger.isDebugEnabled()) {
          DynamoDbStatement.logger.debug(
              "Executing statement: {} (type={})",
              sql.substring(0, Math.min(sql.length(), 50)),
              queryType);
        }

        if (this.isDMLStatement(sql)) {
          this.executeUpdate(sql);
          return false; // DML statements don't return ResultSet
        }

        this.currentResultSet = this.executeQuery(sql);
        return true; // Query statements return ResultSet
      }
    } finally {
      CorrelationContext.clear();
    }
  }

  private void validatePartiQLSyntax(final String sql) throws SQLException {
    // Skip validation for DynamoDB-specific VALUE syntax which the standard parser doesn't understand
    if (sql != null && sql.toUpperCase().contains(" VALUE ")) {
      return;
    }
    PartiQLUtils.validateSyntax(sql, "Failed to validate PartiQL syntax");
  }

  private boolean isDMLStatement(final String sql) {
    return PartiQLUtils.isDMLStatement(sql);
  }

  private void validateNotClosed() throws SQLException {
    if (this.closed) {
      throw new SQLException("Statement is closed");
    }
    if (this.connection.isClosed()) {
      throw new SQLException("Connection is closed");
    }
  }

  private void addWarning(final SQLWarning warning) {
    if (warning == null) {
      return;
    }

    if (this.warningChain == null) {
      this.warningChain = warning;
    } else {
      // Add to the end of the chain
      SQLWarning current = this.warningChain;
      while (current.getNextWarning() != null) {
        current = current.getNextWarning();
      }
      current.setNextWarning(warning);
    }
  }

  private void analyzeQueryAndAddWarnings(final String sql) {
    final var warnings = QueryAnalyzer.analyzeQuery(sql);

    if (warnings.isEmpty()) {
      return;
    }

    // Convert query warnings to SQL warnings and chain them
    for (final var warning : warnings) {
      final var sqlWarning = warning.toSQLWarning();
      this.addWarning(sqlWarning);
    }

    if (DynamoDbStatement.logger.isWarnEnabled()) {
      DynamoDbStatement.logger.warn(
          "Query analysis found {} warnings for query: {}",
          warnings.size(),
          sql.substring(0, Math.min(sql.length(), 50)));
    }
  }

  private void recordQueryMetrics(
      final String queryType,
      final Duration executionTime,
      final ExecuteStatementResponse response) {
    this.queryMetrics.recordQueryExecution(executionTime, queryType);

    // Record capacity consumption if available
    if (response.consumedCapacity() != null) {
      final var consumedCapacity = response.consumedCapacity();
      final var readCU =
          consumedCapacity.readCapacityUnits() != null ? consumedCapacity.readCapacityUnits() : 0.0;
      final var writeCU =
          consumedCapacity.writeCapacityUnits() != null
              ? consumedCapacity.writeCapacityUnits()
              : 0.0;

      this.queryMetrics.recordCapacityConsumption(readCU, writeCU);

      if (DynamoDbStatement.logger.isDebugEnabled()) {
        DynamoDbStatement.logger.debug(
            "Capacity consumed: read={} CU, write={} CU", readCU, writeCU);
      }
    }
  }

  private String extractQueryType(final String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return "UNKNOWN";
    }

    final var trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("SELECT")) {
      return "SELECT";
    } else if (trimmed.startsWith("INSERT")) {
      return "INSERT";
    } else if (trimmed.startsWith("UPDATE")) {
      return "UPDATE";
    } else if (trimmed.startsWith("DELETE")) {
      return "DELETE";
    } else if (trimmed.startsWith("UPSERT")) {
      return "UPSERT";
    } else if (trimmed.startsWith("REPLACE")) {
      return "REPLACE";
    } else {
      return "OTHER";
    }
  }

  @Override
  public void close() throws SQLException {
    synchronized (this.lock) {
      if (!this.closed) {
        try {
          if (this.currentResultSet != null) {
            try {
              this.currentResultSet.close();
            } catch (final SQLException e) {
              // Log warning but continue closing
            } finally {
              this.currentResultSet = null;
            }
          }
        } finally {
          try {
            this.connection.removeStatement(this);
          } finally {
            this.closed = true;
          }
        }
      }
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(final int max) throws SQLException {
    // No-op
  }

  @Override
  public int getMaxRows() throws SQLException {
    return this.maxRows;
  }

  /**
   * Sets the maximum number of rows that any ResultSet can contain.
   *
   * <p>This limit applies to all ResultSet objects generated by this Statement. If the limit is
   * exceeded, the excess rows are silently dropped. A value of 0 means no limit.
   *
   * <p><strong>Row Limiting Priority:</strong>
   *
   * <ol>
   *   <li>SQL LIMIT clause in the query (highest priority)
   *   <li>This maxRows setting
   *   <li>fetchSize (used as safety limit when maxRows is 0)
   * </ol>
   *
   * @param max the new max rows limit; 0 means no limit
   * @throws SQLException if a database access error occurs or the condition max &gt;= 0 is not
   *     satisfied
   */
  @Override
  public void setMaxRows(final int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("Max rows cannot be negative: " + max);
    }
    if (DynamoDbStatement.logger.isDebugEnabled()) {
      DynamoDbStatement.logger.debug("Setting maxRows to: {}", max);
    }
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(final boolean enable) throws SQLException {
    // No-op
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return this.queryTimeout;
  }

  @Override
  public void setQueryTimeout(final int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    // DynamoDB doesn't support query cancellation
    throw new SQLException("Query cancellation not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return this.warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    this.warningChain = null;
  }

  @Override
  public void setCursorName(final String name) throws SQLException {
    throw new SQLException("Named cursors not supported");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    synchronized (this.lock) {
      this.validateNotClosed();
      return this.currentResultSet;
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1; // No update count available
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false; // Only single result set supported
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

  /**
   * Sets the number of rows to fetch from DynamoDB per request.
   *
   * <p>The fetch size controls pagination behavior and serves as a safety limit to prevent
   * unbounded queries. When no explicit LIMIT is specified in the SQL query and maxRows is 0, the
   * fetchSize is used as the effective limit to prevent fetching all rows from large tables.
   *
   * <p><strong>Important behaviors:</strong>
   *
   * <ul>
   *   <li>Used as the page size for result set pagination
   *   <li>Acts as a safety limit when no other limits are specified
   *   <li>Default value is 100 rows
   *   <li>Can be overridden by connection property 'defaultFetchSize'
   * </ul>
   *
   * @param rows the number of rows to fetch; 0 means fetch all rows (not recommended)
   * @throws SQLException if a database access error occurs or the condition rows &gt;= 0 is not
   *     satisfied
   * @see #setMaxRows(int)
   */
  @Override
  public void setFetchSize(final int rows) throws SQLException {
    if (rows < 0) {
      throw new SQLException("Fetch size cannot be negative: " + rows);
    }
    if (DynamoDbStatement.logger.isDebugEnabled()) {
      DynamoDbStatement.logger.debug("Setting fetchSize to: {}", rows);
    }
    this.fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return this.fetchSize;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return resultSetConcurrency;
  }
  
  /**
   * Sets the result set concurrency for statements executed by this Statement object.
   *
   * @param concurrency either ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
   * @throws SQLException if concurrency is not a valid concurrency level
   */
  public void setResultSetConcurrency(int concurrency) throws SQLException {
    if (concurrency != ResultSet.CONCUR_READ_ONLY && concurrency != ResultSet.CONCUR_UPDATABLE) {
      throw new SQLException("Invalid concurrency level: " + concurrency);
    }
    this.resultSetConcurrency = concurrency;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(final String sql) throws SQLException {
    synchronized (this.lock) {
      this.validateNotClosed();
      if (sql == null || sql.trim().isEmpty()) {
        throw new SQLException("Cannot add null or empty SQL to batch");
      }
      
      // Only DML statements are allowed in batch
      if (!isDMLStatement(sql)) {
        throw new SQLException("Only DML statements (INSERT, UPDATE, DELETE, UPSERT, REPLACE) are allowed in batch operations");
      }
      
      this.batchCommands.add(sql);
      
      if (logger.isDebugEnabled()) {
        logger.debug("Added SQL to batch. Total batch size: {}", this.batchCommands.size());
      }
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    synchronized (this.lock) {
      this.validateNotClosed();
      this.batchCommands.clear();
      
      if (logger.isDebugEnabled()) {
        logger.debug("Batch cleared");
      }
    }
  }

  @Override
  public int[] executeBatch() throws SQLException {
    synchronized (this.lock) {
      this.validateNotClosed();
      
      if (this.batchCommands.isEmpty()) {
        return new int[0];
      }
      
      final int batchSize = this.batchCommands.size();
      final int[] results = new int[batchSize];
      int successCount = 0;
      int failureCount = 0;
      
      if (logger.isInfoEnabled()) {
        logger.info("Executing batch with {} commands", batchSize);
      }
      
      // Execute each command in the batch
      for (int i = 0; i < batchSize; i++) {
        final String sql = this.batchCommands.get(i);
        try {
          // Execute the DML statement
          final int updateCount = this.executeUpdate(sql);
          results[i] = updateCount;
          successCount++;
        } catch (Exception e) {
          // According to JDBC spec, set to EXECUTE_FAILED on error
          results[i] = Statement.EXECUTE_FAILED;
          failureCount++;
          
          logger.warn("Batch command {} failed: {}", i, e.getMessage());
          
          // Continue processing remaining commands
          // This follows the JDBC spec for non-transactional batch execution
        }
      }
      
      // Clear the batch after execution
      this.batchCommands.clear();
      
      if (logger.isInfoEnabled()) {
        logger.info("Batch execution completed. Success: {}, Failed: {}", successCount, failureCount);
      }
      
      // If any commands failed, throw BatchUpdateException
      if (failureCount > 0) {
        throw new java.sql.BatchUpdateException(
            String.format("Batch execution completed with %d failures out of %d commands", failureCount, batchSize),
            results
        );
      }
      
      return results;
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  /**
   * Retrieves table key information for column ordering.
   *
   * @param tableName the name of the table
   * @return table key information, or null if not available
   */
  private TableKeyInfo getTableKeyInfo(final String tableName) {
    try {
      // Get table description
      final var describeTableRequest =
          software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.builder()
              .tableName(tableName)
              .build();
      final var tableDesc = this.client.describeTable(describeTableRequest).table();

      // Extract primary key columns
      final List<String> primaryKeys = new ArrayList<>();
      for (final var keyElement : tableDesc.keySchema()) {
        primaryKeys.add(keyElement.attributeName());
      }

      // Extract secondary index keys (both GSI and LSI)
      final Set<String> secondaryKeys = new java.util.LinkedHashSet<>();

      // Global Secondary Indexes
      if (tableDesc.globalSecondaryIndexes() != null) {
        for (final var gsi : tableDesc.globalSecondaryIndexes()) {
          for (final var keyElement : gsi.keySchema()) {
            secondaryKeys.add(keyElement.attributeName());
          }
        }
      }

      // Local Secondary Indexes
      if (tableDesc.localSecondaryIndexes() != null) {
        for (final var lsi : tableDesc.localSecondaryIndexes()) {
          for (final var keyElement : lsi.keySchema()) {
            secondaryKeys.add(keyElement.attributeName());
          }
        }
      }

      // Remove primary keys from secondary keys to avoid duplication
      secondaryKeys.removeAll(primaryKeys);

      return new TableKeyInfo(tableName, primaryKeys, new ArrayList<>(secondaryKeys));
    } catch (final Exception e) {
      // If we can't get table info, just return null and use default ordering
      DynamoDbStatement.logger.debug(
          "Unable to retrieve table key information for {}: {}", tableName, e.getMessage());
      return null;
    }
  }

  /**
   * Checks if a query is a simple single-table query that can be made updatable.
   *
   * @param sql the SQL query
   * @return true if the query is simple and updatable
   */
  private boolean isSimpleQuery(final String sql) {
    if (sql == null) {
      return false;
    }
    
    final String upperSql = sql.toUpperCase().trim();
    
    // Must be a SELECT statement
    if (!upperSql.startsWith("SELECT")) {
      return false;
    }
    
    // Cannot have JOINs
    if (upperSql.contains(" JOIN ")) {
      return false;
    }
    
    // Cannot have GROUP BY
    if (upperSql.contains(" GROUP BY ")) {
      return false;
    }
    
    // Cannot have aggregations
    if (upperSql.contains("COUNT(") || upperSql.contains("SUM(") || 
        upperSql.contains("AVG(") || upperSql.contains("MAX(") || 
        upperSql.contains("MIN(")) {
      return false;
    }
    
    // Cannot have UNION
    if (upperSql.contains(" UNION ")) {
      return false;
    }
    
    return true;
  }

  /**
   * Gets the primary key columns for a table.
   *
   * @param tableName the table name
   * @return map of column names to their DynamoDB types
   */
  private Map<String, String> getPrimaryKeyColumns(final String tableName) {
    final Map<String, String> keyColumns = new java.util.HashMap<>();
    
    try {
      final var describeTableRequest =
          software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.builder()
              .tableName(tableName)
              .build();
      final var tableDesc = this.client.describeTable(describeTableRequest).table();
      
      // Get attribute definitions
      final Map<String, String> attributeTypes = new java.util.HashMap<>();
      for (final var attrDef : tableDesc.attributeDefinitions()) {
        attributeTypes.put(attrDef.attributeName(), attrDef.attributeType().toString());
      }
      
      // Get primary key columns
      for (final var keyElement : tableDesc.keySchema()) {
        final String columnName = keyElement.attributeName();
        final String columnType = attributeTypes.get(columnName);
        if (columnType != null) {
          keyColumns.put(columnName, columnType);
        }
      }
    } catch (final Exception e) {
      logger.debug("Unable to retrieve primary key information for {}: {}", tableName, e.getMessage());
    }
    
    return keyColumns;
  }

  @Override
  public boolean getMoreResults(final int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Generated keys not supported");
  }

  @Override
  public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
    return this.executeUpdate(sql);
  }

  @Override
  public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
    return this.executeUpdate(sql);
  }

  @Override
  public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
    return this.executeUpdate(sql);
  }

  @Override
  public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
    return this.execute(sql);
  }

  @Override
  public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
    return this.execute(sql);
  }

  @Override
  public boolean execute(final String sql, final String[] columnNames) throws SQLException {
    return this.execute(sql);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public void setPoolable(final boolean poolable) throws SQLException {
    // No-op
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // No-op
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

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

  /**
   * Handles information_schema queries by redirecting them to appropriate JDBC metadata API calls.
   *
   * <p>This method intercepts SQL queries targeting information_schema tables and converts them to
   * equivalent JDBC DatabaseMetaData API calls. Supported schemas include:
   *
   * <ul>
   *   <li><strong>information_schema.tables</strong> - redirects to {@code getTables()}
   *   <li><strong>information_schema.columns</strong> - redirects to {@code getColumns()}
   *   <li><strong>information_schema.indexes</strong> - redirects to {@code getIndexInfo()}
   *   <li><strong>information_schema.index_columns</strong> - redirects to {@code getIndexInfo()}
   * </ul>
   *
   * <p>The method supports filtering by table name, field/column name, and index name through WHERE
   * clause extraction:
   *
   * <ul>
   *   <li>Table filtering: {@code WHERE table_name = 'tablename'} or {@code WHERE tab =
   *       'tablename'}
   *   <li>Field filtering: {@code WHERE column_name = 'fieldname'} or {@code WHERE name =
   *       'fieldname'}
   *   <li>Index filtering: {@code WHERE INDEX_NAME = 'PRIMARY'} or {@code WHERE index_name =
   *       'GSI-1'}
   * </ul>
   *
   * @param sql the SQL query to check and potentially handle
   * @return ResultSet if this is an information_schema query that was handled, null otherwise
   * @throws SQLException if there's an error handling the metadata query
   * @see #extractTableNameFromIndexQuery(String)
   * @see #extractFieldNameFromQuery(String)
   * @see #extractIndexNameFromQuery(String)
   * @see #filterResultSetByColumnName(ResultSet, String)
   * @see #filterResultSetByIndexName(ResultSet, String)
   */
  private ResultSet handleInformationSchemaQuery(final String sql) throws SQLException {
    final String normalizedSql = sql.trim().toLowerCase();

    // Check for information_schema.indexes queries
    if (normalizedSql.contains("information_schema.indexes")) {
      if (DynamoDbStatement.logger.isDebugEnabled()) {
        DynamoDbStatement.logger.debug("Intercepting information_schema.indexes query: {}", sql);
      }

      // Extract table name, field name, and index name from WHERE clause if present
      final String tableName = this.extractTableNameFromIndexQuery(sql);
      final String fieldName = this.extractFieldNameFromQuery(sql);
      final String indexName = this.extractIndexNameFromQuery(sql);

      // Use JDBC metadata API to get index information
      final var metaData = this.connection.getMetaData();
      final ResultSet allIndexes = metaData.getIndexInfo(null, null, tableName, false, false);

      // Filter by field name if specified
      if (fieldName != null) {
        return this.filterResultSetByColumnName(allIndexes, fieldName);
      }

      // Filter by index name if specified
      if (indexName != null) {
        return this.filterResultSetByIndexName(allIndexes, indexName);
      }

      return allIndexes;
    }

    // Check for other information_schema queries that could be added in the future
    if (normalizedSql.contains("information_schema.tables")) {
      if (DynamoDbStatement.logger.isDebugEnabled()) {
        DynamoDbStatement.logger.debug("Intercepting information_schema.tables query: {}", sql);
      }

      final var metaData = this.connection.getMetaData();
      return metaData.getTables(null, null, "%", null);
    }

    if (normalizedSql.contains("information_schema.columns")) {
      if (DynamoDbStatement.logger.isDebugEnabled()) {
        DynamoDbStatement.logger.debug("Intercepting information_schema.columns query: {}", sql);
      }

      // Extract table name if specified
      String tableName = this.extractTableNameFromColumnQuery(sql);
      if (tableName == null) {
        tableName = "%"; // Get all columns for all tables
      }

      final var metaData = this.connection.getMetaData();
      return metaData.getColumns(null, null, tableName, "%");
    }

    // Check for information_schema.index_columns queries
    if (normalizedSql.contains("information_schema.index_columns")) {
      if (DynamoDbStatement.logger.isDebugEnabled()) {
        DynamoDbStatement.logger.debug(
            "Intercepting information_schema.index_columns query: {}", sql);
      }

      // Extract table name and index name from WHERE clause if present
      final String tableName = this.extractTableNameFromIndexQuery(sql);
      final String indexName = this.extractIndexNameFromQuery(sql);

      // For index_columns, we need to return individual columns, not the combined view
      return this.getIndexColumnsAsRows(tableName, indexName);
    }

    return null; // Not an information_schema query
  }

  /**
   * Extracts table name from an information_schema.indexes query.
   *
   * @param sql the SQL query
   * @return the table name, or null if not found
   */
  private String extractTableNameFromIndexQuery(final String sql) {
    // Look for patterns like "WHERE table_name = 'tablename'" or "WHERE tab = 'tablename'"
    // Use case-insensitive matching but preserve original case in capture

    // Common patterns for table name in index queries
    final String[] patterns = {
      "(?i)where\\s+table_name\\s*=\\s*['\"]([^'\"]+)['\"]",
      "(?i)where\\s+tab\\s*=\\s*['\"]([^'\"]+)['\"]",
      "(?i)where\\s+table_schema\\s*=\\s*['\"]([^'\"]+)['\"]"
    };

    for (final String pattern : patterns) {
      final Pattern p = Pattern.compile(pattern);
      final Matcher m = p.matcher(sql);
      if (m.find()) {
        return m.group(1);
      }
    }

    return null; // Return null to get indexes for all tables
  }

  /**
   * Extracts table name from an information_schema.columns query.
   *
   * @param sql the SQL query
   * @return the table name, or null if not found
   */
  private String extractTableNameFromColumnQuery(final String sql) {
    // Look for WHERE table_name = 'tablename'
    final Pattern pattern = Pattern.compile("(?i)where\\s+table_name\\s*=\\s*['\"]([^'\"]+)['\"]");
    final Matcher matcher = pattern.matcher(sql);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  /**
   * Filters a ResultSet to only include rows where COLUMN_NAME matches the specified field name.
   *
   * <p>This method is used to implement field-level filtering for information_schema queries. It
   * processes the entire ResultSet, extracts rows that match the given field name, and returns a
   * new filtered ResultSet containing only the matching rows.
   *
   * <p>The filtering is performed by examining the 'COLUMN_NAME' column in each row and comparing
   * it with the provided field name using exact string matching.
   *
   * @param resultSet the ResultSet to filter (typically from getIndexInfo())
   * @param fieldName the field name to filter by (extracted from WHERE clause)
   * @return a new filtered DynamoDbResultSet containing only matching rows
   * @throws SQLException if there's an error processing the ResultSet or accessing metadata
   * @see #extractFieldNameFromQuery(String)
   * @see DynamoDbResultSet#DynamoDbResultSet(java.util.List)
   */
  private ResultSet filterResultSetByColumnName(final ResultSet resultSet, final String fieldName)
      throws SQLException {
    final List<Map<String, AttributeValue>> filteredRows = new ArrayList<>();

    while (resultSet.next()) {
      final String columnName = resultSet.getString("COLUMN_NAME");
      if (fieldName.equals(columnName)) {
        // Convert the current row to a Map<String, AttributeValue>
        final Map<String, AttributeValue> row = new LinkedHashMap<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          final String columnLabel = metaData.getColumnLabel(i);
          final Object value = resultSet.getObject(i);

          if (value == null) {
            row.put(columnLabel, AttributeValue.builder().nul(true).build());
          } else if (value instanceof String) {
            row.put(columnLabel, AttributeValue.builder().s((String) value).build());
          } else if (value instanceof Boolean) {
            row.put(columnLabel, AttributeValue.builder().bool((Boolean) value).build());
          } else {
            row.put(columnLabel, AttributeValue.builder().s(value.toString()).build());
          }
        }

        filteredRows.add(row);
      }
    }

    return new DynamoDbResultSet(filteredRows);
  }

  /**
   * Extracts field/column name from an information_schema query WHERE clause.
   *
   * @param sql the SQL query
   * @return the field name, or null if not found
   */
  private String extractFieldNameFromQuery(final String sql) {
    // Look for patterns like "column_name = 'fieldname'" or "name = 'fieldname'" (not preceded by
    // table_)
    final String[] patterns = {
      "(?i)(?<!table_)\\bcolumn_name\\s*=\\s*['\"]([^'\"]+)['\"]",
      "(?i)(?<!table_)\\bname\\s*=\\s*['\"]([^'\"]+)['\"]",
      "(?i)\\bfield_name\\s*=\\s*['\"]([^'\"]+)['\"]"
    };

    for (final String pattern : patterns) {
      final Pattern p = Pattern.compile(pattern);
      final Matcher m = p.matcher(sql);
      if (m.find()) {
        return m.group(1);
      }
    }

    return null; // Return null to get all fields
  }

  /**
   * Gets index columns as individual rows for information_schema.index_columns queries.
   *
   * <p>This method returns separate rows for each column in an index, unlike getIndexInfo which
   * returns one row per index with combined column information. It's specifically designed to
   * support DbVisualizer and other database tools that query information_schema.index_columns.
   *
   * <p><strong>Enhanced Fields for GUI Tool Compatibility:</strong>
   *
   * <ul>
   *   <li><strong>index_key</strong> - The attribute name (lowercase for DbVisualizer
   *       compatibility)
   *   <li><strong>index_key_type</strong> - Data type: "String", "Number", or "Binary"
   *   <li><strong>TYPE_NAME</strong> - Standard JDBC field with data type (critical for
   *       DbVisualizer)
   *   <li><strong>KEY_TYPE</strong> - Key role: "HASH" or "RANGE"
   *   <li><strong>INDEX_NAME</strong> - The index name: "PRIMARY", GSI name, or LSI name
   * </ul>
   *
   * <p>The TYPE_NAME field is particularly important as it's used by DbVisualizer to display type
   * hints like "PK (String)" instead of "PK (null)" in the database browser.
   *
   * @param tableName the table name to filter by (null for all tables)
   * @param indexName the index name to filter by (null for all indexes)
   * @return ResultSet containing individual column rows for indexes with enhanced metadata
   * @throws SQLException if there's an error accessing table metadata
   */
  private ResultSet getIndexColumnsAsRows(final String tableName, final String indexName)
      throws SQLException {
    final List<Map<String, AttributeValue>> columnRows = new ArrayList<>();

    try {
      // If no table specified, get all tables
      if (tableName == null) {
        final var metaData = this.connection.getMetaData();
        final ResultSet tables = metaData.getTables(null, null, "%", null);
        while (tables.next()) {
          final String currentTable = tables.getString("TABLE_NAME");
          this.addIndexColumnsForTable(currentTable, indexName, columnRows);
        }
      } else {
        // Get columns for specific table
        this.addIndexColumnsForTable(tableName, indexName, columnRows);
      }
    } catch (final Exception e) {
      DynamoDbStatement.logger.warn("Error getting index columns: {}", e.getMessage());
    }

    return new DynamoDbResultSet(columnRows);
  }

  /**
   * Adds index column rows for a specific table.
   *
   * <p>This method processes all indexes for a given table (PRIMARY, GSI, and LSI) and creates
   * individual rows for each key column. Each row includes comprehensive metadata including the
   * TYPE_NAME field which is essential for GUI database tools like DbVisualizer.
   *
   * <p>The method uses the getAttributeTypeName helper to convert DynamoDB attribute types (S, N,
   * B) to human-readable type names (String, Number, Binary) for the TYPE_NAME field.
   *
   * @param tableName the table name to process
   * @param indexNameFilter optional index name filter (null to include all indexes)
   * @param columnRows the list to add generated column rows to
   * @throws SQLException if there's an error accessing table metadata
   * @see #getAttributeTypeName(String, java.util.List)
   */
  private void addIndexColumnsForTable(
      final String tableName,
      final String indexNameFilter,
      final List<Map<String, AttributeValue>> columnRows)
      throws SQLException {
    try {
      final var client = this.connection.getDynamoDbClient();
      final var describeResponse = client.describeTable(builder -> builder.tableName(tableName));
      final var tableDesc = describeResponse.table();

      // Add PRIMARY index columns
      if (indexNameFilter == null || "PRIMARY".equalsIgnoreCase(indexNameFilter)) {
        short ordinalPosition = 1;
        for (final var keyElement : tableDesc.keySchema()) {
          final Map<String, AttributeValue> row = new LinkedHashMap<>();
          row.put("TABLE_CAT", AttributeValue.builder().nul(true).build());
          row.put("TABLE_SCHEM", AttributeValue.builder().nul(true).build());
          row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
          row.put("INDEX_NAME", AttributeValue.builder().s("PRIMARY").build());
          row.put("COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());
          row.put(
              "INDEX_KEY",
              AttributeValue.builder().s(keyElement.attributeName()).build()); // For DbVisualizer
          // Get the data type for this attribute
          final String dataType =
              this.getAttributeTypeName(
                  keyElement.attributeName(), tableDesc.attributeDefinitions());
          row.put(
              "INDEX_KEY_TYPE",
              AttributeValue.builder()
                  .s(dataType)
                  .build()); // For DbVisualizer - data type (String/Number/Binary)
          row.put(
              "INDEX_KEY_TYPE_NAME",
              AttributeValue.builder()
                  .s(dataType)
                  .build()); // For DbVisualizer - correct field name
          row.put(
              "TYPE_NAME",
              AttributeValue.builder().s(dataType).build()); // Standard JDBC TYPE_NAME field
          row.put(
              "ORDINAL_POSITION",
              AttributeValue.builder().n(String.valueOf(ordinalPosition++)).build());
          row.put("KEY_TYPE", AttributeValue.builder().s(keyElement.keyType().toString()).build());
          columnRows.add(row);
        }
      }

      // Add GSI columns
      if (tableDesc.globalSecondaryIndexes() != null) {
        for (final var gsi : tableDesc.globalSecondaryIndexes()) {
          if (indexNameFilter == null || gsi.indexName().equalsIgnoreCase(indexNameFilter)) {
            short ordinalPosition = 1;
            for (final var keyElement : gsi.keySchema()) {
              final Map<String, AttributeValue> row = new LinkedHashMap<>();
              row.put("TABLE_CAT", AttributeValue.builder().nul(true).build());
              row.put("TABLE_SCHEM", AttributeValue.builder().nul(true).build());
              row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
              row.put("INDEX_NAME", AttributeValue.builder().s(gsi.indexName()).build());
              row.put(
                  "COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());
              row.put(
                  "INDEX_KEY",
                  AttributeValue.builder()
                      .s(keyElement.attributeName())
                      .build()); // For DbVisualizer
              // Get the data type for this attribute
              final String dataType =
                  this.getAttributeTypeName(
                      keyElement.attributeName(), tableDesc.attributeDefinitions());
              row.put(
                  "INDEX_KEY_TYPE",
                  AttributeValue.builder()
                      .s(dataType)
                      .build()); // For DbVisualizer - data type (String/Number/Binary)
              row.put(
                  "INDEX_KEY_TYPE_NAME",
                  AttributeValue.builder()
                      .s(dataType)
                      .build()); // For DbVisualizer - correct field name
              row.put(
                  "TYPE_NAME",
                  AttributeValue.builder().s(dataType).build()); // Standard JDBC TYPE_NAME field
              row.put(
                  "ORDINAL_POSITION",
                  AttributeValue.builder().n(String.valueOf(ordinalPosition++)).build());
              row.put(
                  "KEY_TYPE", AttributeValue.builder().s(keyElement.keyType().toString()).build());
              columnRows.add(row);
            }
          }
        }
      }

      // Add LSI columns
      if (tableDesc.localSecondaryIndexes() != null) {
        for (final var lsi : tableDesc.localSecondaryIndexes()) {
          if (indexNameFilter == null || lsi.indexName().equalsIgnoreCase(indexNameFilter)) {
            short ordinalPosition = 1;
            for (final var keyElement : lsi.keySchema()) {
              final Map<String, AttributeValue> row = new LinkedHashMap<>();
              row.put("TABLE_CAT", AttributeValue.builder().nul(true).build());
              row.put("TABLE_SCHEM", AttributeValue.builder().nul(true).build());
              row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
              row.put("INDEX_NAME", AttributeValue.builder().s(lsi.indexName()).build());
              row.put(
                  "COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());
              row.put(
                  "INDEX_KEY",
                  AttributeValue.builder()
                      .s(keyElement.attributeName())
                      .build()); // For DbVisualizer
              // Get the data type for this attribute
              final String dataType =
                  this.getAttributeTypeName(
                      keyElement.attributeName(), tableDesc.attributeDefinitions());
              row.put(
                  "INDEX_KEY_TYPE",
                  AttributeValue.builder()
                      .s(dataType)
                      .build()); // For DbVisualizer - data type (String/Number/Binary)
              row.put(
                  "INDEX_KEY_TYPE_NAME",
                  AttributeValue.builder()
                      .s(dataType)
                      .build()); // For DbVisualizer - correct field name
              row.put(
                  "TYPE_NAME",
                  AttributeValue.builder().s(dataType).build()); // Standard JDBC TYPE_NAME field
              row.put(
                  "ORDINAL_POSITION",
                  AttributeValue.builder().n(String.valueOf(ordinalPosition++)).build());
              row.put(
                  "KEY_TYPE", AttributeValue.builder().s(keyElement.keyType().toString()).build());
              columnRows.add(row);
            }
          }
        }
      }
    } catch (final Exception e) {
      // Ignore errors for individual tables
      DynamoDbStatement.logger.debug(
          "Error getting index columns for table {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Gets the DynamoDB attribute type name for a given attribute.
   *
   * @param attributeName the attribute name to look up
   * @param attributeDefinitions the table's attribute definitions
   * @return the human-readable type name (String, Number, Binary, or String as default)
   */
  private String getAttributeTypeName(
      final String attributeName,
      final List<software.amazon.awssdk.services.dynamodb.model.AttributeDefinition>
          attributeDefinitions) {
    for (final var attr : attributeDefinitions) {
      if (attr.attributeName().equals(attributeName)) {
        // Convert DynamoDB type to readable name
        return switch (attr.attributeType()) {
          case S -> "String";
          case N -> "Number";
          case B -> "Binary";
          default -> "String";
        };
      }
    }
    return "String"; // Default type if not found in definitions
  }

  /**
   * Extracts index name from an information_schema query WHERE clause.
   *
   * <p>This method looks for WHERE clauses that filter by index name, such as:
   *
   * <ul>
   *   <li>{@code WHERE INDEX_NAME = 'PRIMARY'}
   *   <li>{@code WHERE index_name = 'GSI-1'}
   * </ul>
   *
   * @param sql the SQL query
   * @return the index name, or null if not found
   */
  private String extractIndexNameFromQuery(final String sql) {
    // Look for patterns like "WHERE index_name = 'indexname'"
    final String[] patterns = {"(?i)\\bindex_name\\s*=\\s*['\"]([^'\"]+)['\"]"};

    for (final String pattern : patterns) {
      final Pattern p = Pattern.compile(pattern);
      final Matcher m = p.matcher(sql);
      if (m.find()) {
        return m.group(1);
      }
    }

    return null; // Return null to get all indexes
  }

  /**
   * Filters a ResultSet to only include rows where INDEX_NAME matches the specified index name.
   *
   * <p>This method is used to implement index-level filtering for information_schema queries. It
   * processes the entire ResultSet, extracts rows that match the given index name, and returns a
   * new filtered ResultSet containing only the matching rows.
   *
   * <p>The filtering is performed by examining the 'INDEX_NAME' column in each row and comparing it
   * with the provided index name using exact string matching.
   *
   * @param resultSet the ResultSet to filter (typically from getIndexInfo())
   * @param indexName the index name to filter by (extracted from WHERE clause)
   * @return a new filtered DynamoDbResultSet containing only matching rows
   * @throws SQLException if there's an error processing the ResultSet or accessing metadata
   * @see #extractIndexNameFromQuery(String)
   * @see DynamoDbResultSet#DynamoDbResultSet(java.util.List)
   */
  private ResultSet filterResultSetByIndexName(final ResultSet resultSet, final String indexName)
      throws SQLException {
    final List<Map<String, AttributeValue>> filteredRows = new ArrayList<>();

    while (resultSet.next()) {
      final String currentIndexName = resultSet.getString("INDEX_NAME");
      if (indexName.equals(currentIndexName)) {
        // Convert the current row to a Map<String, AttributeValue>
        final Map<String, AttributeValue> row = new LinkedHashMap<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          final String columnLabel = metaData.getColumnLabel(i);
          final Object value = resultSet.getObject(i);

          if (value == null) {
            row.put(columnLabel, AttributeValue.builder().nul(true).build());
          } else if (value instanceof String) {
            row.put(columnLabel, AttributeValue.builder().s((String) value).build());
          } else if (value instanceof Boolean) {
            row.put(columnLabel, AttributeValue.builder().bool((Boolean) value).build());
          } else {
            row.put(columnLabel, AttributeValue.builder().s(value.toString()).build());
          }
        }

        filteredRows.add(row);
      }
    }

    return new DynamoDbResultSet(filteredRows);
  }
}
