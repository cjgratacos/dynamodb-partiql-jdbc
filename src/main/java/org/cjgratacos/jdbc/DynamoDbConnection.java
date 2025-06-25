package org.cjgratacos.jdbc;

import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * JDBC Connection implementation for Amazon DynamoDB with PartiQL support.
 *
 * <p>This class represents a connection (session) with the DynamoDB database. It manages the AWS
 * DynamoDB client and provides the standard JDBC Connection interface for executing PartiQL
 * statements.
 *
 * <p>The connection is established using AWS SDK v2 and supports various credential providers as
 * specified in the JDBC URL properties.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Automatic AWS DynamoDB client configuration
 *   <li>Support for custom endpoints (useful for DynamoDB Local)
 *   <li>Multiple credential provider types (DEFAULT, STATIC, PROFILE)
 *   <li>Forward-only, read-only result sets
 *   <li>Transaction isolation level: TRANSACTION_NONE (DynamoDB is NoSQL)
 *   <li>Configurable default fetch size and max rows for all statements
 * </ul>
 *
 * <h2>Connection Properties:</h2>
 *
 * <ul>
 *   <li><strong>defaultFetchSize</strong>: Default number of rows to fetch per page (default: 100)
 *   <li><strong>defaultMaxRows</strong>: Default maximum rows limit for all queries
 * </ul>
 *
 * <h2>Limitations:</h2>
 *
 * <ul>
 *   <li>Transactions are not supported (DynamoDB limitation)
 *   <li>Prepared statements are converted to regular statements
 *   <li>Callable statements are not supported
 *   <li>Savepoints are not supported
 * </ul>
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * String url = "jdbc:dynamodb:partiql:region=us-east-1;";
 * Connection conn = DriverManager.getConnection(url);
 *
 * try (Statement stmt = conn.createStatement()) {
 *     ResultSet rs = stmt.executeQuery("SELECT * FROM MyTable");
 *     // Process results...
 * } finally {
 *     conn.close();
 * }
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see DynamoDbDriver
 * @see DynamoDbStatement
 * @see DynamoDbResultSet
 */
public class DynamoDbConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbConnection.class);

  private final DynamoDbClient client;
  private final RetryHandler retryHandler;
  private final QueryMetrics queryMetrics;
  private final SchemaCache schemaCache;
  private final OffsetTokenCache offsetTokenCache;
  private boolean closed = false;
  private final Properties properties;
  private final List<Statement> openStatements = Collections.synchronizedList(new ArrayList<>());
  private volatile boolean stale = false;

  /**
   * Creates a new DynamoDB connection using the specified URL and properties.
   *
   * <p>This constructor parses the JDBC URL to extract connection properties, merges them with any
   * additional properties provided, and initializes the AWS DynamoDB client with the appropriate
   * configuration.
   *
   * <p>The constructor performs the following operations:
   *
   * <ol>
   *   <li>Extracts properties from the JDBC URL using {@link JdbcParser}
   *   <li>Merges URL properties with additional properties
   *   <li>Creates AWS credentials provider based on credentials type
   *   <li>Configures AWS region (required)
   *   <li>Optionally configures custom endpoint
   *   <li>Builds and initializes the DynamoDB client
   * </ol>
   *
   * @param url the JDBC URL containing connection properties
   * @param info additional connection properties (optional)
   * @throws SQLException if the URL is malformed, required properties are missing, or the DynamoDB
   *     client cannot be created
   * @see JdbcParser#extractProperties(String)
   * @see CredentialsProviderFactory#createCredentialsProvider(Properties)
   */
  public DynamoDbConnection(final String url, final Properties info) throws SQLException {
    this.properties = JdbcParser.extractProperties(url);
    this.properties.putAll(info);

    if (logger.isInfoEnabled()) {
      logger.info(
          "Creating DynamoDB connection: region={}, endpoint={}",
          this.properties.getProperty("region"),
          this.properties.getProperty("endpoint", "default"));
    }

    this.client = this.buildDynamoDbClient(this.properties);
    this.queryMetrics = new QueryMetrics();

    final var retryPolicy = RetryPolicy.fromProperties(this.properties);
    this.retryHandler = new RetryHandler(retryPolicy, this.queryMetrics);

    // Initialize schema detection system
    final var schemaDetector = new SchemaDetector(this.client, this.properties);
    this.schemaCache = new SchemaCache(schemaDetector, this.properties);

    // Initialize offset token cache
    boolean offsetCacheEnabled =
        Boolean.parseBoolean(this.properties.getProperty("offsetCacheEnabled", "true"));

    if (offsetCacheEnabled) {
      int cacheSize = Integer.parseInt(this.properties.getProperty("offsetCacheSize", "100"));
      int cacheInterval =
          Integer.parseInt(this.properties.getProperty("offsetCacheInterval", "100"));
      long cacheTtl = Long.parseLong(this.properties.getProperty("offsetCacheTtlSeconds", "3600"));

      this.offsetTokenCache = new OffsetTokenCache(cacheSize, cacheInterval, cacheTtl);

      if (logger.isInfoEnabled()) {
        logger.info(
            "Offset token cache enabled with size={}, interval={}, ttl={}s",
            cacheSize,
            cacheInterval,
            cacheTtl);
      }
    } else {
      this.offsetTokenCache = null;
      if (logger.isInfoEnabled()) {
        logger.info("Offset token cache disabled");
      }
    }
  }

  private DynamoDbClient buildDynamoDbClient(final Properties props) throws SQLException {
    final var builder = DynamoDbClient.builder();

    // Configure credentials
    final var credentialsProvider = CredentialsProviderFactory.createCredentialsProvider(props);
    builder.credentialsProvider(credentialsProvider);

    // Configure region - required
    final var regionName = this.getRegion(props);
    if (regionName == null || regionName.trim().isEmpty()) {
      throw new SQLException(
          "Region is required. Specify 'region' property or set AWS_DEFAULT_REGION environment"
              + " variable.");
    }
    builder.region(Region.of(regionName));

    // Configure endpoint override if provided
    final var endpoint = props.getProperty("endpoint");
    if (endpoint != null && !endpoint.trim().isEmpty()) {
      try {
        builder.endpointOverride(URI.create(endpoint));
      } catch (final Exception e) {
        throw new SQLException("Invalid endpoint URL: " + endpoint, e);
      }
    }

    // Configure client override configuration
    builder.overrideConfiguration(this.buildClientOverrideConfiguration(props));

    return builder.build();
  }

  private String getRegion(final Properties props) {
    // Check properties first
    var region = props.getProperty("region");
    if (region != null && !region.trim().isEmpty()) {
      return region;
    }

    // Fall back to environment variables
    region = System.getenv("AWS_DEFAULT_REGION");
    if (region != null && !region.trim().isEmpty()) {
      return region;
    }

    region = System.getenv("AWS_REGION");
    return region;
  }

  private ClientOverrideConfiguration buildClientOverrideConfiguration(final Properties props) {
    final var configBuilder = ClientOverrideConfiguration.builder();

    // API call timeout
    final var apiCallTimeoutStr = props.getProperty("apiCallTimeoutMs");
    if (apiCallTimeoutStr != null && !apiCallTimeoutStr.trim().isEmpty()) {
      try {
        final var apiCallTimeout = Duration.ofMillis(Long.parseLong(apiCallTimeoutStr));
        configBuilder.apiCallTimeout(apiCallTimeout);
      } catch (final NumberFormatException e) {
        // Log warning but continue with defaults
      }
    }

    // API call attempt timeout
    final var apiCallAttemptTimeoutStr = props.getProperty("apiCallAttemptTimeoutMs");
    if (apiCallAttemptTimeoutStr != null && !apiCallAttemptTimeoutStr.trim().isEmpty()) {
      try {
        final var apiCallAttemptTimeout =
            Duration.ofMillis(Long.parseLong(apiCallAttemptTimeoutStr));
        configBuilder.apiCallAttemptTimeout(apiCallAttemptTimeout);
      } catch (final NumberFormatException e) {
        // Log warning but continue with defaults
      }
    }

    // Request/response interceptors
    final var interceptorClassNames = props.getProperty("interceptors");
    if (interceptorClassNames != null && !interceptorClassNames.trim().isEmpty()) {
      final var interceptors = this.createInterceptors(interceptorClassNames);
      for (final var interceptor : interceptors) {
        configBuilder.addExecutionInterceptor(interceptor);
      }
    }

    return configBuilder.build();
  }

  private List<ExecutionInterceptor> createInterceptors(final String interceptorClassNames) {
    final var interceptors = new ArrayList<ExecutionInterceptor>();
    final var classNames = interceptorClassNames.split(",");

    for (final var className : classNames) {
      final var trimmedClassName = className.trim();
      if (trimmedClassName.isEmpty()) {
        continue;
      }

      try {
        final var clazz = Class.forName(trimmedClassName);
        if (ExecutionInterceptor.class.isAssignableFrom(clazz)) {
          final var interceptor =
              (ExecutionInterceptor) clazz.getDeclaredConstructor().newInstance();
          interceptors.add(interceptor);
        }
      } catch (final Exception e) {
        // Log warning but continue with other interceptors
      }
    }

    return interceptors;
  }

  /**
   * Gets the underlying AWS DynamoDB client used by this connection.
   *
   * <p>This method provides access to the configured DynamoDB client for advanced use cases that
   * require direct access to AWS SDK functionality. Most applications should use the standard JDBC
   * interfaces instead.
   *
   * @return the AWS DynamoDB client instance
   */
  public DynamoDbClient getDynamoDbClient() {
    return this.client;
  }

  /**
   * Gets the retry metrics for monitoring throttling and retry behavior.
   *
   * <p>This method provides access to retry statistics that can be used for monitoring, alerting,
   * and performance tuning. The metrics include counts of successful retries, failed operations,
   * and retry attempts.
   *
   * @return the current retry metrics
   */
  public RetryHandler.RetryMetrics getRetryMetrics() {
    return this.retryHandler.getMetrics();
  }

  /**
   * Gets the query metrics for comprehensive performance monitoring.
   *
   * <p>This method provides access to detailed query execution statistics including timing,
   * capacity consumption, error rates, and throughput metrics.
   *
   * @return the current query metrics
   */
  public QueryMetrics getQueryMetrics() {
    return this.queryMetrics;
  }

  /**
   * Gets the schema cache for accessing table schema information.
   *
   * <p>This method provides access to the schema cache which contains detected column types and
   * metadata for DynamoDB tables. The cache automatically refreshes schema information in the
   * background based on the configured refresh interval.
   *
   * @return the schema cache instance
   */
  public SchemaCache getSchemaCache() {
    return this.schemaCache;
  }

  /**
   * Gets the offset token cache for optimizing pagination with OFFSET.
   *
   * <p>This cache stores DynamoDB NextToken values at specific offset positions to improve
   * performance when using large OFFSET values. May return null if caching is disabled.
   *
   * @return the offset token cache instance, or null if disabled
   */
  public OffsetTokenCache getOffsetTokenCache() {
    return this.offsetTokenCache;
  }

  /**
   * Removes a statement from the connection's tracking list. Called by statements when they are
   * closed.
   *
   * @param statement the statement to remove
   */
  void removeStatement(final Statement statement) {
    this.openStatements.remove(statement);
  }

  @Override
  public void close() throws SQLException {
    if (!this.closed) {
      if (logger.isInfoEnabled()) {
        logger.info(
            "Closing DynamoDB connection with {} open statements. Final metrics: {}",
            this.openStatements.size(),
            this.queryMetrics.getSummary());
      }

      try {
        // Close all open statements
        final var statements = new ArrayList<>(this.openStatements);
        for (final var statement : statements) {
          try {
            statement.close();
          } catch (final SQLException e) {
            logger.warn("Error closing statement during connection cleanup", e);
          }
        }
        this.openStatements.clear();
      } finally {
        try {
          this.schemaCache.shutdown();
        } finally {
          try {
            this.client.close();
          } finally {
            this.closed = true;
            if (logger.isDebugEnabled()) {
              logger.debug("DynamoDB connection closed successfully");
            }
          }
        }
      }
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  // Minimal JDBC Connection interface implementation
  // Most methods will throw UnsupportedOperationException for now

  /**
   * Creates a Statement object for executing PartiQL queries.
   *
   * <p>The created statement will have default settings applied from connection properties:
   *
   * <ul>
   *   <li><strong>defaultFetchSize</strong>: Sets the default fetch size for result sets. If not
   *       specified, defaults to 100 rows. This serves as both a pagination size and a safety limit
   *       to prevent unbounded queries.
   *   <li><strong>defaultMaxRows</strong>: Sets the default maximum rows limit for all queries. If
   *       not specified, no limit is applied (maxRows = 0).
   * </ul>
   *
   * <p>Example connection URL with defaults:
   *
   * <pre>
   * jdbc:dynamodb:partiql:region=us-east-1;defaultFetchSize=50;defaultMaxRows=1000
   * </pre>
   *
   * @return a new Statement object for executing queries
   * @throws SQLException if the connection is closed or an error occurs
   * @see Statement#setFetchSize(int)
   * @see Statement#setMaxRows(int)
   */
  @Override
  public Statement createStatement() throws SQLException {
    if (this.closed) {
      throw new SQLException("Connection is closed");
    }

    final var statement = new DynamoDbStatement(this, this.client, this.retryHandler);
    this.openStatements.add(statement);

    // Apply default fetchSize and maxRows from connection properties if specified
    String defaultFetchSize = this.properties.getProperty("defaultFetchSize");
    if (defaultFetchSize != null) {
      try {
        int fetchSize = Integer.parseInt(defaultFetchSize);
        statement.setFetchSize(fetchSize);
        if (logger.isDebugEnabled()) {
          logger.debug("Applied defaultFetchSize from connection properties: {}", fetchSize);
        }
      } catch (NumberFormatException e) {
        logger.warn("Invalid defaultFetchSize in connection properties: {}", defaultFetchSize);
      }
    }

    String defaultMaxRows = this.properties.getProperty("defaultMaxRows");
    if (defaultMaxRows != null) {
      try {
        int maxRows = Integer.parseInt(defaultMaxRows);
        statement.setMaxRows(maxRows);
        if (logger.isDebugEnabled()) {
          logger.debug("Applied defaultMaxRows from connection properties: {}", maxRows);
        }
      } catch (NumberFormatException e) {
        logger.warn("Invalid defaultMaxRows in connection properties: {}", defaultMaxRows);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Created new statement, total open statements: {}", this.openStatements.size());
    }

    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(final String sql) throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public CallableStatement prepareCall(final String sql) throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public String nativeSQL(final String sql) throws SQLException {
    return sql;
  }

  @Override
  public void setAutoCommit(final boolean autoCommit) throws SQLException {
    // DynamoDB doesn't support transactions in the traditional sense
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true; // DynamoDB operations are always auto-committed
  }

  @Override
  public void commit() throws SQLException {
    // No-op for DynamoDB
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLException("Rollback not supported by DynamoDB");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new DynamoDbDatabaseMetaData(this, this.properties, this.schemaCache);
  }

  @Override
  public void setReadOnly(final boolean readOnly) throws SQLException {
    // No-op
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(final String catalog) throws SQLException {
    // No-op
  }

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public void setTransactionIsolation(final int level) throws SQLException {
    // No-op
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // No-op
  }

  /**
   * Creates a Statement with the specified result set type and concurrency.
   *
   * <p>Note: DynamoDB only supports forward-only, read-only result sets. Any other type will be
   * downgraded to these supported types.
   *
   * @param resultSetType ignored, always TYPE_FORWARD_ONLY
   * @param resultSetConcurrency ignored, always CONCUR_READ_ONLY
   * @return a new Statement object
   * @throws SQLException if the connection is closed
   */
  @Override
  public Statement createStatement(final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Creating statement with resultSetType={}, concurrency={} (will use TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)",
          resultSetType,
          resultSetConcurrency);
    }
    // DynamoDB only supports forward-only, read-only result sets
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public CallableStatement prepareCall(
      final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public void setHoldability(final int holdability) throws SQLException {
    // No-op
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Savepoints not supported by DynamoDB");
  }

  @Override
  public Savepoint setSavepoint(final String name) throws SQLException {
    throw new SQLException("Savepoints not supported by DynamoDB");
  }

  @Override
  public void rollback(final Savepoint savepoint) throws SQLException {
    throw new SQLException("Savepoints not supported by DynamoDB");
  }

  @Override
  public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
    throw new SQLException("Savepoints not supported by DynamoDB");
  }

  @Override
  public Statement createStatement(
      final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public CallableStatement prepareCall(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final String[] columnNames)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public boolean isValid(final int timeout) throws SQLException {
    if (this.closed) {
      return false;
    }

    if (this.stale) {
      return false;
    }

    // Perform a lightweight operation to test connectivity
    CorrelationContext.newOperation("connection-validation");
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Validating connection with timeout={}s", timeout);
      }

      final var request = ListTablesRequest.builder().limit(1).build();

      if (timeout > 0) {
        // Use timeout-aware execution
        final var future =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  try {
                    return this.client.listTables(request);
                  } catch (final Exception e) {
                    throw new RuntimeException(e);
                  }
                });

        future.get(timeout, java.util.concurrent.TimeUnit.SECONDS);
      } else {
        // No timeout specified, use default behavior
        this.client.listTables(request);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Connection validation successful");
      }
      return true;
    } catch (final Exception e) {
      this.stale = true;
      logger.warn("Connection validation failed, marking connection as stale", e);
      return false;
    } finally {
      CorrelationContext.clear();
    }
  }

  @Override
  public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
    // No-op
  }

  @Override
  public void setClientInfo(final Properties properties) throws SQLClientInfoException {
    // No-op
  }

  @Override
  public String getClientInfo(final String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return new Properties();
  }

  @Override
  public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public void setSchema(final String schema) throws SQLException {
    // No-op
  }

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort(final Executor executor) throws SQLException {
    this.close();
  }

  @Override
  public void setNetworkTimeout(final Executor executor, final int milliseconds)
      throws SQLException {
    throw new SQLException("Not yet implemented");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
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
}
