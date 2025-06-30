package org.cjgratacos.jdbc.pool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * DataSource implementation for DynamoDB connection pooling.
 *
 * <p>This class provides a standard JDBC DataSource interface for the DynamoDB connection pool,
 * allowing it to be used with standard Java EE containers and connection pool managers.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * DynamoDbConnectionPoolDataSource dataSource = new DynamoDbConnectionPoolDataSource();
 * dataSource.setJdbcUrl("jdbc:dynamodb:partiql:region=us-east-1");
 * dataSource.setMinPoolSize(5);
 * dataSource.setMaxPoolSize(20);
 *
 * // Use with JNDI or dependency injection
 * Connection conn = dataSource.getConnection();
 * try {
 *     // Use connection
 * } finally {
 *     conn.close(); // Returns to pool
 * }
 * }</pre>
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class DynamoDbConnectionPoolDataSource implements DataSource, AutoCloseable {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DynamoDbConnectionPoolDataSource.class);

  private String jdbcUrl;
  private Properties connectionProperties = new Properties();
  private PoolConfiguration poolConfig;
  private volatile DynamoDbConnectionPool pool;
  private PrintWriter logWriter;
  private int loginTimeout = 30; // seconds

  /**
   * Creates a new connection pool data source.
   */
  public DynamoDbConnectionPoolDataSource() {
    // Default constructor
  }

  /**
   * Creates a new connection pool data source with the specified JDBC URL.
   *
   * @param jdbcUrl the JDBC URL
   */
  public DynamoDbConnectionPoolDataSource(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  /**
   * Creates a new connection pool data source with the specified JDBC URL and configuration.
   *
   * @param jdbcUrl the JDBC URL
   * @param poolConfig the pool configuration
   */
  public DynamoDbConnectionPoolDataSource(String jdbcUrl, PoolConfiguration poolConfig) {
    this.jdbcUrl = jdbcUrl;
    this.poolConfig = poolConfig;
  }

  /**
   * Initializes the connection pool if not already initialized.
   *
   * @throws SQLException if the pool cannot be initialized
   */
  private synchronized void ensurePoolInitialized() throws SQLException {
    if (pool == null) {
      if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
        throw new SQLException("JDBC URL is not set");
      }

      if (poolConfig == null) {
        poolConfig = new PoolConfiguration(connectionProperties);
      }

      pool = new DynamoDbConnectionPool(jdbcUrl, poolConfig);
      logger.info("Connection pool initialized for URL: {}", jdbcUrl);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    ensurePoolInitialized();
    return pool.borrowConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    // DynamoDB doesn't use username/password authentication
    // Credentials are handled via AWS SDK
    return getConnection();
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  /**
   * Closes the connection pool and releases all resources.
   */
  @Override
  public void close() {
    if (pool != null) {
      pool.close();
      pool = null;
      logger.info("Connection pool closed");
    }
  }

  // Property setters for configuration

  /**
   * Sets the JDBC URL for creating connections.
   *
   * @param jdbcUrl the JDBC URL
   */
  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  /**
   * Gets the JDBC URL.
   *
   * @return the JDBC URL
   */
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  /**
   * Sets a connection property.
   *
   * @param name the property name
   * @param value the property value
   */
  public void setConnectionProperty(String name, String value) {
    connectionProperties.setProperty(name, value);
  }

  /**
   * Sets the minimum pool size.
   *
   * @param size the minimum pool size
   */
  public void setMinPoolSize(int size) {
    setConnectionProperty("pool.minSize", String.valueOf(size));
  }

  /**
   * Sets the maximum pool size.
   *
   * @param size the maximum pool size
   */
  public void setMaxPoolSize(int size) {
    setConnectionProperty("pool.maxSize", String.valueOf(size));
  }

  /**
   * Sets the initial pool size.
   *
   * @param size the initial pool size
   */
  public void setInitialPoolSize(int size) {
    setConnectionProperty("pool.initialSize", String.valueOf(size));
  }

  /**
   * Sets the connection timeout in seconds.
   *
   * @param seconds the timeout in seconds
   */
  public void setConnectionTimeout(int seconds) {
    setConnectionProperty("pool.connectionTimeout", String.valueOf(seconds));
  }

  /**
   * Sets the idle timeout in seconds.
   *
   * @param seconds the idle timeout in seconds
   */
  public void setIdleTimeout(int seconds) {
    setConnectionProperty("pool.idleTimeout", String.valueOf(seconds));
  }

  /**
   * Sets the maximum lifetime in seconds.
   *
   * @param seconds the maximum lifetime in seconds
   */
  public void setMaxLifetime(int seconds) {
    setConnectionProperty("pool.maxLifetime", String.valueOf(seconds));
  }

  /**
   * Sets whether to test connections on borrow.
   *
   * @param test true to test on borrow
   */
  public void setTestOnBorrow(boolean test) {
    setConnectionProperty("pool.testOnBorrow", String.valueOf(test));
  }

  /**
   * Sets whether to test connections on return.
   *
   * @param test true to test on return
   */
  public void setTestOnReturn(boolean test) {
    setConnectionProperty("pool.testOnReturn", String.valueOf(test));
  }

  /**
   * Sets whether to test idle connections.
   *
   * @param test true to test idle connections
   */
  public void setTestWhileIdle(boolean test) {
    setConnectionProperty("pool.testWhileIdle", String.valueOf(test));
  }

  /**
   * Sets the pool configuration.
   *
   * @param poolConfig the pool configuration
   */
  public void setPoolConfiguration(PoolConfiguration poolConfig) {
    this.poolConfig = poolConfig;
  }

  /**
   * Gets the pool configuration.
   *
   * @return the pool configuration
   */
  public PoolConfiguration getPoolConfiguration() {
    return poolConfig;
  }

  /**
   * Gets pool statistics.
   *
   * @return pool statistics or null if pool not initialized
   */
  public PoolStatistics getPoolStatistics() {
    if (pool == null) {
      return null;
    }

    return new PoolStatistics(
        pool.getTotalConnections(),
        pool.getActiveConnections(),
        pool.getIdleConnections(),
        pool.getConnectionsCreated(),
        pool.getConnectionsDestroyed(),
        pool.getConnectionsBorrowed(),
        pool.getConnectionsReturned(),
        pool.getValidationsFailed(),
        pool.getWaitTimeouts());
  }

  /**
   * Pool statistics snapshot.
   */
  public static class PoolStatistics {
    private final int totalConnections;
    private final int activeConnections;
    private final int idleConnections;
    private final long connectionsCreated;
    private final long connectionsDestroyed;
    private final long connectionsBorrowed;
    private final long connectionsReturned;
    private final long validationsFailed;
    private final long waitTimeouts;

    public PoolStatistics(
        int totalConnections,
        int activeConnections,
        int idleConnections,
        long connectionsCreated,
        long connectionsDestroyed,
        long connectionsBorrowed,
        long connectionsReturned,
        long validationsFailed,
        long waitTimeouts) {
      this.totalConnections = totalConnections;
      this.activeConnections = activeConnections;
      this.idleConnections = idleConnections;
      this.connectionsCreated = connectionsCreated;
      this.connectionsDestroyed = connectionsDestroyed;
      this.connectionsBorrowed = connectionsBorrowed;
      this.connectionsReturned = connectionsReturned;
      this.validationsFailed = validationsFailed;
      this.waitTimeouts = waitTimeouts;
    }

    public int getTotalConnections() {
      return totalConnections;
    }

    public int getActiveConnections() {
      return activeConnections;
    }

    public int getIdleConnections() {
      return idleConnections;
    }

    public long getConnectionsCreated() {
      return connectionsCreated;
    }

    public long getConnectionsDestroyed() {
      return connectionsDestroyed;
    }

    public long getConnectionsBorrowed() {
      return connectionsBorrowed;
    }

    public long getConnectionsReturned() {
      return connectionsReturned;
    }

    public long getValidationsFailed() {
      return validationsFailed;
    }

    public long getWaitTimeouts() {
      return waitTimeouts;
    }

    @Override
    public String toString() {
      return String.format(
          "PoolStatistics{total=%d, active=%d, idle=%d, created=%d, destroyed=%d, "
              + "borrowed=%d, returned=%d, validationsFailed=%d, waitTimeouts=%d}",
          totalConnections,
          activeConnections,
          idleConnections,
          connectionsCreated,
          connectionsDestroyed,
          connectionsBorrowed,
          connectionsReturned,
          validationsFailed,
          waitTimeouts);
    }
  }
}