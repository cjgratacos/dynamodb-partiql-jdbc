package org.cjgratacos.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.cjgratacos.jdbc.DynamoDbDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection pool implementation for DynamoDB JDBC connections.
 *
 * <p>This class manages a pool of DynamoDB connections, providing efficient connection reuse,
 * automatic eviction of idle connections, connection validation, and configurable pool sizing.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Configurable min/max pool size
 *   <li>Connection validation on borrow/return
 *   <li>Idle connection eviction
 *   <li>Maximum connection lifetime enforcement
 *   <li>Connection creation with backoff on failure
 *   <li>Comprehensive metrics and monitoring
 * </ul>
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class DynamoDbConnectionPool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbConnectionPool.class);

  private final PoolConfiguration config;
  private final String jdbcUrl;
  private final BlockingDeque<PooledConnection> availableConnections;
  private final AtomicInteger totalConnections = new AtomicInteger(0);
  private final AtomicInteger activeConnections = new AtomicInteger(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ScheduledExecutorService maintenanceExecutor;

  // Metrics
  private final AtomicLong connectionsCreated = new AtomicLong(0);
  private final AtomicLong connectionsDestroyed = new AtomicLong(0);
  private final AtomicLong connectionsBorrowed = new AtomicLong(0);
  private final AtomicLong connectionsReturned = new AtomicLong(0);
  private final AtomicLong validationsFailed = new AtomicLong(0);
  private final AtomicLong waitTimeouts = new AtomicLong(0);

  /**
   * Creates a new connection pool with the specified configuration.
   *
   * @param jdbcUrl the JDBC URL for creating connections
   * @param config the pool configuration
   * @throws SQLException if the pool cannot be initialized
   */
  public DynamoDbConnectionPool(String jdbcUrl, PoolConfiguration config) throws SQLException {
    this.jdbcUrl = jdbcUrl;
    this.config = config;
    this.availableConnections = new LinkedBlockingDeque<>();

    // Create maintenance executor
    this.maintenanceExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "DynamoDbPool-Maintenance");
                thread.setDaemon(true);
                return thread;
              }
            });

    // Initialize pool with initial connections
    initializePool();

    // Schedule maintenance tasks
    scheduleMaintenanceTasks();

    logger.info(
        "DynamoDB connection pool initialized with min={}, max={}, initial={}",
        config.getMinPoolSize(),
        config.getMaxPoolSize(),
        config.getInitialPoolSize());
  }

  /**
   * Initializes the pool with the configured initial number of connections.
   *
   * @throws SQLException if connections cannot be created
   */
  private void initializePool() throws SQLException {
    for (int i = 0; i < config.getInitialPoolSize(); i++) {
      try {
        PooledConnection connection = createConnection();
        availableConnections.offer(connection);
      } catch (SQLException e) {
        // Log but continue - we'll create connections on demand
        logger.warn("Failed to create initial connection {}: {}", i + 1, e.getMessage());
      }
    }

    int created = availableConnections.size();
    if (created == 0 && config.getInitialPoolSize() > 0) {
      throw new SQLException("Failed to create any initial connections");
    }

    logger.debug("Created {} initial connections", created);
  }

  /**
   * Schedules periodic maintenance tasks for the pool.
   */
  private void scheduleMaintenanceTasks() {
    // Schedule eviction task
    long evictionInterval = config.getTimeBetweenEvictionRuns().toMillis();
    maintenanceExecutor.scheduleWithFixedDelay(
        this::evictIdleConnections, evictionInterval, evictionInterval, TimeUnit.MILLISECONDS);

    // Schedule min size enforcement
    maintenanceExecutor.scheduleWithFixedDelay(
        this::ensureMinimumConnections, 30, 30, TimeUnit.SECONDS);
  }

  /**
   * Borrows a connection from the pool.
   *
   * @return a pooled connection
   * @throws SQLException if a connection cannot be obtained
   */
  public Connection borrowConnection() throws SQLException {
    checkClosed();

    connectionsBorrowed.incrementAndGet();
    long startTime = System.currentTimeMillis();

    while (!closed.get()) {
      // Try to get an available connection
      PooledConnection connection = availableConnections.poll();

      if (connection != null) {
        // Validate if needed
        if (config.isTestOnBorrow() && !validateConnection(connection)) {
          destroyConnection(connection);
          continue;
        }

        // Check max lifetime
        if (isConnectionExpired(connection)) {
          destroyConnection(connection);
          continue;
        }

        activeConnections.incrementAndGet();
        connection.closed.set(false); // Reset closed flag
        return connection;
      }

      // No available connection - try to create one
      if (totalConnections.get() < config.getMaxPoolSize()) {
        try {
          connection = createConnection();
          activeConnections.incrementAndGet();
          return connection;
        } catch (SQLException e) {
          logger.warn("Failed to create new connection: {}", e.getMessage());
        }
      }

      // Wait for a connection to become available
      if (config.isBlockWhenExhausted()) {
        long waitTime = config.getMaxWaitTime().toMillis() - (System.currentTimeMillis() - startTime);
        if (waitTime <= 0) {
          waitTimeouts.incrementAndGet();
          throw new SQLException(
              "Timeout waiting for connection. Active: "
                  + activeConnections.get()
                  + ", Total: "
                  + totalConnections.get());
        }

        try {
          connection = availableConnections.poll(waitTime, TimeUnit.MILLISECONDS);
          if (connection != null) {
            if (config.isTestOnBorrow() && !validateConnection(connection)) {
              destroyConnection(connection);
              continue;
            }
            activeConnections.incrementAndGet();
            connection.closed.set(false);
            return connection;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException("Interrupted while waiting for connection", e);
        }
      } else {
        throw new SQLException("No connections available and pool is exhausted");
      }
    }

    throw new SQLException("Pool is closed");
  }

  /**
   * Returns a connection to the pool.
   *
   * @param connection the connection to return
   * @throws SQLException if the connection cannot be returned
   */
  void returnConnection(PooledConnection connection) throws SQLException {
    if (closed.get()) {
      destroyConnection(connection);
      return;
    }

    connectionsReturned.incrementAndGet();
    activeConnections.decrementAndGet();

    // Validate on return if configured
    if (config.isTestOnReturn() && !validateConnection(connection)) {
      destroyConnection(connection);
      return;
    }

    // Check if connection has exceeded max lifetime
    if (isConnectionExpired(connection)) {
      destroyConnection(connection);
      return;
    }

    // Return to pool
    if (config.isLifo()) {
      availableConnections.offerFirst(connection);
    } else {
      availableConnections.offerLast(connection);
    }
  }

  /**
   * Creates a new physical connection.
   *
   * @return a new pooled connection
   * @throws SQLException if the connection cannot be created
   */
  private PooledConnection createConnection() throws SQLException {
    DynamoDbDriver driver = new DynamoDbDriver();
    Connection physicalConnection =
        driver.connect(jdbcUrl, config.getConnectionProperties());
    
    if (physicalConnection == null) {
      throw new SQLException("Failed to create connection with URL: " + jdbcUrl);
    }

    if (!(physicalConnection instanceof DynamoDbConnection)) {
      throw new SQLException("Expected DynamoDbConnection but got: " + physicalConnection.getClass());
    }

    totalConnections.incrementAndGet();
    connectionsCreated.incrementAndGet();

    return new PooledConnection((DynamoDbConnection) physicalConnection, this);
  }

  /**
   * Destroys a connection and removes it from the pool.
   *
   * @param connection the connection to destroy
   */
  private void destroyConnection(PooledConnection connection) {
    totalConnections.decrementAndGet();
    connectionsDestroyed.incrementAndGet();

    try {
      connection.closePhysicalConnection();
    } catch (SQLException e) {
      logger.warn("Error closing physical connection: {}", e.getMessage());
    }
  }

  /**
   * Validates a connection.
   *
   * @param connection the connection to validate
   * @return true if valid, false otherwise
   */
  private boolean validateConnection(PooledConnection connection) {
    try {
      boolean valid = connection.isValid((int) config.getValidationTimeout().toSeconds());
      if (!valid) {
        validationsFailed.incrementAndGet();
      }
      connection.setLastValidationTime(Instant.now());
      return valid;
    } catch (SQLException e) {
      validationsFailed.incrementAndGet();
      logger.debug("Connection validation failed: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Checks if a connection has exceeded its maximum lifetime.
   *
   * @param connection the connection to check
   * @return true if expired, false otherwise
   */
  private boolean isConnectionExpired(PooledConnection connection) {
    Duration age = Duration.between(connection.getCreationTime(), Instant.now());
    return age.compareTo(config.getMaxLifetime()) > 0;
  }

  /**
   * Evicts idle connections from the pool.
   */
  private void evictIdleConnections() {
    if (closed.get()) {
      return;
    }

    int evicted = 0;
    int tested = 0;
    int toTest = Math.min(config.getNumTestsPerEvictionRun(), availableConnections.size());

    while (tested < toTest) {
      PooledConnection connection = availableConnections.poll();
      if (connection == null) {
        break;
      }

      tested++;

      // Check if connection is idle too long
      Duration idleTime = Duration.between(connection.getLastAccessTime(), Instant.now());
      if (idleTime.compareTo(config.getIdleTimeout()) > 0) {
        destroyConnection(connection);
        evicted++;
        continue;
      }

      // Check if connection exceeded max lifetime
      if (isConnectionExpired(connection)) {
        destroyConnection(connection);
        evicted++;
        continue;
      }

      // Test while idle if configured
      if (config.isTestWhileIdle() && !validateConnection(connection)) {
        destroyConnection(connection);
        evicted++;
        continue;
      }

      // Connection is good, return to pool
      availableConnections.offerLast(connection);
    }

    if (evicted > 0) {
      logger.debug("Evicted {} idle connections", evicted);
    }
  }

  /**
   * Ensures the pool has at least the minimum number of connections.
   */
  private void ensureMinimumConnections() {
    if (closed.get()) {
      return;
    }

    int current = totalConnections.get();
    int needed = config.getMinPoolSize() - current;

    for (int i = 0; i < needed; i++) {
      try {
        PooledConnection connection = createConnection();
        availableConnections.offer(connection);
      } catch (SQLException e) {
        logger.warn("Failed to create connection for minimum pool size: {}", e.getMessage());
        break;
      }
    }
  }

  /**
   * Checks if the pool is closed.
   *
   * @throws SQLException if the pool is closed
   */
  private void checkClosed() throws SQLException {
    if (closed.get()) {
      throw new SQLException("Connection pool is closed");
    }
  }

  /**
   * Closes the connection pool and all connections.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      logger.info("Closing DynamoDB connection pool");

      // Shutdown maintenance executor
      maintenanceExecutor.shutdown();
      try {
        if (!maintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          maintenanceExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        maintenanceExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      // Close all connections
      PooledConnection connection;
      while ((connection = availableConnections.poll()) != null) {
        destroyConnection(connection);
      }

      logger.info(
          "Connection pool closed. Created: {}, Destroyed: {}, Borrowed: {}, Returned: {}",
          connectionsCreated.get(),
          connectionsDestroyed.get(),
          connectionsBorrowed.get(),
          connectionsReturned.get());
    }
  }

  // Getters for metrics

  public int getTotalConnections() {
    return totalConnections.get();
  }

  public int getActiveConnections() {
    return activeConnections.get();
  }

  public int getIdleConnections() {
    return availableConnections.size();
  }

  public long getConnectionsCreated() {
    return connectionsCreated.get();
  }

  public long getConnectionsDestroyed() {
    return connectionsDestroyed.get();
  }

  public long getConnectionsBorrowed() {
    return connectionsBorrowed.get();
  }

  public long getConnectionsReturned() {
    return connectionsReturned.get();
  }

  public long getValidationsFailed() {
    return validationsFailed.get();
  }

  public long getWaitTimeouts() {
    return waitTimeouts.get();
  }
}