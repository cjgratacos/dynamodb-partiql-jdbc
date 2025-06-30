package org.cjgratacos.jdbc.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.cjgratacos.jdbc.DynamoDbDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class DynamoDbConnectionPoolTest {

  private static final String TEST_JDBC_URL = "jdbc:dynamodb:partiql:region=us-east-1";
  private DynamoDbConnectionPool pool;
  private PoolConfiguration config;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty("region", "us-east-1");
    config =
        new PoolConfiguration.Builder(props)
            .minPoolSize(2)
            .maxPoolSize(10)
            .initialPoolSize(3)
            .connectionTimeout(Duration.ofSeconds(5))
            .idleTimeout(Duration.ofMinutes(5))
            .maxLifetime(Duration.ofMinutes(30))
            .testOnBorrow(true)
            .build();
  }

  @AfterEach
  void tearDown() {
    if (pool != null) {
      pool.close();
    }
  }

  @Test
  void testPoolInitialization() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Verify initial connections were created
      assertThat(pool.getTotalConnections()).isEqualTo(config.getInitialPoolSize());
      assertThat(pool.getIdleConnections()).isEqualTo(config.getInitialPoolSize());
      assertThat(pool.getActiveConnections()).isEqualTo(0);
    }
  }

  @Test
  void testBorrowConnection() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Borrow a connection
      Connection conn = pool.borrowConnection();
      assertThat(conn).isNotNull();
      assertThat(conn).isInstanceOf(PooledConnection.class);

      // Check metrics
      assertThat(pool.getActiveConnections()).isEqualTo(1);
      assertThat(pool.getIdleConnections()).isEqualTo(config.getInitialPoolSize() - 1);
      assertThat(pool.getConnectionsBorrowed()).isEqualTo(1);

      // Return connection
      conn.close();
      assertThat(pool.getActiveConnections()).isEqualTo(0);
      assertThat(pool.getIdleConnections()).isEqualTo(config.getInitialPoolSize());
      assertThat(pool.getConnectionsReturned()).isEqualTo(1);
    }
  }

  @Test
  void testMaxPoolSize() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      // Create pool with small max size and no blocking
      config =
          new PoolConfiguration.Builder(new Properties())
              .minPoolSize(1)
              .maxPoolSize(3)
              .initialPoolSize(1)
              .build();
      config.getConnectionProperties().setProperty("pool.blockWhenExhausted", "false");

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Borrow all available connections
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < config.getMaxPoolSize(); i++) {
        connections.add(pool.borrowConnection());
      }

      assertThat(pool.getTotalConnections()).isEqualTo(config.getMaxPoolSize());
      assertThat(pool.getActiveConnections()).isEqualTo(config.getMaxPoolSize());

      // Try to borrow one more - should fail (or timeout)
      assertThatThrownBy(() -> pool.borrowConnection())
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Timeout waiting for connection");

      // Return connections
      for (Connection conn : connections) {
        conn.close();
      }
    }
  }

  @Test
  void testConnectionValidation() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              // First call returns true, subsequent calls return false
              when(connection.isValid(any(Integer.class))).thenReturn(true, false);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Borrow and return a connection
      Connection conn = pool.borrowConnection();
      conn.close();

      // Borrow again - validation should fail and create new connection
      conn = pool.borrowConnection();
      assertThat(conn).isNotNull();

      // Verify validation failure was recorded
      assertThat(pool.getValidationsFailed()).isGreaterThan(0);
    }
  }

  @Test
  @Timeout(10)
  void testConcurrentAccess() throws Exception {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      int threadCount = 20;
      int operationsPerThread = 100;
      ExecutorService executor = Executors.newFixedThreadPool(threadCount);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threadCount);
      AtomicInteger successCount = new AtomicInteger(0);
      AtomicInteger errorCount = new AtomicInteger(0);

      // Submit concurrent tasks
      for (int i = 0; i < threadCount; i++) {
        executor.submit(
            () -> {
              try {
                startLatch.await();
                for (int j = 0; j < operationsPerThread; j++) {
                  try {
                    Connection conn = pool.borrowConnection();
                    // Simulate some work
                    Thread.sleep(1);
                    conn.close();
                    successCount.incrementAndGet();
                  } catch (SQLException e) {
                    errorCount.incrementAndGet();
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                doneLatch.countDown();
              }
            });
      }

      // Start all threads
      startLatch.countDown();

      // Wait for completion
      assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

      executor.shutdown();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

      // Verify results
      assertThat(successCount.get()).isEqualTo(threadCount * operationsPerThread);
      assertThat(errorCount.get()).isEqualTo(0);
      assertThat(pool.getConnectionsBorrowed()).isEqualTo(threadCount * operationsPerThread);
      assertThat(pool.getConnectionsReturned()).isEqualTo(threadCount * operationsPerThread);
    }
  }

  @Test
  void testConnectionTimeout() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      // Configure with very short timeout
      config =
          new PoolConfiguration.Builder(new Properties())
              .minPoolSize(1)
              .maxPoolSize(1)
              .initialPoolSize(1)
              .maxWaitTime(Duration.ofMillis(100))
              .build();

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Borrow the only connection
      Connection conn1 = pool.borrowConnection();

      // Try to borrow another - should timeout
      long start = System.currentTimeMillis();
      assertThatThrownBy(() -> pool.borrowConnection())
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Timeout waiting for connection");
      long elapsed = System.currentTimeMillis() - start;

      // Verify timeout was respected (with some tolerance)
      assertThat(elapsed).isLessThan(500);
      assertThat(pool.getWaitTimeouts()).isEqualTo(1);

      conn1.close();
    }
  }

  @Test
  void testPoolClose() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Borrow some connections
      Connection conn1 = pool.borrowConnection();
      Connection conn2 = pool.borrowConnection();

      // Close pool
      pool.close();

      // Verify connections were destroyed (at least the borrowed ones)
      assertThat(pool.getConnectionsDestroyed()).isGreaterThanOrEqualTo(1);

      // Try to borrow after close - should fail
      assertThatThrownBy(() -> pool.borrowConnection())
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("closed");

      // Closing borrowed connections should not cause errors
      conn1.close();
      conn2.close();
    }
  }

  @Test
  void testConnectionLifecycle() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(connection.isClosed()).thenReturn(false);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      pool = new DynamoDbConnectionPool(TEST_JDBC_URL, config);

      // Test that physical connection methods are delegated
      Connection pooledConn = pool.borrowConnection();
      assertThat(pooledConn).isInstanceOf(PooledConnection.class);

      // Verify delegation
      pooledConn.createStatement();
      verify(
              ((PooledConnection) pooledConn).getPhysicalConnection(),
              times(1))
          .createStatement();

      // Test that close returns to pool instead of closing physical connection
      pooledConn.close();
      verify(((PooledConnection) pooledConn).getPhysicalConnection(), times(0)).close();

      assertThat(pool.getIdleConnections()).isEqualTo(config.getInitialPoolSize());
    }
  }
}