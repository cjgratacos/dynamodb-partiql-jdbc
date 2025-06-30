package org.cjgratacos.jdbc.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.cjgratacos.jdbc.DynamoDbDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class DynamoDbConnectionPoolDataSourceTest {

  private static final String TEST_JDBC_URL = "jdbc:dynamodb:partiql:region=us-east-1";
  private DynamoDbConnectionPoolDataSource dataSource;

  @BeforeEach
  void setUp() {
    dataSource = new DynamoDbConnectionPoolDataSource();
  }

  @AfterEach
  void tearDown() {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  void testDataSourceConfiguration() {
    // Test property setters
    dataSource.setJdbcUrl(TEST_JDBC_URL);
    dataSource.setMinPoolSize(5);
    dataSource.setMaxPoolSize(20);
    dataSource.setInitialPoolSize(10);
    dataSource.setConnectionTimeout(30);
    dataSource.setIdleTimeout(600);
    dataSource.setMaxLifetime(1800);
    dataSource.setTestOnBorrow(true);
    dataSource.setTestOnReturn(false);
    dataSource.setTestWhileIdle(true);

    assertThat(dataSource.getJdbcUrl()).isEqualTo(TEST_JDBC_URL);

    // Test that properties are stored
    dataSource.setConnectionProperty("customProp", "customValue");
  }

  @Test
  void testGetConnectionWithoutUrl() {
    // Should throw when URL not set
    assertThatThrownBy(() -> dataSource.getConnection())
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("JDBC URL is not set");
  }

  @Test
  void testGetConnection() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      dataSource.setJdbcUrl(TEST_JDBC_URL);
      dataSource.setMinPoolSize(2);
      dataSource.setMaxPoolSize(10);
      dataSource.setInitialPoolSize(3);

      // Get connection - should initialize pool
      Connection conn = dataSource.getConnection();
      assertThat(conn).isNotNull();
      assertThat(conn).isInstanceOf(PooledConnection.class);

      // Get statistics
      DynamoDbConnectionPoolDataSource.PoolStatistics stats = dataSource.getPoolStatistics();
      assertThat(stats).isNotNull();
      assertThat(stats.getTotalConnections()).isGreaterThanOrEqualTo(3);
      assertThat(stats.getActiveConnections()).isEqualTo(1);
      assertThat(stats.getIdleConnections()).isGreaterThanOrEqualTo(2);

      conn.close();

      // After close, active should be 0
      stats = dataSource.getPoolStatistics();
      assertThat(stats.getActiveConnections()).isEqualTo(0);
    }
  }

  @Test
  void testGetConnectionWithUsernamePassword() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      dataSource.setJdbcUrl(TEST_JDBC_URL);

      // Should ignore username/password since DynamoDB uses AWS credentials
      Connection conn = dataSource.getConnection("user", "pass");
      assertThat(conn).isNotNull();
      conn.close();
    }
  }

  @Test
  void testDataSourceWrapper() throws SQLException {
    // Test wrapper methods
    assertThat(dataSource.isWrapperFor(DynamoDbConnectionPoolDataSource.class)).isTrue();
    assertThat(dataSource.isWrapperFor(String.class)).isFalse();

    assertThat(dataSource.unwrap(DynamoDbConnectionPoolDataSource.class)).isEqualTo(dataSource);
    assertThatThrownBy(() -> dataSource.unwrap(String.class))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Cannot unwrap");
  }

  @Test
  void testPoolStatisticsBeforeInit() {
    // Should return null before pool is initialized
    assertThat(dataSource.getPoolStatistics()).isNull();
  }

  @Test
  void testDataSourceClose() throws SQLException {
    try (MockedConstruction<DynamoDbDriver> driverMock =
        Mockito.mockConstruction(
            DynamoDbDriver.class,
            (mock, context) -> {
              DynamoDbConnection connection = mock(DynamoDbConnection.class);
              when(connection.isValid(any(Integer.class))).thenReturn(true);
              when(mock.connect(anyString(), any(Properties.class))).thenReturn(connection);
            })) {

      dataSource.setJdbcUrl(TEST_JDBC_URL);

      // Initialize pool
      Connection conn = dataSource.getConnection();
      conn.close();

      // Close data source
      dataSource.close();

      // Should initialize a new pool after close
      Connection conn2 = dataSource.getConnection();
      assertThat(conn2).isNotNull();
      conn2.close();
    }
  }

  @Test
  void testConstructorVariants() {
    // Test different constructors
    DynamoDbConnectionPoolDataSource ds1 = new DynamoDbConnectionPoolDataSource(TEST_JDBC_URL);
    assertThat(ds1.getJdbcUrl()).isEqualTo(TEST_JDBC_URL);
    ds1.close();

    PoolConfiguration config = new PoolConfiguration(new Properties());
    DynamoDbConnectionPoolDataSource ds2 =
        new DynamoDbConnectionPoolDataSource(TEST_JDBC_URL, config);
    assertThat(ds2.getJdbcUrl()).isEqualTo(TEST_JDBC_URL);
    assertThat(ds2.getPoolConfiguration()).isEqualTo(config);
    ds2.close();
  }

  @Test
  void testPoolStatisticsToString() {
    DynamoDbConnectionPoolDataSource.PoolStatistics stats =
        new DynamoDbConnectionPoolDataSource.PoolStatistics(
            10, // total
            3, // active
            7, // idle
            15L, // created
            5L, // destroyed
            100L, // borrowed
            97L, // returned
            2L, // validation failed
            1L // wait timeouts
            );

    String str = stats.toString();
    assertThat(str).contains("total=10");
    assertThat(str).contains("active=3");
    assertThat(str).contains("idle=7");
    assertThat(str).contains("created=15");
    assertThat(str).contains("destroyed=5");
    assertThat(str).contains("borrowed=100");
    assertThat(str).contains("returned=97");
    assertThat(str).contains("validationsFailed=2");
    assertThat(str).contains("waitTimeouts=1");
  }
}