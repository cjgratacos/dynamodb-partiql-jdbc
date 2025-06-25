package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Simple DynamoDB Integration Tests")
class SimpleDynamoDbIntegrationTest extends BaseIntegrationTest {

  @Nested
  @DisplayName("Connection Tests")
  class ConnectionTests {

    @Test
    @DisplayName("Can establish connection to DynamoDB Local")
    void canEstablishConnectionToDynamoDbLocal() throws SQLException {
      // When: Creating connection
      try (final var connection = getConnection()) {
        // Then: Should be connected and valid
        assertThat(connection.isValid(5)).isTrue();
        assertThat(connection.isClosed()).isFalse();
        assertThat(connection.getMetaData()).isNotNull();
      }
    }

    @Test
    @DisplayName("Connection with properties works")
    void connectionWithPropertiesWorks() throws SQLException {
      // Given: Connection with specific properties
      final var properties = new Properties();
      properties.setProperty("retryMaxAttempts", "3");
      properties.setProperty("schemaDiscovery", "auto");

      // When: Creating connection with properties
      try (final var connection = getConnection(properties)) {
        // Then: Should work
        assertThat(connection.isValid(5)).isTrue();
        assertThat(connection.getMetaData()).isNotNull();
      }
    }

    @Test
    @DisplayName("Multiple connections work concurrently")
    void multipleConnectionsWorkConcurrently() throws Exception {
      // Given: Multiple connection attempts
      final var futures = new java.util.concurrent.CompletableFuture[3];

      // When: Creating multiple connections concurrently
      for (int i = 0; i < futures.length; i++) {
        futures[i] =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  try (final var connection = getConnection()) {
                    return Boolean.valueOf(connection.isValid(5));
                  } catch (SQLException e) {
                    return Boolean.FALSE;
                  }
                });
      }

      // Then: All connections should succeed
      for (final var future : futures) {
        assertThat(future.join()).isEqualTo(Boolean.TRUE);
      }
    }
  }

  @Nested
  @DisplayName("Basic Table Operations")
  class BasicTableOperationsTests {

    @Test
    @DisplayName("Can create table and query through JDBC")
    void canCreateTableAndQueryThroughJdbc() throws SQLException {
      // Given: Connection and test data
      try (final var connection = getConnection()) {
        createTestTable("test_users");
        populateTestData("test_users", 3);

        // When: Querying the table
        try (final var statement = connection.createStatement();
            final var resultSet = statement.executeQuery("SELECT * FROM test_users")) {

          // Then: Should return results
          var rowCount = 0;
          while (resultSet.next()) {
            rowCount++;
            assertThat(resultSet.getString("id")).isNotNull();
          }
          assertThat(rowCount).isEqualTo(3);
        }
      }
    }

    @Test
    @DisplayName("Database metadata returns table information")
    void databaseMetadataReturnsTableInformation() throws SQLException {
      // Given: Connection with test table
      try (final var connection = getConnection()) {
        createTestTable("meta_test");

        // When: Getting database metadata
        final var metaData = connection.getMetaData();

        // Then: Should have metadata capabilities
        assertThat(metaData.getDatabaseProductName()).contains("DynamoDB");
        assertThat(metaData.getDriverName()).contains("DynamoDB");
        assertThat(metaData.supportsResultSetType(java.sql.ResultSet.TYPE_FORWARD_ONLY))
            .isEqualTo(true);
      }
    }

    @Test
    @DisplayName("Can query count from populated table")
    void canQueryCountFromPopulatedTable() throws SQLException {
      // Given: Connection with populated table
      try (final var connection = getConnection()) {
        createTestTable("count_test");
        populateTestData("count_test", 5);

        // When: Querying all items (count manually)
        try (final var statement = connection.createStatement();
            final var resultSet = statement.executeQuery("SELECT * FROM count_test")) {

          // Then: Should return correct count
          var count = 0;
          while (resultSet.next()) {
            count++;
          }
          assertThat(count).isEqualTo(5);
        }
      }
    }
  }

  @Nested
  @DisplayName("Property Integration")
  class PropertyIntegrationTests {

    @Test
    @DisplayName("Schema discovery property is applied")
    void schemaDiscoveryPropertyIsApplied() throws SQLException {
      // Given: Properties with schema discovery settings
      final var properties = new Properties();
      properties.setProperty("schemaDiscovery", "auto");
      properties.setProperty("sampleSize", "100");

      // When: Creating connection
      try (final var connection = getConnection(properties)) {
        // Then: Should connect successfully
        assertThat(connection.isValid(5)).isTrue();

        // Create and access table to trigger schema discovery
        createTestTable("schema_test");
        final var metaData = connection.getMetaData();
        assertThat(metaData).isNotNull();
      }
    }

    @Test
    @DisplayName("Retry properties are applied")
    void retryPropertiesAreApplied() throws SQLException {
      // Given: Properties with retry settings
      final var properties = new Properties();
      properties.setProperty("retryMaxAttempts", "2");
      properties.setProperty("retryBaseDelayMs", "50");

      // When: Creating connection
      try (final var connection = getConnection(properties)) {
        // Then: Should connect and have retry configuration
        assertThat(connection.isValid(5)).isTrue();

        // Access driver-specific metrics to verify retry configuration
        assertThat(connection.getRetryMetrics()).isNotNull();
      }
    }

    @Test
    @DisplayName("Multiple property combinations work")
    void multiplePropertyCombinationsWork() throws SQLException {
      // Given: Properties with multiple configurations
      final var properties = new Properties();
      properties.setProperty("schemaDiscovery", "sampling");
      properties.setProperty("retryMaxAttempts", "3");
      properties.setProperty("sampleSize", "50");
      properties.setProperty("retryJitterEnabled", "true");

      // When: Creating connection
      try (final var connection = getConnection(properties)) {
        // Then: Should work with all properties
        assertThat(connection.isValid(5)).isTrue();
        assertThat(connection.getMetaData()).isNotNull();
      }
    }
  }

  @Nested
  @DisplayName("Performance Integration")
  class PerformanceIntegrationTests {

    @Test
    @DisplayName("Connection creation is reasonably fast")
    void connectionCreationIsReasonablyFast() throws SQLException {
      // When: Measuring connection creation time
      final var startTime = System.currentTimeMillis();

      try (final var connection = getConnection()) {
        final var connectionTime = System.currentTimeMillis() - startTime;

        // Then: Should connect reasonably quickly
        assertThat(connection.isValid(5)).isTrue();
        assertThat(connectionTime).isLessThan(5000L); // Less than 5 seconds
      }
    }

    @Test
    @DisplayName("Simple query execution is fast")
    void simpleQueryExecutionIsFast() throws SQLException {
      // Given: Connection with test data
      try (final var connection = getConnection()) {
        createTestTable("perf_test");
        populateTestData("perf_test", 10);

        // When: Executing simple query
        final var startTime = System.currentTimeMillis();

        try (final var statement = connection.createStatement();
            final var resultSet = statement.executeQuery("SELECT * FROM perf_test")) {

          var count = 0;
          while (resultSet.next() && count < 5) { // Manual limit
            count++;
          }

          final var queryTime = System.currentTimeMillis() - startTime;

          // Then: Should execute quickly and return results
          assertThat(count).isEqualTo(5);
          assertThat(queryTime).isLessThan(3000L); // Less than 3 seconds
        }
      }
    }
  }
}
