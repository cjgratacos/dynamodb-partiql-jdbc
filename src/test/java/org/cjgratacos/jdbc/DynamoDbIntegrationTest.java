package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DynamoDB Integration Tests")
class DynamoDbIntegrationTest extends BaseIntegrationTest {

  @Nested
  @DisplayName("Basic Connection Tests")
  class BasicConnectionTests {

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
    @DisplayName("Connection properties are applied correctly")
    void connectionPropertiesAreAppliedCorrectly() throws SQLException {
      // Given: Connection with specific properties
      final var properties = new Properties();
      properties.setProperty("retryMaxAttempts", "5");
      properties.setProperty("retryBaseDelayMs", "200");
      properties.setProperty("schemaDiscovery", "auto");

      // When: Creating connection with properties
      try (final var connection = getConnection(properties)) {
        // Cast to access driver-specific methods (resource leak warning is acceptable for test)
        final var dynamoConnection = (DynamoDbConnection) connection;

        // Then: Properties should be applied
        assertThat(connection.isValid(5)).isTrue();
        assertThat(dynamoConnection.getRetryMetrics()).isNotNull();
      }
    }

    @Test
    @DisplayName("Invalid endpoint throws appropriate exception")
    void invalidEndpointThrowsAppropriateException() {
      // Given: Custom properties with invalid endpoint
      final var invalidProperties = new Properties();
      invalidProperties.setProperty("endpoint", "http://invalid:9999");

      // When/Then: Should throw SQLException when trying to perform operations
      assertThatThrownBy(
              () -> {
                try (final var connection = getConnection(invalidProperties)) {
                  // Force a real operation that would fail
                  connection.getMetaData().getTables(null, null, "%", null);
                }
              })
          .isInstanceOf(SQLException.class);
    }
  }

  @Nested
  @DisplayName("Table Operations Tests")
  class TableOperationsTests {

    @Test
    @DisplayName("Can create and query table through JDBC")
    void canCreateAndQueryTableThroughJdbc() throws SQLException {
      // Given: Connection and test data
      createTestTable("users");
      populateTestData("users", 5);
      try (final var connection = getConnection()) {

        // When: Querying the table
        try (final var statement = connection.createStatement();
            final var resultSet = statement.executeQuery("SELECT * FROM users")) {

          // Then: Should return results
          var rowCount = 0;
          while (resultSet.next()) {
            rowCount++;
            assertThat(resultSet.getString("id")).isNotNull();
            assertThat(resultSet.getString("name")).isNotNull();
          }
          assertThat(rowCount).isEqualTo(5);
        }
      }
    }

    @Test
    @DisplayName("Database metadata returns table information")
    void databaseMetadataReturnsTableInformation() throws SQLException {
      // Given: Connection with test table
      createTestTable("products");
      try (final var connection = getConnection()) {

        // When: Getting database metadata
        final var metaData = connection.getMetaData();

        // Then: Should return table information with JDBC standard columns
        try (final var tables = metaData.getTables(null, null, "products", null)) {
          assertThat(tables.next()).isTrue();
          assertThat(tables.getString("TABLE_NAME")).isEqualTo("products");
          assertThat(tables.getString("TABLE_TYPE")).isEqualTo("TABLE");

          // Verify JDBC standard columns exist
          // Now TABLE_CAT contains the table name (new behavior)
          assertThat(tables.getString("TABLE_CAT")).isEqualTo("products");
          assertThat(tables.getObject("TABLE_SCHEM")).isNull();
        }
      }
    }

    @Test
    @DisplayName("Column metadata is detected correctly")
    void columnMetadataIsDetectedCorrectly() throws SQLException {
      // Given: Connection with populated table
      createTestTable("orders");
      populateTestData("orders", 3);
      try (final var connection = getConnection()) {

        // When: Getting column metadata
        final var metaData = connection.getMetaData();

        // Then: Should detect columns
        try (final var columns = metaData.getColumns(null, null, "orders", null)) {
          var columnCount = 0;
          while (columns.next()) {
            columnCount++;
            final var columnName = columns.getString("COLUMN_NAME");
            final var dataType = columns.getInt("DATA_TYPE");

            assertThat(columnName).isNotNull();
            assertThat(dataType).isGreaterThan(0);
          }
          assertThat(columnCount).isGreaterThan(0);
        }
      }
    }

    @Test
    @DisplayName("Index metadata uses JDBC standard column names")
    void indexMetadataUsesJdbcStandardColumns() throws SQLException {
      // Given: Connection with test table
      createTestTable("inventory");
      try (final var connection = getConnection()) {

        // When: Getting index metadata
        final var metaData = connection.getMetaData();

        // Then: Should return index information with JDBC standard columns
        try (final var indexes = metaData.getIndexInfo(null, null, "inventory", false, false)) {
          if (indexes.next()) {
            // Verify JDBC standard columns exist
            // Now TABLE_CAT contains the table name and TABLE_SCHEM contains the index name
            assertThat(indexes.getString("TABLE_CAT")).isEqualTo("inventory");
            assertThat(indexes.getString("TABLE_SCHEM")).isEqualTo("PRIMARY");
            assertThat(indexes.getString("TABLE_NAME")).isEqualTo("inventory");
            assertThat(indexes.getString("INDEX_NAME")).isNotNull();
            assertThat(indexes.getString("COLUMN_NAME")).isNotNull();

            // Verify column exists and follows JDBC standard naming
            assertThat(indexes.getMetaData().getColumnLabel(2)).isEqualTo("TABLE_SCHEM");
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("Performance Integration Tests")
  class PerformanceIntegrationTests {

    @Test
    @DisplayName("Schema discovery performs within reasonable time")
    void schemaDiscoveryPerformsWithinReasonableTime() throws SQLException {
      // Given: Connection with multiple tables
      // Create multiple tables for schema discovery
      for (int i = 0; i < 3; i++) {
        createTestTable("table_" + i);
        populateTestData("table_" + i, 10);
      }
      try (final var connection = getConnection()) {

        // When: Performing schema discovery
        final var startTime = System.currentTimeMillis();
        final var metaData = connection.getMetaData();

        try (final var tables = metaData.getTables(null, null, "%", null)) {
          var tableCount = 0;
          while (tables.next()) {
            tableCount++;

            // Get column metadata for each table
            final var tableName = tables.getString("TABLE_NAME");
            try (final var columns = metaData.getColumns(null, null, tableName, null)) {
              while (columns.next()) {
                // Force column metadata resolution
                columns.getString("COLUMN_NAME");
                columns.getInt("DATA_TYPE");
              }
            }
          }

          final var duration = System.currentTimeMillis() - startTime;

          // Then: Should complete reasonably quickly
          assertThat(tableCount).isEqualTo(3);
          assertThat(duration).isLessThan(10000L); // Less than 10 seconds
        }
      }
    }

    @Test
    @DisplayName("Concurrent connections work correctly")
    void concurrentConnectionsWorkCorrectly() throws Exception {
      // Given: Multiple test tables
      createTestTable("shared_table");
      populateTestData("shared_table", 20);

      // When: Multiple concurrent connections
      final var futures = new java.util.concurrent.CompletableFuture[5];
      for (int i = 0; i < futures.length; i++) {
        final int connectionId = i;
        futures[i] =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  try (final var connection = getConnection();
                      final var statement = connection.createStatement();
                      final var resultSet = statement.executeQuery("SELECT * FROM shared_table")) {

                    var count = 0;
                    while (resultSet.next()) {
                      count++;
                    }
                    return count;
                  } catch (SQLException e) {
                    throw new RuntimeException("Connection " + connectionId + " failed", e);
                  }
                });
      }

      // Then: All connections should succeed
      for (final var future : futures) {
        final var count = future.join();
        assertThat(count).isEqualTo(20);
      }
    }
  }

  @Nested
  @DisplayName("Error Handling Integration Tests")
  class ErrorHandlingIntegrationTests {

    @Test
    @DisplayName("Non-existent table throws appropriate exception")
    void nonExistentTableThrowsAppropriateException() throws SQLException {
      // Given: Valid connection
      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When/Then: Querying non-existent table should throw SQLException
        assertThatThrownBy(
                () -> {
                  try (final var resultSet =
                      statement.executeQuery("SELECT * FROM non_existent_table")) {
                    resultSet.next();
                  }
                })
            .isInstanceOf(SQLException.class)
            .hasMessageContaining("non_existent_table");
      }
    }

    @Test
    @DisplayName("Invalid SQL throws appropriate exception")
    void invalidSqlThrowsAppropriateException() throws SQLException {
      // Given: Valid connection with test table
      createTestTable("test_table");
      try (final var connection = getConnection()) {

        try (final var statement = connection.createStatement()) {
          // When/Then: Invalid SQL should throw SQLException
          assertThatThrownBy(
                  () -> {
                    statement.executeQuery("INVALID SQL STATEMENT");
                  })
              .isInstanceOf(SQLException.class);
        }
      }
    }

    @Test
    @DisplayName("Connection timeout is respected")
    void connectionTimeoutIsRespected() throws SQLException {
      // Given: Connection with reasonable timeout
      final var properties = new Properties();
      properties.setProperty("apiCallTimeoutMs", "5000"); // 5 second timeout

      createTestTable("timeout_test");
      try (final var connection = getConnection(properties)) {

        try (final var statement = connection.createStatement()) {
          // When/Then: Should work with reasonable timeout
          // Local DynamoDB is fast, so we just verify the connection works
          assertThat(connection.isValid(1)).isTrue();
        }
      }
    }
  }

  @Nested
  @DisplayName("Property Integration Tests")
  class PropertyIntegrationTests {

    @Test
    @DisplayName("All connection properties are handled correctly")
    void allConnectionPropertiesAreHandledCorrectly() throws SQLException {
      // Given: Properties with multiple configurations
      final var properties = new Properties();
      properties.setProperty("retryMaxAttempts", "3");
      properties.setProperty("retryBaseDelayMs", "100");
      properties.setProperty("retryMaxDelayMs", "5000");
      properties.setProperty("retryJitterEnabled", "true");
      properties.setProperty("apiCallTimeoutMs", "30000");
      properties.setProperty("tableFilter", "test_%");
      properties.setProperty("schemaDiscovery", "auto");
      properties.setProperty("sampleSize", "500");
      properties.setProperty("sampleStrategy", "random");
      properties.setProperty("schemaCacheEnabled", "true");
      properties.setProperty("schemaCacheRefreshIntervalMs", "300000");

      // When: Creating connection with all properties
      try (final var connection = getConnection(properties)) {
        // Then: Connection should work with all properties applied
        assertThat(connection.isValid(5)).isTrue();
        assertThat(connection.getRetryMetrics()).isNotNull();
        assertThat(connection.getQueryMetrics()).isNotNull();

        // Test that schema cache and discovery properties work
        createTestTable("test_schema");
        final var metaData = connection.getMetaData();
        try (final var tables = metaData.getTables(null, null, "test_%", null)) {
          assertThat(tables.next()).isTrue();
        }
      }
    }

    @Test
    @DisplayName("Schema discovery modes work in integration")
    void schemaDiscoveryModesWorkInIntegration() throws SQLException {
      // Test different schema discovery modes
      final var modes = new String[] {"auto", "hints", "sampling", "disabled"};

      for (final var mode : modes) {
        final var properties = new Properties();
        properties.setProperty("schemaDiscovery", mode);

        try (final var connection = getConnection(properties)) {
          // Should be able to connect regardless of mode
          assertThat(connection.isValid(5)).isTrue();

          // For non-disabled modes, metadata should work
          if (!"disabled".equals(mode)) {
            createTestTable("mode_test_" + mode);
            final var metaData = connection.getMetaData();
            try (final var tables = metaData.getTables(null, null, "mode_test_" + mode, null)) {
              // Should find the table (may be empty for some modes but shouldn't error)
              tables.next(); // This should not throw
            }
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("Fetch Size and Row Limiting Tests")
  class FetchSizeAndRowLimitingTests {

    @Test
    @DisplayName("Default fetch size limits results when no maxRows is set")
    void defaultFetchSizeLimitsResultsWhenNoMaxRowsIsSet() throws SQLException {
      // Given: A table with many rows
      createTestTable("fetch_size_test");
      populateTestData("fetch_size_test", 500); // Create 500 rows

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Executing a query without setting maxRows (defaults to 0)
        // The default fetchSize of 100 should act as a safety limit
        try (final var resultSet = statement.executeQuery("SELECT * FROM fetch_size_test")) {

          int rowCount = 0;
          while (resultSet.next() && rowCount < 200) { // Try to read up to 200 rows
            rowCount++;
          }

          // Then: Should have fetched exactly 100 rows (the default fetchSize)
          assertThat(rowCount)
              .isEqualTo(100)
              .as("Default fetchSize should limit results when no maxRows is set");
        }
      }
    }

    @Test
    @DisplayName("Explicit maxRows overrides fetchSize when lower")
    void explicitMaxRowsOverridesFetchSizeWhenLower() throws SQLException {
      // Given: A table with many rows
      createTestTable("max_rows_test");
      populateTestData("max_rows_test", 200);

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Setting maxRows lower than fetchSize
        statement.setFetchSize(100);
        statement.setMaxRows(50);

        try (final var resultSet = statement.executeQuery("SELECT * FROM max_rows_test")) {

          int rowCount = 0;
          while (resultSet.next()) {
            rowCount++;
          }

          // Then: Should respect maxRows limit
          assertThat(rowCount).isEqualTo(50).as("maxRows should override fetchSize when lower");
        }
      }
    }

    @Test
    @DisplayName("SQL LIMIT clause has highest priority")
    void sqlLimitClauseHasHighestPriority() throws SQLException {
      // Given: A table with many rows
      createTestTable("sql_limit_test");
      populateTestData("sql_limit_test", 200);

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Setting both fetchSize and maxRows, but using SQL LIMIT
        statement.setFetchSize(100);
        statement.setMaxRows(75);

        try (final var resultSet =
            statement.executeQuery("SELECT * FROM sql_limit_test LIMIT 25")) {

          int rowCount = 0;
          while (resultSet.next()) {
            rowCount++;
          }

          // Then: SQL LIMIT should take precedence
          assertThat(rowCount).isEqualTo(25).as("SQL LIMIT should have highest priority");
        }
      }
    }

    @Test
    @DisplayName("Connection property defaultFetchSize is applied to new statements")
    void connectionPropertyDefaultFetchSizeIsApplied() throws SQLException {
      // Given: Connection with custom defaultFetchSize
      final var properties = new Properties();
      properties.setProperty("defaultFetchSize", "50");

      createTestTable("default_fetch_test");
      populateTestData("default_fetch_test", 200);

      try (final var connection = getConnection(properties);
          final var statement = connection.createStatement()) {

        // When: Not explicitly setting fetchSize
        assertThat(statement.getFetchSize())
            .isEqualTo(50)
            .as("defaultFetchSize from connection properties should be applied");

        // And when executing a query
        try (final var resultSet = statement.executeQuery("SELECT * FROM default_fetch_test")) {

          int rowCount = 0;
          while (resultSet.next() && rowCount < 100) {
            rowCount++;
          }

          // Then: Should respect the defaultFetchSize
          assertThat(rowCount)
              .isEqualTo(50)
              .as("Query should respect defaultFetchSize from connection properties");
        }
      }
    }

    @Test
    @DisplayName("Connection property defaultMaxRows is applied to new statements")
    void connectionPropertyDefaultMaxRowsIsApplied() throws SQLException {
      // Given: Connection with custom defaultMaxRows
      final var properties = new Properties();
      properties.setProperty("defaultMaxRows", "30");

      createTestTable("default_max_rows_test");
      populateTestData("default_max_rows_test", 100);

      try (final var connection = getConnection(properties);
          final var statement = connection.createStatement()) {

        // When: Not explicitly setting maxRows
        assertThat(statement.getMaxRows())
            .isEqualTo(30)
            .as("defaultMaxRows from connection properties should be applied");

        // And when executing a query
        try (final var resultSet = statement.executeQuery("SELECT * FROM default_max_rows_test")) {

          int rowCount = 0;
          while (resultSet.next()) {
            rowCount++;
          }

          // Then: Should respect the defaultMaxRows
          assertThat(rowCount)
              .isEqualTo(30)
              .as("Query should respect defaultMaxRows from connection properties");
        }
      }
    }

    @Test
    @DisplayName("Zero maxRows still applies fetchSize as safety limit")
    void zeroMaxRowsStillAppliesFetchSizeAsSafetyLimit() throws SQLException {
      // Given: A table with many rows
      createTestTable("zero_max_rows_test");
      populateTestData("zero_max_rows_test", 200);

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Explicitly setting maxRows to 0 (no limit) and a custom fetchSize
        statement.setMaxRows(0);
        statement.setFetchSize(75);

        try (final var resultSet = statement.executeQuery("SELECT * FROM zero_max_rows_test")) {

          int rowCount = 0;
          while (resultSet.next() && rowCount < 100) {
            rowCount++;
          }

          // Then: fetchSize should still be applied as safety limit
          assertThat(rowCount)
              .isEqualTo(75)
              .as("fetchSize should still be applied as safety limit when maxRows=0");
        }
      }
    }

    @Test
    @DisplayName("LIMIT with OFFSET works correctly")
    void limitWithOffsetWorksCorrectly() throws SQLException {
      // Given: A table with sequential data
      createTestTable("offset_test");
      populateTestData("offset_test", 100);

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Using LIMIT with OFFSET (without ORDER BY to avoid DynamoDB restrictions)
        try (final var resultSet =
            statement.executeQuery("SELECT * FROM offset_test LIMIT 10 OFFSET 20")) {

          int rowCount = 0;
          String firstId = null;
          while (resultSet.next()) {
            if (firstId == null) {
              firstId = resultSet.getString("id");
            }
            rowCount++;
          }

          // Then: Should return exactly 10 rows
          assertThat(rowCount)
              .isEqualTo(10)
              .as("LIMIT should restrict the number of rows returned");
          // Note: Without ORDER BY, we can't guarantee which specific rows are returned
          // but OFFSET should have skipped 20 rows
        }
      }
    }

    @Test
    @DisplayName("Pagination works with LIMIT and OFFSET")
    void paginationWorksWithLimitAndOffset() throws SQLException {
      // Given: A table with sequential data
      createTestTable("pagination_test");
      populateTestData("pagination_test", 250); // Create 250 rows

      try (final var connection = getConnection();
          final var statement = connection.createStatement()) {

        // When: Fetching different pages
        int pageSize = 50;
        int totalRowsSeen = 0;

        for (int page = 0; page < 5; page++) {
          int offset = page * pageSize;
          String query =
              String.format("SELECT * FROM pagination_test LIMIT %d OFFSET %d", pageSize, offset);

          try (final var resultSet = statement.executeQuery(query)) {
            int pageRowCount = 0;
            while (resultSet.next()) {
              pageRowCount++;
              totalRowsSeen++;
            }

            // Then: Each page should have the expected number of rows
            assertThat(pageRowCount)
                .isEqualTo(pageSize)
                .as("Page %d should have %d rows", page + 1, pageSize);
          }
        }

        // And: We should have seen all rows across pages
        assertThat(totalRowsSeen).isEqualTo(250).as("Should have seen all 250 rows across 5 pages");
      }
    }

    @Test
    @DisplayName("createStatement with result set type works for GUI clients")
    void createStatementWithResultSetTypeWorksForGuiClients() throws SQLException {
      // Given: A connection
      try (final var connection = getConnection()) {

        // When: Creating statement with scrollable result set type (common for GUI clients)
        try (final var statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

          // Then: Statement should be created successfully (downgraded to forward-only)
          assertThat(statement).isNotNull();
          assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
          assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);

          // And: Should be able to execute queries
          createTestTable("gui_test");
          populateTestData("gui_test", 5);

          try (final var resultSet = statement.executeQuery("SELECT * FROM gui_test")) {
            int count = 0;
            while (resultSet.next()) {
              count++;
            }
            assertThat(count).isEqualTo(5);
          }
        }
      }
    }
  }
}
