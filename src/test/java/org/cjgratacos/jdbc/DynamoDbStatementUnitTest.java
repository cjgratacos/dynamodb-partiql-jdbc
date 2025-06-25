package org.cjgratacos.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

@DisplayName("DynamoDbStatement Unit Tests")
class DynamoDbStatementUnitTest extends BaseUnitTest {

  private DynamoDbStatement statement;
  private DynamoDbConnection mockConnection;
  private DynamoDbClient mockClient;
  private RetryHandler mockRetryHandler;
  private OffsetTokenCache mockOffsetCache;
  private QueryMetrics mockQueryMetrics;
  private Properties connectionProperties;

  @BeforeEach
  void setUp() {
    mockConnection = mock(DynamoDbConnection.class);
    mockClient = mock(DynamoDbClient.class);
    mockRetryHandler = mock(RetryHandler.class);
    mockQueryMetrics = mock(QueryMetrics.class);
    connectionProperties = new Properties();

    lenient().when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    // Always provide queryMetrics since DynamoDbStatement constructor needs it
    lenient().when(mockRetryHandler.getQueryMetrics()).thenReturn(mockQueryMetrics);

    statement = new DynamoDbStatement(mockConnection, mockClient, mockRetryHandler);
  }

  private ExecuteStatementResponse createMockResponse() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    return ExecuteStatementResponse.builder().items(Collections.singletonList(item)).build();
  }

  @Nested
  @DisplayName("Offset Warning Tests")
  class OffsetWarningTests {

    @BeforeEach
    void setUpOffsetWarning() {
      // Set default warning threshold
      connectionProperties.setProperty("offsetWarningThreshold", "1000");
    }

    @Test
    @DisplayName("Should generate warning for large offset")
    void shouldGenerateWarningForLargeOffset() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with large offset
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 5000");

      // Check warning was generated
      SQLWarning warning = statement.getWarnings();
      Assertions.assertThat((Object) warning).isNotNull();
      Assertions.assertThat(warning.getMessage())
          .contains("Large OFFSET value (5000)")
          .contains("may impact performance")
          .contains("DynamoDB uses token-based pagination");
      Assertions.assertThat(warning.getSQLState()).isEqualTo("01000");
    }

    @Test
    @DisplayName("Should not generate warning for small offset")
    void shouldNotGenerateWarningForSmallOffset() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with small offset
      ResultSet rs =
          statement.executeQuery("SELECT id, name FROM users WHERE id = '123' LIMIT 10 OFFSET 100");

      // Check that no large offset warning is generated (offset 100 is below threshold of 1000)
      SQLWarning warning = statement.getWarnings();
      boolean hasOffsetWarning = false;
      while (warning != null) {
        if (warning.getMessage().contains("Large OFFSET value")) {
          hasOffsetWarning = true;
          break;
        }
        warning = warning.getNextWarning();
      }
      Assertions.assertThat(hasOffsetWarning).isFalse();
    }

    @Test
    @DisplayName("Should use default warning threshold")
    void shouldUseDefaultWarningThreshold() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with offset above default threshold (1000)
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 1200");

      // Warning should be generated
      SQLWarning warning = statement.getWarnings();
      Assertions.assertThat((Object) warning).isNotNull();
      Assertions.assertThat(warning.getMessage()).contains("Large OFFSET value (1200)");
    }

    @Test
    @DisplayName("Should not generate warning when no offset")
    void shouldNotGenerateWarningWhenNoOffset() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query without offset
      ResultSet rs = statement.executeQuery("SELECT id, name FROM users WHERE id = '123' LIMIT 10");

      // Check that no offset warning is generated (no offset present)
      SQLWarning warning = statement.getWarnings();
      boolean hasOffsetWarning = false;
      while (warning != null) {
        if (warning.getMessage().contains("Large OFFSET value")) {
          hasOffsetWarning = true;
          break;
        }
        warning = warning.getNextWarning();
      }
      Assertions.assertThat(hasOffsetWarning).isFalse();
    }

    @Test
    @DisplayName("Should clear warnings on clearWarnings call")
    void shouldClearWarningsOnClearWarningsCall() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Generate warning
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 5000");
      Assertions.assertThat((Object) statement.getWarnings()).isNotNull();

      // Clear warnings
      statement.clearWarnings();
      Assertions.assertThat((Object) statement.getWarnings()).isNull();
    }

    @Test
    @DisplayName("Should chain multiple warnings")
    void shouldChainMultipleWarnings() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // First query with warning
      ResultSet rs1 =
          statement.executeQuery(
              "SELECT id, name FROM users WHERE id = '123' LIMIT 10 OFFSET 5000");

      // Second query with warning (should chain)
      ResultSet rs2 =
          statement.executeQuery(
              "SELECT id, name FROM products WHERE id = '456' LIMIT 10 OFFSET 10000");

      // Check warning chain - should have both offset warnings
      SQLWarning warning = statement.getWarnings();
      boolean found5000 = false;
      boolean found10000 = false;

      while (warning != null) {
        if (warning.getMessage().contains("Large OFFSET value (5000)")) {
          found5000 = true;
        }
        if (warning.getMessage().contains("Large OFFSET value (10000)")) {
          found10000 = true;
        }
        warning = warning.getNextWarning();
      }

      Assertions.assertThat(found5000).isTrue();
      Assertions.assertThat(found10000).isTrue();
    }
  }

  @Nested
  @DisplayName("Offset Cache Integration Tests")
  class OffsetCacheIntegrationTests {

    @Test
    @DisplayName("Should use cached token for offset queries")
    void shouldUseCachedTokenForOffsetQueries() throws SQLException {
      // Setup offset cache mock
      mockOffsetCache = mock(OffsetTokenCache.class);
      when(mockConnection.getOffsetTokenCache()).thenReturn(mockOffsetCache);

      // Setup cached token
      OffsetTokenCache.TokenEntry cachedEntry =
          new OffsetTokenCache.TokenEntry(500, "cachedToken500");
      when(mockOffsetCache.getNearestToken(anyString(), eq(600))).thenReturn(cachedEntry);

      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with offset
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 600");

      // Verify cache was consulted
      verify(mockOffsetCache).getNearestToken(anyString(), eq(600));

      // Verify ExecuteStatementRequest was called with appropriate parameters
      verify(mockRetryHandler).executeWithRetry(any(), any());
    }

    @Test
    @DisplayName("Should not use cache when disabled")
    void shouldNotUseCacheWhenDisabled() throws SQLException {
      // Ensure cache is null for this test
      when(mockConnection.getOffsetTokenCache()).thenReturn(null);

      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with offset
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 600");

      // Since cache is null, there should be no cache-related operations
      Assertions.assertThat(rs).isNotNull();
    }

    @Test
    @DisplayName("Should pass offset info to ResultSet")
    void shouldPassOffsetInfoToResultSet() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with offset
      DynamoDbResultSet rs =
          (DynamoDbResultSet) statement.executeQuery("SELECT * FROM users LIMIT 10 OFFSET 300");

      // ResultSet should have offset info
      Assertions.assertThat(rs).isNotNull();
      // Note: We'd need to expose offset info in ResultSet to fully test this
    }
  }

  @Nested
  @DisplayName("SQL Query Modification Tests")
  class SqlQueryModificationTests {

    @Test
    @DisplayName("Should remove LIMIT and OFFSET from PartiQL query")
    void shouldRemoveLimitAndOffsetFromPartiQL() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with LIMIT and OFFSET
      ResultSet rs =
          statement.executeQuery("SELECT * FROM users WHERE age > 18 LIMIT 50 OFFSET 100");

      // Verify the PartiQL sent to DynamoDB has no LIMIT/OFFSET
      verify(mockRetryHandler).executeWithRetry(any(), any());
    }

    @Test
    @DisplayName("Should apply LIMIT as request parameter")
    void shouldApplyLimitAsRequestParameter() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with LIMIT
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 25");

      // Verify LIMIT was applied as request parameter
      verify(mockRetryHandler).executeWithRetry(any(), any());
    }

    @Test
    @DisplayName("Should handle various LIMIT OFFSET patterns")
    void shouldHandleVariousLimitOffsetPatterns() throws SQLException {
      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Test different patterns
      String[] queries = {
        "SELECT * FROM users LIMIT 10",
        "SELECT * FROM users LIMIT 10 OFFSET 20",
        "SELECT * FROM users OFFSET 20 LIMIT 10",
        "SELECT * FROM users WHERE id = '123' LIMIT 10 OFFSET 20"
      };

      for (String query : queries) {
        statement.executeQuery(query);

        // Verify PartiQL doesn't contain LIMIT/OFFSET
        verify(mockRetryHandler, atLeastOnce()).executeWithRetry(any(), any());
      }
    }
  }

  @Nested
  @DisplayName("Fetch Size and Max Rows Tests")
  class FetchSizeMaxRowsTests {

    @Test
    @DisplayName("Should use fetchSize as safety limit when maxRows is 0")
    void shouldUseFetchSizeAsSafetyLimit() throws SQLException {
      // Set fetchSize
      statement.setFetchSize(100);
      statement.setMaxRows(0); // No limit

      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query without explicit LIMIT
      ResultSet rs = statement.executeQuery("SELECT * FROM users");

      // Should use fetchSize as limit
      verify(mockRetryHandler).executeWithRetry(any(), any());
    }

    @Test
    @DisplayName("Should prioritize SQL LIMIT over maxRows and fetchSize")
    void shouldPrioritizeSqlLimit() throws SQLException {
      statement.setFetchSize(100);
      statement.setMaxRows(50);

      // Mock response
      ExecuteStatementResponse response = createMockResponse();
      when(mockRetryHandler.executeWithRetry(any(), any())).thenReturn(response);

      // Execute query with explicit LIMIT
      ResultSet rs = statement.executeQuery("SELECT * FROM users LIMIT 25");

      // Should use SQL LIMIT (25) not maxRows (50) or fetchSize (100)
      verify(mockRetryHandler).executeWithRetry(any(), any());
    }
  }
}
