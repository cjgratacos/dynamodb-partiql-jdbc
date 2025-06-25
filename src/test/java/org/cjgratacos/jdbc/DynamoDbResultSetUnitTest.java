package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

@DisplayName("DynamoDbResultSet Unit Tests")
class DynamoDbResultSetUnitTest extends BaseUnitTest {

  private DynamoDbResultSet resultSet;
  private DynamoDbStatement mockStatement;
  private DynamoDbConnection mockConnection;
  private DynamoDbClient mockClient;
  private OffsetTokenCache mockOffsetCache;
  private ExecuteStatementResponse initialResponse;

  @BeforeEach
  void setUp() {
    mockStatement = mock(DynamoDbStatement.class);
    mockConnection = mock(DynamoDbConnection.class);
    mockClient = mock(DynamoDbClient.class);
    mockOffsetCache = mock(OffsetTokenCache.class);
  }

  private List<Map<String, AttributeValue>> createItems(int count, int startId) {
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s(String.valueOf(startId + i)).build());
      item.put("name", AttributeValue.builder().s("User" + (startId + i)).build());
      items.add(item);
    }
    return items;
  }

  @Nested
  @DisplayName("Token Caching During Pagination")
  class TokenCachingTests {

    @Test
    @DisplayName("Should cache tokens at configured intervals")
    void shouldCacheTokensAtIntervals() throws SQLException {
      // Create initial response with nextToken
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 1))
              .nextToken("token_page1")
              .build();

      // Create result set with caching interval of 10
      // Mock shouldCache to return true for any value that's a multiple of 10
      when(mockOffsetCache.shouldCache(anyInt()))
          .thenAnswer(
              invocation -> {
                int offset = invocation.getArgument(0);
                return offset % 10 == 0;
              });

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null), // no limit/offset
              null, // no table key info
              100, // maxRows
              mockOffsetCache // offset cache
              );

      // Iterate through first 10 items
      int count = 0;
      while (count < 10 && resultSet.next()) {
        count++;
      }

      // Should cache token at offset 10
      verify(mockOffsetCache).put(eq("SELECT * FROM users"), eq(10), eq("token_page1"));
    }

    @Test
    @DisplayName("Should cache tokens during multi-page iteration")
    void shouldCacheTokensMultiPage() throws SQLException {
      // First page
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 1))
              .nextToken("token_page1")
              .build();

      // Second page response
      ExecuteStatementResponse secondPage =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 11))
              .nextToken("token_page2")
              .build();

      when(mockClient.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(secondPage);
      when(mockOffsetCache.shouldCache(anyInt())).thenReturn(true);

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // Iterate through 20 items (2 pages)
      int count = 0;
      while (count < 20 && resultSet.next()) {
        count++;
      }

      // Should cache tokens at intervals
      verify(mockOffsetCache, atLeastOnce()).shouldCache(anyInt());
      verify(mockOffsetCache, atLeastOnce()).put(anyString(), anyInt(), anyString());
    }

    @Test
    @DisplayName("Should not cache when cache is disabled")
    void shouldNotCacheWhenDisabled() throws SQLException {
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 1))
              .nextToken("token_page1")
              .build();

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              100, // maxRows
              null // cache disabled
              );

      // Iterate through items
      int count = 0;
      while (count < 10 && resultSet.next()) {
        count++;
      }

      // No cache interaction expected when cache is null
      assertThat(count).isEqualTo(10);
    }
  }

  @Nested
  @DisplayName("Offset Skipping with Cached Tokens")
  class OffsetSkippingTests {

    @Test
    @DisplayName("Should skip to offset when starting from cached token")
    void shouldSkipToOffsetFromCachedToken() throws SQLException {
      // Initial response with 20 items starting from item 1
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(20, 1)) // Starting from item 1
              .nextToken(null) // No more pages
              .build();

      // Create result set with offset 5
      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, 5), // offset 5
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // First next() should skip to position 6 (after offset 5)
      assertThat(resultSet.next()).isTrue();

      // Should be at item 6 (offset 5 means skip first 5 items)
      assertThat(resultSet.getString("id")).isEqualTo("6");
    }

    @Test
    @DisplayName("Should handle offset spanning multiple pages")
    void shouldHandleOffsetAcrossPages() throws SQLException {
      // First page with 10 items
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 1))
              .nextToken("token_page1")
              .build();

      // Second page
      ExecuteStatementResponse secondPage =
          ExecuteStatementResponse.builder()
              .items(createItems(10, 11))
              .nextToken("token_page2")
              .build();

      when(mockClient.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(secondPage);

      // Want offset 15, need to skip 15 items
      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, 15), // offset 15
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // First next() should skip to position 15
      assertThat(resultSet.next()).isTrue();

      // Should be at item 16
      assertThat(resultSet.getString("id")).isEqualTo("16");

      // Should have fetched second page
      verify(mockClient).executeStatement(any(ExecuteStatementRequest.class));
    }

    @Test
    @DisplayName("Should return false when offset exceeds available items")
    void shouldReturnFalseWhenOffsetExceedsItems() throws SQLException {
      // Only 5 items available
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(5, 1))
              .nextToken(null) // No more pages
              .build();

      // Want offset 10, but only 5 items exist
      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, 10), // offset exceeds available items
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // Should return false as offset is beyond available data
      assertThat(resultSet.next()).isFalse();
    }
  }

  @Nested
  @DisplayName("Token Cache Integration")
  class TokenCacheIntegrationTests {

    @Test
    @DisplayName("Should use cache interval from connection properties")
    void shouldUseCacheIntervalFromProperties() throws SQLException {
      // Mock cache with interval checking - allow any position
      when(mockOffsetCache.shouldCache(anyInt()))
          .thenAnswer(
              invocation -> {
                int offset = invocation.getArgument(0);
                return offset == 50 || offset == 100;
              });

      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(50, 1))
              .nextToken("token_at_50")
              .build();

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              50, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              200, // maxRows
              mockOffsetCache);

      // Iterate through 50 items
      int count = 0;
      while (count < 50 && resultSet.next()) {
        count++;
      }

      // Should check if caching needed at position 50
      verify(mockOffsetCache).shouldCache(50);
    }

    @Test
    @DisplayName("Should handle query normalization for cache keys")
    void shouldNormalizeQueryForCache() throws SQLException {
      initialResponse =
          ExecuteStatementResponse.builder().items(createItems(10, 1)).nextToken("token1").build();

      when(mockOffsetCache.shouldCache(anyInt()))
          .thenAnswer(
              invocation -> {
                int offset = invocation.getArgument(0);
                return offset == 10;
              });

      // Query with extra spaces
      String query = "SELECT   *   FROM   users   WHERE   status = 'active'";

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              query,
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // Iterate to trigger caching
      int count = 0;
      while (count < 10 && resultSet.next()) {
        count++;
      }

      // Cache should be called with original query (normalization happens in cache)
      verify(mockOffsetCache).put(eq(query), eq(10), eq("token1"));
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Should handle empty result set")
    void shouldHandleEmptyResultSet() throws SQLException {
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(new ArrayList<>()) // Empty results
              .nextToken(null)
              .build();

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // Should return false immediately
      assertThat(resultSet.next()).isFalse();

      // No caching should occur
      verify(mockOffsetCache, never()).put(anyString(), anyInt(), anyString());
    }

    @Test
    @DisplayName("Should handle single page result")
    void shouldHandleSinglePageResult() throws SQLException {
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(5, 1))
              .nextToken(null) // No more pages
              .build();

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              100, // maxRows
              mockOffsetCache);

      // Iterate through all items
      int count = 0;
      while (resultSet.next()) {
        count++;
      }

      assertThat(count).isEqualTo(5);
    }

    @Test
    @DisplayName("Should respect maxRows limit")
    void shouldRespectMaxRowsLimit() throws SQLException {
      initialResponse =
          ExecuteStatementResponse.builder()
              .items(createItems(20, 1))
              .nextToken("more_data")
              .build();

      resultSet =
          new DynamoDbResultSet(
              mockClient,
              "SELECT * FROM users",
              initialResponse,
              10, // fetchSize
              new LimitOffsetInfo(null, null),
              null, // no table key info
              5, // maxRows limit
              mockOffsetCache);

      // Should only return 5 rows
      int count = 0;
      while (resultSet.next()) {
        count++;
      }

      assertThat(count).isEqualTo(5);
    }
  }
}
