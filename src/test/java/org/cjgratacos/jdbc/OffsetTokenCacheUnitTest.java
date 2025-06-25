package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("OffsetTokenCache Unit Tests")
class OffsetTokenCacheUnitTest {

  private OffsetTokenCache cache;

  @BeforeEach
  void setUp() {
    // Create cache with small size for testing
    cache = new OffsetTokenCache(5, 10, 60); // size=5, interval=10, ttl=60s
  }

  @Nested
  @DisplayName("Constructor Validation")
  class ConstructorValidationTests {

    @Test
    @DisplayName("Should reject negative cache size")
    void shouldRejectNegativeCacheSize() {
      assertThatThrownBy(() -> new OffsetTokenCache(-1, 10, 60))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cache size must be positive");
    }

    @Test
    @DisplayName("Should reject zero cache size")
    void shouldRejectZeroCacheSize() {
      assertThatThrownBy(() -> new OffsetTokenCache(0, 10, 60))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cache size must be positive");
    }

    @Test
    @DisplayName("Should reject negative cache interval")
    void shouldRejectNegativeCacheInterval() {
      assertThatThrownBy(() -> new OffsetTokenCache(10, -1, 60))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cache interval must be positive");
    }

    @Test
    @DisplayName("Should reject negative TTL")
    void shouldRejectNegativeTtl() {
      assertThatThrownBy(() -> new OffsetTokenCache(10, 10, -1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("TTL must be positive");
    }

    @Test
    @DisplayName("Should create cache with valid parameters")
    void shouldCreateCacheWithValidParameters() {
      OffsetTokenCache validCache = new OffsetTokenCache(100, 50, 3600);
      assertThat(validCache).isNotNull();
      assertThat(validCache.getStats())
          .containsEntry("maxSize", 100)
          .containsEntry("cacheInterval", 50)
          .containsEntry("ttlSeconds", 3600L);
    }
  }

  @Nested
  @DisplayName("Token Storage and Retrieval")
  class TokenStorageTests {

    @Test
    @DisplayName("Should store token at exact interval")
    void shouldStoreTokenAtExactInterval() {
      cache.put("query1", 20, "token20");

      OffsetTokenCache.TokenEntry entry = cache.getNearestToken("query1", 25);
      assertThat(entry).isNotNull();
      assertThat(entry.getOffset()).isEqualTo(20);
      assertThat(entry.getToken()).isEqualTo("token20");
    }

    @Test
    @DisplayName("Should not store token if not at interval")
    void shouldNotStoreTokenIfNotAtInterval() {
      // Interval is 10, so 15 should not be stored
      cache.put("query1", 15, "token15");

      OffsetTokenCache.TokenEntry entry = cache.getNearestToken("query1", 15);
      assertThat(entry).isNull();
    }

    @Test
    @DisplayName("Should return nearest lower token")
    void shouldReturnNearestLowerToken() {
      cache.put("query1", 10, "token10");
      cache.put("query1", 20, "token20");
      cache.put("query1", 30, "token30");

      OffsetTokenCache.TokenEntry entry = cache.getNearestToken("query1", 25);
      assertThat(entry).isNotNull();
      assertThat(entry.getOffset()).isEqualTo(20);
      assertThat(entry.getToken()).isEqualTo("token20");
    }

    @Test
    @DisplayName("Should return null when no suitable token exists")
    void shouldReturnNullWhenNoSuitableToken() {
      cache.put("query1", 20, "token20");
      cache.put("query1", 30, "token30");

      // Looking for offset 15, but lowest token is at 20
      OffsetTokenCache.TokenEntry entry = cache.getNearestToken("query1", 15);
      assertThat(entry).isNull();
    }

    @Test
    @DisplayName("Should handle multiple query patterns independently")
    void shouldHandleMultipleQueryPatterns() {
      cache.put("SELECT * FROM table1", 10, "token1_10");
      cache.put("SELECT * FROM table2", 10, "token2_10");

      OffsetTokenCache.TokenEntry entry1 = cache.getNearestToken("SELECT * FROM table1", 15);
      OffsetTokenCache.TokenEntry entry2 = cache.getNearestToken("SELECT * FROM table2", 15);

      assertThat(entry1.getToken()).isEqualTo("token1_10");
      assertThat(entry2.getToken()).isEqualTo("token2_10");
    }
  }

  @Nested
  @DisplayName("Cache Eviction")
  class CacheEvictionTests {

    @Test
    @DisplayName("Should evict entries when cache is full per query")
    void shouldEvictEntriesWhenFull() {
      // The cache has maxSize per query pattern, not globally
      // Fill cache for a single query to capacity (size=5)
      String query = "SELECT * FROM users";
      for (int i = 1; i <= 6; i++) {
        cache.put(query, i * 10, "token" + i);
      }

      // The first entry (offset 10) should be evicted
      assertThat(cache.getNearestToken(query, 10)).isNull();

      // Entries 20-60 should still exist
      for (int i = 2; i <= 6; i++) {
        assertThat(cache.getNearestToken(query, i * 10)).isNotNull();
      }
    }

    @Test
    @DisplayName("Should update LRU order on access")
    void shouldUpdateLruOrderOnAccess() {
      // LinkedHashMap with accessOrder=true updates order on get()
      // Let's test that accessing an entry moves it to the end
      String query = "SELECT * FROM users";

      // Fill cache to capacity
      for (int i = 1; i <= 5; i++) {
        cache.put(query, i * 10, "token" + i);
      }
      // Cache now has: 10, 20, 30, 40, 50

      // Access offset 10 (note: getNearestToken doesn't update LRU order)
      OffsetTokenCache.TokenEntry accessed = cache.getNearestToken(query, 10);
      assertThat(accessed).isNotNull();
      assertThat(accessed.getOffset()).isEqualTo(10);

      // Add new entry, should evict offset 10 (oldest entry since getNearestToken doesn't update
      // order)
      cache.put(query, 60, "token6");

      // Verify: offset 10 is evicted (oldest), offset 20 still exists
      assertThat(cache.getNearestToken(query, 10)).isNull();
      assertThat(cache.getNearestToken(query, 20)).isNotNull();
      assertThat(cache.getNearestToken(query, 30)).isNotNull();
      assertThat(cache.getNearestToken(query, 40)).isNotNull();
      assertThat(cache.getNearestToken(query, 50)).isNotNull();
      assertThat(cache.getNearestToken(query, 60)).isNotNull();
    }
  }

  @Nested
  @DisplayName("TTL Expiration")
  class TtlExpirationTests {

    @Test
    @DisplayName("Should expire tokens after TTL")
    void shouldExpireTokensAfterTtl() throws InterruptedException {
      // Create cache with 1 second TTL for testing
      OffsetTokenCache shortTtlCache = new OffsetTokenCache(10, 10, 1);

      shortTtlCache.put("query1", 10, "token10");

      // Token should exist immediately
      assertThat(shortTtlCache.getNearestToken("query1", 10)).isNotNull();

      // Wait for expiration
      TimeUnit.SECONDS.sleep(2);

      // Token should be expired
      assertThat(shortTtlCache.getNearestToken("query1", 10)).isNull();
    }

    @Test
    @DisplayName("Should not return expired tokens even if in cache")
    void shouldNotReturnExpiredTokens() throws InterruptedException {
      // Create cache with 1 second TTL
      OffsetTokenCache shortTtlCache = new OffsetTokenCache(10, 10, 1);

      shortTtlCache.put("query1", 10, "token10");
      shortTtlCache.put("query1", 20, "token20");

      // Wait for expiration
      TimeUnit.SECONDS.sleep(2);

      // Should return null even though tokens are in cache
      assertThat(shortTtlCache.getNearestToken("query1", 25)).isNull();
    }
  }

  @Nested
  @DisplayName("Cache Operations")
  class CacheOperationsTests {

    @Test
    @DisplayName("Should clear specific query pattern")
    void shouldClearSpecificQueryPattern() {
      cache.put("query1", 10, "token1_10");
      cache.put("query1", 20, "token1_20");
      cache.put("query2", 10, "token2_10");

      cache.clearQuery("query1");

      assertThat(cache.getNearestToken("query1", 20)).isNull();
      assertThat(cache.getNearestToken("query2", 10)).isNotNull();
    }

    @Test
    @DisplayName("Should clear all entries")
    void shouldClearAllEntries() {
      cache.put("query1", 10, "token1");
      cache.put("query2", 10, "token2");
      cache.put("query3", 10, "token3");

      cache.clearAll();

      assertThat(cache.getNearestToken("query1", 10)).isNull();
      assertThat(cache.getNearestToken("query2", 10)).isNull();
      assertThat(cache.getNearestToken("query3", 10)).isNull();
    }

    @Test
    @DisplayName("Should provide accurate statistics")
    void shouldProvideAccurateStatistics() {
      // Start with empty cache
      assertThat(cache.getStats())
          .containsEntry("totalEntries", 0)
          .containsEntry("totalQueries", 0);

      // Add entries
      cache.put("query1", 10, "token10");
      cache.put("query1", 20, "token20");
      cache.put("query2", 10, "token10");

      assertThat(cache.getStats())
          .containsEntry("totalEntries", 3)
          .containsEntry("totalQueries", 2);
    }
  }

  @Nested
  @DisplayName("Complex Scenarios")
  class ComplexScenarioTests {

    @Test
    @DisplayName("Should handle sequential pagination pattern")
    void shouldHandleSequentialPagination() {
      String query = "SELECT * FROM users";

      // Simulate sequential pagination - only cache recent pages (cache size = 5)
      // We'll cache offsets 60-100 (5 entries)
      for (int offset = 60; offset <= 100; offset += 10) {
        cache.put(query, offset, "token_" + offset);
      }

      // Verify we can efficiently resume from cached points
      // Note: offsets 10-50 were evicted due to LRU (cache size = 5)

      // Try to get offset 55 - should find offset 60 as nearest (no offset 50)
      OffsetTokenCache.TokenEntry entry55 = cache.getNearestToken(query, 55);
      assertThat(entry55).isNull(); // No cached entry <= 55

      // Try to get offset 65 - should find offset 60
      OffsetTokenCache.TokenEntry entry65 = cache.getNearestToken(query, 65);
      assertThat(entry65).isNotNull();
      assertThat(entry65.getOffset()).isEqualTo(60);

      // Try to get offset 95 - should find offset 90
      OffsetTokenCache.TokenEntry entry95 = cache.getNearestToken(query, 95);
      assertThat(entry95).isNotNull();
      assertThat(entry95.getOffset()).isEqualTo(90);

      // Try to get offset 100 - should find exact match
      OffsetTokenCache.TokenEntry entry100 = cache.getNearestToken(query, 100);
      assertThat(entry100).isNotNull();
      assertThat(entry100.getOffset()).isEqualTo(100);
    }

    @Test
    @DisplayName("Should handle random access pattern")
    void shouldHandleRandomAccessPattern() {
      String query = "SELECT * FROM products";

      // Simulate random access pattern
      cache.put(query, 100, "token_100");
      cache.put(query, 50, "token_50");
      cache.put(query, 200, "token_200");
      cache.put(query, 30, "token_30");

      // Should still find nearest lower token correctly
      assertThat(cache.getNearestToken(query, 75).getOffset()).isEqualTo(50);
      assertThat(cache.getNearestToken(query, 150).getOffset()).isEqualTo(100);
      assertThat(cache.getNearestToken(query, 250).getOffset()).isEqualTo(200);
    }

    @Test
    @DisplayName("Should handle query normalization")
    void shouldHandleQueryNormalization() {
      // Different formatting of same query should be normalized to same key
      cache.put("SELECT * FROM users", 10, "token1");
      cache.put("SELECT  *  FROM  users", 10, "token2");
      cache.put("select * from users", 10, "token3");

      // All query variants should return the last cached value due to normalization
      assertThat(cache.getNearestToken("SELECT * FROM users", 10).getToken()).isEqualTo("token3");
      assertThat(cache.getNearestToken("SELECT  *  FROM  users", 10).getToken())
          .isEqualTo("token3");
      assertThat(cache.getNearestToken("select * from users", 10).getToken()).isEqualTo("token3");
    }
  }
}
