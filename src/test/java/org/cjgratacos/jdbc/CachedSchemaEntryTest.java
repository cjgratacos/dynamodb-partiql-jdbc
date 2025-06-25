package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CachedSchemaEntry Tests")
class CachedSchemaEntryTest {

  @Nested
  @DisplayName("Basic Functionality Tests")
  class BasicFunctionalityTests {

    @Test
    @DisplayName("New entry is valid and contains data")
    void newEntryIsValidAndContainsData() {
      // Given: Schema data and TTL
      final var schemaData = createTestSchemaData();
      final var ttlMs = 5000L; // 5 seconds

      // When: Creating cached entry
      final var entry = new CachedSchemaEntry<>("test_table", schemaData, ttlMs);

      // Then: Entry should be valid and contain data
      assertThat(entry.isValid()).isTrue();
      assertThat(entry.getSchemaData()).isEqualTo(schemaData);
      assertThat(entry.getTableName()).isEqualTo("test_table");
      assertThat(entry.getTtlMs()).isEqualTo(ttlMs);
    }

    @Test
    @DisplayName("Entry expires after TTL")
    void entryExpiresAfterTtl() throws InterruptedException {
      // Given: Schema data with short TTL
      final var schemaData = createTestSchemaData();
      final var ttlMs = 100L; // 100ms

      // When: Creating entry and waiting for expiration
      final var entry = new CachedSchemaEntry<>("test_table", schemaData, ttlMs);

      // Then: Initially valid
      assertThat(entry.isValid()).isTrue();

      // Wait for expiration
      Thread.sleep(150);

      // Then: Should be expired
      assertThat(entry.isValid()).isFalse();
    }

    @Test
    @DisplayName("Last access time is updated on read")
    void lastAccessTimeIsUpdatedOnRead() throws InterruptedException {
      // Given: Cached entry
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 5000L);
      final var initialAccessTime = entry.getLastAccessTime();

      // When: Waiting and accessing data
      Thread.sleep(10);
      entry.getSchemaData();

      // Then: Access time should be updated
      assertThat(entry.getLastAccessTime()).isGreaterThan(initialAccessTime);
    }

    @Test
    @DisplayName("Creation time is set correctly")
    void creationTimeIsSetCorrectly() {
      // Given: Current time before creation
      final var beforeCreation = System.currentTimeMillis();

      // When: Creating entry
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 5000L);

      // Then: Creation time should be recent
      final var afterCreation = System.currentTimeMillis();
      assertThat(entry.getCreatedAt()).isBetween(beforeCreation, afterCreation);
    }

    @Test
    @DisplayName("Zero TTL makes entry always invalid")
    void zeroTtlMakesEntryAlwaysInvalid() {
      // Given: Entry with zero TTL
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 0L);

      // Then: Should always be invalid
      assertThat(entry.isValid()).isFalse();
    }

    @Test
    @DisplayName("Negative TTL makes entry always invalid")
    void negativeTtlMakesEntryAlwaysInvalid() {
      // Given: Entry with negative TTL
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), -1000L);

      // Then: Should always be invalid
      assertThat(entry.isValid()).isFalse();
    }
  }

  @Nested
  @DisplayName("Thread Safety Tests")
  class ThreadSafetyTests {

    @Test
    @DisplayName("Concurrent reads are thread-safe")
    void concurrentReadsAreThreadSafe() throws Exception {
      // Given: Cached entry and thread pool
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 5000L);
      final var threadCount = 20;
      final var executor = Executors.newFixedThreadPool(threadCount);
      final var readCounter = new AtomicInteger(0);
      final var errorCounter = new AtomicInteger(0);

      try {
        // When: Multiple threads read concurrently
        final var futures = new CompletableFuture[threadCount];
        for (int i = 0; i < threadCount; i++) {
          futures[i] =
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      for (int j = 0; j < 100; j++) {
                        final var data = entry.getSchemaData();
                        if (data != null) {
                          readCounter.incrementAndGet();
                        }
                      }
                    } catch (Exception e) {
                      errorCounter.incrementAndGet();
                    }
                  },
                  executor);
        }

        CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);

        // Then: All reads should succeed without errors
        assertThat(errorCounter.get()).isEqualTo(0);
        assertThat(readCounter.get()).isEqualTo(threadCount * 100);
      } finally {
        executor.shutdown();
      }
    }

    @Test
    @DisplayName("Read-write concurrency is safe")
    void readWriteConcurrencyIsSafe() throws Exception {
      // Given: Cached entry
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 10000L);
      final var latch = new CountDownLatch(1);
      final var readResults = new AtomicReference<Map<String, ColumnMetadata>>();
      final var writeComplete = new AtomicReference<Boolean>(false);

      // When: One thread reads while validity is being checked
      final var readFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  latch.await();
                  for (int i = 0; i < 1000; i++) {
                    readResults.set(entry.getSchemaData());
                    if (readResults.get() == null) {
                      break; // Entry became invalid
                    }
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });

      final var validityFuture =
          CompletableFuture.runAsync(
              () -> {
                latch.countDown();
                for (int i = 0; i < 1000; i++) {
                  entry.isValid();
                }
                writeComplete.set(true);
              });

      // Then: Both operations should complete successfully
      CompletableFuture.allOf(readFuture, validityFuture).get(5, TimeUnit.SECONDS);
      assertThat(writeComplete.get()).isTrue();
    }

    @Test
    @DisplayName("Concurrent access time updates are handled safely")
    void concurrentAccessTimeUpdatesAreHandledSafely() throws Exception {
      // Given: Cached entry and multiple readers
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 10000L);
      final var threadCount = 10;
      final var executor = Executors.newFixedThreadPool(threadCount);
      final var accessTimes = new long[threadCount];

      try {
        // When: Multiple threads access concurrently
        final var futures = new CompletableFuture[threadCount];
        for (int i = 0; i < threadCount; i++) {
          final var index = i;
          futures[i] =
              CompletableFuture.runAsync(
                  () -> {
                    entry.getSchemaData();
                    accessTimes[index] = entry.getLastAccessTime();
                  },
                  executor);
        }

        CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);

        // Then: All access times should be valid (> 0)
        for (long accessTime : accessTimes) {
          assertThat(accessTime).isGreaterThan(0);
        }
      } finally {
        executor.shutdown();
      }
    }
  }

  @Nested
  @DisplayName("Edge Cases Tests")
  class EdgeCasesTests {

    @Test
    @DisplayName("Null schema data is handled correctly")
    void nullSchemaDataIsHandledCorrectly() {
      // Given: Entry with null data
      final var entry = new CachedSchemaEntry<ColumnMetadata>("test_table", null, 5000L);

      // Then: Should handle null gracefully
      assertThat(entry.getSchemaData()).isNull();
      assertThat(entry.isValid()).isTrue(); // TTL-based validity, not data-based
    }

    @Test
    @DisplayName("Empty schema data is handled correctly")
    void emptySchemaDataIsHandledCorrectly() {
      // Given: Entry with empty data
      final var emptyData = Map.<String, ColumnMetadata>of();
      final var entry = new CachedSchemaEntry<>("test_table", emptyData, 5000L);

      // Then: Should handle empty data gracefully
      assertThat(entry.getSchemaData()).isEmpty();
      assertThat(entry.isValid()).isTrue();
    }

    @Test
    @DisplayName("Very large TTL works correctly")
    void veryLargeTtlWorksCorrectly() {
      // Given: Entry with very large TTL
      final var largeTtl = Long.MAX_VALUE;
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), largeTtl);

      // Then: Should be valid for a very long time
      assertThat(entry.isValid()).isTrue();
      assertThat(entry.getTtlMs()).isEqualTo(largeTtl);
    }

    @Test
    @DisplayName("Null table name is handled")
    void nullTableNameIsHandled() {
      // Given: Entry with null table name
      final var entry = new CachedSchemaEntry<ColumnMetadata>(null, createTestSchemaData(), 5000L);

      // Then: Should handle null table name
      assertThat(entry.getTableName()).isNull();
      assertThat(entry.isValid()).isTrue();
    }

    @Test
    @DisplayName("Empty table name is handled")
    void emptyTableNameIsHandled() {
      // Given: Entry with empty table name
      final var entry = new CachedSchemaEntry<>("", createTestSchemaData(), 5000L);

      // Then: Should handle empty table name
      assertThat(entry.getTableName()).isEmpty();
      assertThat(entry.isValid()).isTrue();
    }
  }

  @Nested
  @DisplayName("Performance Tests")
  class PerformanceTests {

    @Test
    @DisplayName("Read performance is acceptable")
    void readPerformanceIsAcceptable() {
      // Given: Cached entry with data
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 30000L);

      // When: Measuring read performance
      final var result =
          PerformanceTestUtils.benchmark(
              () -> entry.getSchemaData(),
              1000, // iterations
              100 // warmup
              );

      // Then: Average read time should be very fast (< 1ms)
      assertThat(result.getAverageTime()).isLessThan(1.0);
    }

    @Test
    @DisplayName("Validity check performance is acceptable")
    void validityCheckPerformanceIsAcceptable() {
      // Given: Cached entry
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 30000L);

      // When: Measuring validity check performance
      final var result =
          PerformanceTestUtils.benchmark(
              () -> entry.isValid(),
              1000, // iterations
              100 // warmup
              );

      // Then: Average validity check should be very fast (< 0.1ms)
      assertThat(result.getAverageTime()).isLessThan(0.1);
    }

    @Test
    @DisplayName("Memory usage is reasonable")
    void memoryUsageIsReasonable() {
      // When: Creating many cached entries
      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                final var entries = new CachedSchemaEntry[1000];
                for (int i = 0; i < 1000; i++) {
                  entries[i] =
                      new CachedSchemaEntry<>("table_" + i, createTestSchemaData(), 60000L);
                }
                // Keep reference to prevent GC
                assertThat(entries[0]).isNotNull();
              });

      // Then: Memory usage should be reasonable (< 5MB for 1000 entries)
      assertThat(memoryResult.getUsedMemoryIncrease()).isLessThan(5 * 1024 * 1024);
    }
  }

  @Nested
  @DisplayName("Time-based Tests")
  class TimeBasedTests {

    @Test
    @DisplayName("TTL expiration is precise")
    void ttlExpirationIsPrecise() throws InterruptedException {
      // Given: Entry with known TTL
      final var ttlMs = 200L; // 200ms
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), ttlMs);
      final var creationTime = entry.getCreatedAt();

      // When: Checking validity at different times
      assertThat(entry.isValid()).isTrue();

      // Wait for half TTL
      Thread.sleep(ttlMs / 2);
      assertThat(entry.isValid()).isTrue();

      // Wait for TTL to expire (with small buffer)
      Thread.sleep(ttlMs / 2 + 50);
      assertThat(entry.isValid()).isFalse();

      // Then: Expiration time should be approximately correct
      final var currentTime = System.currentTimeMillis();
      final var expectedExpirationTime = creationTime + ttlMs;
      assertThat(currentTime).isCloseTo(expectedExpirationTime, within(100L));
    }

    @Test
    @DisplayName("Access time tracking is accurate")
    void accessTimeTrackingIsAccurate() throws InterruptedException {
      // Given: Cached entry
      final var entry = new CachedSchemaEntry<>("test_table", createTestSchemaData(), 10000L);
      final var initialAccessTime = entry.getLastAccessTime();

      // When: Accessing after delay
      Thread.sleep(100);
      final var beforeAccess = System.currentTimeMillis();
      entry.getSchemaData();
      final var afterAccess = System.currentTimeMillis();

      // Then: Access time should be updated to recent time
      final var newAccessTime = entry.getLastAccessTime();
      assertThat(newAccessTime).isGreaterThan(initialAccessTime);
      assertThat(newAccessTime).isBetween(beforeAccess - 10, afterAccess + 10);
    }
  }

  // Helper methods

  private Map<String, ColumnMetadata> createTestSchemaData() {
    final var schema = new HashMap<String, ColumnMetadata>();

    final var idColumn = new ColumnMetadata("test_table", "id");
    idColumn.recordTypeObservation(java.sql.Types.VARCHAR, false);

    final var nameColumn = new ColumnMetadata("test_table", "name");
    nameColumn.recordTypeObservation(java.sql.Types.VARCHAR, false);

    final var ageColumn = new ColumnMetadata("test_table", "age");
    ageColumn.recordTypeObservation(java.sql.Types.INTEGER, false);

    schema.put("id", idColumn);
    schema.put("name", nameColumn);
    schema.put("age", ageColumn);

    return schema;
  }
}
