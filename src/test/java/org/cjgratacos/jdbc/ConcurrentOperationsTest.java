package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Concurrent Operations Tests")
class ConcurrentOperationsTest {

  @Nested
  @DisplayName("Thread Safety Tests")
  class ThreadSafetyTests {

    @Test
    @DisplayName("ColumnMetadata handles concurrent-like usage patterns")
    void columnMetadataHandlesConcurrentLikeUsagePatterns() throws Exception {
      // Given: Multiple separate ColumnMetadata instances (realistic usage)
      final var metadataList = new java.util.concurrent.CopyOnWriteArrayList<ColumnMetadata>();
      final var threadCount = 10;
      final var operationsPerThread = 100;

      // When: Multiple threads create and use their own metadata instances
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    threadId ->
                        CompletableFuture.runAsync(
                            () -> {
                              // Each thread creates its own metadata (realistic pattern)
                              final var metadata =
                                  new ColumnMetadata("table_" + threadId, "column_" + threadId);

                              for (int i = 0; i < operationsPerThread; i++) {
                                final var isNull = i % 10 == 0;
                                final var sqlType = i % 2 == 0 ? Types.VARCHAR : Types.INTEGER;
                                metadata.recordTypeObservation(sqlType, isNull);
                              }

                              metadataList.add(metadata);
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: All metadata instances should be complete and correct
      assertThat(metadataList).hasSize(threadCount);
      metadataList.forEach(
          metadata -> {
            assertThat(metadata.getTotalObservations()).isEqualTo(operationsPerThread);
            assertThat(metadata.getNullObservations()).isEqualTo(operationsPerThread / 10);
            assertThat(metadata.isNullable()).isTrue();
            assertThat(metadata.hasTypeConflict()).isTrue();
            assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
          });
    }

    @Test
    @DisplayName("CachedSchemaEntry concurrent access is thread-safe")
    void cachedSchemaEntryConcurrentAccessIsThreadSafe() throws Exception {
      // Given: Shared cached entry
      final var schemaData = new java.util.HashMap<String, ColumnMetadata>();
      schemaData.put("col1", new ColumnMetadata("test", "col1"));
      final var entry = new CachedSchemaEntry<>("test_table", schemaData, 60000L);

      final var threadCount = 20;
      final var readsPerThread = 1000;
      final var successCount = new AtomicInteger(0);

      // When: Multiple threads read concurrently
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    i ->
                        CompletableFuture.runAsync(
                            () -> {
                              for (int j = 0; j < readsPerThread; j++) {
                                // Concurrent reads should be safe
                                final var data = entry.getSchemaData();
                                final var valid = entry.isValid();
                                final var accessTime = entry.getLastAccessTime();

                                if (data != null && valid && accessTime > 0) {
                                  successCount.incrementAndGet();
                                }
                              }
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: All reads should succeed
      final var expectedReads = threadCount * readsPerThread;
      assertThat(successCount.get()).isEqualTo(expectedReads);
      assertThat(entry.isValid()).isTrue();
    }

    @Test
    @DisplayName("JdbcParser concurrent parsing is thread-safe")
    void jdbcParserConcurrentParsingIsThreadSafe() throws Exception {
      // Given: Various JDBC URLs
      final var urls =
          new String[] {
            "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT",
            "jdbc:dynamodb:partiql:region=us-west-2;endpoint=http://localhost:8000",
            "jdbc:dynamodb:partiql:region=eu-west-1;accessKey=test;secretKey=secret",
            "jdbc:dynamodb:partiql:region=ap-southeast-1;schemaDiscovery=auto;sampleSize=1000"
          };

      final var threadCount = 8;
      final var parsesPerThread = 250;
      final var successCount = new AtomicInteger(0);

      // When: Multiple threads parse URLs concurrently
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    threadId ->
                        CompletableFuture.runAsync(
                            () -> {
                              for (int i = 0; i < parsesPerThread; i++) {
                                final var url = urls[i % urls.length];
                                final var properties = JdbcParser.extractProperties(url);

                                // Verify parsing worked correctly
                                if (properties.getProperty("region") != null) {
                                  successCount.incrementAndGet();
                                }
                              }
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: All parses should succeed
      final var expectedParses = threadCount * parsesPerThread;
      assertThat(successCount.get()).isEqualTo(expectedParses);
    }

    @Test
    @DisplayName("SchemaDiscoveryMode concurrent parsing is thread-safe")
    void schemaDiscoveryModeConcurrentParsingIsThreadSafe() throws Exception {
      // Given: Various mode strings
      final var modeStrings =
          new String[] {
            "auto", "hints", "sampling", "disabled",
            "AUTO", "HINTS", "SAMPLING", "DISABLED",
            "invalid", null, "", "   "
          };

      final var threadCount = 6;
      final var parsesPerThread = 500;
      final var successCount = new AtomicInteger(0);

      // When: Multiple threads parse modes concurrently
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    threadId ->
                        CompletableFuture.runAsync(
                            () -> {
                              for (int i = 0; i < parsesPerThread; i++) {
                                final var modeString = modeStrings[i % modeStrings.length];
                                final var mode = SchemaDiscoveryMode.fromString(modeString);

                                // Should never return null
                                if (mode != null) {
                                  successCount.incrementAndGet();
                                }
                              }
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: All parses should succeed (never return null)
      final var expectedParses = threadCount * parsesPerThread;
      assertThat(successCount.get()).isEqualTo(expectedParses);
    }
  }

  @Nested
  @DisplayName("Resource Contention Tests")
  class ResourceContentionTests {

    @Test
    @DisplayName("High concurrent load does not cause deadlocks")
    void highConcurrentLoadDoesNotCauseDeadlocks() throws Exception {
      // Given: Multiple shared resources
      final var metadata1 = new ColumnMetadata("table1", "column1");
      final var metadata2 = new ColumnMetadata("table2", "column2");
      final var schemaMap = new java.util.HashMap<String, ColumnMetadata>();
      schemaMap.put("col1", metadata1);
      schemaMap.put("col2", metadata2);

      final var entry = new CachedSchemaEntry<>("shared_table", schemaMap, 30000L);
      final var threadCount = 15;
      final var operationsPerThread = 200;

      // When: Many threads perform mixed operations
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    threadId ->
                        CompletableFuture.runAsync(
                            () -> {
                              for (int i = 0; i < operationsPerThread; i++) {
                                // Mix different operations to create contention
                                switch (i % 4) {
                                  case 0 -> metadata1.recordTypeObservation(Types.VARCHAR, false);
                                  case 1 ->
                                      metadata2.recordTypeObservation(Types.INTEGER, i % 20 == 0);
                                  case 2 -> entry.getSchemaData();
                                  case 3 -> {
                                    final var url =
                                        "jdbc:dynamodb:partiql:region=us-east-1;prop=" + i;
                                    JdbcParser.extractProperties(url);
                                  }
                                }
                              }
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        // Should complete without deadlock
        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: All operations should complete successfully
      assertThat(metadata1.getTotalObservations()).isGreaterThan(0);
      assertThat(metadata2.getTotalObservations()).isGreaterThan(0);
      assertThat(entry.isValid()).isTrue();
    }

    @Test
    @DisplayName("Concurrent schema entry creation and access is safe")
    void concurrentSchemaEntryCreationAndAccessIsSafe() throws Exception {
      // Given: Shared container for entries
      final var entryContainer =
          new java.util.concurrent.ConcurrentHashMap<String, CachedSchemaEntry<?>>();
      final var threadCount = 12;
      final var operationsPerThread = 100;

      // When: Threads create and access entries concurrently
      final var executor = Executors.newFixedThreadPool(threadCount);
      try {
        final var futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    threadId ->
                        CompletableFuture.runAsync(
                            () -> {
                              for (int i = 0; i < operationsPerThread; i++) {
                                final var tableName = "table_" + (i % 10);

                                // Create or access entry
                                entryContainer.computeIfAbsent(
                                    tableName,
                                    name -> {
                                      final var metadata = new ColumnMetadata(name, "id");
                                      metadata.recordTypeObservation(Types.VARCHAR, false);
                                      final var schemaData = java.util.Map.of("id", metadata);
                                      return new CachedSchemaEntry<>(name, schemaData, 60000L);
                                    });

                                // Access existing entry
                                final var entry = entryContainer.get(tableName);
                                if (entry != null) {
                                  entry.getSchemaData();
                                  entry.isValid();
                                }
                              }
                            },
                            executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } finally {
        executor.shutdown();
      }

      // Then: Should have created exactly 10 unique entries
      assertThat(entryContainer).hasSize(10);
      entryContainer
          .values()
          .forEach(
              entry -> {
                assertThat(entry.isValid()).isTrue();
                assertThat(entry.getSchemaData()).isNotEmpty();
              });
    }
  }
}
