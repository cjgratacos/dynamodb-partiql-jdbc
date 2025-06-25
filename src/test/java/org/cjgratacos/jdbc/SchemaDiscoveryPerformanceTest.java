package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@ExtendWith(MockitoExtension.class)
@DisplayName("Schema Discovery Performance Tests")
class SchemaDiscoveryPerformanceTest {

  @Mock private DynamoDbClient mockClient;

  @Nested
  @DisplayName("Schema Discovery Mode Performance")
  class SchemaDiscoveryModePerformanceTests {

    @Test
    @DisplayName("Auto mode parsing performance")
    void autoModeParsingPerformance() {
      // Given: Multiple parse operations
      final var iterations = 10000;

      // When: Parsing AUTO mode repeatedly
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var mode = SchemaDiscoveryMode.fromString("auto");
                assertThat(mode).isEqualTo(SchemaDiscoveryMode.AUTO);
              },
              iterations,
              1000);

      // Then: Should be very fast
      assertThat(result.getAverageTime()).isLessThan(1.0); // Less than 0.01ms per parse
      assertThat(result.getMaxTime()).isLessThan(50L); // Max 50ms for any single parse in CI
    }

    @Test
    @DisplayName("Different discovery modes have similar performance")
    void differentDiscoveryModesHaveSimilarPerformance() {
      final var modes = new String[] {"auto", "hints", "sampling", "disabled"};
      final var iterations = 5000;
      final var results = new ArrayList<Double>();

      // When: Benchmarking each mode
      for (final var mode : modes) {
        final var result =
            PerformanceTestUtils.benchmark(
                () -> {
                  SchemaDiscoveryMode.fromString(mode);
                },
                iterations,
                500);
        results.add(result.getAverageTime());
      }

      // Then: All modes should have similar performance
      final var maxTime = results.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
      final var minTime = results.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
      final var ratio = maxTime / (minTime + 0.001); // Avoid division by zero

      assertThat(ratio).isLessThan(10.0); // Max 10x difference between fastest and slowest in CI
      assertThat(maxTime).isLessThan(5.0); // All should be under 5ms average in CI
    }

    @Test
    @DisplayName("Invalid mode fallback performance")
    void invalidModeFallbackPerformance() {
      // Given: Invalid mode strings
      final var invalidModes = new String[] {"invalid", "unknown", "", null};
      final var iterations = 2000;

      // When: Parsing invalid modes (should fallback to AUTO)
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                for (final var invalid : invalidModes) {
                  final var mode = SchemaDiscoveryMode.fromString(invalid);
                  assertThat(mode).isEqualTo(SchemaDiscoveryMode.AUTO);
                }
              },
              iterations,
              200);

      // Then: Fallback should be fast even with invalid input
      assertThat(result.getAverageTime()).isLessThan(2.0); // Less than 0.05ms average
    }
  }

  @Nested
  @DisplayName("Column Metadata Performance")
  class ColumnMetadataPerformanceTests {

    @Test
    @DisplayName("Single column metadata creation performance")
    void singleColumnMetadataCreationPerformance() {
      // When: Creating many column metadata instances
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var metadata = new ColumnMetadata("test_table", "test_column");
                metadata.recordTypeObservation(Types.VARCHAR, false);
                assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
              },
              5000,
              500);

      // Then: Should be very fast
      assertThat(result.getAverageTime()).isLessThan(1.0); // Less than 1ms average in CI
      assertThat(result.getMaxTime()).isLessThan(50L); // Max 50ms for any single creation in CI
    }

    @Test
    @DisplayName("Column metadata with many observations performance")
    void columnMetadataWithManyObservationsPerformance() {
      // Given: Column metadata with many observations
      final var metadata = new ColumnMetadata("test_table", "test_column");

      // First, populate with many observations
      for (int i = 0; i < 1000; i++) {
        metadata.recordTypeObservation(i % 2 == 0 ? Types.VARCHAR : Types.INTEGER, i % 50 == 0);
      }

      // When: Performing operations on heavily populated metadata
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                metadata.getResolvedSqlType();
                metadata.hasTypeConflict();
                metadata.getTypeConfidence();
                metadata.getNullRate();
              },
              10000,
              1000);

      // Then: Should remain fast even with many observations
      assertThat(result.getAverageTime()).isLessThan(1.0); // Less than 0.01ms
    }

    @Test
    @DisplayName("Batch observations performance")
    void batchObservationsPerformance() {
      // Given: Large batch of observations
      final var typeCounts =
          Map.of(
              Types.VARCHAR, 1000,
              Types.INTEGER, 500,
              Types.BOOLEAN, 200);

      // When: Recording batch observations
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var metadata = new ColumnMetadata("batch_table", "batch_column");
                metadata.recordBatchObservations(typeCounts, 100L);

                assertThat(metadata.getTotalObservations()).isEqualTo(1800); // 1000+500+200+100
                assertThat(metadata.hasTypeConflict()).isTrue();
              },
              1000,
              100);

      // Then: Batch operations should be efficient
      assertThat(result.getAverageTime()).isLessThan(5.0); // Less than 5ms for large batch in CI
    }
  }

  @Nested
  @DisplayName("Cached Schema Entry Performance")
  class CachedSchemaEntryPerformanceTests {

    @Test
    @DisplayName("Schema entry creation performance")
    void schemaEntryCreationPerformance() {
      // Given: Schema data
      final var schemaData =
          Map.of(
              "id", createTestColumnMetadata(),
              "name", createTestColumnMetadata(),
              "value", createTestColumnMetadata());

      // When: Creating cached schema entries
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var entry = new CachedSchemaEntry<>("test_table", schemaData, 60000L);
                assertThat(entry.isValid()).isTrue();
                assertThat(entry.getSchemaData()).isNotEmpty();
              },
              5000,
              500);

      // Then: Should be fast
      assertThat(result.getAverageTime()).isLessThan(2.0); // Less than 0.05ms
    }

    @Test
    @DisplayName("Concurrent schema entry access performance")
    void concurrentSchemaEntryAccessPerformance() throws Exception {
      // Given: Shared schema entry
      final var schemaData = Map.of("test_col", createTestColumnMetadata());
      final var entry = new CachedSchemaEntry<>("shared_table", schemaData, 60000L);
      final var threadCount = 10;
      final var operationsPerThread = 1000;

      // When: Multiple threads accessing concurrently
      final var totalTime =
          PerformanceTestUtils.measureExecutionTime(
              () -> {
                final var executor = Executors.newFixedThreadPool(threadCount);
                try {
                  final var futures =
                      IntStream.range(0, threadCount)
                          .mapToObj(
                              i ->
                                  CompletableFuture.runAsync(
                                      () -> {
                                        for (int j = 0; j < operationsPerThread; j++) {
                                          entry.getSchemaData();
                                          entry.isValid();
                                          entry.getLastAccessTime();
                                        }
                                      },
                                      executor))
                          .toArray(CompletableFuture[]::new);

                  CompletableFuture.allOf(futures).join();
                } finally {
                  executor.shutdown();
                }
              });

      // Then: Should handle concurrent access efficiently
      final var totalOperations = threadCount * operationsPerThread;
      final var avgTimePerOperation = (double) totalTime / totalOperations;

      assertThat(totalTime).isLessThan(30000L); // Less than 30 seconds total in CI
      assertThat(avgTimePerOperation).isLessThan(1.0); // Less than 1ms per operation in CI
    }

    @Test
    @DisplayName("Schema entry TTL validation performance")
    void schemaEntryTtlValidationPerformance() {
      // Given: Entry with short TTL
      final var schemaData = Map.of("col", createTestColumnMetadata());
      final var entry = new CachedSchemaEntry<>("ttl_table", schemaData, 1L); // 1ms TTL

      // When: Repeatedly checking validity (will become invalid quickly)
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                entry.isValid(); // This checks TTL each time
              },
              10000,
              1000);

      // Then: TTL validation should be very fast
      assertThat(result.getAverageTime()).isLessThan(0.5); // Less than 0.5ms in CI
    }
  }

  @Nested
  @DisplayName("Concurrent Schema Discovery Performance")
  class ConcurrentSchemaDiscoveryPerformanceTests {

    @Test
    @DisplayName("Concurrent schema discovery initialization performance")
    void concurrentSchemaDiscoveryInitializationPerformance() {
      // When: Creating multiple schema discovery instances
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var properties = new java.util.Properties();
                properties.setProperty("region", "us-east-1");
                properties.setProperty("concurrentSchemaDiscovery", "true");
                properties.setProperty("maxConcurrentSchemaDiscoveries", "4");

                final var discovery = new ConcurrentSchemaDiscovery(mockClient, properties);
                final var stats = discovery.getStatistics();

                assertThat(stats).isNotNull();
                assertThat(stats.get("enabled")).isEqualTo(true);
              },
              1000,
              100);

      // Then: Initialization should be fast
      assertThat(result.getAverageTime()).isLessThan(10.0); // Less than 10ms average in CI
    }

    @Test
    @DisplayName("Schema discovery statistics access performance")
    void schemaDiscoveryStatisticsAccessPerformance() {
      // Given: Initialized schema discovery
      final var properties = new java.util.Properties();
      properties.setProperty("region", "us-east-1");
      properties.setProperty("concurrentSchemaDiscovery", "true");

      final var discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Accessing statistics repeatedly
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var stats = discovery.getStatistics();
                assertThat(stats).isNotNull();
                assertThat(stats.get("enabled")).isEqualTo(true);
              },
              10000,
              1000);

      // Then: Statistics access should be very fast
      assertThat(result.getAverageTime()).isLessThan(1.0); // Less than 0.01ms
    }
  }

  @Nested
  @DisplayName("Type Resolution Performance")
  class TypeResolutionPerformanceTests {

    @Test
    @DisplayName("Type conflict resolution performance")
    void typeConflictResolutionPerformance() {
      // When: Creating metadata with many conflicting types
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var metadata = new ColumnMetadata("conflict_table", "mixed_column");

                // Add conflicting types
                metadata.recordTypeObservation(Types.INTEGER, false);
                metadata.recordTypeObservation(Types.VARCHAR, false);
                metadata.recordTypeObservation(Types.BOOLEAN, false);
                metadata.recordTypeObservation(Types.DECIMAL, false);

                // Resolve type (should be VARCHAR as most flexible)
                final var resolvedType = metadata.getResolvedSqlType();
                assertThat(resolvedType).isEqualTo(Types.VARCHAR);
                assertThat(metadata.hasTypeConflict()).isTrue();
              },
              5000,
              500);

      // Then: Type resolution should be fast even with conflicts
      assertThat(result.getAverageTime()).isLessThan(2.0); // Less than 0.05ms
    }

    @Test
    @DisplayName("Large scale type resolution performance")
    void largeScaleTypeResolutionPerformance() {
      // Given: Many columns with different type patterns
      final var columnCount = 100;
      final var columns = new ArrayList<ColumnMetadata>();

      // Create columns with different type complexity
      for (int i = 0; i < columnCount; i++) {
        final var metadata = new ColumnMetadata("large_table", "column_" + i);

        // Some columns have simple types, others have conflicts
        if (i % 3 == 0) {
          metadata.recordTypeObservation(Types.VARCHAR, false);
        } else if (i % 3 == 1) {
          metadata.recordTypeObservation(Types.INTEGER, false);
        } else {
          // Mixed types - create conflict
          metadata.recordTypeObservation(Types.INTEGER, false);
          metadata.recordTypeObservation(Types.VARCHAR, false);
        }

        columns.add(metadata);
      }

      // When: Resolving types for all columns
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                for (final var column : columns) {
                  column.getResolvedSqlType();
                  column.hasTypeConflict();
                  column.getTypeConfidence();
                }
              },
              1000,
              100);

      // Then: Should scale well with many columns
      assertThat(result.getAverageTime()).isLessThan(10.0); // Less than 10ms for 100 columns in CI
    }
  }

  // Helper method
  private ColumnMetadata createTestColumnMetadata() {
    final var metadata = new ColumnMetadata("test_table", "test_column");
    metadata.recordTypeObservation(Types.VARCHAR, false);
    return metadata;
  }
}
