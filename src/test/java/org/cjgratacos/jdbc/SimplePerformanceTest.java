package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
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
@DisplayName("Simple Performance Tests")
class SimplePerformanceTest {

  @Mock private DynamoDbClient mockClient;

  @Nested
  @DisplayName("Object Creation Performance")
  class ObjectCreationPerformanceTests {

    @Test
    @DisplayName("ColumnMetadata creation is fast")
    void columnMetadataCreationIsFast() {
      // When: Measuring creation time
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                final var metadata = new ColumnMetadata("test_table", "test_column");
                metadata.recordTypeObservation(java.sql.Types.VARCHAR, false);
              },
              1000,
              100);

      // Then: Should be very fast (< 1ms average)
      assertThat(result.getAverageTime()).isLessThan(5.0);
      assertThat(result.getMaxTime()).isLessThan(50L);
    }

    @Test
    @DisplayName("CachedSchemaEntry creation is fast")
    void cachedSchemaEntryCreationIsFast() {
      // Given: Test data
      final var schemaData = java.util.Map.of("test", createTestColumnMetadata());

      // When: Measuring creation time
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                new CachedSchemaEntry<>("test_table", schemaData, 60000L);
              },
              1000,
              100);

      // Then: Should be very fast
      assertThat(result.getAverageTime()).isLessThan(5.0);
    }

    @Test
    @DisplayName("SchemaDiscoveryMode parsing is fast")
    void schemaDiscoveryModeParsingIsFast() {
      // Given: Various mode strings
      final var modes = new String[] {"auto", "hints", "sampling", "disabled", "invalid"};

      // When: Measuring parsing time
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                for (String mode : modes) {
                  SchemaDiscoveryMode.fromString(mode);
                }
              },
              1000,
              100);

      // Then: Should be very fast
      assertThat(result.getAverageTime()).isLessThan(5.0);
    }

    @Test
    @DisplayName("JdbcParser property extraction is fast")
    void jdbcParserPropertyExtractionIsFast() {
      // Given: Complex URL
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000;"
              + "credentialsType=STATIC;accessKey=test;secretKey=secret;"
              + "schemaDiscovery=auto;schemaOptimizations=true;sampleSize=1000";

      // When: Measuring parsing time
      final var result =
          PerformanceTestUtils.benchmark(
              () -> {
                JdbcParser.extractProperties(url);
              },
              1000,
              100);

      // Then: Should be fast
      assertThat(result.getAverageTime()).isLessThan(5.0);
    }
  }

  @Nested
  @DisplayName("Memory Usage Performance")
  class MemoryUsagePerformanceTests {

    @Test
    @DisplayName("ColumnMetadata memory usage is reasonable")
    void columnMetadataMemoryUsageIsReasonable() {
      // When: Creating many ColumnMetadata objects
      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                final var metadataArray = new ColumnMetadata[1000];
                for (int i = 0; i < 1000; i++) {
                  metadataArray[i] = new ColumnMetadata("table_" + i, "column_" + i);
                  metadataArray[i].recordTypeObservation(java.sql.Types.VARCHAR, false);
                }
                // Keep reference to prevent premature GC
                assertThat(metadataArray[0]).isNotNull();
              });

      // Then: Memory usage should be reasonable (< 2MB for 1000 objects)
      assertThat(memoryResult.getUsedMemoryIncrease()).isLessThan(2 * 1024 * 1024);
    }

    @Test
    @DisplayName("CachedSchemaEntry memory usage is reasonable")
    void cachedSchemaEntryMemoryUsageIsReasonable() {
      // When: Creating many cached entries
      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                final var entries = new CachedSchemaEntry[500];
                for (int i = 0; i < 500; i++) {
                  final var schemaData =
                      java.util.Map.of(
                          "id", createTestColumnMetadata(),
                          "name", createTestColumnMetadata(),
                          "value", createTestColumnMetadata());
                  entries[i] = new CachedSchemaEntry<>("table_" + i, schemaData, 60000L);
                }
                // Keep reference
                assertThat(entries[0]).isNotNull();
              });

      // Then: Memory usage should be reasonable (< 5MB for 500 entries with 3 columns each)
      assertThat(memoryResult.getUsedMemoryIncrease()).isLessThan(5 * 1024 * 1024);
    }

    @Test
    @DisplayName("Driver property info memory is minimal")
    void driverPropertyInfoMemoryIsMinimal() {
      // When: Creating driver and getting property info multiple times
      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                final var driver = new DynamoDbDriver();
                final var url = "jdbc:dynamodb:partiql:region=us-east-1";

                // Call multiple times to test for leaks
                for (int i = 0; i < 100; i++) {
                  try {
                    driver.getPropertyInfo(url, new Properties());
                  } catch (Exception e) {
                    // Ignore for performance test
                  }
                }
              });

      // Then: Should have minimal memory impact
      assertThat(memoryResult.getUsedMemoryIncrease())
          .isLessThanOrEqualTo(2 * 1024 * 1024); // <= 2MB
    }
  }

  @Nested
  @DisplayName("Concurrent Operations Performance")
  class ConcurrentOperationsPerformanceTests {

    @Test
    @DisplayName("Concurrent ColumnMetadata operations are thread-safe and fast")
    void concurrentColumnMetadataOperationsAreThreadSafeAndFast() throws Exception {
      // Given: Shared ColumnMetadata
      final var metadata = new ColumnMetadata("shared_table", "shared_column");
      final var threadCount = 10;
      final var operationsPerThread = 100;

      // When: Multiple threads operate concurrently
      final var result =
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
                                          metadata.recordTypeObservation(
                                              java.sql.Types.VARCHAR, j % 10 == 0);
                                        }
                                      },
                                      executor))
                          .toArray(CompletableFuture[]::new);

                  CompletableFuture.allOf(futures).join();
                } finally {
                  executor.shutdown();
                }
              });

      // Then: Should complete reasonably quickly and maintain data integrity
      assertThat(result).isLessThan(10000); // < 10 seconds in CI
      assertThat(metadata.getTotalObservations()).isEqualTo(threadCount * operationsPerThread);
    }

    @Test
    @DisplayName("Concurrent CachedSchemaEntry access is performant")
    void concurrentCachedSchemaEntryAccessIsPerformant() throws Exception {
      // Given: Cached entry
      final var schemaData = java.util.Map.of("test", createTestColumnMetadata());
      final var entry = new CachedSchemaEntry<>("test_table", schemaData, 60000L);
      final var threadCount = 20;
      final var readsPerThread = 1000;

      // When: Multiple threads read concurrently
      final var result =
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
                                        for (int j = 0; j < readsPerThread; j++) {
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

      // Then: Should handle concurrent reads efficiently
      assertThat(result).isLessThan(5000); // < 5 seconds for 20,000 reads in CI
    }

    @Test
    @DisplayName("Concurrent property parsing is safe")
    void concurrentPropertyParsingIsSafe() {
      // Given: Complex URL
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000;"
              + "credentialsType=STATIC;accessKey=test;secretKey=secret;"
              + "schemaDiscovery=auto;schemaOptimizations=true;sampleSize=1000";
      final var threadCount = 10;
      final var operationsPerThread = 100;

      // When: Multiple threads parse URLs concurrently
      final var result =
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
                                          final var properties = JdbcParser.extractProperties(url);
                                          assertThat(properties.getProperty("region"))
                                              .isEqualTo("us-east-1");
                                        }
                                      },
                                      executor))
                          .toArray(CompletableFuture[]::new);

                  CompletableFuture.allOf(futures).join();
                } finally {
                  executor.shutdown();
                }
              });

      // Then: Should complete safely and quickly
      assertThat(result).isLessThan(10000L); // < 10 seconds in CI
    }
  }

  @Nested
  @DisplayName("Scalability Tests")
  class ScalabilityTests {

    @Test
    @DisplayName("ColumnMetadata scales with observation count")
    void columnMetadataScalesWithObservationCount() {
      // Given: Different observation counts
      final var counts = new int[] {100, 1000, 10000};

      for (int count : counts) {
        // When: Recording many observations
        final var result =
            PerformanceTestUtils.measureExecutionTime(
                () -> {
                  final var metadata = new ColumnMetadata("test_table", "test_column");
                  for (int i = 0; i < count; i++) {
                    metadata.recordTypeObservation(
                        i % 2 == 0 ? java.sql.Types.VARCHAR : java.sql.Types.INTEGER, i % 10 == 0);
                  }

                  // Verify final state
                  assertThat(metadata.getTotalObservations()).isEqualTo(count);
                  assertThat(metadata.hasTypeConflict()).isTrue();
                });

        // Then: Should scale reasonably (roughly linear)
        // In CI environments, performance can vary significantly, so we allow more time
        final var expectedMaxTime =
            (long) (count * 5 + 100); // Allow 5ms per observation + 100ms buffer for CI
        assertThat(result).isLessThan(expectedMaxTime);
      }
    }

    @Test
    @DisplayName("Property parsing scales with URL complexity")
    void propertyParsingScalesWithUrlComplexity() {
      // Given: URLs with increasing complexity
      final var baseUrl = "jdbc:dynamodb:partiql:region=us-east-1";
      final var propertyTemplates =
          new String[] {
            ";prop%d=value%d",
            ";nested%d=nested_value_%d",
            ";complex%d=http://host:8000/path?param%d=value%d&other=test"
          };

      for (int propCount : new int[] {10, 50, 100}) {
        // Build complex URL
        final var urlBuilder = new StringBuilder(baseUrl);
        for (int i = 0; i < propCount; i++) {
          final var template = propertyTemplates[i % propertyTemplates.length];
          if (template.contains("complex")) {
            urlBuilder.append(String.format(template, i, i, i));
          } else {
            urlBuilder.append(String.format(template, i, i));
          }
        }
        final var complexUrl = urlBuilder.toString();

        // When: Parsing complex URL
        final var result =
            PerformanceTestUtils.measureExecutionTime(
                () -> {
                  final var properties = JdbcParser.extractProperties(complexUrl);
                  assertThat(properties.size()).isGreaterThanOrEqualTo(propCount);
                });

        // Then: Should scale reasonably
        assertThat(result).isLessThan(500L); // Should be under 500ms even for 100 properties in CI
      }
    }
  }

  // Helper methods
  private ColumnMetadata createTestColumnMetadata() {
    final var metadata = new ColumnMetadata("test_table", "test_column");
    metadata.recordTypeObservation(java.sql.Types.VARCHAR, false);
    return metadata;
  }
}
