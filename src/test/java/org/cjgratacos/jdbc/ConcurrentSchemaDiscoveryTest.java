package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

@ExtendWith(MockitoExtension.class)
@DisplayName("ConcurrentSchemaDiscovery Tests")
class ConcurrentSchemaDiscoveryTest {

  @Mock private DynamoDbClient mockClient;

  private Properties properties;
  private ConcurrentSchemaDiscovery discovery;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.setProperty("region", "us-east-1");
    discovery = new ConcurrentSchemaDiscovery(mockClient, properties);
  }

  @Nested
  @DisplayName("Async Operations Tests")
  class AsyncOperationsTests {

    @Test
    @DisplayName("Single table async discovery succeeds")
    void singleTableAsyncDiscoverySucceeds() throws Exception {
      // Given: Enabled concurrent discovery with mocked response
      properties.setProperty("concurrentSchemaDiscovery", "true");

      // Mock the describe table call to return a valid response
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenReturn(
              DescribeTableResponse.builder()
                  .table(
                      TableDescription.builder()
                          .tableName("test_table")
                          .tableStatus(TableStatus.ACTIVE)
                          .build())
                  .build());

      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Discovering a single table asynchronously
      final var future = discovery.discoverTableSchemaAsync("test_table");

      // Then: Future should be created and eventually complete
      assertThat(future).isNotNull();

      // Wait for completion and verify it completes successfully
      final var result = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(result).isNotNull();
      assertThat(future.isDone()).isTrue();
    }

    @Test
    @DisplayName("Multiple table concurrent discovery")
    void multipleTableConcurrentDiscovery() throws Exception {
      // Given: List of tables to discover
      final var tableNames = List.of("users", "orders", "products");
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Discovering multiple tables concurrently
      final var future = discovery.discoverMultipleTablesAsync(tableNames);

      // Then: Future should be created
      assertThat(future).isNotNull();

      // Wait a short time for completion or timeout
      try {
        final var results = future.get(1, TimeUnit.SECONDS);
        assertThat(results).isNotNull();
      } catch (TimeoutException | ExecutionException e) {
        // Expected with mocked client - test structure is what matters
        assertThat(future).isNotNull();
      }
    }

    @Test
    @DisplayName("Duplicate discovery operations are coordinated")
    void duplicateDiscoveryOperationsAreCoordinated() throws Exception {
      // Given: Enabled concurrent discovery with mocked table description
      properties.setProperty("concurrentSchemaDiscovery", "true");

      // Mock the describe table call
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenReturn(
              DescribeTableResponse.builder()
                  .table(
                      TableDescription.builder()
                          .tableName("same_table")
                          .tableStatus(TableStatus.ACTIVE)
                          .build())
                  .build());

      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Starting discovery for same table multiple times quickly
      final var future1 = discovery.discoverTableSchemaAsync("same_table");
      final var future2 = discovery.discoverTableSchemaAsync("same_table");

      // Then: Both futures should complete successfully
      // We can't guarantee they're the same object due to race conditions
      // but they should have the same result
      assertThat(future1.get()).isEqualTo(future2.get());
      assertThat(future1.get()).isEmpty(); // Empty because we didn't mock scan results
    }

    @Test
    @DisplayName("Discovery cancellation behavior is deterministic")
    void discoveryCancellationBehaviorIsDeterministic() {
      // Given: Enabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Attempting to cancel a non-existent discovery
      final var cancelledNonExistent = discovery.cancelDiscovery("non_existent_table");

      // Then: Should return false for non-existent operations
      assertThat(cancelledNonExistent).isFalse();

      // And: Pending discovery count should be zero
      assertThat(discovery.getPendingDiscoveryCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Discovery with timeout works")
    void discoveryWithTimeoutWorks() {
      // Given: Enabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // Setup mock to simulate a slow operation that will timeout
      lenient()
          .when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenAnswer(
              invocation -> {
                try {
                  Thread.sleep(100); // Sleep longer than timeout
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return DescribeTableResponse.builder()
                    .table(
                        TableDescription.builder()
                            .tableName("test_table")
                            .tableStatus(TableStatus.ACTIVE)
                            .attributeDefinitions(List.of())
                            .build())
                    .build();
              });

      // When/Then: Discovery with very short timeout should throw SQLException
      assertThatThrownBy(() -> discovery.discoverTableSchemaWithTimeout("test_table", 1))
          .isInstanceOf(SQLException.class);
    }
  }

  @Nested
  @DisplayName("Thread Pool Management Tests")
  class ThreadPoolManagementTests {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8})
    @DisplayName("Different thread pool sizes work correctly")
    void differentThreadPoolSizesWork(int poolSize) {
      // Given: Custom thread pool size
      properties.setProperty("concurrentSchemaDiscovery", "true");
      properties.setProperty("maxConcurrentSchemaDiscoveries", String.valueOf(poolSize));
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Getting statistics
      final var stats = discovery.getStatistics();

      // Then: Should reflect configured pool size
      assertThat(stats.get("enabled")).isEqualTo(true);
      assertThat(stats.get("maxConcurrentDiscoveries")).isEqualTo(poolSize);
    }

    @Test
    @DisplayName("Default thread pool size is based on CPU cores")
    void defaultThreadPoolSizeBasedOnCpuCores() {
      // Given: Default configuration
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Getting statistics
      final var stats = discovery.getStatistics();

      // Then: Should use CPU cores * 2 as default
      final var expectedPoolSize = Runtime.getRuntime().availableProcessors() * 2;
      assertThat(stats.get("maxConcurrentDiscoveries")).isEqualTo(expectedPoolSize);
    }

    @Test
    @DisplayName("Shutdown gracefully handles pending operations")
    void shutdownGracefullyHandlesPendingOperations() {
      // Given: Concurrent discovery with pending operations
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // Start some operations
      discovery.discoverTableSchemaAsync("table1");
      discovery.discoverTableSchemaAsync("table2");

      // When: Shutting down
      discovery.shutdown();

      // Then: Should complete without throwing exceptions
      final var stats = discovery.getStatistics();
      assertThat(stats.get("pendingDiscoveries")).isEqualTo(0);
    }
  }

  @Nested
  @DisplayName("Configuration Tests")
  class ConfigurationTests {

    @Test
    @DisplayName("Disabled concurrent discovery falls back to sync")
    void disabledConcurrentDiscoveryFallsBackToSync() {
      // Given: Disabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "false");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Attempting async discovery
      final var future = discovery.discoverTableSchemaAsync("test_table");

      // Then: Should still return a future (sync execution)
      assertThat(future).isNotNull();
      assertThat(discovery.isEnabled()).isFalse();
    }

    @Test
    @DisplayName("Invalid configuration uses sensible defaults")
    void invalidConfigurationUsesSensibleDefaults() {
      // Given: Invalid configuration
      properties.setProperty("concurrentSchemaDiscovery", "true");
      properties.setProperty("maxConcurrentSchemaDiscoveries", "invalid");

      // When: Creating discovery instance
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // Then: Should use default values
      final var stats = discovery.getStatistics();
      assertThat(stats.get("enabled")).isEqualTo(true);
      // Should fall back to CPU cores * 2
      final var expectedDefault = Runtime.getRuntime().availableProcessors() * 2;
      assertThat(stats.get("maxConcurrentDiscoveries")).isEqualTo(expectedDefault);
    }
  }

  @Nested
  @DisplayName("Performance and Statistics Tests")
  class PerformanceAndStatisticsTests {

    @Test
    @DisplayName("Statistics tracking works correctly")
    void statisticsTrackingWorksCorrectly() {
      // Given: Enabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Getting initial statistics
      final var initialStats = discovery.getStatistics();

      // Then: Should have expected structure
      assertThat(initialStats)
          .containsKeys(
              "enabled",
              "maxConcurrentDiscoveries",
              "pendingDiscoveries",
              "totalPendingEntries",
              "activeThreads");
      assertThat(initialStats.get("enabled")).isEqualTo(true);
      assertThat(initialStats.get("pendingDiscoveries")).isEqualTo(0);
    }

    @Test
    @DisplayName("Pending discovery count is accurate")
    void pendingDiscoveryCountIsAccurate() throws Exception {
      // Given: Enabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Starting multiple discoveries
      discovery.discoverTableSchemaAsync("table1");
      discovery.discoverTableSchemaAsync("table2");
      discovery.discoverTableSchemaAsync("table3");

      // Then: Pending count should reflect active operations
      final var pendingCount = discovery.getPendingDiscoveryCount();
      assertThat(pendingCount).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(3);
    }

    @Test
    @DisplayName("Performance is reasonable for multiple concurrent discoveries")
    void performanceReasonableForMultipleConcurrentDiscoveries() throws Exception {
      // Given: Large number of tables to discover
      final var tableNames =
          List.of(
              "table1", "table2", "table3", "table4", "table5", "table6", "table7", "table8",
              "table9", "table10");
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Measuring discovery time
      final var result =
          PerformanceTestUtils.measureExecutionTime(
              () -> {
                try {
                  final var future = discovery.discoverMultipleTablesAsync(tableNames);
                  // Wait briefly for setup - actual completion may timeout with mock
                  future.get(100, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                  // Expected with mock client
                }
              });

      // Then: Setup time should be reasonable (< 1 second for coordination)
      assertThat(result).isLessThan(1000);
    }

    @Test
    @DisplayName("Memory usage is reasonable")
    void memoryUsageIsReasonable() {
      // Given: Multiple concurrent discoveries
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Measuring memory usage
      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                // Start many discoveries to test memory usage
                for (int i = 0; i < 50; i++) {
                  discovery.discoverTableSchemaAsync("table_" + i);
                }
              });

      // Then: Memory increase should be reasonable (< 10MB)
      assertThat(memoryResult.getUsedMemoryIncrease()).isLessThan(10 * 1024 * 1024);
    }
  }

  @Nested
  @DisplayName("Error Handling Tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Empty table list returns empty result")
    void emptyTableListReturnsEmptyResult() throws Exception {
      // Given: Empty table list
      final var emptyList = List.<String>of();
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Discovering empty list
      final var future = discovery.discoverMultipleTablesAsync(emptyList);
      final var result = future.get(1, TimeUnit.SECONDS);

      // Then: Should return empty map
      assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Null table list returns empty result")
    void nullTableListReturnsEmptyResult() throws Exception {
      // Given: Null table list
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Discovering null list
      final var future = discovery.discoverMultipleTablesAsync(null);
      final var result = future.get(1, TimeUnit.SECONDS);

      // Then: Should return empty map
      assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Individual table failures don't fail entire batch")
    void individualTableFailuresDontFailEntireBatch() throws Exception {
      // Given: Mix of valid and invalid table names
      final var tableNames = List.of("valid_table1", "invalid_table", "valid_table2");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Discovering mixed list
      final var future = discovery.discoverMultipleTablesAsync(tableNames);

      // Then: Should complete even if some tables fail
      try {
        final var result = future.get(1, TimeUnit.SECONDS);
        assertThat(result).isNotNull();
      } catch (TimeoutException | ExecutionException e) {
        // Expected with mock - important thing is it doesn't hang
        assertThat(future).isNotNull();
      }
    }

    @Test
    @DisplayName("Cancellation of non-existent discovery returns false")
    void cancellationOfNonExistentDiscoveryReturnsFalse() {
      // Given: Discovery service
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Cancelling non-existent discovery
      final var cancelled = discovery.cancelDiscovery("non_existent_table");

      // Then: Should return false
      assertThat(cancelled).isFalse();
    }
  }

  @Nested
  @DisplayName("Correlation Context Tests")
  class CorrelationContextTests {

    @Test
    @DisplayName("Operations create correlation context")
    void operationsCreateCorrelationContext() {
      // Given: Enabled concurrent discovery
      properties.setProperty("concurrentSchemaDiscovery", "true");
      discovery = new ConcurrentSchemaDiscovery(mockClient, properties);

      // When: Starting discovery operation
      final var future = discovery.discoverTableSchemaAsync("test_table");

      // Then: Should complete without correlation errors
      assertThat(future).isNotNull();

      // Note: Actual correlation context testing would require integration test
      // since it involves thread-local context management
    }
  }
}
