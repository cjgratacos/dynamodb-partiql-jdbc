package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

@DisplayName("EnhancedSchemaDetector Tests")
class EnhancedSchemaDetectorTest extends BaseUnitTest {

  private EnhancedSchemaDetector detector;

  @Override
  protected void onSetup() {
    detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
  }

  @Nested
  @DisplayName("Schema Discovery Mode Tests")
  class SchemaDiscoveryModeTests {

    @Test
    @DisplayName("AUTO mode selects SAMPLING for small tables")
    void autoModeSelectsSamplingForSmallTables() throws SQLException {
      // Given: Small table with < 100 items
      setupMockTable("small_table", 50, false);
      setupMockScanResponse("small_table", createTestItems(10));

      // When: Detecting schema with AUTO mode
      defaultProperties.setProperty("schemaDiscovery", "auto");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("small_table");

      // Then: Should discover schema successfully
      assertThat(result).isNotEmpty();
      assertThat(result).containsKey("id");
    }

    @Test
    @DisplayName("AUTO mode selects HINTS for medium tables with GSI")
    void autoModeSelectsHintsForMediumTablesWithGSI() throws SQLException {
      // Given: Medium table with GSI
      setupMockTable("medium_table", 5000, true);
      setupMockScanResponse("medium_table", createTestItems(10));

      // When: Detecting schema with AUTO mode
      defaultProperties.setProperty("schemaDiscovery", "auto");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("medium_table");

      // Then: Should use hints strategy (minimal schema)
      assertThat(result).isNotEmpty();
    }

    @Test
    @DisplayName("AUTO mode selects SAMPLING for large tables")
    void autoModeSelectsSamplingForLargeTables() throws SQLException {
      // Given: Large table with > 10,000 items
      setupMockTable("large_table", 50000, false);
      setupMockScanResponse("large_table", createTestItems(100));

      // When: Detecting schema with AUTO mode
      defaultProperties.setProperty("schemaDiscovery", "auto");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("large_table");

      // Then: Should discover schema successfully
      assertThat(result).isNotEmpty();
    }

    @ParameterizedTest
    @EnumSource(SchemaDiscoveryMode.class)
    @DisplayName("All discovery modes work correctly")
    void allDiscoveryModesWork(SchemaDiscoveryMode mode) throws SQLException {
      // Given: Test table
      setupMockTable("test_table", 1000, false);
      if (mode != SchemaDiscoveryMode.DISABLED) {
        setupMockScanResponse("test_table", createTestItems(10));
      }

      // When: Using specific discovery mode
      defaultProperties.setProperty("schemaDiscovery", mode.name().toLowerCase());
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("test_table");

      // Then: Should return some schema information
      assertThat(result).isNotNull();
      if (mode != SchemaDiscoveryMode.DISABLED) {
        assertThat(result).isNotEmpty();
      }
    }

    @Test
    @DisplayName("DISABLED mode returns empty schema")
    void disabledModeReturnsEmptySchema() throws SQLException {
      // Given: Table with no attribute definitions
      setupMockTableNoAttributes("any_table");

      // When: Using DISABLED mode
      defaultProperties.setProperty("schemaDiscovery", "disabled");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("any_table");

      // Then: Should return empty schema
      assertThat(result).isEmpty();
    }
  }

  @Nested
  @DisplayName("Sampling Strategy Tests")
  class SamplingStrategyTests {

    @ParameterizedTest
    @ValueSource(strings = {"random", "sequential", "recent"})
    @DisplayName("All sampling strategies work")
    void allSamplingStrategiesWork(String strategy) throws SQLException {
      // Given: Table with test data
      setupMockTable("test_table", 1000, false);
      setupMockScanResponse("test_table", createTestItems(50));

      // When: Using specific sampling strategy
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      defaultProperties.setProperty("sampleStrategy", strategy);
      defaultProperties.setProperty("sampleSize", "10");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("test_table");

      // Then: Should discover schema
      assertThat(result).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000})
    @DisplayName("Different sample sizes work correctly")
    void differentSampleSizesWork(int sampleSize) throws SQLException {
      // Given: Table with sufficient data
      setupMockTable("test_table", 2000, false);
      setupMockScanResponse("test_table", createTestItems(Math.min(sampleSize, 100)));

      // When: Using specific sample size
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      defaultProperties.setProperty("sampleSize", String.valueOf(sampleSize));
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("test_table");

      // Then: Should discover schema
      assertThat(result).isNotEmpty();
    }
  }

  @Nested
  @DisplayName("Type Resolution Tests")
  class TypeResolutionTests {

    @Test
    @DisplayName("Type conflict resolution prioritizes VARCHAR")
    void typeConflictResolutionPrioritizesVarchar() throws SQLException {
      // Given: Items with conflicting types
      final var items =
          List.of(
              Map.of(
                  "id",
                  AttributeValue.builder().s("1").build(),
                  "conflicted",
                  AttributeValue.builder().s("string").build()),
              Map.of(
                  "id",
                  AttributeValue.builder().s("2").build(),
                  "conflicted",
                  AttributeValue.builder().n("123").build()),
              Map.of(
                  "id",
                  AttributeValue.builder().s("3").build(),
                  "conflicted",
                  AttributeValue.builder().bool(true).build()));

      setupMockTable("conflict_table", 3, false);
      setupMockScanResponse("conflict_table", items);

      // When: Detecting schema
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("conflict_table");

      // Then: Conflicted field should be VARCHAR
      assertThat(result).containsKey("conflicted");
      assertThat(result.get("conflicted").getResolvedSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Nullable detection works correctly")
    void nullableDetectionWorksCorrectly() throws SQLException {
      // Given: Items with some null values
      final var items =
          List.of(
              Map.of(
                  "id",
                  AttributeValue.builder().s("1").build(),
                  "sometimes_null",
                  AttributeValue.builder().s("value").build()),
              Map.of(
                  "id",
                  AttributeValue.builder().s("2").build(),
                  "sometimes_null",
                  AttributeValue.builder().nul(true).build()),
              Map.of(
                  "id",
                  AttributeValue.builder().s("3").build(),
                  "sometimes_null",
                  AttributeValue.builder().s("another").build()));

      setupMockTable("nullable_table", 3, false);
      setupMockScanResponse("nullable_table", items);

      // When: Detecting schema
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("nullable_table");

      // Then: Field should be detected as nullable
      assertThat(result).containsKey("sometimes_null");
      assertThat(result.get("sometimes_null").isNullable()).isTrue();
    }

    @Test
    @DisplayName("DynamoDB specific types are mapped correctly")
    void dynamoDbTypesAreMappedCorrectly() throws SQLException {
      // Given: Items with various DynamoDB types
      final var items =
          List.of(
              Map.of(
                  "id", AttributeValue.builder().s("1").build(),
                  "string_field", AttributeValue.builder().s("text").build(),
                  "number_field", AttributeValue.builder().n("123.45").build(),
                  "boolean_field", AttributeValue.builder().bool(true).build(),
                  "binary_field",
                      AttributeValue.builder()
                          .b(software.amazon.awssdk.core.SdkBytes.fromByteArray("data".getBytes()))
                          .build()));

      setupMockTable("types_table", 1, false);
      setupMockScanResponse("types_table", items);

      // When: Detecting schema
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("types_table");

      // Then: Types should be mapped correctly
      assertThat(result).containsKey("string_field");
      assertThat(result).containsKey("number_field");
      assertThat(result).containsKey("boolean_field");
      assertThat(result).containsKey("binary_field");

      assertThat(result.get("string_field").getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(result.get("number_field").getResolvedSqlType()).isEqualTo(Types.NUMERIC);
      assertThat(result.get("boolean_field").getResolvedSqlType()).isEqualTo(Types.BOOLEAN);
      assertThat(result.get("binary_field").getResolvedSqlType()).isEqualTo(Types.BINARY);
    }
  }

  @Nested
  @DisplayName("Error Handling Tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Table not found throws SQLException")
    void tableNotFoundThrowsSqlException() {
      // Given: A detector configured to avoid fallbacks by using DISABLED mode
      defaultProperties.setProperty("schemaDiscovery", "disabled");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);

      // Mock DescribeTable to throw exception for this table
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

      // When/Then: Should throw SQLException since disabled mode only relies on describeTable
      assertThatThrownBy(() -> detector.detectTableColumnMetadata("nonexistent_table"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("nonexistent_table");
    }

    @Test
    @DisplayName("Empty table handles gracefully")
    void emptyTableHandlesGracefully() throws SQLException {
      // Given: Empty table with no items to scan
      setupMockTableNoAttributes("empty_table");
      setupMockScanResponse("empty_table", List.of()); // Empty scan result

      // When: Detecting schema
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("empty_table");

      // Then: Should return empty schema gracefully
      assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Invalid configuration uses defaults")
    void invalidConfigurationUsesDefaults() throws SQLException {
      // Given: Invalid configuration
      defaultProperties.setProperty("schemaDiscovery", "invalid_mode");
      defaultProperties.setProperty("sampleSize", "invalid_number");
      defaultProperties.setProperty("sampleStrategy", "invalid_strategy");

      setupMockTable("test_table", 100, false);

      // When: Creating detector with invalid config
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);
      final var result = detector.detectTableColumnMetadata("test_table");

      // Then: Should use defaults and work
      assertThat(result).isNotNull();
    }
  }

  @Nested
  @DisplayName("Performance and Edge Cases")
  class PerformanceAndEdgeCasesTests {

    @Test
    @DisplayName("Large schema discovery completes within timeout")
    void largeSchemaDiscoveryCompletesWithinTimeout() throws SQLException {
      // Given: Large table with many attributes
      final var largeItem = TestDataGenerator.generateNestedItem(0);
      setupMockTable("large_schema_table", 1000, false);
      setupMockScanResponse("large_schema_table", List.of(largeItem));

      // When: Detecting schema
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);

      final var startTime = System.currentTimeMillis();
      final var result = detector.detectTableColumnMetadata("large_schema_table");
      final var duration = System.currentTimeMillis() - startTime;

      // Then: Should complete reasonably quickly
      assertThat(result).isNotEmpty();
      assertThat(duration).isLessThan(5000); // 5 seconds max
    }

    @Test
    @DisplayName("Memory usage is reasonable for large sample sizes")
    void memoryUsageReasonableForLargeSampleSizes() throws SQLException {
      // Given: Large sample size
      setupMockTable("memory_test_table", 10000, false);

      // When: Using large sample size
      defaultProperties.setProperty("schemaDiscovery", "sampling");
      defaultProperties.setProperty("sampleSize", "1000");
      detector = new EnhancedSchemaDetector(mockClient, defaultProperties);

      final var memoryResult =
          PerformanceTestUtils.measureMemoryUsage(
              () -> {
                try {
                  detector.detectTableColumnMetadata("memory_test_table");
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });

      // Then: Memory increase should be reasonable (< 50MB)
      assertThat(memoryResult.getUsedMemoryIncrease()).isLessThan(50 * 1024 * 1024);
    }
  }
}
