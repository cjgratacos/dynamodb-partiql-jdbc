package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ColumnMetadata Unit Tests")
class ColumnMetadataUnitTest {

  @Nested
  @DisplayName("Basic Functionality Tests")
  class BasicFunctionalityTests {

    @Test
    @DisplayName("Can create column metadata with table and column name")
    void canCreateColumnMetadataWithTableAndColumnName() {
      // Given: Table and column name
      final var tableName = "users";
      final var columnName = "id";

      // When: Creating column metadata
      final var metadata = new ColumnMetadata(tableName, columnName);

      // Then: Should have correct names
      assertThat(metadata.getTableName()).isEqualTo(tableName);
      assertThat(metadata.getColumnName()).isEqualTo(columnName);
      assertThat(metadata.getTotalObservations()).isEqualTo(0);
      assertThat(metadata.getNullObservations()).isEqualTo(0);
    }

    @Test
    @DisplayName("Records type observations correctly")
    void recordsTypeObservationsCorrectly() {
      // Given: Column metadata
      final var metadata = new ColumnMetadata("users", "name");

      // When: Recording type observations
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.VARCHAR, true); // null value

      // Then: Should track observations
      assertThat(metadata.getTotalObservations()).isEqualTo(3);
      assertThat(metadata.getNullObservations()).isEqualTo(1);
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(metadata.isNullable()).isTrue();
    }

    @Test
    @DisplayName("Calculates null rate correctly")
    void calculatesNullRateCorrectly() {
      // Given: Column metadata with mixed observations
      final var metadata = new ColumnMetadata("users", "optional_field");

      // When: Recording observations (2 nulls out of 5 total)
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.VARCHAR, true); // null
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.VARCHAR, true); // null
      metadata.recordTypeObservation(Types.VARCHAR, false);

      // Then: Null rate should be 40%
      assertThat(metadata.getNullRate()).isEqualTo(0.4);
      assertThat(metadata.isNullable()).isTrue();
    }
  }

  @Nested
  @DisplayName("Type Resolution Tests")
  class TypeResolutionTests {

    @Test
    @DisplayName("Single type resolves correctly")
    void singleTypeResolvesCorrectly() {
      // Given: Column with single type
      final var metadata = new ColumnMetadata("products", "price");

      // When: Recording numeric observations
      metadata.recordTypeObservation(Types.DECIMAL, false);
      metadata.recordTypeObservation(Types.DECIMAL, false);
      metadata.recordTypeObservation(Types.DECIMAL, false);

      // Then: Should resolve to DECIMAL
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.DECIMAL);
      assertThat(metadata.getTypeName()).isEqualTo("DECIMAL");
      assertThat(metadata.hasTypeConflict()).isFalse();
      assertThat(metadata.getTypeConfidence()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Type conflict resolves to most flexible type")
    void typeConflictResolvesToMostFlexibleType() {
      // Given: Column with conflicting types
      final var metadata = new ColumnMetadata("mixed_table", "mixed_field");

      // When: Recording different types (VARCHAR should win)
      metadata.recordTypeObservation(Types.INTEGER, false);
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.BOOLEAN, false);

      // Then: Should resolve to VARCHAR (most flexible)
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(metadata.hasTypeConflict()).isTrue();
      assertThat(metadata.getTypeConfidence()).isLessThan(1.0);
    }

    @Test
    @DisplayName("DynamoDB specific types are handled correctly")
    void dynamoDbSpecificTypesAreHandledCorrectly() {
      // Given: Different DynamoDB type scenarios
      final var stringMetadata = new ColumnMetadata("table", "string_attr");
      final var numberMetadata = new ColumnMetadata("table", "number_attr");
      final var booleanMetadata = new ColumnMetadata("table", "boolean_attr");
      final var binaryMetadata = new ColumnMetadata("table", "binary_attr");

      // When: Recording DynamoDB-style observations
      stringMetadata.recordTypeObservation(Types.VARCHAR, false);
      numberMetadata.recordTypeObservation(Types.DECIMAL, false);
      booleanMetadata.recordTypeObservation(Types.BOOLEAN, false);
      binaryMetadata.recordTypeObservation(Types.BINARY, false);

      // Then: Should resolve to appropriate JDBC types
      assertThat(stringMetadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(numberMetadata.getResolvedSqlType()).isEqualTo(Types.DECIMAL);
      assertThat(booleanMetadata.getResolvedSqlType()).isEqualTo(Types.BOOLEAN);
      assertThat(binaryMetadata.getResolvedSqlType()).isEqualTo(Types.BINARY);
    }

    @Test
    @DisplayName("Type priority follows DynamoDB flexibility rules")
    void typePriorityFollowsDynamoDbFlexibilityRules() {
      // Given: Column metadata
      final var metadata = new ColumnMetadata("test_table", "flexible_field");

      // When: Recording types in order of increasing flexibility
      metadata.recordTypeObservation(Types.BOOLEAN, false); // Least flexible
      metadata.recordTypeObservation(Types.INTEGER, false); // More flexible
      metadata.recordTypeObservation(Types.VARCHAR, false); // Most flexible

      // Then: Should resolve to VARCHAR
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(metadata.hasTypeConflict()).isTrue();
    }
  }

  @Nested
  @DisplayName("Batch Operations Tests")
  class BatchOperationsTests {

    @Test
    @DisplayName("Batch observations work correctly")
    void batchObservationsWorkCorrectly() {
      // Given: Column metadata
      final var metadata = new ColumnMetadata("orders", "status");

      // When: Recording batch observations
      final var typeCounts =
          Map.of(
              Types.VARCHAR, 80, // 80 string observations
              Types.INTEGER, 20 // 20 integer observations
              );
      metadata.recordBatchObservations(typeCounts, 10L); // 10 nulls

      // Then: Should have correct totals
      assertThat(metadata.getTotalObservations()).isEqualTo(110); // 80 + 20 + 10
      assertThat(metadata.getNullObservations()).isEqualTo(10);
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR); // More observations
      assertThat(metadata.hasTypeConflict()).isTrue();
    }

    @Test
    @DisplayName("Multiple batch operations accumulate correctly")
    void multipleBatchOperationsAccumulateCorrectly() {
      // Given: Column metadata
      final var metadata = new ColumnMetadata("logs", "level");

      // When: Recording multiple batches
      metadata.recordBatchObservations(Map.of(Types.VARCHAR, 50), 5L);
      metadata.recordBatchObservations(Map.of(Types.VARCHAR, 30), 2L);
      metadata.recordBatchObservations(Map.of(Types.INTEGER, 10), 1L);

      // Then: Should accumulate correctly
      assertThat(metadata.getTotalObservations()).isEqualTo(98); // 50+30+10+5+2+1
      assertThat(metadata.getNullObservations()).isEqualTo(8); // 5+2+1
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR); // 80 vs 10
    }
  }

  @Nested
  @DisplayName("Edge Cases Tests")
  class EdgeCasesTests {

    @Test
    @DisplayName("Only null observations result in nullable unknown type")
    void onlyNullObservationsResultInNullableUnknownType() {
      // Given: Column with only null observations
      final var metadata = new ColumnMetadata("sparse_table", "rarely_used");

      // When: Recording only null observations
      metadata.recordTypeObservation(Types.VARCHAR, true);
      metadata.recordTypeObservation(Types.VARCHAR, true);
      metadata.recordTypeObservation(Types.VARCHAR, true);

      // Then: Should be nullable with minimal type info
      assertThat(metadata.isNullable()).isTrue();
      assertThat(metadata.getTotalObservations()).isEqualTo(3);
      assertThat(metadata.getNullObservations()).isEqualTo(3);
      assertThat(metadata.getNullRate()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Zero observations have default state")
    void zeroObservationsHaveDefaultState() {
      // Given: New column metadata
      final var metadata = new ColumnMetadata("empty_table", "unused_column");

      // Then: Should have sensible defaults
      assertThat(metadata.getTotalObservations()).isEqualTo(0);
      assertThat(metadata.getNullObservations()).isEqualTo(0);
      assertThat(metadata.getNullRate()).isEqualTo(0.0);
      assertThat(metadata.getTypeConfidence()).isEqualTo(0.0);
      assertThat(metadata.hasTypeConflict()).isFalse();
    }

    @Test
    @DisplayName("Extreme type mixing resolves predictably")
    void extremeTypeMixingResolvesPredictably() {
      // Given: Column with many different types
      final var metadata = new ColumnMetadata("chaos_table", "anything_goes");

      // When: Recording many different types
      metadata.recordTypeObservation(Types.VARCHAR, false);
      metadata.recordTypeObservation(Types.INTEGER, false);
      metadata.recordTypeObservation(Types.DECIMAL, false);
      metadata.recordTypeObservation(Types.BOOLEAN, false);
      metadata.recordTypeObservation(Types.BINARY, false);
      metadata.recordTypeObservation(Types.ARRAY, false);

      // Then: Should resolve to VARCHAR (most flexible)
      assertThat(metadata.getResolvedSqlType()).isEqualTo(Types.VARCHAR);
      assertThat(metadata.hasTypeConflict()).isTrue();
      assertThat(metadata.getTypeConfidence()).isLessThan(0.5); // Low confidence
    }
  }

  @Nested
  @DisplayName("DynamoDB Specific Tests")
  class DynamoDbSpecificTests {

    @Test
    @DisplayName("Column size follows DynamoDB constraints")
    void columnSizeFollowsDynamoDbConstraints() {
      // Given: Different column types
      final var stringCol = new ColumnMetadata("table", "string_col");
      final var numberCol = new ColumnMetadata("table", "number_col");
      final var booleanCol = new ColumnMetadata("table", "boolean_col");

      // When: Recording observations
      stringCol.recordTypeObservation(Types.VARCHAR, false);
      numberCol.recordTypeObservation(Types.DECIMAL, false);
      booleanCol.recordTypeObservation(Types.BOOLEAN, false);

      // Then: Should follow DynamoDB size constraints
      assertThat(stringCol.getColumnSize()).isEqualTo(2048); // DynamoDB string limit
      assertThat(numberCol.getColumnSize()).isEqualTo(38); // DynamoDB number precision
      assertThat(booleanCol.getColumnSize()).isEqualTo(1); // Boolean size

      // Decimal digits should be 0 for DynamoDB (stored as strings)
      assertThat(numberCol.getDecimalDigits()).isEqualTo(0);
    }

    @Test
    @DisplayName("Complex types have variable size")
    void complexTypesHaveVariableSize() {
      // Given: Complex type columns
      final var arrayCol = new ColumnMetadata("table", "list_col");
      final var structCol = new ColumnMetadata("table", "map_col");

      // When: Recording complex type observations
      arrayCol.recordTypeObservation(Types.ARRAY, false);
      structCol.recordTypeObservation(Types.STRUCT, false);

      // Then: Should have variable size (0 indicates variable)
      assertThat(arrayCol.getColumnSize()).isEqualTo(0);
      assertThat(structCol.getColumnSize()).isEqualTo(0);
    }

    @Test
    @DisplayName("Type names match JDBC standards")
    void typeNamesMatchJdbcStandards() {
      // Given: Various column types
      final var metadata = new ColumnMetadata("table", "test_col");

      // When/Then: Recording different types should produce correct names
      metadata.recordTypeObservation(Types.VARCHAR, false);
      assertThat(metadata.getTypeName()).isEqualTo("VARCHAR");

      // Reset and try numeric
      final var numericMetadata = new ColumnMetadata("table", "numeric_col");
      numericMetadata.recordTypeObservation(Types.DECIMAL, false);
      assertThat(numericMetadata.getTypeName()).isEqualTo("DECIMAL");

      // Reset and try boolean
      final var booleanMetadata = new ColumnMetadata("table", "bool_col");
      booleanMetadata.recordTypeObservation(Types.BOOLEAN, false);
      assertThat(booleanMetadata.getTypeName()).isEqualTo("BOOLEAN");
    }
  }
}
