package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Comprehensive tests for type information across various DatabaseMetaData methods.
 *
 * <p>This test class verifies that type information is correctly provided by various JDBC metadata
 * methods including getColumns, getTypeInfo, getBestRowIdentifier, and others. It ensures
 * consistent type naming across all methods and proper handling of DynamoDB's three data types:
 * String, Number, and Binary.
 */
@DisplayName("Database Metadata Type Tests")
class DatabaseMetadataTypeTest extends BaseIntegrationTest {

  /**
   * Creates a simple test table with all three DynamoDB data types. Includes both key and non-key
   * attributes to test comprehensive type detection.
   */
  private void createMixedTypeTable() {
    try {
      final var dynamoDbClient = testContainer.getClient();

      // Delete table if it exists
      try {
        dynamoDbClient.deleteTable(builder -> builder.tableName("mixed_type_table"));
        Thread.sleep(500);
      } catch (Exception ignored) {
        // Table doesn't exist, which is fine
      }

      // Create table with mixed attribute types
      final CreateTableRequest request =
          CreateTableRequest.builder()
              .tableName("mixed_type_table")
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("stringKey")
                      .attributeType(ScalarAttributeType.S)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("numberSort")
                      .attributeType(ScalarAttributeType.N)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("binaryGsiKey")
                      .attributeType(ScalarAttributeType.B)
                      .build())
              .keySchema(
                  KeySchemaElement.builder()
                      .attributeName("stringKey")
                      .keyType(KeyType.HASH)
                      .build(),
                  KeySchemaElement.builder()
                      .attributeName("numberSort")
                      .keyType(KeyType.RANGE)
                      .build())
              .globalSecondaryIndexes(
                  GlobalSecondaryIndex.builder()
                      .indexName("BinaryIndex")
                      .keySchema(
                          KeySchemaElement.builder()
                              .attributeName("binaryGsiKey")
                              .keyType(KeyType.HASH)
                              .build())
                      .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                      .provisionedThroughput(
                          ProvisionedThroughput.builder()
                              .readCapacityUnits(5L)
                              .writeCapacityUnits(5L)
                              .build())
                      .build())
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(5L)
                      .writeCapacityUnits(5L)
                      .build())
              .build();

      dynamoDbClient.createTable(request);
      dynamoDbClient
          .waiter()
          .waitUntilTableExists(builder -> builder.tableName("mixed_type_table"));

      // Note: Not populating test data since the table has a Binary key attribute
      // which the test data generator doesn't handle well
    } catch (Exception e) {
      throw new RuntimeException("Failed to create mixed type table", e);
    }
  }

  /** Creates a simple table with only a partition key (no sort key). */
  private void createSingleKeyTable() {
    try {
      final var dynamoDbClient = testContainer.getClient();

      // Delete table if it exists
      try {
        dynamoDbClient.deleteTable(builder -> builder.tableName("single_key_table"));
        Thread.sleep(500);
      } catch (Exception ignored) {
        // Table doesn't exist, which is fine
      }

      final CreateTableRequest request =
          CreateTableRequest.builder()
              .tableName("single_key_table")
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("id")
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(5L)
                      .writeCapacityUnits(5L)
                      .build())
              .build();

      dynamoDbClient.createTable(request);
      dynamoDbClient
          .waiter()
          .waitUntilTableExists(builder -> builder.tableName("single_key_table"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create single key table", e);
    }
  }

  @Test
  @DisplayName("getColumns returns TYPE_NAME field with correct data types")
  void getColumnsReturnsTypeNameField() throws SQLException {
    // Given: A table with various attribute types
    createMixedTypeTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting columns
      try (final ResultSet columns = metaData.getColumns(null, null, "mixed_type_table", null)) {

        // Then: Should return columns with correct TYPE_NAME
        Set<String> foundColumns = new HashSet<>();
        Set<String> foundTypes = new HashSet<>();

        while (columns.next()) {
          final String columnName = columns.getString("COLUMN_NAME");
          final String typeName = columns.getString("TYPE_NAME");
          final int dataType = columns.getInt("DATA_TYPE");

          foundColumns.add(columnName);
          foundTypes.add(typeName);

          // Verify TYPE_NAME is populated and correct
          assertThat(typeName).isNotNull().isIn("VARCHAR", "NUMERIC", "BINARY");

          // Verify consistency between DATA_TYPE and TYPE_NAME
          switch (columnName) {
            case "stringKey":
              assertThat(typeName).isEqualTo("VARCHAR");
              assertThat(dataType).isEqualTo(Types.VARCHAR);
              break;
            case "numberSort":
              assertThat(typeName).isEqualTo("NUMERIC");
              assertThat(dataType).isEqualTo(Types.NUMERIC);
              break;
            case "binaryGsiKey":
              assertThat(typeName).isEqualTo("BINARY");
              assertThat(dataType).isEqualTo(Types.BINARY);
              break;
          }
        }

        // Verify we found at least the key attributes
        assertThat(foundColumns).contains("stringKey", "numberSort", "binaryGsiKey");
        assertThat(foundTypes).contains("VARCHAR", "NUMERIC", "BINARY");
      }
    }
  }

  @Test
  @DisplayName("getTypeInfo returns DynamoDB supported data types")
  void getTypeInfoReturnsSupportedTypes() throws SQLException {
    // Given: A connection to DynamoDB
    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting type info
      try (final ResultSet typeInfo = metaData.getTypeInfo()) {

        // Then: Should return DynamoDB's three data types
        Set<String> foundTypeNames = new HashSet<>();
        Set<Integer> foundDataTypes = new HashSet<>();

        while (typeInfo.next()) {
          final String typeName = typeInfo.getString("TYPE_NAME");
          final int dataType = typeInfo.getInt("DATA_TYPE");
          final int precision = typeInfo.getInt("PRECISION");
          final boolean nullable = typeInfo.getBoolean("NULLABLE");
          final boolean caseSensitive = typeInfo.getBoolean("CASE_SENSITIVE");
          final int searchable = typeInfo.getInt("SEARCHABLE");

          foundTypeNames.add(typeName);
          foundDataTypes.add(dataType);

          // Verify required fields are populated
          assertThat(typeName).isNotNull();
          assertThat(dataType).isNotNull();

          // Verify DynamoDB-specific type characteristics
          switch (typeName) {
            case "String":
            case "VARCHAR":
              assertThat(dataType).isEqualTo(Types.VARCHAR);
              assertThat(caseSensitive).isTrue();
              assertThat(searchable).isEqualTo(DatabaseMetaData.typeSearchable);
              break;
            case "Number":
            case "NUMERIC":
              assertThat(dataType).isEqualTo(Types.NUMERIC);
              assertThat(caseSensitive).isFalse();
              assertThat(searchable).isEqualTo(DatabaseMetaData.typeSearchable);
              break;
            case "Binary":
            case "BINARY":
              assertThat(dataType).isEqualTo(Types.BINARY);
              assertThat(caseSensitive).isTrue();
              assertThat(searchable).isEqualTo(DatabaseMetaData.typePredNone);
              break;
          }

          // All DynamoDB attributes are nullable
          assertThat(nullable).isTrue();

          // DynamoDB doesn't have fixed precision
          assertThat(precision).isEqualTo(0);
        }

        // Verify all three DynamoDB types are present
        assertThat(foundDataTypes)
            .containsExactlyInAnyOrder(Types.VARCHAR, Types.NUMERIC, Types.BINARY);
      }
    }
  }

  @Test
  @DisplayName("getBestRowIdentifier returns primary key with TYPE_NAME")
  void getBestRowIdentifierReturnsTypeInfo() throws SQLException {
    // Given: A table with composite primary key
    createMixedTypeTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting best row identifier
      try (final ResultSet bestRow =
          metaData.getBestRowIdentifier(
              null, null, "mixed_type_table", DatabaseMetaData.bestRowSession, true)) {

        // Then: Should return primary key columns with TYPE_NAME

        // First column: partition key
        assertThat(bestRow.next()).isTrue();
        assertThat(bestRow.getString("COLUMN_NAME")).isEqualTo("stringKey");
        assertThat(bestRow.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(bestRow.getInt("DATA_TYPE")).isEqualTo(Types.VARCHAR);
        assertThat(bestRow.getInt("PSEUDO_COLUMN")).isEqualTo(DatabaseMetaData.bestRowNotPseudo);

        // Second column: sort key
        assertThat(bestRow.next()).isTrue();
        assertThat(bestRow.getString("COLUMN_NAME")).isEqualTo("numberSort");
        assertThat(bestRow.getString("TYPE_NAME")).isEqualTo("Number");
        assertThat(bestRow.getInt("DATA_TYPE")).isEqualTo(Types.NUMERIC);

        // No more columns
        assertThat(bestRow.next()).isFalse();
      }
    }
  }

  @Test
  @DisplayName("Single key table returns correct type information")
  void singleKeyTableReturnsCorrectTypeInfo() throws SQLException {
    // Given: A table with only partition key
    createSingleKeyTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // Test getPrimaryKeys
      try (final ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "single_key_table")) {
        assertThat(primaryKeys.next()).isTrue();
        assertThat(primaryKeys.getString("COLUMN_NAME")).isEqualTo("id");
        assertThat(primaryKeys.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(primaryKeys.getInt("KEY_SEQ")).isEqualTo(1);
        assertThat(primaryKeys.next()).isFalse(); // Only one key
      }

      // Test getBestRowIdentifier
      try (final ResultSet bestRow =
          metaData.getBestRowIdentifier(
              null, null, "single_key_table", DatabaseMetaData.bestRowSession, true)) {
        assertThat(bestRow.next()).isTrue();
        assertThat(bestRow.getString("COLUMN_NAME")).isEqualTo("id");
        assertThat(bestRow.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(bestRow.next()).isFalse(); // Only one key
      }

      // Test getIndexColumns (should only have PRIMARY)
      final DynamoDbDatabaseMetaData dynaMeta = (DynamoDbDatabaseMetaData) metaData;
      try (final ResultSet indexColumns =
          dynaMeta.getIndexColumns("single_key_table", null, "single_key_table")) {
        assertThat(indexColumns.next()).isTrue();
        assertThat(indexColumns.getString("INDEX_NAME")).isEqualTo("PRIMARY");
        assertThat(indexColumns.getString("COLUMN_NAME")).isEqualTo("id");
        assertThat(indexColumns.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(indexColumns.next()).isFalse(); // Only one column in primary index
      }
    }
  }

  @Test
  @DisplayName("Consistent type naming across all metadata methods")
  void consistentTypeNamingAcrossAllMethods() throws SQLException {
    // Given: A table with all types
    createMixedTypeTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();
      final DynamoDbDatabaseMetaData dynaMeta = (DynamoDbDatabaseMetaData) metaData;

      // Collect type names from various methods
      Set<String> primaryKeyTypes = new HashSet<>();
      Set<String> indexColumnTypes = new HashSet<>();
      Set<String> bestRowTypes = new HashSet<>();

      // From getPrimaryKeys
      try (final ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "mixed_type_table")) {
        while (primaryKeys.next()) {
          primaryKeyTypes.add(primaryKeys.getString("TYPE_NAME"));
        }
      }

      // From getIndexColumns
      try (final ResultSet indexColumns =
          dynaMeta.getIndexColumns("mixed_type_table", null, "mixed_type_table")) {
        while (indexColumns.next()) {
          indexColumnTypes.add(indexColumns.getString("TYPE_NAME"));
        }
      }

      // From getBestRowIdentifier
      try (final ResultSet bestRow =
          metaData.getBestRowIdentifier(
              null, null, "mixed_type_table", DatabaseMetaData.bestRowSession, true)) {
        while (bestRow.next()) {
          bestRowTypes.add(bestRow.getString("TYPE_NAME"));
        }
      }

      // Then: All methods should use consistent type names
      assertThat(primaryKeyTypes).containsExactlyInAnyOrder("String", "Number");
      assertThat(bestRowTypes).containsExactlyInAnyOrder("String", "Number");
      assertThat(indexColumnTypes).contains("String", "Number", "Binary"); // Includes GSI
    }
  }

  @Test
  @DisplayName("getTableTypes returns supported table types")
  void getTableTypesReturnsSupportedTypes() throws SQLException {
    // Given: A connection
    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting table types
      try (final ResultSet tableTypes = metaData.getTableTypes()) {

        // Then: Should return at least "TABLE"
        Set<String> foundTypes = new HashSet<>();
        while (tableTypes.next()) {
          foundTypes.add(tableTypes.getString("TABLE_TYPE"));
        }

        assertThat(foundTypes).contains("TABLE");
      }
    }
  }
}
