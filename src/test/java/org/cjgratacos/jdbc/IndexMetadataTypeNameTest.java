package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Integration tests for TYPE_NAME field in index metadata.
 *
 * <p>This test class verifies that the TYPE_NAME field is properly populated in various metadata
 * methods including getPrimaryKeys, getIndexColumns, and information_schema.index_columns queries.
 * The TYPE_NAME field is essential for GUI database tools like DbVisualizer to display proper type
 * hints.
 */
@DisplayName("Index Metadata TYPE_NAME Tests")
class IndexMetadataTypeNameTest extends BaseIntegrationTest {

  /**
   * Creates a test table with various index types and attribute types. The table includes: -
   * Primary key: userId (String) as partition key, timestamp (Number) as sort key - GSI: category
   * (String) as partition key, status (String) as sort key - LSI: userId (String) as partition key,
   * priority (Number) as sort key
   */
  private void createComplexTestTable() {
    try {
      // Get DynamoDB client from test container
      final var dynamoDbClient = testContainer.getClient();

      // Delete table if it exists
      try {
        dynamoDbClient.deleteTable(builder -> builder.tableName("complex_test_table"));
        Thread.sleep(500); // Wait for deletion
      } catch (Exception ignored) {
        // Table doesn't exist, which is fine
      }

      // Create table with various index types
      final CreateTableRequest request =
          CreateTableRequest.builder()
              .tableName("complex_test_table")
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("userId")
                      .attributeType(ScalarAttributeType.S)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("timestamp")
                      .attributeType(ScalarAttributeType.N)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("category")
                      .attributeType(ScalarAttributeType.S)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("status")
                      .attributeType(ScalarAttributeType.S)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("priority")
                      .attributeType(ScalarAttributeType.N)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName("data")
                      .attributeType(ScalarAttributeType.B)
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build(),
                  KeySchemaElement.builder()
                      .attributeName("timestamp")
                      .keyType(KeyType.RANGE)
                      .build())
              .globalSecondaryIndexes(
                  GlobalSecondaryIndex.builder()
                      .indexName("CategoryStatusIndex")
                      .keySchema(
                          KeySchemaElement.builder()
                              .attributeName("category")
                              .keyType(KeyType.HASH)
                              .build(),
                          KeySchemaElement.builder()
                              .attributeName("status")
                              .keyType(KeyType.RANGE)
                              .build())
                      .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                      .provisionedThroughput(
                          ProvisionedThroughput.builder()
                              .readCapacityUnits(5L)
                              .writeCapacityUnits(5L)
                              .build())
                      .build(),
                  GlobalSecondaryIndex.builder()
                      .indexName("DataIndex")
                      .keySchema(
                          KeySchemaElement.builder()
                              .attributeName("data")
                              .keyType(KeyType.HASH)
                              .build())
                      .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                      .provisionedThroughput(
                          ProvisionedThroughput.builder()
                              .readCapacityUnits(5L)
                              .writeCapacityUnits(5L)
                              .build())
                      .build())
              .localSecondaryIndexes(
                  LocalSecondaryIndex.builder()
                      .indexName("PriorityIndex")
                      .keySchema(
                          KeySchemaElement.builder()
                              .attributeName("userId")
                              .keyType(KeyType.HASH)
                              .build(),
                          KeySchemaElement.builder()
                              .attributeName("priority")
                              .keyType(KeyType.RANGE)
                              .build())
                      .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                      .build())
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(5L)
                      .writeCapacityUnits(5L)
                      .build())
              .build();

      dynamoDbClient.createTable(request);
      // Wait for table to be active
      dynamoDbClient
          .waiter()
          .waitUntilTableExists(builder -> builder.tableName("complex_test_table"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create complex test table", e);
    }
  }

  @Test
  @DisplayName("getPrimaryKeys includes TYPE_NAME field with correct data types")
  void getPrimaryKeysIncludesTypeNameField() throws SQLException {
    // Given: A table with primary keys of different types
    createComplexTestTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting primary keys
      try (final ResultSet primaryKeys =
          metaData.getPrimaryKeys(null, null, "complex_test_table")) {

        // Then: First key should be partition key (userId) with String type
        assertThat(primaryKeys.next()).isTrue();
        assertThat(primaryKeys.getString("COLUMN_NAME")).isEqualTo("userId");
        assertThat(primaryKeys.getString("PK_NAME")).isEqualTo("PK_PARTITION");
        assertThat(primaryKeys.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(primaryKeys.getInt("KEY_SEQ")).isEqualTo(1);

        // And: Second key should be sort key (timestamp) with Number type
        assertThat(primaryKeys.next()).isTrue();
        assertThat(primaryKeys.getString("COLUMN_NAME")).isEqualTo("timestamp");
        assertThat(primaryKeys.getString("PK_NAME")).isEqualTo("PK_SORT");
        assertThat(primaryKeys.getString("TYPE_NAME")).isEqualTo("Number");
        assertThat(primaryKeys.getInt("KEY_SEQ")).isEqualTo(2);

        // And: No more keys
        assertThat(primaryKeys.next()).isFalse();
      }
    }
  }

  @Test
  @DisplayName("getIndexColumns includes TYPE_NAME field for all index types")
  void getIndexColumnsIncludesTypeNameField() throws SQLException {
    // Given: A table with various indexes
    createComplexTestTable();

    try (final var connection = getConnection()) {
      final DynamoDbDatabaseMetaData metaData = (DynamoDbDatabaseMetaData) connection.getMetaData();

      // When: Getting index columns for PRIMARY index
      try (final ResultSet indexColumns =
          metaData.getIndexColumns("complex_test_table", "PRIMARY", "complex_test_table")) {

        // Then: PRIMARY index columns should have correct TYPE_NAME
        assertThat(indexColumns.next()).isTrue();
        assertThat(indexColumns.getString("INDEX_NAME")).isEqualTo("PRIMARY");
        assertThat(indexColumns.getString("COLUMN_NAME")).isEqualTo("userId");
        assertThat(indexColumns.getString("KEY_NAME")).isEqualTo("userId");
        assertThat(indexColumns.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(indexColumns.getString("KEY_TYPE")).isEqualTo("HASH");

        assertThat(indexColumns.next()).isTrue();
        assertThat(indexColumns.getString("COLUMN_NAME")).isEqualTo("timestamp");
        assertThat(indexColumns.getString("TYPE_NAME")).isEqualTo("Number");
        assertThat(indexColumns.getString("KEY_TYPE")).isEqualTo("RANGE");
      }

      // When: Getting index columns for GSI
      try (final ResultSet gsiColumns =
          metaData.getIndexColumns(
              "complex_test_table", "CategoryStatusIndex", "complex_test_table")) {

        // Then: GSI columns should have correct TYPE_NAME
        assertThat(gsiColumns.next()).isTrue();
        assertThat(gsiColumns.getString("INDEX_NAME")).isEqualTo("CategoryStatusIndex");
        assertThat(gsiColumns.getString("COLUMN_NAME")).isEqualTo("category");
        assertThat(gsiColumns.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(gsiColumns.getString("KEY_TYPE")).isEqualTo("HASH");

        assertThat(gsiColumns.next()).isTrue();
        assertThat(gsiColumns.getString("COLUMN_NAME")).isEqualTo("status");
        assertThat(gsiColumns.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(gsiColumns.getString("KEY_TYPE")).isEqualTo("RANGE");
      }

      // When: Getting index columns for Binary type GSI
      try (final ResultSet binaryGsiColumns =
          metaData.getIndexColumns("complex_test_table", "DataIndex", "complex_test_table")) {

        // Then: Binary type should be correctly identified
        assertThat(binaryGsiColumns.next()).isTrue();
        assertThat(binaryGsiColumns.getString("INDEX_NAME")).isEqualTo("DataIndex");
        assertThat(binaryGsiColumns.getString("COLUMN_NAME")).isEqualTo("data");
        assertThat(binaryGsiColumns.getString("TYPE_NAME")).isEqualTo("Binary");
        assertThat(binaryGsiColumns.getString("KEY_TYPE")).isEqualTo("HASH");
      }

      // When: Getting index columns for LSI
      try (final ResultSet lsiColumns =
          metaData.getIndexColumns("complex_test_table", "PriorityIndex", "complex_test_table")) {

        // Then: LSI columns should have correct TYPE_NAME
        assertThat(lsiColumns.next()).isTrue();
        assertThat(lsiColumns.getString("INDEX_NAME")).isEqualTo("PriorityIndex");
        assertThat(lsiColumns.getString("COLUMN_NAME")).isEqualTo("userId");
        assertThat(lsiColumns.getString("TYPE_NAME")).isEqualTo("String");
        assertThat(lsiColumns.getString("KEY_TYPE")).isEqualTo("HASH");

        assertThat(lsiColumns.next()).isTrue();
        assertThat(lsiColumns.getString("COLUMN_NAME")).isEqualTo("priority");
        assertThat(lsiColumns.getString("TYPE_NAME")).isEqualTo("Number");
        assertThat(lsiColumns.getString("KEY_TYPE")).isEqualTo("RANGE");
      }
    }
  }

  @Test
  @DisplayName("information_schema.index_columns query returns TYPE_NAME field")
  void informationSchemaIndexColumnsReturnsTypeName() throws SQLException {
    // Given: A table with indexes
    createComplexTestTable();

    try (final var connection = getConnection();
        final var statement = connection.createStatement()) {

      // When: Querying information_schema.index_columns (simulating DbVisualizer)
      final String query =
          "SELECT * FROM information_schema.index_columns "
              + "WHERE table_name = 'complex_test_table' AND INDEX_NAME = 'PRIMARY'";

      try (final ResultSet resultSet = statement.executeQuery(query)) {

        // Then: Should return columns with TYPE_NAME field
        assertThat(resultSet.next()).isTrue();

        // Verify all expected fields exist
        // Note: information_schema returns fields with mixed case
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo("complex_test_table");
        assertThat(resultSet.getString("INDEX_NAME")).isEqualTo("PRIMARY");
        assertThat(resultSet.getString("COLUMN_NAME")).isEqualTo("userId");
        assertThat(resultSet.getString("INDEX_KEY"))
            .isEqualTo("userId"); // Uppercase in our implementation
        assertThat(resultSet.getString("TYPE_NAME"))
            .isEqualTo("String"); // Critical field for DbVisualizer
        assertThat(resultSet.getString("INDEX_KEY_TYPE")).isEqualTo("String"); // Alternative field
        assertThat(resultSet.getString("KEY_TYPE")).isEqualTo("HASH");

        // Check second column
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("COLUMN_NAME")).isEqualTo("timestamp");
        assertThat(resultSet.getString("TYPE_NAME")).isEqualTo("Number");
        assertThat(resultSet.getString("KEY_TYPE")).isEqualTo("RANGE");
      }
    }
  }

  @Test
  @DisplayName("getIndexInfo includes KEY_NAME field (for DbVisualizer compatibility)")
  void getIndexInfoIncludesKeyNameField() throws SQLException {
    // Given: A table with indexes
    createComplexTestTable();

    try (final var connection = getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      // When: Getting index info
      try (final ResultSet indexInfo =
          metaData.getIndexInfo(null, null, "complex_test_table", false, false)) {

        // Then: PRIMARY index should include KEY_NAME field
        assertThat(indexInfo.next()).isTrue();
        assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("PRIMARY");
        assertThat(indexInfo.getString("KEY_NAME"))
            .isNotNull()
            .isEqualTo("userId"); // First key name
        assertThat(indexInfo.getString("COLUMN_NAME")).contains("userId").contains("HASH");

        // Check GSI
        assertThat(indexInfo.next()).isTrue();
        assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("CategoryStatusIndex");
        assertThat(indexInfo.getString("KEY_NAME"))
            .isNotNull()
            .isEqualTo("category"); // First key name
      }
    }
  }

  @Test
  @DisplayName("All index types return proper TYPE_NAME values")
  void allIndexTypesReturnProperTypeNameValues() throws SQLException {
    // Given: A table with all DynamoDB data types represented
    createComplexTestTable();

    try (final var connection = getConnection()) {
      final DynamoDbDatabaseMetaData metaData = (DynamoDbDatabaseMetaData) connection.getMetaData();

      // When: Getting all index columns for the table
      try (final ResultSet allIndexColumns =
          metaData.getIndexColumns("complex_test_table", null, "complex_test_table")) {

        // Then: Collect all TYPE_NAME values
        var foundString = false;
        var foundNumber = false;
        var foundBinary = false;

        while (allIndexColumns.next()) {
          final String typeName = allIndexColumns.getString("TYPE_NAME");
          assertThat(typeName).isNotNull().isIn("String", "Number", "Binary");

          switch (typeName) {
            case "String" -> foundString = true;
            case "Number" -> foundNumber = true;
            case "Binary" -> foundBinary = true;
          }
        }

        // Verify all three DynamoDB types were found
        assertThat(foundString).isTrue().as("Should find String type");
        assertThat(foundNumber).isTrue().as("Should find Number type");
        assertThat(foundBinary).isTrue().as("Should find Binary type");
      }
    }
  }
}
