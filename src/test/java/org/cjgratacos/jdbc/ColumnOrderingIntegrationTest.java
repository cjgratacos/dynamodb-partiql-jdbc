package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Integration tests for column ordering functionality with primary and secondary keys. */
@DisplayName("Column Ordering Integration Tests")
class ColumnOrderingIntegrationTest extends BaseIntegrationTest {

  private static final String TABLE_NAME = "ColumnOrderingTestTable";

  @Override
  protected void onSetup() throws SQLException {
    try {
      // Get DynamoDB client from test container
      var client = testContainer.getClient();

      // Create test table with primary key and GSI
      client.createTable(
          CreateTableRequest.builder()
              .tableName(TABLE_NAME)
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
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build(),
                  KeySchemaElement.builder()
                      .attributeName("timestamp")
                      .keyType(KeyType.RANGE)
                      .build())
              .globalSecondaryIndexes(
                  GlobalSecondaryIndex.builder()
                      .indexName("category-status-index")
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
                      .build())
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build());

      // Wait for table to be active
      Thread.sleep(2000);

      // Insert test data with various attributes
      client.putItem(
          PutItemRequest.builder()
              .tableName(TABLE_NAME)
              .item(
                  java.util.Map.of(
                      "userId", AttributeValue.builder().s("user123").build(),
                      "timestamp", AttributeValue.builder().n("1000").build(),
                      "category", AttributeValue.builder().s("electronics").build(),
                      "status", AttributeValue.builder().s("active").build(),
                      "description", AttributeValue.builder().s("Test item").build(),
                      "price", AttributeValue.builder().n("99.99").build(),
                      "tags", AttributeValue.builder().ss("tag1", "tag2").build()))
              .build());

      client.putItem(
          PutItemRequest.builder()
              .tableName(TABLE_NAME)
              .item(
                  java.util.Map.of(
                      "userId", AttributeValue.builder().s("user456").build(),
                      "timestamp", AttributeValue.builder().n("2000").build(),
                      "category", AttributeValue.builder().s("books").build(),
                      "status", AttributeValue.builder().s("pending").build(),
                      "title", AttributeValue.builder().s("Another item").build(),
                      "author", AttributeValue.builder().s("John Doe").build()))
              .build());

    } catch (Exception e) {
      throw new SQLException("Failed to set up test data", e);
    }
  }

  @Test
  @DisplayName("should order primary key columns first in result set")
  void shouldOrderPrimaryKeyColumnsFirst() throws SQLException {
    // When
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

      // Then
      ResultSetMetaData metaData = rs.getMetaData();

      // Extract column names in order
      List<String> columnNames = new ArrayList<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columnNames.add(metaData.getColumnName(i));
      }

      // Primary keys (userId, timestamp) should be first
      assertThat(columnNames.get(0)).isEqualTo("userId");
      assertThat(columnNames.get(1)).isEqualTo("timestamp");

      // Secondary keys (category, status) should come next
      assertThat(columnNames.get(2)).isEqualTo("category");
      assertThat(columnNames.get(3)).isEqualTo("status");

      // Other columns should follow
      assertThat(columnNames.subList(0, 4))
          .containsExactly("userId", "timestamp", "category", "status");
    }
  }

  @Test
  @DisplayName("should maintain column ordering across multiple rows")
  void shouldMaintainColumnOrderingAcrossRows() throws SQLException {
    // When
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT 10")) {

      ResultSetMetaData metaData = rs.getMetaData();

      // Get initial column order
      List<String> firstRowColumns = new ArrayList<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        firstRowColumns.add(metaData.getColumnName(i));
      }

      // Process all rows and verify column access works correctly
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;

        // Verify we can access columns by their reordered positions
        assertThat(rs.getString(1)).isNotNull(); // userId (primary key)
        assertThat(rs.getString(2)).isNotNull(); // timestamp (primary key)
        assertThat(rs.getString(3)).isNotNull(); // category (secondary key)
        assertThat(rs.getString(4)).isNotNull(); // status (secondary key)

        // Verify we can also access by column name
        assertThat(rs.getString("userId")).isNotNull();
        assertThat(rs.getString("timestamp")).isNotNull();
        assertThat(rs.getString("category")).isNotNull();
        assertThat(rs.getString("status")).isNotNull();
      }

      assertThat(rowCount).isGreaterThan(0);
    }
  }

  @Test
  @DisplayName("should handle queries with specific column selection")
  void shouldHandleSpecificColumnSelection() throws SQLException {
    // When selecting specific columns, the original order should be preserved
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT description, userId, price FROM " + TABLE_NAME + " LIMIT 1")) {

      ResultSetMetaData metaData = rs.getMetaData();

      // Column order should match SELECT clause, not key ordering
      assertThat(metaData.getColumnCount()).isEqualTo(3);
      assertThat(metaData.getColumnName(1)).isEqualTo("description");
      assertThat(metaData.getColumnName(2)).isEqualTo("userId");
      assertThat(metaData.getColumnName(3)).isEqualTo("price");
    }
  }

  @Test
  @DisplayName("should handle tables without secondary indexes")
  void shouldHandleTablesWithoutSecondaryIndexes() throws SQLException {
    // Create a simple table with only primary key
    String simpleTableName = "SimpleTestTable";

    try {
      var client = testContainer.getClient();

      client.createTable(
          CreateTableRequest.builder()
              .tableName(simpleTableName)
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("id")
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build());

      Thread.sleep(1000);

      // Insert test data
      client.putItem(
          PutItemRequest.builder()
              .tableName(simpleTableName)
              .item(
                  java.util.Map.of(
                      "id", AttributeValue.builder().s("test-id").build(),
                      "name", AttributeValue.builder().s("Test Name").build(),
                      "value", AttributeValue.builder().n("42").build()))
              .build());

    } catch (Exception e) {
      throw new SQLException("Failed to create simple table", e);
    }

    // When
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + simpleTableName)) {

      ResultSetMetaData metaData = rs.getMetaData();

      // Primary key should still be first
      assertThat(metaData.getColumnName(1)).isEqualTo("id");

      // Other columns follow
      List<String> columnNames = new ArrayList<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columnNames.add(metaData.getColumnName(i));
      }

      assertThat(columnNames.get(0)).isEqualTo("id");
      assertThat(columnNames).contains("name", "value");
    }
  }
}
