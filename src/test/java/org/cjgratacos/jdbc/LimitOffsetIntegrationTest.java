package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Integration tests for LIMIT and OFFSET functionality in DynamoDB PartiQL JDBC driver. */
@DisplayName("LIMIT and OFFSET Integration Tests")
class LimitOffsetIntegrationTest extends BaseIntegrationTest {

  private static final String TABLE_NAME = "LimitOffsetTestTable";
  private static final int TOTAL_ITEMS = 50;

  @Override
  protected void onSetup() throws SQLException {
    try {
      // Get DynamoDB client from test container
      var client = testContainer.getClient();

      // Create test table
      client.createTable(
          CreateTableRequest.builder()
              .tableName(TABLE_NAME)
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("id")
                      .attributeType(ScalarAttributeType.N)
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build());

      // Wait for table to be active
      Thread.sleep(1000);

      // Insert test data
      for (int i = 1; i <= TOTAL_ITEMS; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(
                    java.util.Map.of(
                        "id", AttributeValue.builder().n(String.valueOf(i)).build(),
                        "name", AttributeValue.builder().s("Item " + i).build(),
                        "value", AttributeValue.builder().n(String.valueOf(i * 10)).build()))
                .build());
      }
    } catch (Exception e) {
      throw new SQLException("Failed to set up test data", e);
    }
  }

  @Nested
  @DisplayName("LIMIT clause tests")
  class LimitTests {

    @Test
    @DisplayName("should return limited number of rows")
    void shouldReturnLimitedRows() throws SQLException {
      // Given
      int limit = 10;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT " + limit)) {

        // Then
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
          ids.add(rs.getInt("id"));
        }

        assertThat(ids).hasSize(limit);
      }
    }

    @ParameterizedTest
    @CsvSource({"5", "10", "20", "30"})
    @DisplayName("should respect different LIMIT values")
    void shouldRespectDifferentLimitValues(int limit) throws SQLException {
      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT " + limit)) {

        // Then
        int count = 0;
        while (rs.next()) {
          count++;
        }

        assertThat(count).isEqualTo(Math.min(limit, TOTAL_ITEMS));
      }
    }

    @Test
    @DisplayName("should handle LIMIT greater than total items")
    void shouldHandleLimitGreaterThanTotal() throws SQLException {
      // Given
      int limit = TOTAL_ITEMS + 10;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT " + limit)) {

        // Then
        int count = 0;
        while (rs.next()) {
          count++;
        }

        assertThat(count).isEqualTo(TOTAL_ITEMS);
      }
    }

    @Test
    @DisplayName("should handle LIMIT 0")
    void shouldHandleLimitZero() throws SQLException {
      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT 0")) {

        // Then
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Nested
  @DisplayName("OFFSET clause tests")
  class OffsetTests {

    @Test
    @DisplayName("should skip rows with OFFSET")
    void shouldSkipRowsWithOffset() throws SQLException {
      // Given
      int offset = 10;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " OFFSET " + offset)) {

        // Then
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
          ids.add(rs.getInt("id"));
        }

        assertThat(ids).hasSize(TOTAL_ITEMS - offset);
      }
    }

    @Test
    @DisplayName("should handle OFFSET greater than total items")
    void shouldHandleOffsetGreaterThanTotal() throws SQLException {
      // Given
      int offset = TOTAL_ITEMS + 10;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " OFFSET " + offset)) {

        // Then
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Nested
  @DisplayName("Combined LIMIT and OFFSET tests")
  class CombinedLimitOffsetTests {

    @ParameterizedTest
    @CsvSource({"10, 0, 10", "10, 5, 10", "10, 40, 10", "10, 45, 5", "20, 30, 20"})
    @DisplayName("should handle combined LIMIT and OFFSET")
    void shouldHandleCombinedLimitAndOffset(int limit, int offset, int expectedCount)
        throws SQLException {
      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  String.format(
                      "SELECT * FROM %s LIMIT %d OFFSET %d", TABLE_NAME, limit, offset))) {

        // Then
        int count = 0;
        while (rs.next()) {
          count++;
        }

        assertThat(count).isEqualTo(expectedCount);
      }
    }

    @Test
    @DisplayName("should support OFFSET before LIMIT syntax")
    void shouldSupportOffsetBeforeLimitSyntax() throws SQLException {
      // Given
      int limit = 10;
      int offset = 5;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  String.format(
                      "SELECT * FROM %s OFFSET %d LIMIT %d", TABLE_NAME, offset, limit))) {

        // Then
        int count = 0;
        while (rs.next()) {
          count++;
        }

        assertThat(count).isEqualTo(limit);
      }
    }

    @Test
    @DisplayName("should handle pagination with LIMIT and OFFSET")
    void shouldHandlePaginationWithLimitAndOffset() throws SQLException {
      // Given
      int pageSize = 10;
      List<Integer> allIds = new ArrayList<>();

      // When - simulate pagination through all data
      try (Connection conn = getConnection()) {
        for (int page = 0; page < (TOTAL_ITEMS / pageSize); page++) {
          try (Statement stmt = conn.createStatement();
              ResultSet rs =
                  stmt.executeQuery(
                      String.format(
                          "SELECT * FROM %s LIMIT %d OFFSET %d",
                          TABLE_NAME, pageSize, page * pageSize))) {

            while (rs.next()) {
              allIds.add(rs.getInt("id"));
            }
          }
        }
      }

      // Then
      assertThat(allIds).hasSize(TOTAL_ITEMS);
      assertThat(allIds)
          .containsExactlyInAnyOrderElementsOf(
              IntStream.rangeClosed(1, TOTAL_ITEMS).boxed().toList());
    }
  }

  @Nested
  @DisplayName("Edge cases and error handling")
  class EdgeCasesTests {

    @Test
    @DisplayName("should handle LIMIT in WHERE clause context")
    void shouldHandleLimitInWhereClause() throws SQLException {
      // When - using = instead of LIKE since DynamoDB PartiQL doesn't support LIKE
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE name = 'Item 1' LIMIT 5")) {

        // Then
        int count = 0;
        while (rs.next()) {
          count++;
        }

        // Should return only 1 row since we're looking for exact match
        assertThat(count).isEqualTo(1);
      }
    }

    @Test
    @DisplayName("should use fetch size as limit when maxRows is 0")
    void shouldUseFetchSizeAsLimitWhenMaxRowsIsZero() throws SQLException {
      // Given
      int fetchSize = 15;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement()) {

        stmt.setFetchSize(fetchSize);
        // maxRows defaults to 0, so fetchSize becomes the effective limit

        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
          // Count all items returned
          int totalCount = 0;
          while (rs.next()) {
            totalCount++;
          }

          // Then: fetchSize acts as the limit when maxRows is 0
          assertThat(totalCount).isEqualTo(fetchSize);
        }
      }
    }

    @Test
    @DisplayName("should prefer LIMIT over fetch size")
    void shouldPreferLimitOverFetchSize() throws SQLException {
      // Given
      int fetchSize = 20;
      int limit = 10;

      // When
      try (Connection conn = getConnection();
          Statement stmt = conn.createStatement()) {

        stmt.setFetchSize(fetchSize);

        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " LIMIT " + limit)) {
          // Count all items
          int count = 0;
          while (rs.next()) {
            count++;
          }

          // Then - LIMIT should take precedence
          assertThat(count).isEqualTo(limit);
        }
      }
    }
  }
}
