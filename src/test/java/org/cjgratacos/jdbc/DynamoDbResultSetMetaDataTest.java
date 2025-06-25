package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Unit tests for DynamoDbResultSetMetaData implementation. */
@DisplayName("DynamoDB ResultSetMetaData Tests")
class DynamoDbResultSetMetaDataTest {

  @Test
  @DisplayName("Should provide correct metadata for mixed data types")
  void shouldProvideCorrectMetadataForMixedDataTypes() throws SQLException {
    // Given - Create test data with various DynamoDB types
    List<Map<String, AttributeValue>> testData = new ArrayList<>();

    Map<String, AttributeValue> row1 = new HashMap<>();
    row1.put("id", AttributeValue.builder().s("user123").build());
    row1.put("age", AttributeValue.builder().n("25").build());
    row1.put("active", AttributeValue.builder().bool(true).build());
    row1.put(
        "data", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
    testData.add(row1);

    Map<String, AttributeValue> row2 = new HashMap<>();
    row2.put("id", AttributeValue.builder().s("user456").build());
    row2.put("age", AttributeValue.builder().n("30").build());
    row2.put("active", AttributeValue.builder().bool(false).build());
    row2.put("name", AttributeValue.builder().s("John Doe").build());
    testData.add(row2);

    // When
    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(testData);

    // Then
    assertThat(metadata.getColumnCount()).isEqualTo(5); // id, age, active, data, name

    // Test column names (order may vary due to LinkedHashSet)
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      columnNames.add(metadata.getColumnName(i));
    }
    assertThat(columnNames).containsExactlyInAnyOrder("id", "age", "active", "data", "name");

    // Find columns by name and test their types
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      switch (columnName) {
        case "id", "name" -> {
          assertThat(metadata.getColumnType(i)).isEqualTo(Types.VARCHAR);
          assertThat(metadata.getColumnTypeName(i)).isEqualTo("VARCHAR");
          assertThat(metadata.getColumnClassName(i)).isEqualTo("java.lang.String");
        }
        case "age" -> {
          assertThat(metadata.getColumnType(i)).isEqualTo(Types.NUMERIC);
          assertThat(metadata.getColumnTypeName(i)).isEqualTo("NUMERIC");
          assertThat(metadata.getColumnClassName(i)).isEqualTo("java.math.BigDecimal");
        }
        case "active" -> {
          assertThat(metadata.getColumnType(i)).isEqualTo(Types.BOOLEAN);
          assertThat(metadata.getColumnTypeName(i)).isEqualTo("BOOLEAN");
          assertThat(metadata.getColumnClassName(i)).isEqualTo("java.lang.Boolean");
        }
        case "data" -> {
          assertThat(metadata.getColumnType(i)).isEqualTo(Types.VARBINARY);
          assertThat(metadata.getColumnTypeName(i)).isEqualTo("VARBINARY");
          assertThat(metadata.getColumnClassName(i)).isEqualTo("byte[]");
        }
      }
    }
  }

  @Test
  @DisplayName("Should handle empty result set")
  void shouldHandleEmptyResultSet() throws SQLException {
    // Given
    List<Map<String, AttributeValue>> emptyData = new ArrayList<>();

    // When
    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(emptyData);

    // Then
    assertThat(metadata.getColumnCount()).isEqualTo(0);
  }

  @Test
  @DisplayName("Should handle null values correctly")
  void shouldHandleNullValuesCorrectly() throws SQLException {
    // Given
    List<Map<String, AttributeValue>> testData = new ArrayList<>();

    Map<String, AttributeValue> row = new HashMap<>();
    row.put("nullable_field", AttributeValue.builder().nul(true).build());
    row.put("string_field", AttributeValue.builder().s("value").build());
    testData.add(row);

    // When
    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(testData);

    // Then
    assertThat(metadata.getColumnCount()).isEqualTo(2);

    // Find the string field and verify it has correct type
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if ("string_field".equals(metadata.getColumnName(i))) {
        assertThat(metadata.getColumnType(i)).isEqualTo(Types.VARCHAR);
      }
    }
  }

  @Test
  @DisplayName("Should validate column indices")
  void shouldValidateColumnIndices() throws SQLException {
    // Given
    List<Map<String, AttributeValue>> testData = new ArrayList<>();
    Map<String, AttributeValue> row = new HashMap<>();
    row.put("test_column", AttributeValue.builder().s("value").build());
    testData.add(row);

    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(testData);

    // When/Then - Invalid column indices should throw exception
    assertThatThrownBy(() -> metadata.getColumnName(0))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Column index out of range");

    assertThatThrownBy(() -> metadata.getColumnName(2))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Column index out of range");

    assertThatThrownBy(() -> metadata.getColumnType(0))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Column index out of range");
  }

  @Test
  @DisplayName("Should provide correct metadata properties")
  void shouldProvideCorrectMetadataProperties() throws SQLException {
    // Given
    List<Map<String, AttributeValue>> testData = new ArrayList<>();
    Map<String, AttributeValue> row = new HashMap<>();
    row.put("numeric_field", AttributeValue.builder().n("123.45").build());
    row.put("string_field", AttributeValue.builder().s("test").build());
    testData.add(row);

    // When
    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(testData);

    // Then
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      // Common properties
      assertThat(metadata.isAutoIncrement(i)).isFalse();
      assertThat(metadata.isSearchable(i)).isTrue();
      assertThat(metadata.isCurrency(i)).isFalse();
      assertThat(metadata.isNullable(i)).isEqualTo(ResultSetMetaData.columnNullable);
      assertThat(metadata.isReadOnly(i)).isTrue();
      assertThat(metadata.isWritable(i)).isFalse();
      assertThat(metadata.isDefinitelyWritable(i)).isFalse();

      // Schema/catalog/table names should be empty for DynamoDB
      assertThat(metadata.getTableName(i)).isEmpty();
      assertThat(metadata.getCatalogName(i)).isEmpty();
      assertThat(metadata.getSchemaName(i)).isEmpty();

      // Column labels should match column names
      assertThat(metadata.getColumnLabel(i)).isEqualTo(metadata.getColumnName(i));
    }
  }

  @Test
  @DisplayName("Should handle complex data types as OTHER")
  void shouldHandleComplexDataTypesAsOther() throws SQLException {
    // Given
    List<Map<String, AttributeValue>> testData = new ArrayList<>();
    Map<String, AttributeValue> row = new HashMap<>();

    // Add complex types
    row.put("string_set", AttributeValue.builder().ss("a", "b", "c").build());
    row.put("number_set", AttributeValue.builder().ns("1", "2", "3").build());
    row.put(
        "list_field",
        AttributeValue.builder()
            .l(
                AttributeValue.builder().s("item1").build(),
                AttributeValue.builder().s("item2").build())
            .build());
    row.put(
        "map_field",
        AttributeValue.builder()
            .m(Map.of("nested", AttributeValue.builder().s("value").build()))
            .build());

    testData.add(row);

    // When
    ResultSetMetaData metadata = new DynamoDbResultSetMetaData(testData);

    // Then
    assertThat(metadata.getColumnCount()).isEqualTo(4);

    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      assertThat(metadata.getColumnType(i)).isEqualTo(Types.OTHER);
      assertThat(metadata.getColumnTypeName(i)).isEqualTo("OTHER");
      assertThat(metadata.getColumnClassName(i)).isEqualTo("java.lang.Object");
    }
  }
}
