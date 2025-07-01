package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoDbParameterMetaDataTest {

  private Map<Integer, AttributeValue> parameterValues;
  private DynamoDbParameterMetaData metaData;

  @BeforeEach
  void setUp() {
    parameterValues = new HashMap<>();
  }

  @Test
  void testStringParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().s("test").build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterCount()).isEqualTo(1);
    assertThat(metaData.getParameterType(1)).isEqualTo(Types.VARCHAR);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("String");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(String.class.getName());
    assertThat(metaData.isSigned(1)).isFalse();
    assertThat(metaData.getPrecision(1)).isEqualTo(0);
    assertThat(metaData.getScale(1)).isEqualTo(0);
    assertThat(metaData.isNullable(1)).isEqualTo(ParameterMetaData.parameterNullable);
    assertThat(metaData.getParameterMode(1)).isEqualTo(ParameterMetaData.parameterModeIn);
  }

  @Test
  void testNumberParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().n("123.45").build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.NUMERIC);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("Number");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.math.BigDecimal.class.getName());
    assertThat(metaData.isSigned(1)).isTrue();
  }

  @Test
  void testBinaryParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[]{1, 2, 3})).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.BINARY);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("Binary");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(byte[].class.getName());
    assertThat(metaData.isSigned(1)).isFalse();
  }

  @Test
  void testBooleanParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().bool(true).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.BOOLEAN);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("Boolean");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(Boolean.class.getName());
  }

  @Test
  void testNullParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().nul(true).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.NULL);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("NULL");
    assertThat(metaData.getParameterClassName(1)).isNull();
  }

  @Test
  void testMapParameter() throws SQLException {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("key", AttributeValue.builder().s("value").build());
    parameterValues.put(1, AttributeValue.builder().m(map).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.STRUCT);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("Map");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.util.Map.class.getName());
  }

  @Test
  void testListParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().l(
        AttributeValue.builder().s("item1").build(),
        AttributeValue.builder().s("item2").build()
    ).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.ARRAY);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("List");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.util.List.class.getName());
  }

  @Test
  void testStringSetParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().ss("val1", "val2").build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.ARRAY);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("StringSet");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.util.Set.class.getName());
  }

  @Test
  void testNumberSetParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().ns("1", "2", "3").build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.ARRAY);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("NumberSet");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.util.Set.class.getName());
  }

  @Test
  void testBinarySetParameter() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().bs(
        SdkBytes.fromByteArray(new byte[]{1}),
        SdkBytes.fromByteArray(new byte[]{2})
    ).build());
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.ARRAY);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("BinarySet");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(java.util.Set.class.getName());
  }

  @Test
  void testUnsetParameter() throws SQLException {
    // Parameter not set yet
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.getParameterType(1)).isEqualTo(Types.NULL);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("NULL");
    assertThat(metaData.getParameterClassName(1)).isNull();
  }

  @Test
  void testMultipleParameters() throws SQLException {
    parameterValues.put(1, AttributeValue.builder().s("string").build());
    parameterValues.put(2, AttributeValue.builder().n("123").build());
    parameterValues.put(3, AttributeValue.builder().bool(true).build());
    metaData = new DynamoDbParameterMetaData(3, parameterValues);

    assertThat(metaData.getParameterCount()).isEqualTo(3);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("String");
    assertThat(metaData.getParameterTypeName(2)).isEqualTo("Number");
    assertThat(metaData.getParameterTypeName(3)).isEqualTo("Boolean");
  }

  @Test
  void testParameterIndexValidation() {
    metaData = new DynamoDbParameterMetaData(2, parameterValues);

    // Test invalid indices
    assertThatThrownBy(() -> metaData.getParameterType(0))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Parameter index 0 is out of range");

    assertThatThrownBy(() -> metaData.getParameterType(3))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Parameter index 3 is out of range");
  }

  @Test
  void testWrapperMethods() throws SQLException {
    metaData = new DynamoDbParameterMetaData(1, parameterValues);

    assertThat(metaData.isWrapperFor(DynamoDbParameterMetaData.class)).isTrue();
    assertThat(metaData.isWrapperFor(String.class)).isFalse();

    assertThat(metaData.unwrap(DynamoDbParameterMetaData.class)).isEqualTo(metaData);
    assertThatThrownBy(() -> metaData.unwrap(String.class))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Cannot unwrap to java.lang.String");
  }
}