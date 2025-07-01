package org.cjgratacos.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * ParameterMetaData implementation for DynamoDB PreparedStatements.
 *
 * <p>This class provides metadata about the parameters in a prepared statement, including parameter
 * count and type information based on the values that have been set.
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class DynamoDbParameterMetaData implements ParameterMetaData {

  private final int parameterCount;
  private final Map<Integer, AttributeValue> parameterValues;

  /**
   * Creates a new parameter metadata instance.
   *
   * @param parameterCount the number of parameters
   * @param parameterValues the parameter values that have been set
   */
  public DynamoDbParameterMetaData(
      int parameterCount, Map<Integer, AttributeValue> parameterValues) {
    this.parameterCount = parameterCount;
    this.parameterValues = parameterValues;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return parameterCount;
  }

  @Override
  public int isNullable(int param) throws SQLException {
    validateParameterIndex(param);
    return ParameterMetaData.parameterNullable; // All parameters can be null in DynamoDB
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    validateParameterIndex(param);
    AttributeValue value = parameterValues.get(param);
    if (value != null && value.n() != null) {
      return true; // Numbers can be signed
    }
    return false;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    validateParameterIndex(param);
    return 0; // DynamoDB doesn't have fixed precision
  }

  @Override
  public int getScale(int param) throws SQLException {
    validateParameterIndex(param);
    return 0; // DynamoDB doesn't have fixed scale
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    validateParameterIndex(param);
    AttributeValue value = parameterValues.get(param);
    if (value == null) {
      return Types.NULL;
    }

    if (value.s() != null) {
      return Types.VARCHAR;
    } else if (value.n() != null) {
      return Types.NUMERIC;
    } else if (value.b() != null) {
      return Types.BINARY;
    } else if (value.bool() != null) {
      return Types.BOOLEAN;
    } else if (value.nul() != null && value.nul()) {
      return Types.NULL;
    } else if (value.ss() != null && !value.ss().isEmpty()) {
      return Types.ARRAY; // String Set
    } else if (value.ns() != null && !value.ns().isEmpty()) {
      return Types.ARRAY; // Number Set
    } else if (value.bs() != null && !value.bs().isEmpty()) {
      return Types.ARRAY; // Binary Set
    } else if (value.l() != null && !value.l().isEmpty()) {
      return Types.ARRAY; // List
    } else if (value.m() != null && !value.m().isEmpty()) {
      return Types.STRUCT; // Map
    }

    return Types.OTHER;
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    validateParameterIndex(param);
    AttributeValue value = parameterValues.get(param);
    if (value == null) {
      return "NULL";
    }

    if (value.s() != null) {
      return "String";
    } else if (value.n() != null) {
      return "Number";
    } else if (value.b() != null) {
      return "Binary";
    } else if (value.bool() != null) {
      return "Boolean";
    } else if (value.nul() != null && value.nul()) {
      return "NULL";
    } else if (value.ss() != null && !value.ss().isEmpty()) {
      return "StringSet";
    } else if (value.ns() != null && !value.ns().isEmpty()) {
      return "NumberSet";
    } else if (value.bs() != null && !value.bs().isEmpty()) {
      return "BinarySet";
    } else if (value.l() != null && !value.l().isEmpty()) {
      return "List";
    } else if (value.m() != null && !value.m().isEmpty()) {
      return "Map";
    }

    return "Unknown";
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    validateParameterIndex(param);
    AttributeValue value = parameterValues.get(param);
    if (value == null) {
      return null;
    }

    if (value.s() != null) {
      return String.class.getName();
    } else if (value.n() != null) {
      return java.math.BigDecimal.class.getName();
    } else if (value.b() != null) {
      return byte[].class.getName();
    } else if (value.bool() != null) {
      return Boolean.class.getName();
    } else if (value.nul() != null && value.nul()) {
      return null;
    } else if (value.ss() != null && !value.ss().isEmpty()) {
      return java.util.Set.class.getName();
    } else if (value.ns() != null && !value.ns().isEmpty()) {
      return java.util.Set.class.getName();
    } else if (value.bs() != null && !value.bs().isEmpty()) {
      return java.util.Set.class.getName();
    } else if (value.l() != null && !value.l().isEmpty()) {
      return java.util.List.class.getName();
    } else if (value.m() != null && !value.m().isEmpty()) {
      return java.util.Map.class.getName();
    }

    return Object.class.getName();
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    validateParameterIndex(param);
    return ParameterMetaData.parameterModeIn; // All parameters are IN parameters
  }

  /**
   * Validates that the parameter index is valid.
   *
   * @param param the parameter index (1-based)
   * @throws SQLException if the index is invalid
   */
  private void validateParameterIndex(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw new SQLException(
          String.format(
              "Parameter index %d is out of range. Valid range is 1 to %d", param, parameterCount));
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }
}