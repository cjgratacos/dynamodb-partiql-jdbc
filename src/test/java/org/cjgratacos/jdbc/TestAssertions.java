package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import org.assertj.core.api.AbstractAssert;

/**
 * Custom AssertJ assertions for JDBC and DynamoDB-specific testing.
 *
 * <p>This class provides fluent assertions tailored for testing the DynamoDB JDBC driver, including
 * connection validation, schema metadata verification, and result set checks.
 */
public final class TestAssertions {

  private TestAssertions() {
    // Utility class
  }

  /**
   * Creates assertions for a JDBC connection.
   *
   * @param actual the connection to assert on
   * @return connection assertions
   */
  public static ConnectionAssert assertThat(final Connection actual) {
    return new ConnectionAssert(actual);
  }

  /**
   * Creates assertions for column metadata.
   *
   * @param actual the column metadata to assert on
   * @return column metadata assertions
   */
  public static ColumnMetadataAssert assertThat(final ColumnMetadata actual) {
    return new ColumnMetadataAssert(actual);
  }

  /**
   * Creates assertions for a map of column metadata.
   *
   * @param actual the column metadata map to assert on
   * @return column metadata map assertions
   */
  public static ColumnMetadataMapAssert assertThat(final Map<String, ColumnMetadata> actual) {
    return new ColumnMetadataMapAssert(actual);
  }

  /** Custom assertions for JDBC connections. */
  public static class ConnectionAssert extends AbstractAssert<ConnectionAssert, Connection> {

    public ConnectionAssert(final Connection actual) {
      super(actual, ConnectionAssert.class);
    }

    /**
     * Verifies that the connection is not closed.
     *
     * @return this assertion object
     * @throws SQLException if checking connection status fails
     */
    public ConnectionAssert isOpen() throws SQLException {
      isNotNull();
      if (actual.isClosed()) {
        failWithMessage("Expected connection to be open but it was closed");
      }
      return this;
    }

    /**
     * Verifies that the connection is closed.
     *
     * @return this assertion object
     * @throws SQLException if checking connection status fails
     */
    public ConnectionAssert isClosed() throws SQLException {
      isNotNull();
      if (!actual.isClosed()) {
        failWithMessage("Expected connection to be closed but it was open");
      }
      return this;
    }

    /**
     * Verifies that the connection has the expected schema.
     *
     * @param expectedSchema the expected schema name
     * @return this assertion object
     * @throws SQLException if getting schema fails
     */
    public ConnectionAssert hasSchema(final String expectedSchema) throws SQLException {
      isNotNull();
      final var actualSchema = actual.getSchema();
      if (!expectedSchema.equals(actualSchema)) {
        failWithMessage(
            "Expected connection schema to be <%s> but was <%s>", expectedSchema, actualSchema);
      }
      return this;
    }

    /**
     * Verifies that the connection can execute a simple query.
     *
     * @return this assertion object
     * @throws SQLException if query execution fails
     */
    public ConnectionAssert canExecuteQuery() throws SQLException {
      isNotNull();
      try (final var statement = actual.createStatement()) {
        // This should not throw an exception for a valid connection
        statement.execute("SELECT 1");
      }
      return this;
    }
  }

  /** Custom assertions for ColumnMetadata. */
  public static class ColumnMetadataAssert
      extends AbstractAssert<ColumnMetadataAssert, ColumnMetadata> {

    public ColumnMetadataAssert(final ColumnMetadata actual) {
      super(actual, ColumnMetadataAssert.class);
    }

    /**
     * Verifies that the column has the expected SQL type.
     *
     * @param expectedType the expected SQL type
     * @return this assertion object
     */
    public ColumnMetadataAssert hasSqlType(final int expectedType) {
      isNotNull();
      final var actualType = actual.getResolvedSqlType();
      if (actualType != expectedType) {
        failWithMessage(
            "Expected column <%s> to have SQL type <%s> but was <%s>",
            actual.getColumnName(), expectedType, actualType);
      }
      return this;
    }

    /**
     * Verifies that the column is nullable.
     *
     * @return this assertion object
     */
    public ColumnMetadataAssert isNullable() {
      isNotNull();
      if (!actual.isNullable()) {
        failWithMessage(
            "Expected column <%s> to be nullable but it was not", actual.getColumnName());
      }
      return this;
    }

    /**
     * Verifies that the column is not nullable.
     *
     * @return this assertion object
     */
    public ColumnMetadataAssert isNotNullable() {
      isNotNull();
      if (actual.isNullable()) {
        failWithMessage(
            "Expected column <%s> to not be nullable but it was", actual.getColumnName());
      }
      return this;
    }

    /**
     * Verifies that the column has the expected table name.
     *
     * @param expectedTableName the expected table name
     * @return this assertion object
     */
    public ColumnMetadataAssert hasTableName(final String expectedTableName) {
      isNotNull();
      final var actualTableName = actual.getTableName();
      if (!expectedTableName.equals(actualTableName)) {
        failWithMessage(
            "Expected column <%s> to have table name <%s> but was <%s>",
            actual.getColumnName(), expectedTableName, actualTableName);
      }
      return this;
    }

    /**
     * Verifies that the column was discovered using the expected source.
     *
     * @param expectedSource the expected discovery source
     * @return this assertion object
     */
    public ColumnMetadataAssert hasDiscoverySource(final String expectedSource) {
      isNotNull();
      final var actualSource = actual.getDiscoverySource();
      if (!expectedSource.equals(actualSource)) {
        failWithMessage(
            "Expected column <%s> to have discovery source <%s> but was <%s>",
            actual.getColumnName(), expectedSource, actualSource);
      }
      return this;
    }
  }

  /** Custom assertions for maps of ColumnMetadata. */
  public static class ColumnMetadataMapAssert
      extends AbstractAssert<ColumnMetadataMapAssert, Map<String, ColumnMetadata>> {

    public ColumnMetadataMapAssert(final Map<String, ColumnMetadata> actual) {
      super(actual, ColumnMetadataMapAssert.class);
    }

    /**
     * Verifies that the map contains a column with the given name.
     *
     * @param columnName the column name to check
     * @return this assertion object
     */
    public ColumnMetadataMapAssert containsColumn(final String columnName) {
      isNotNull();
      if (!actual.containsKey(columnName)) {
        failWithMessage("Expected schema to contain column <%s> but it did not", columnName);
      }
      return this;
    }

    /**
     * Verifies that the map does not contain a column with the given name.
     *
     * @param columnName the column name to check
     * @return this assertion object
     */
    public ColumnMetadataMapAssert doesNotContainColumn(final String columnName) {
      isNotNull();
      if (actual.containsKey(columnName)) {
        failWithMessage("Expected schema to not contain column <%s> but it did", columnName);
      }
      return this;
    }

    /**
     * Verifies that the map has exactly the expected number of columns.
     *
     * @param expectedCount the expected column count
     * @return this assertion object
     */
    public ColumnMetadataMapAssert hasColumnCount(final int expectedCount) {
      isNotNull();
      final var actualCount = actual.size();
      if (actualCount != expectedCount) {
        failWithMessage(
            "Expected schema to have <%d> columns but had <%d>", expectedCount, actualCount);
      }
      return this;
    }

    /**
     * Verifies that a specific column has the expected SQL type.
     *
     * @param columnName the column name
     * @param expectedType the expected SQL type
     * @return this assertion object
     */
    public ColumnMetadataMapAssert columnHasSqlType(
        final String columnName, final int expectedType) {
      containsColumn(columnName);
      final var column = actual.get(columnName);
      assertThat(column).hasSqlType(expectedType);
      return this;
    }

    /**
     * Verifies that the schema contains common DynamoDB types correctly mapped.
     *
     * @return this assertion object
     */
    public ColumnMetadataMapAssert hasCommonDynamoDbTypes() {
      // Check for at least one string field
      final var hasStringField =
          actual.values().stream().anyMatch(col -> col.getResolvedSqlType() == Types.VARCHAR);
      if (!hasStringField) {
        failWithMessage("Expected schema to contain at least one VARCHAR field");
      }
      return this;
    }
  }

  /** Utility methods for common test validations. */
  public static final class ValidationUtils {

    private ValidationUtils() {
      // Utility class
    }

    /**
     * Validates that a ResultSet is not empty and can be navigated.
     *
     * @param resultSet the result set to validate
     * @throws SQLException if result set operations fail
     */
    public static void validateNonEmptyResultSet(final ResultSet resultSet) throws SQLException {
      org.assertj.core.api.Assertions.assertThat(resultSet).isNotNull();
      org.assertj.core.api.Assertions.assertThat(resultSet.next()).isTrue();
      org.assertj.core.api.Assertions.assertThat(resultSet.getMetaData()).isNotNull();
      org.assertj.core.api.Assertions.assertThat(resultSet.getMetaData().getColumnCount())
          .isGreaterThan(0);
    }

    /**
     * Validates that a connection can perform basic JDBC operations.
     *
     * @param connection the connection to validate
     * @throws SQLException if validation fails
     */
    public static void validateBasicJdbcOperations(final Connection connection)
        throws SQLException {
      org.assertj.core.api.Assertions.assertThat(connection).isNotNull();

      // Test creating statements
      try (final var statement = connection.createStatement()) {
        org.assertj.core.api.Assertions.assertThat(statement).isNotNull();
      }

      // Test getting metadata
      final var metaData = connection.getMetaData();
      org.assertj.core.api.Assertions.assertThat(metaData).isNotNull();
      org.assertj.core.api.Assertions.assertThat(metaData.getDatabaseProductName()).isNotEmpty();
    }
  }
}
