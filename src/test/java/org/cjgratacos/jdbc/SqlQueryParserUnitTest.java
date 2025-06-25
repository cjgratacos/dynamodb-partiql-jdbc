package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests for SqlQueryParser class that handles LIMIT and OFFSET extraction. */
@DisplayName("SqlQueryParser")
class SqlQueryParserUnitTest {

  @Nested
  @DisplayName("extractLimitOffset")
  class ExtractLimitOffsetTests {

    @Test
    @DisplayName("should return empty LimitOffsetInfo for null query")
    void shouldReturnEmptyForNullQuery() {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(null);

      // Then
      assertThat(result).isNotNull();
      assertThat(result.hasLimit()).isFalse();
      assertThat(result.hasOffset()).isFalse();
    }

    @Test
    @DisplayName("should return empty LimitOffsetInfo for empty query")
    void shouldReturnEmptyForEmptyQuery() {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset("");

      // Then
      assertThat(result).isNotNull();
      assertThat(result.hasLimit()).isFalse();
      assertThat(result.hasOffset()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users LIMIT 10, 10, ",
      "SELECT * FROM users limit 10, 10, ",
      "SELECT * FROM users LIMIT 5, 5, ",
      "SELECT * FROM users WHERE active = true LIMIT 100, 100, "
    })
    @DisplayName("should extract LIMIT value correctly")
    void shouldExtractLimitValue(String sql, Integer expectedLimit, Integer expectedOffset) {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(sql);

      // Then
      assertThat(result.hasLimit()).isTrue();
      assertThat(result.getLimit()).isEqualTo(expectedLimit);
      assertThat(result.hasOffset()).isFalse();
      assertThat(result.getOffset()).isNull();
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users LIMIT 10 OFFSET 20, 10, 20",
      "SELECT * FROM users limit 10 offset 20, 10, 20",
      "SELECT * FROM users LIMIT 5 OFFSET 0, 5, 0",
      "SELECT name FROM users WHERE active = true LIMIT 100 OFFSET 50, 100, 50"
    })
    @DisplayName("should extract LIMIT and OFFSET values correctly")
    void shouldExtractLimitAndOffsetValues(
        String sql, Integer expectedLimit, Integer expectedOffset) {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(sql);

      // Then
      assertThat(result.hasLimit()).isTrue();
      assertThat(result.getLimit()).isEqualTo(expectedLimit);
      assertThat(result.hasOffset()).isTrue();
      assertThat(result.getOffset()).isEqualTo(expectedOffset);
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users OFFSET 20 LIMIT 10, 10, 20",
      "SELECT * FROM users offset 20 limit 10, 10, 20",
      "SELECT * FROM users OFFSET 0 LIMIT 5, 5, 0"
    })
    @DisplayName("should extract values when OFFSET comes before LIMIT")
    void shouldExtractValuesWhenOffsetBeforeLimit(
        String sql, Integer expectedLimit, Integer expectedOffset) {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(sql);

      // Then
      assertThat(result.hasLimit()).isTrue();
      assertThat(result.getLimit()).isEqualTo(expectedLimit);
      assertThat(result.hasOffset()).isTrue();
      assertThat(result.getOffset()).isEqualTo(expectedOffset);
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users OFFSET 20, , 20",
      "SELECT * FROM users offset 20, , 20",
      "SELECT * FROM users WHERE active = true OFFSET 100, , 100"
    })
    @DisplayName("should extract OFFSET without LIMIT")
    void shouldExtractOffsetWithoutLimit(
        String sql, Integer expectedLimit, Integer expectedOffset) {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(sql);

      // Then
      assertThat(result.hasLimit()).isFalse();
      assertThat(result.getLimit()).isNull();
      assertThat(result.hasOffset()).isTrue();
      assertThat(result.getOffset()).isEqualTo(expectedOffset);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "SELECT * FROM users WHERE id = 'LIMIT'",
          "SELECT * FROM users WHERE name LIKE '%LIMIT%'",
          "SELECT * FROM users WHERE description = 'OFFSET 10 LIMIT 20'",
          "SELECT * FROM users",
          "INSERT INTO users VALUES ('test')"
        })
    @DisplayName("should not extract LIMIT/OFFSET from non-clause contexts")
    void shouldNotExtractFromNonClauseContexts(String sql) {
      // When
      LimitOffsetInfo result = SqlQueryParser.extractLimitOffset(sql);

      // Then
      assertThat(result.hasLimit()).isFalse();
      assertThat(result.hasOffset()).isFalse();
    }

    @Test
    @DisplayName("should throw exception for negative LIMIT")
    void shouldThrowExceptionForNegativeLimit() {
      // Given
      String sql = "SELECT * FROM users LIMIT -5";

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.extractLimitOffset(sql))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LIMIT value cannot be negative");
    }

    @Test
    @DisplayName("should throw exception for negative OFFSET")
    void shouldThrowExceptionForNegativeOffset() {
      // Given
      String sql = "SELECT * FROM users OFFSET -10";

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.extractLimitOffset(sql))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("OFFSET value cannot be negative");
    }
  }

  @Nested
  @DisplayName("removeLimitOffset")
  class RemoveLimitOffsetTests {

    @Test
    @DisplayName("should handle null query")
    void shouldHandleNullQuery() {
      // When
      String result = SqlQueryParser.removeLimitOffset(null);

      // Then
      assertThat(result).isNull();
    }

    @Test
    @DisplayName("should handle empty query")
    void shouldHandleEmptyQuery() {
      // When
      String result = SqlQueryParser.removeLimitOffset("");

      // Then
      assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users LIMIT 10, SELECT * FROM users",
      "SELECT * FROM users limit 10, SELECT * FROM users",
      "SELECT * FROM users WHERE active = true LIMIT 100, SELECT * FROM users WHERE active = true"
    })
    @DisplayName("should remove LIMIT clause")
    void shouldRemoveLimitClause(String input, String expected) {
      // When
      String result = SqlQueryParser.removeLimitOffset(input);

      // Then
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users LIMIT 10 OFFSET 20, SELECT * FROM users",
      "SELECT * FROM users OFFSET 20 LIMIT 10, SELECT * FROM users",
      "SELECT * FROM users WHERE active = true LIMIT 50 OFFSET 100, SELECT * FROM users WHERE active = true"
    })
    @DisplayName("should remove both LIMIT and OFFSET clauses")
    void shouldRemoveBothLimitAndOffsetClauses(String input, String expected) {
      // When
      String result = SqlQueryParser.removeLimitOffset(input);

      // Then
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
      "SELECT * FROM users OFFSET 20, SELECT * FROM users",
      "SELECT * FROM users WHERE active = true OFFSET 100, SELECT * FROM users WHERE active = true"
    })
    @DisplayName("should remove OFFSET clause without LIMIT")
    void shouldRemoveOffsetClauseWithoutLimit(String input, String expected) {
      // When
      String result = SqlQueryParser.removeLimitOffset(input);

      // Then
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "SELECT * FROM users WHERE id = 'LIMIT'",
          "SELECT * FROM users WHERE name LIKE '%LIMIT%'",
          "SELECT * FROM users WHERE description = 'OFFSET 10 LIMIT 20'",
          "SELECT * FROM users"
        })
    @DisplayName("should not modify queries without LIMIT/OFFSET clauses")
    void shouldNotModifyQueriesWithoutClauses(String sql) {
      // When
      String result = SqlQueryParser.removeLimitOffset(sql);

      // Then
      assertThat(result).isEqualTo(sql);
    }
  }

  @Nested
  @DisplayName("validateLimitOffset")
  class ValidateLimitOffsetTests {

    @Test
    @DisplayName("should accept null LimitOffsetInfo")
    void shouldAcceptNullLimitOffsetInfo() {
      // When/Then - should not throw
      SqlQueryParser.validateLimitOffset(null);
    }

    @Test
    @DisplayName("should accept empty LimitOffsetInfo")
    void shouldAcceptEmptyLimitOffsetInfo() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(null, null);

      // When/Then - should not throw
      SqlQueryParser.validateLimitOffset(info);
    }

    @Test
    @DisplayName("should accept reasonable LIMIT values")
    void shouldAcceptReasonableLimitValues() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(100, null);

      // When/Then - should not throw
      SqlQueryParser.validateLimitOffset(info);
    }

    @Test
    @DisplayName("should accept reasonable OFFSET values")
    void shouldAcceptReasonableOffsetValues() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(null, 1000);

      // When/Then - should not throw
      SqlQueryParser.validateLimitOffset(info);
    }

    @Test
    @DisplayName("should reject negative LIMIT")
    void shouldRejectNegativeLimit() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(-1, null);

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.validateLimitOffset(info))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LIMIT value cannot be negative");
    }

    @Test
    @DisplayName("should reject negative OFFSET")
    void shouldRejectNegativeOffset() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(null, -1);

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.validateLimitOffset(info))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("OFFSET value cannot be negative");
    }

    @Test
    @DisplayName("should reject extremely large LIMIT")
    void shouldRejectExtremelyLargeLimit() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(1000001, null);

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.validateLimitOffset(info))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LIMIT value is too large");
    }

    @Test
    @DisplayName("should reject extremely large OFFSET")
    void shouldRejectExtremelyLargeOffset() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(null, 10000001);

      // When/Then
      assertThatThrownBy(() -> SqlQueryParser.validateLimitOffset(info))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("OFFSET value is too large");
    }

    @Test
    @DisplayName("should accept maximum allowed values")
    void shouldAcceptMaximumAllowedValues() {
      // Given
      LimitOffsetInfo info = new LimitOffsetInfo(1000000, 10000000);

      // When/Then - should not throw
      SqlQueryParser.validateLimitOffset(info);
    }
  }

  @Nested
  @DisplayName("extractTableName")
  class ExtractTableNameTests {

    @Test
    @DisplayName("should extract table name from simple SELECT")
    void shouldExtractTableNameFromSimpleSelect() {
      // Given
      String sql = "SELECT * FROM users";

      // When
      String tableName = SqlQueryParser.extractTableName(sql);

      // Then
      assertThat(tableName).isEqualTo("users");
    }

    @Test
    @DisplayName("should extract table name with WHERE clause")
    void shouldExtractTableNameWithWhereClause() {
      // Given
      String sql = "SELECT id, name FROM customers WHERE status = 'active'";

      // When
      String tableName = SqlQueryParser.extractTableName(sql);

      // Then
      assertThat(tableName).isEqualTo("customers");
    }

    @Test
    @DisplayName("should extract table name with LIMIT and OFFSET")
    void shouldExtractTableNameWithLimitOffset() {
      // Given
      String sql = "SELECT * FROM products LIMIT 10 OFFSET 20";

      // When
      String tableName = SqlQueryParser.extractTableName(sql);

      // Then
      assertThat(tableName).isEqualTo("products");
    }

    @Test
    @DisplayName("should handle case insensitive FROM keyword")
    void shouldHandleCaseInsensitiveFrom() {
      // Given
      String sql1 = "SELECT * from orders";
      String sql2 = "SELECT * FROM orders";
      String sql3 = "SELECT * FrOm orders";

      // When/Then
      assertThat(SqlQueryParser.extractTableName(sql1)).isEqualTo("orders");
      assertThat(SqlQueryParser.extractTableName(sql2)).isEqualTo("orders");
      assertThat(SqlQueryParser.extractTableName(sql3)).isEqualTo("orders");
    }

    @Test
    @DisplayName("should return null for null or empty query")
    void shouldReturnNullForNullOrEmptyQuery() {
      // When/Then
      assertThat(SqlQueryParser.extractTableName(null)).isNull();
      assertThat(SqlQueryParser.extractTableName("")).isNull();
      assertThat(SqlQueryParser.extractTableName("   ")).isNull();
    }

    @Test
    @DisplayName("should return null for queries without FROM clause")
    void shouldReturnNullForQueriesWithoutFrom() {
      // Given
      String sql = "INSERT INTO users VALUES ('test')";

      // When
      String tableName = SqlQueryParser.extractTableName(sql);

      // Then
      assertThat(tableName).isNull();
    }

    @Test
    @DisplayName("should extract first table from JOIN queries")
    void shouldExtractFirstTableFromJoinQueries() {
      // Given
      String sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";

      // When
      String tableName = SqlQueryParser.extractTableName(sql);

      // Then
      assertThat(tableName).isEqualTo("users");
    }
  }

  @Nested
  @DisplayName("isSelectAllQuery")
  class IsSelectAllQueryTests {

    @Test
    @DisplayName("should return true for SELECT * queries")
    void shouldReturnTrueForSelectAllQueries() {
      // When/Then
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT * FROM users")).isTrue();
      assertThat(SqlQueryParser.isSelectAllQuery("select * from customers")).isTrue();
      assertThat(SqlQueryParser.isSelectAllQuery("  SELECT  *  FROM  products  ")).isTrue();
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT * FROM users WHERE active = true"))
          .isTrue();
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT * FROM users LIMIT 10")).isTrue();
    }

    @Test
    @DisplayName("should return false for specific column queries")
    void shouldReturnFalseForSpecificColumnQueries() {
      // When/Then
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT id, name FROM users")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT u.* FROM users u")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT users.* FROM users")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("SELECT COUNT(*) FROM users")).isFalse();
    }

    @Test
    @DisplayName("should return false for non-SELECT queries")
    void shouldReturnFalseForNonSelectQueries() {
      // When/Then
      assertThat(SqlQueryParser.isSelectAllQuery("INSERT INTO users VALUES (1)")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("UPDATE users SET name = 'test'")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("DELETE FROM users")).isFalse();
    }

    @Test
    @DisplayName("should return false for null or empty")
    void shouldReturnFalseForNullOrEmpty() {
      // When/Then
      assertThat(SqlQueryParser.isSelectAllQuery(null)).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("")).isFalse();
      assertThat(SqlQueryParser.isSelectAllQuery("   ")).isFalse();
    }
  }

  @Nested
  @DisplayName("extractTableIndexInfo")
  class ExtractTableIndexInfoTests {

    @Test
    @DisplayName("should extract table and index from quoted syntax")
    void shouldExtractTableAndIndexFromQuotedSyntax() {
      // Given
      String sql = "SELECT * FROM \"my-table\".\"PRIMARY\"";

      // When
      TableIndexInfo result = SqlQueryParser.extractTableIndexInfo(sql);

      // Then
      assertThat(result).isNotNull();
      assertThat(result.getTableName()).isEqualTo("my-table");
      assertThat(result.getIndexName()).isEqualTo("PRIMARY");
      assertThat(result.hasIndex()).isTrue();
      assertThat(result.isPrimaryIndex()).isTrue();
    }

    @Test
    @DisplayName("should extract table and index from unquoted syntax")
    void shouldExtractTableAndIndexFromUnquotedSyntax() {
      // Given
      String sql = "SELECT * FROM users.email_index WHERE active = true";

      // When
      TableIndexInfo result = SqlQueryParser.extractTableIndexInfo(sql);

      // Then
      assertThat(result).isNotNull();
      assertThat(result.getTableName()).isEqualTo("users");
      assertThat(result.getIndexName()).isEqualTo("email_index");
      assertThat(result.hasIndex()).isTrue();
      assertThat(result.isPrimaryIndex()).isFalse();
    }

    @Test
    @DisplayName("should extract table without index")
    void shouldExtractTableWithoutIndex() {
      // Given
      String sql1 = "SELECT * FROM \"users\"";
      String sql2 = "SELECT * FROM products WHERE price > 100";

      // When
      TableIndexInfo result1 = SqlQueryParser.extractTableIndexInfo(sql1);
      TableIndexInfo result2 = SqlQueryParser.extractTableIndexInfo(sql2);

      // Then
      assertThat(result1).isNotNull();
      assertThat(result1.getTableName()).isEqualTo("users");
      assertThat(result1.getIndexName()).isNull();
      assertThat(result1.hasIndex()).isFalse();

      assertThat(result2).isNotNull();
      assertThat(result2.getTableName()).isEqualTo("products");
      assertThat(result2.getIndexName()).isNull();
      assertThat(result2.hasIndex()).isFalse();
    }

    @Test
    @DisplayName("should return null for queries without FROM clause")
    void shouldReturnNullForQueriesWithoutFrom() {
      // Given
      String sql = "INSERT INTO users VALUES ('test')";

      // When
      TableIndexInfo result = SqlQueryParser.extractTableIndexInfo(sql);

      // Then
      assertThat(result).isNull();
    }

    @Test
    @DisplayName("should handle null and empty queries")
    void shouldHandleNullAndEmptyQueries() {
      // When/Then
      assertThat(SqlQueryParser.extractTableIndexInfo(null)).isNull();
      assertThat(SqlQueryParser.extractTableIndexInfo("")).isNull();
      assertThat(SqlQueryParser.extractTableIndexInfo("   ")).isNull();
    }
  }

  @Nested
  @DisplayName("normalizeIndexSyntax")
  class NormalizeIndexSyntaxTests {

    @Test
    @DisplayName("should convert table.PRIMARY to just table name")
    void shouldConvertTablePrimaryToJustTable() {
      // Given
      String sql = "SELECT * FROM \"my-table.PRIMARY\"";

      // When
      String result = SqlQueryParser.normalizeIndexSyntax(sql);

      // Then
      assertThat(result).isEqualTo("SELECT * FROM \"my-table\"");
    }

    @Test
    @DisplayName("should handle multiple table.index patterns")
    void shouldHandleMultipleTableIndexPatterns() {
      // Given
      String sql =
          "SELECT * FROM \"users.email_index\" JOIN \"orders.GSI1\" ON users.id = orders.user_id";

      // When
      String result = SqlQueryParser.normalizeIndexSyntax(sql);

      // Then
      assertThat(result)
          .isEqualTo(
              "SELECT * FROM \"users\".\"email_index\" JOIN \"orders\".\"GSI1\" ON users.id = orders.user_id");
    }

    @Test
    @DisplayName("should handle multiple patterns including PRIMARY")
    void shouldHandleMultiplePatternsIncludingPrimary() {
      // Given
      String sql =
          "SELECT * FROM \"users.PRIMARY\" JOIN \"orders.GSI1\" ON users.id = orders.user_id";

      // When
      String result = SqlQueryParser.normalizeIndexSyntax(sql);

      // Then
      assertThat(result)
          .isEqualTo(
              "SELECT * FROM \"users\" JOIN \"orders\".\"GSI1\" ON users.id = orders.user_id");
    }

    @Test
    @DisplayName("should not modify queries with proper index syntax")
    void shouldNotModifyQueriesWithProperIndexSyntax() {
      // Given
      String sql = "SELECT * FROM \"users\".\"email_index\" WHERE active = true";

      // When
      String result = SqlQueryParser.normalizeIndexSyntax(sql);

      // Then
      assertThat(result).isEqualTo(sql);
    }

    @Test
    @DisplayName("should not modify queries without index")
    void shouldNotModifyQueriesWithoutIndex() {
      // Given
      String sql = "SELECT * FROM \"users\" WHERE active = true";

      // When
      String result = SqlQueryParser.normalizeIndexSyntax(sql);

      // Then
      assertThat(result).isEqualTo(sql);
    }

    @Test
    @DisplayName("should handle case insensitive FROM keyword and PRIMARY index")
    void shouldHandleCaseInsensitiveFromAndPrimary() {
      // Given
      String sql1 = "SELECT * from \"table.PRIMARY\"";
      String sql2 = "SELECT * FROM \"table.Primary\"";
      String sql3 = "SELECT * FrOm \"table.primary\"";

      // When/Then
      assertThat(SqlQueryParser.normalizeIndexSyntax(sql1)).isEqualTo("SELECT * from \"table\"");
      assertThat(SqlQueryParser.normalizeIndexSyntax(sql2)).isEqualTo("SELECT * FROM \"table\"");
      assertThat(SqlQueryParser.normalizeIndexSyntax(sql3)).isEqualTo("SELECT * FrOm \"table\"");
    }

    @Test
    @DisplayName("should handle null and empty queries")
    void shouldHandleNullAndEmptyQueries() {
      // When/Then
      assertThat(SqlQueryParser.normalizeIndexSyntax(null)).isNull();
      assertThat(SqlQueryParser.normalizeIndexSyntax("")).isEmpty();
      assertThat(SqlQueryParser.normalizeIndexSyntax("   ")).isEqualTo("   ");
    }
  }
}
