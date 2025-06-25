package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for PartiQLKeywords class that provides PartiQL syntax support. */
@DisplayName("PartiQLKeywords")
class PartiQLKeywordsUnitTest {

  @Nested
  @DisplayName("Constructor")
  class ConstructorTests {

    @Test
    @DisplayName("should not be instantiable")
    void shouldNotBeInstantiable() {
      // When/Then
      assertThatThrownBy(
              () -> {
                var constructor = PartiQLKeywords.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
              })
          .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Nested
  @DisplayName("isReservedKeyword")
  class IsReservedKeywordTests {

    @Test
    @DisplayName("should identify common reserved keywords")
    void shouldIdentifyCommonReservedKeywords() {
      // Common SQL keywords
      assertThat(PartiQLKeywords.isReservedKeyword("SELECT")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("FROM")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("WHERE")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("INSERT")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("UPDATE")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("DELETE")).isTrue();
    }

    @Test
    @DisplayName("should identify DynamoDB-specific reserved keywords")
    void shouldIdentifyDynamoDbSpecificReservedKeywords() {
      assertThat(PartiQLKeywords.isReservedKeyword("YEAR")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("HASH")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("RANGE")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("PRIMARY")).isTrue();
    }

    @Test
    @DisplayName("should be case insensitive")
    void shouldBeCaseInsensitive() {
      assertThat(PartiQLKeywords.isReservedKeyword("select")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("Select")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("SELECT")).isTrue();
      assertThat(PartiQLKeywords.isReservedKeyword("SeLeCt")).isTrue();
    }

    @Test
    @DisplayName("should return false for non-reserved words")
    void shouldReturnFalseForNonReservedWords() {
      assertThat(PartiQLKeywords.isReservedKeyword("mytable")).isFalse();
      assertThat(PartiQLKeywords.isReservedKeyword("username")).isFalse();
      assertThat(PartiQLKeywords.isReservedKeyword("id")).isFalse();
    }

    @Test
    @DisplayName("should handle null and empty strings")
    void shouldHandleNullAndEmptyStrings() {
      assertThat(PartiQLKeywords.isReservedKeyword(null)).isFalse();
      assertThat(PartiQLKeywords.isReservedKeyword("")).isFalse();
      assertThat(PartiQLKeywords.isReservedKeyword("   ")).isFalse();
    }
  }

  @Nested
  @DisplayName("quoteIfNeeded")
  class QuoteIfNeededTests {

    @Test
    @DisplayName("should quote reserved keywords")
    void shouldQuoteReservedKeywords() {
      assertThat(PartiQLKeywords.quoteIfNeeded("year")).isEqualTo("\"year\"");
      assertThat(PartiQLKeywords.quoteIfNeeded("YEAR")).isEqualTo("\"YEAR\"");
      assertThat(PartiQLKeywords.quoteIfNeeded("select")).isEqualTo("\"select\"");
    }

    @Test
    @DisplayName("should quote identifiers with special characters")
    void shouldQuoteIdentifiersWithSpecialCharacters() {
      assertThat(PartiQLKeywords.quoteIfNeeded("user-name")).isEqualTo("\"user-name\"");
      assertThat(PartiQLKeywords.quoteIfNeeded("table.column")).isEqualTo("\"table.column\"");
      assertThat(PartiQLKeywords.quoteIfNeeded("my table")).isEqualTo("\"my table\"");
    }

    @Test
    @DisplayName("should not quote normal identifiers")
    void shouldNotQuoteNormalIdentifiers() {
      assertThat(PartiQLKeywords.quoteIfNeeded("myTable")).isEqualTo("myTable");
      assertThat(PartiQLKeywords.quoteIfNeeded("userName")).isEqualTo("userName");
      assertThat(PartiQLKeywords.quoteIfNeeded("id123")).isEqualTo("id123");
    }

    @Test
    @DisplayName("should not double quote already quoted identifiers")
    void shouldNotDoubleQuoteAlreadyQuotedIdentifiers() {
      assertThat(PartiQLKeywords.quoteIfNeeded("\"year\"")).isEqualTo("\"year\"");
      assertThat(PartiQLKeywords.quoteIfNeeded("\"user-name\"")).isEqualTo("\"user-name\"");
    }

    @Test
    @DisplayName("should handle null and empty strings")
    void shouldHandleNullAndEmptyStrings() {
      assertThat(PartiQLKeywords.quoteIfNeeded(null)).isNull();
      assertThat(PartiQLKeywords.quoteIfNeeded("")).isEmpty();
    }
  }

  @Nested
  @DisplayName("Constants")
  class ConstantsTests {

    @Test
    @DisplayName("should have correct DML statement constants")
    void shouldHaveCorrectDmlStatementConstants() {
      assertThat(PartiQLKeywords.SELECT).isEqualTo("SELECT");
      assertThat(PartiQLKeywords.INSERT).isEqualTo("INSERT");
      assertThat(PartiQLKeywords.UPDATE).isEqualTo("UPDATE");
      assertThat(PartiQLKeywords.DELETE).isEqualTo("DELETE");
    }

    @Test
    @DisplayName("should have correct query clause constants")
    void shouldHaveCorrectQueryClauseConstants() {
      assertThat(PartiQLKeywords.FROM).isEqualTo("FROM");
      assertThat(PartiQLKeywords.WHERE).isEqualTo("WHERE");
      assertThat(PartiQLKeywords.SET).isEqualTo("SET");
      assertThat(PartiQLKeywords.VALUES).isEqualTo("VALUES");
    }

    @Test
    @DisplayName("should have correct logical operator constants")
    void shouldHaveCorrectLogicalOperatorConstants() {
      assertThat(PartiQLKeywords.AND).isEqualTo("AND");
      assertThat(PartiQLKeywords.OR).isEqualTo("OR");
      assertThat(PartiQLKeywords.NOT).isEqualTo("NOT");
      assertThat(PartiQLKeywords.IN).isEqualTo("IN");
    }

    @Test
    @DisplayName("should have correct comparison operator constants")
    void shouldHaveCorrectComparisonOperatorConstants() {
      assertThat(PartiQLKeywords.EQ).isEqualTo("=");
      assertThat(PartiQLKeywords.NE).isEqualTo("<>");
      assertThat(PartiQLKeywords.LT).isEqualTo("<");
      assertThat(PartiQLKeywords.GT).isEqualTo(">");
    }

    @Test
    @DisplayName("should have correct function constants")
    void shouldHaveCorrectFunctionConstants() {
      assertThat(PartiQLKeywords.SIZE).isEqualTo("size");
      assertThat(PartiQLKeywords.CONTAINS).isEqualTo("contains");
      assertThat(PartiQLKeywords.BEGINS_WITH).isEqualTo("begins_with");
      assertThat(PartiQLKeywords.ATTRIBUTE_EXISTS).isEqualTo("attribute_exists");
    }
  }

  @Nested
  @DisplayName("Query Patterns")
  class QueryPatternsTests {

    @Test
    @DisplayName("should have correct SELECT pattern")
    void shouldHaveCorrectSelectPattern() {
      String pattern = PartiQLKeywords.SELECT_PATTERN;
      String query = String.format(pattern, "*", "\"users\"", "age > 21");
      assertThat(query).isEqualTo("SELECT * FROM \"users\" WHERE age > 21");
    }

    @Test
    @DisplayName("should have correct SELECT INDEX pattern")
    void shouldHaveCorrectSelectIndexPattern() {
      String pattern = PartiQLKeywords.SELECT_INDEX_PATTERN;
      String query = String.format(pattern, "*", "users", "email_index", "email = ?");
      assertThat(query).isEqualTo("SELECT * FROM \"users\".\"email_index\" WHERE email = ?");
    }

    @Test
    @DisplayName("should have correct INSERT pattern")
    void shouldHaveCorrectInsertPattern() {
      String pattern = PartiQLKeywords.INSERT_PATTERN;
      String query = String.format(pattern, "\"users\"", "{'id': '123', 'name': 'John'}");
      assertThat(query).isEqualTo("INSERT INTO \"users\" VALUE {'id': '123', 'name': 'John'}");
    }

    @Test
    @DisplayName("should have correct UPDATE pattern")
    void shouldHaveCorrectUpdatePattern() {
      String pattern = PartiQLKeywords.UPDATE_PATTERN;
      String query = String.format(pattern, "\"users\"", "name = 'Jane'", "id = '123'");
      assertThat(query).isEqualTo("UPDATE \"users\" SET name = 'Jane' WHERE id = '123'");
    }

    @Test
    @DisplayName("should have correct DELETE pattern")
    void shouldHaveCorrectDeletePattern() {
      String pattern = PartiQLKeywords.DELETE_PATTERN;
      String query = String.format(pattern, "\"users\"", "id = '123'");
      assertThat(query).isEqualTo("DELETE FROM \"users\" WHERE id = '123'");
    }
  }

  @Nested
  @DisplayName("Reserved Keywords Array")
  class ReservedKeywordsArrayTests {

    @Test
    @DisplayName("should contain expected number of keywords")
    void shouldContainExpectedNumberOfKeywords() {
      // DynamoDB has a large set of reserved keywords
      assertThat(PartiQLKeywords.RESERVED_KEYWORDS.length).isGreaterThan(500);
    }

    @Test
    @DisplayName("should contain specific important keywords")
    void shouldContainSpecificImportantKeywords() {
      String[] keywords = PartiQLKeywords.RESERVED_KEYWORDS;
      assertThat(keywords).contains("SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE");
      assertThat(keywords).contains("YEAR", "TIME", "DATE", "TIMESTAMP");
      assertThat(keywords).contains("PRIMARY", "INDEX", "KEY", "HASH", "RANGE");
    }

    @Test
    @DisplayName("should be in uppercase")
    void shouldBeInUppercase() {
      for (String keyword : PartiQLKeywords.RESERVED_KEYWORDS) {
        assertThat(keyword).isEqualTo(keyword.toUpperCase());
      }
    }
  }
}
