package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for TableIndexInfo class that holds table and index information. */
@DisplayName("TableIndexInfo")
class TableIndexInfoUnitTest {

  @Nested
  @DisplayName("Constructor")
  class ConstructorTests {

    @Test
    @DisplayName("should create instance with table and index")
    void shouldCreateInstanceWithTableAndIndex() {
      // When
      TableIndexInfo info = new TableIndexInfo("users", "email_index");

      // Then
      assertThat(info.getTableName()).isEqualTo("users");
      assertThat(info.getIndexName()).isEqualTo("email_index");
      assertThat(info.hasIndex()).isTrue();
      assertThat(info.isPrimaryIndex()).isFalse();
    }

    @Test
    @DisplayName("should create instance with table only")
    void shouldCreateInstanceWithTableOnly() {
      // When
      TableIndexInfo info = new TableIndexInfo("users", null);

      // Then
      assertThat(info.getTableName()).isEqualTo("users");
      assertThat(info.getIndexName()).isNull();
      assertThat(info.hasIndex()).isFalse();
      assertThat(info.isPrimaryIndex()).isFalse();
    }

    @Test
    @DisplayName("should throw exception for null table name")
    void shouldThrowExceptionForNullTableName() {
      // When/Then
      assertThatThrownBy(() -> new TableIndexInfo(null, "index"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Table name cannot be null or empty");
    }

    @Test
    @DisplayName("should throw exception for empty table name")
    void shouldThrowExceptionForEmptyTableName() {
      // When/Then
      assertThatThrownBy(() -> new TableIndexInfo("", "index"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Table name cannot be null or empty");

      assertThatThrownBy(() -> new TableIndexInfo("   ", "index"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Table name cannot be null or empty");
    }
  }

  @Nested
  @DisplayName("isPrimaryIndex")
  class IsPrimaryIndexTests {

    @Test
    @DisplayName("should return true for PRIMARY index")
    void shouldReturnTrueForPrimaryIndex() {
      // Given
      TableIndexInfo info1 = new TableIndexInfo("users", "PRIMARY");
      TableIndexInfo info2 = new TableIndexInfo("users", "primary");
      TableIndexInfo info3 = new TableIndexInfo("users", "Primary");

      // Then
      assertThat(info1.isPrimaryIndex()).isTrue();
      assertThat(info2.isPrimaryIndex()).isTrue();
      assertThat(info3.isPrimaryIndex()).isTrue();
    }

    @Test
    @DisplayName("should return false for non-PRIMARY index")
    void shouldReturnFalseForNonPrimaryIndex() {
      // Given
      TableIndexInfo info1 = new TableIndexInfo("users", "email_index");
      TableIndexInfo info2 = new TableIndexInfo("users", "GSI1");
      TableIndexInfo info3 = new TableIndexInfo("users", null);

      // Then
      assertThat(info1.isPrimaryIndex()).isFalse();
      assertThat(info2.isPrimaryIndex()).isFalse();
      assertThat(info3.isPrimaryIndex()).isFalse();
    }
  }

  @Nested
  @DisplayName("getQualifiedName")
  class GetQualifiedNameTests {

    @Test
    @DisplayName("should return table.index for non-PRIMARY index")
    void shouldReturnTableDotIndexForNonPrimaryIndex() {
      // Given
      TableIndexInfo info = new TableIndexInfo("users", "email_index");

      // When
      String qualifiedName = info.getQualifiedName();

      // Then
      assertThat(qualifiedName).isEqualTo("\"users\".\"email_index\"");
    }

    @Test
    @DisplayName("should return just table for PRIMARY index")
    void shouldReturnJustTableForPrimaryIndex() {
      // Given
      TableIndexInfo info = new TableIndexInfo("users", "PRIMARY");

      // When
      String qualifiedName = info.getQualifiedName();

      // Then
      assertThat(qualifiedName).isEqualTo("\"users\"");
    }

    @Test
    @DisplayName("should return just table when no index")
    void shouldReturnJustTableWhenNoIndex() {
      // Given
      TableIndexInfo info = new TableIndexInfo("users", null);

      // When
      String qualifiedName = info.getQualifiedName();

      // Then
      assertThat(qualifiedName).isEqualTo("\"users\"");
    }

    @Test
    @DisplayName("should handle complex table names")
    void shouldHandleComplexTableNames() {
      // Given
      TableIndexInfo info1 = new TableIndexInfo("my-table", "GSI1");
      TableIndexInfo info2 = new TableIndexInfo("user_profiles_2024", null);

      // When/Then
      assertThat(info1.getQualifiedName()).isEqualTo("\"my-table\".\"GSI1\"");
      assertThat(info2.getQualifiedName()).isEqualTo("\"user_profiles_2024\"");
    }
  }

  @Nested
  @DisplayName("toString")
  class ToStringTests {

    @Test
    @DisplayName("should provide string representation")
    void shouldProvideStringRepresentation() {
      // Given
      TableIndexInfo info1 = new TableIndexInfo("users", "email_index");
      TableIndexInfo info2 = new TableIndexInfo("users", null);

      // When/Then
      assertThat(info1.toString())
          .isEqualTo("TableIndexInfo{tableName='users', indexName='email_index'}");
      assertThat(info2.toString()).isEqualTo("TableIndexInfo{tableName='users', indexName='null'}");
    }
  }
}
