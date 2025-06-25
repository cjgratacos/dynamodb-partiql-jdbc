package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("CachedSchemaEntry Unit Tests")
class CachedSchemaEntryUnitTest {

  @Test
  @DisplayName("Can create cached entry with valid data")
  void canCreateCachedEntryWithValidData() {
    // Given: Schema data
    final var schemaData =
        Map.of("id", createColumnMetadata("id"), "name", createColumnMetadata("name"));
    final var ttlMs = 5000L;

    // When: Creating cached entry
    final var entry = new CachedSchemaEntry<>("test_table", schemaData, ttlMs);

    // Then: Entry should be valid
    assertThat(entry.isValid()).isTrue();
    assertThat(entry.getSchemaData()).isEqualTo(schemaData);
    assertThat(entry.getTableName()).isEqualTo("test_table");
    assertThat(entry.getTtlMs()).isEqualTo(ttlMs);
  }

  @Test
  @DisplayName("Entry becomes invalid after TTL expires")
  void entryBecomesInvalidAfterTtlExpires() throws InterruptedException {
    // Given: Entry with very short TTL
    final var schemaData = Map.of("id", createColumnMetadata("id"));
    final var entry = new CachedSchemaEntry<>("test_table", schemaData, 50L); // 50ms

    // When: Initially should be valid
    assertThat(entry.isValid()).isTrue();

    // Wait for expiration
    Thread.sleep(100);

    // Then: Should be invalid
    assertThat(entry.isValid()).isFalse();
  }

  @Test
  @DisplayName("Can update schema data")
  void canUpdateSchemaData() {
    // Given: Cached entry
    final var initialData = Map.of("id", createColumnMetadata("id"));
    final var entry = new CachedSchemaEntry<>("test_table", initialData, 10000L);

    // When: Updating with new data
    final var newData =
        Map.of("id", createColumnMetadata("id"), "name", createColumnMetadata("name"));
    final var updated = entry.updateSchemaData(newData);

    // Then: Should update successfully
    assertThat(updated).isTrue();
    assertThat(entry.getSchemaData()).isEqualTo(newData);
    assertThat(entry.getRefreshCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("Can invalidate entry")
  void canInvalidateEntry() {
    // Given: Valid entry
    final var entry =
        new CachedSchemaEntry<>("test_table", Map.of("id", createColumnMetadata("id")), 10000L);
    assertThat(entry.isValid()).isTrue();

    // When: Invalidating
    entry.invalidate();

    // Then: Should be invalid
    assertThat(entry.isValid()).isFalse();
    assertThat(entry.getSchemaData()).isNull();
  }

  @Test
  @DisplayName("Refresh coordination works")
  void refreshCoordinationWorks() {
    // Given: Cached entry
    final var entry =
        new CachedSchemaEntry<>("test_table", Map.of("id", createColumnMetadata("id")), 10000L);

    // When: Marking for refresh
    final var firstRefresh = entry.markRefreshing();
    final var secondRefresh = entry.markRefreshing();

    // Then: First should succeed, second should fail
    assertThat(firstRefresh).isTrue();
    assertThat(secondRefresh).isFalse();
    assertThat(entry.isRefreshing()).isTrue();

    // When: Marking complete
    entry.markRefreshComplete();

    // Then: Should no longer be refreshing
    assertThat(entry.isRefreshing()).isFalse();
  }

  @Test
  @DisplayName("Age calculation is accurate")
  void ageCalculationIsAccurate() throws InterruptedException {
    // Given: Cached entry
    final var entry =
        new CachedSchemaEntry<>("test_table", Map.of("id", createColumnMetadata("id")), 10000L);
    final var initialAge = entry.getAgeMs();

    // When: Waiting
    Thread.sleep(50);
    final var laterAge = entry.getAgeMs();

    // Then: Age should increase
    assertThat(laterAge).isGreaterThan(initialAge);
    assertThat(laterAge - initialAge).isGreaterThanOrEqualTo(40); // Allow some margin
  }

  @Test
  @DisplayName("Access time tracking works")
  void accessTimeTrackingWorks() throws InterruptedException {
    // Given: Cached entry
    final var entry =
        new CachedSchemaEntry<>("test_table", Map.of("id", createColumnMetadata("id")), 10000L);
    final var initialAccessTime = entry.getLastAccessTime();

    // When: Accessing data after delay
    Thread.sleep(50);
    entry.getSchemaData();
    final var newAccessTime = entry.getLastAccessTime();

    // Then: Access time should be updated
    assertThat(newAccessTime).isGreaterThan(initialAccessTime);
  }

  private ColumnMetadata createColumnMetadata(String columnName) {
    final var metadata = new ColumnMetadata("test_table", columnName);
    metadata.recordTypeObservation(java.sql.Types.VARCHAR, false);
    return metadata;
  }
}
