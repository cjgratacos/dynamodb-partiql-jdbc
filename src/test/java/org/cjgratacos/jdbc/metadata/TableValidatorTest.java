package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class TableValidatorTest {

  private DynamoDbClient mockClient;
  private TableValidator validator;

  @BeforeEach
  void setUp() {
    mockClient = mock(DynamoDbClient.class);
    validator = new TableValidator(mockClient);
  }

  @Test
  void testTableExists() {
    // Mock successful describe table
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("TestTable").build())
                .build());

    assertThat(validator.tableExists("TestTable")).isTrue();
  }

  @Test
  void testTableDoesNotExist() {
    // Mock table not found
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    assertThat(validator.tableExists("NonExistentTable")).isFalse();
  }

  @Test
  void testTableExistsWithNullInput() {
    assertThat(validator.tableExists(null)).isFalse();
    assertThat(validator.tableExists("")).isFalse();
  }

  @Test
  void testValidateTableExistsThrowsException() {
    // Mock table not found
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    ForeignKeyValidationException exception =
        assertThrows(
            ForeignKeyValidationException.class,
            () -> validator.validateTableExists("NonExistentTable"));

    assertThat(exception.getMessage())
        .isEqualTo("Table 'NonExistentTable' does not exist in DynamoDB");
  }

  @Test
  void testValidateTablesExistBulk() {
    // Mock table1 exists, table2 doesn't
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("Table1").build())
                .build())
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    Map<String, Boolean> results = validator.validateTablesExist(Arrays.asList("Table1", "Table2"));

    assertThat(results).hasSize(2);
    assertThat(results.get("Table1")).isTrue();
    assertThat(results.get("Table2")).isFalse();
  }

  @Test
  void testColumnExists() {
    // Mock table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(
                    TableDescription.builder()
                        .tableName("TestTable")
                        .keySchema(
                            KeySchemaElement.builder()
                                .attributeName("id")
                                .keyType(KeyType.HASH)
                                .build())
                        .build())
                .build());

    // Mock scan with items containing various attributes
    Map<String, AttributeValue> item1 = new HashMap<>();
    item1.put("id", AttributeValue.builder().s("123").build());
    item1.put("name", AttributeValue.builder().s("John").build());
    item1.put("age", AttributeValue.builder().n("30").build());

    Map<String, AttributeValue> item2 = new HashMap<>();
    item2.put("id", AttributeValue.builder().s("124").build());
    item2.put("name", AttributeValue.builder().s("Jane").build());
    item2.put("email", AttributeValue.builder().s("jane@example.com").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(item1, item2)).build());

    assertThat(validator.columnExists("TestTable", "id")).isTrue();
    assertThat(validator.columnExists("TestTable", "name")).isTrue();
    assertThat(validator.columnExists("TestTable", "age")).isTrue();
    assertThat(validator.columnExists("TestTable", "email")).isTrue();
    assertThat(validator.columnExists("TestTable", "nonexistent")).isFalse();
  }

  @Test
  void testColumnExistsTableNotFound() {
    // Mock table doesn't exist
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    assertThat(validator.columnExists("NonExistentTable", "anyColumn")).isFalse();
  }

  @Test
  void testGetTableColumns() {
    // Mock table with key schema
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(
                    TableDescription.builder()
                        .tableName("TestTable")
                        .keySchema(
                            Arrays.asList(
                                KeySchemaElement.builder()
                                    .attributeName("userId")
                                    .keyType(KeyType.HASH)
                                    .build(),
                                KeySchemaElement.builder()
                                    .attributeName("timestamp")
                                    .keyType(KeyType.RANGE)
                                    .build()))
                        .build())
                .build());

    // Mock scan with sample items
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("userId", AttributeValue.builder().s("123").build());
    item.put("timestamp", AttributeValue.builder().n("1234567890").build());
    item.put("status", AttributeValue.builder().s("active").build());
    item.put("data", AttributeValue.builder().s("sample").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(item)).build());

    List<String> columns = validator.getTableColumns("TestTable");

    assertThat(columns).contains("userId", "timestamp", "status", "data");
    // Verify primary key columns are included
    assertThat(columns).contains("userId", "timestamp");
  }

  @Test
  void testCaching() {
    // Enable caching with 1 minute expiration
    validator = new TableValidator(mockClient, true, 1);

    // Mock successful table exists check
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("TestTable").build())
                .build());

    // First call
    assertThat(validator.tableExists("TestTable")).isTrue();

    // Second call (should use cache)
    assertThat(validator.tableExists("TestTable")).isTrue();

    // Verify describeTable was only called once
    verify(mockClient, times(1)).describeTable(any(java.util.function.Consumer.class));
  }

  @Test
  void testClearCache() {
    // Enable caching
    validator = new TableValidator(mockClient, true, 1);

    // Mock successful table exists check
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("TestTable").build())
                .build());

    // First call
    assertThat(validator.tableExists("TestTable")).isTrue();

    // Clear cache
    validator.clearCache();

    // Second call (should not use cache)
    assertThat(validator.tableExists("TestTable")).isTrue();

    // Verify describeTable was called twice
    verify(mockClient, times(2)).describeTable(any(java.util.function.Consumer.class));
  }

  @Test
  void testClearTableCache() {
    // Enable caching
    validator = new TableValidator(mockClient, true, 1);

    // Mock successful table exists checks
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("TestTable").build())
                .build());

    // Cache results for two tables
    assertThat(validator.tableExists("Table1")).isTrue();
    assertThat(validator.tableExists("Table2")).isTrue();

    // Clear cache for Table1 only
    validator.clearTableCache("Table1");

    // Check both tables again
    assertThat(validator.tableExists("Table1")).isTrue(); // Should hit API
    assertThat(validator.tableExists("Table2")).isTrue(); // Should use cache

    // Verify describeTable was called 3 times (initial 2 + 1 for Table1 after cache clear)
    verify(mockClient, times(3)).describeTable(any(java.util.function.Consumer.class));
  }
}
