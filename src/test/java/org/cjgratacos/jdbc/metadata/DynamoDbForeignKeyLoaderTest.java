package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class DynamoDbForeignKeyLoaderTest {

  private DynamoDbClient mockClient;
  private DynamoDbForeignKeyLoader loader;

  @BeforeEach
  void setUp() {
    mockClient = mock(DynamoDbClient.class);
    loader = new DynamoDbForeignKeyLoader(mockClient);
  }

  @Test
  void testLoadForeignKeys() throws Exception {
    // Mock table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("ForeignKeys").build())
                .build());

    // Mock scan results
    Map<String, AttributeValue> item1 = new HashMap<>();
    item1.put("constraintName", AttributeValue.builder().s("FK_Orders_Users").build());
    item1.put("foreignTable", AttributeValue.builder().s("Orders").build());
    item1.put("foreignColumn", AttributeValue.builder().s("customerId").build());
    item1.put("primaryTable", AttributeValue.builder().s("Users").build());
    item1.put("primaryColumn", AttributeValue.builder().s("userId").build());
    item1.put("updateRule", AttributeValue.builder().s("CASCADE").build());
    item1.put("deleteRule", AttributeValue.builder().s("RESTRICT").build());

    Map<String, AttributeValue> item2 = new HashMap<>();
    item2.put("constraintName", AttributeValue.builder().s("FK_OrderItems_Orders").build());
    item2.put("foreignTable", AttributeValue.builder().s("OrderItems").build());
    item2.put("foreignColumn", AttributeValue.builder().s("orderId").build());
    item2.put("primaryTable", AttributeValue.builder().s("Orders").build());
    item2.put("primaryColumn", AttributeValue.builder().s("orderId").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(item1, item2)).build());

    List<ForeignKeyMetadata> foreignKeys = loader.load("ForeignKeys");

    assertThat(foreignKeys).hasSize(2);

    ForeignKeyMetadata fk1 = foreignKeys.get(0);
    assertThat(fk1.getConstraintName()).isEqualTo("FK_Orders_Users");
    assertThat(fk1.getForeignTable()).isEqualTo("Orders");
    assertThat(fk1.getForeignColumn()).isEqualTo("customerId");
    assertThat(fk1.getPrimaryTable()).isEqualTo("Users");
    assertThat(fk1.getPrimaryColumn()).isEqualTo("userId");
    assertThat(fk1.getUpdateRule()).isEqualTo(DatabaseMetaData.importedKeyCascade);
    assertThat(fk1.getDeleteRule()).isEqualTo(DatabaseMetaData.importedKeyRestrict);
  }

  @Test
  void testLoadWithPagination() throws Exception {
    // Mock table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("ForeignKeys").build())
                .build());

    // First page
    Map<String, AttributeValue> item1 =
        createForeignKeyItem("FK1", "Table1", "col1", "Table2", "col2");
    Map<String, AttributeValue> lastKey = new HashMap<>();
    lastKey.put("constraintName", AttributeValue.builder().s("FK1").build());

    // Second page
    Map<String, AttributeValue> item2 =
        createForeignKeyItem("FK2", "Table3", "col3", "Table4", "col4");

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(Arrays.asList(item1)).lastEvaluatedKey(lastKey).build(),
            ScanResponse.builder().items(Arrays.asList(item2)).build());

    List<ForeignKeyMetadata> foreignKeys = loader.load("ForeignKeys");

    assertThat(foreignKeys).hasSize(2);
    assertThat(foreignKeys.stream().map(ForeignKeyMetadata::getConstraintName))
        .containsExactly("FK1", "FK2");
  }

  @Test
  void testIsValidSource() {
    // Table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("ForeignKeys").build())
                .build());

    assertThat(loader.isValidSource("ForeignKeys")).isTrue();
  }

  @Test
  void testIsValidSourceTableNotFound() {
    // Table doesn't exist
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    assertThat(loader.isValidSource("NonExistentTable")).isFalse();
  }

  @Test
  void testInvalidSource() {
    assertThat(loader.isValidSource(null)).isFalse();
    assertThat(loader.isValidSource("")).isFalse();
  }

  @Test
  void testLoadFromNonExistentTable() {
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());

    assertThrows(
        ForeignKeyLoadException.class,
        () -> {
          loader.load("NonExistentTable");
        });
  }

  @Test
  void testIncompleteItemSkipped() throws Exception {
    // Mock table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("ForeignKeys").build())
                .build());

    // Create incomplete item (missing required fields)
    Map<String, AttributeValue> incompleteItem = new HashMap<>();
    incompleteItem.put("constraintName", AttributeValue.builder().s("FK_Incomplete").build());
    incompleteItem.put("foreignTable", AttributeValue.builder().s("Orders").build());
    // Missing foreignColumn, primaryTable, primaryColumn

    // Create valid item
    Map<String, AttributeValue> validItem =
        createForeignKeyItem("FK_Valid", "Orders", "customerId", "Users", "userId");

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(incompleteItem, validItem)).build());

    List<ForeignKeyMetadata> foreignKeys = loader.load("ForeignKeys");

    // Only valid item should be loaded
    assertThat(foreignKeys).hasSize(1);
    assertThat(foreignKeys.get(0).getConstraintName()).isEqualTo("FK_Valid");
  }

  @Test
  void testOptionalFields() throws Exception {
    // Mock table exists
    when(mockClient.describeTable(any(java.util.function.Consumer.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("ForeignKeys").build())
                .build());

    Map<String, AttributeValue> item =
        createForeignKeyItem("FK1", "Orders", "customerId", "Users", "userId");
    item.put("keySeq", AttributeValue.builder().n("2").build());
    item.put("primaryCatalog", AttributeValue.builder().s("catalog1").build());
    item.put("primarySchema", AttributeValue.builder().s("schema1").build());
    item.put("foreignCatalog", AttributeValue.builder().s("catalog2").build());
    item.put("foreignSchema", AttributeValue.builder().s("schema2").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(item)).build());

    List<ForeignKeyMetadata> foreignKeys = loader.load("ForeignKeys");

    assertThat(foreignKeys).hasSize(1);
    ForeignKeyMetadata fk = foreignKeys.get(0);
    assertThat(fk.getKeySeq()).isEqualTo(2);
    assertThat(fk.getPrimaryCatalog()).isEqualTo("catalog1");
    assertThat(fk.getPrimarySchema()).isEqualTo("schema1");
    assertThat(fk.getForeignCatalog()).isEqualTo("catalog2");
    assertThat(fk.getForeignSchema()).isEqualTo("schema2");
  }

  private Map<String, AttributeValue> createForeignKeyItem(
      String name,
      String foreignTable,
      String foreignColumn,
      String primaryTable,
      String primaryColumn) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("constraintName", AttributeValue.builder().s(name).build());
    item.put("foreignTable", AttributeValue.builder().s(foreignTable).build());
    item.put("foreignColumn", AttributeValue.builder().s(foreignColumn).build());
    item.put("primaryTable", AttributeValue.builder().s(primaryTable).build());
    item.put("primaryColumn", AttributeValue.builder().s(primaryColumn).build());
    return item;
  }
}
