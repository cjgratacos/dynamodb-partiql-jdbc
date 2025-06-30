package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * End-to-end test for foreign key functionality including parsing, validation, and registration.
 */
class ForeignKeyEndToEndTest {

  @TempDir File tempDir;

  private DynamoDbClient mockClient;

  @BeforeEach
  void setUp() {
    mockClient = mock(DynamoDbClient.class);
  }

  @Test
  void testCompleteFlowWithAllSources() throws Exception {
    // Setup mock responses for validation
    setupMockTablesExist("Users", "Orders", "OrderItems", "Products", "ForeignKeyConfig");
    setupMockColumnsExist();

    // 1. Create properties file with foreign keys
    File propsFile = new File(tempDir, "foreign-keys.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("foreignKey.FK_FromFile=Products.categoryId->Categories.categoryId\n");
    }

    // 2. Setup DynamoDB table with foreign keys
    Map<String, AttributeValue> fkItem = new HashMap<>();
    fkItem.put("constraintName", AttributeValue.builder().s("FK_FromTable").build());
    fkItem.put("foreignTable", AttributeValue.builder().s("OrderItems").build());
    fkItem.put("foreignColumn", AttributeValue.builder().s("productId").build());
    fkItem.put("primaryTable", AttributeValue.builder().s("Products").build());
    fkItem.put("primaryColumn", AttributeValue.builder().s("productId").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(fkItem)).build());

    // 3. Create properties with all configuration sources
    Properties props = new Properties();
    props.setProperty("foreignKey.FK_Inline", "Orders.customerId->Users.userId");
    props.setProperty("foreignKeysFile", propsFile.getAbsolutePath());
    props.setProperty("foreignKeysTable", "ForeignKeyConfig");
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "lenient");

    // 4. Parse all foreign keys
    ForeignKeyParser parser = new ForeignKeyParser(mockClient, true);
    List<ForeignKeyMetadata> foreignKeys = parser.parseFromProperties(props);

    // Should have 3 foreign keys from 3 sources
    assertThat(foreignKeys).hasSize(3);
    assertThat(foreignKeys.stream().map(ForeignKeyMetadata::getConstraintName))
        .containsExactlyInAnyOrder("FK_Inline", "FK_FromFile", "FK_FromTable");

    // All should be validated
    assertThat(foreignKeys).allMatch(ForeignKeyMetadata::isValidated);

    // 5. Register in registry
    ForeignKeyRegistry registry = new ForeignKeyRegistry();
    registry.registerForeignKeys(foreignKeys);

    // 6. Verify registry queries work
    assertThat(registry.getImportedKeys("Orders")).hasSize(1);
    assertThat(registry.getImportedKeys("Products")).hasSize(1);
    assertThat(registry.getImportedKeys("OrderItems")).hasSize(1);

    assertThat(registry.getExportedKeys("Users")).hasSize(1);
    assertThat(registry.getExportedKeys("Categories")).hasSize(1);
    assertThat(registry.getExportedKeys("Products")).hasSize(1);
  }

  @Test
  void testValidationModes() throws Exception {
    setupMockTablesExist("Users", "Orders");
    
    // Setup table-specific column mocks
    Map<String, AttributeValue> usersItem = new HashMap<>();
    usersItem.put("userId", AttributeValue.builder().s("user1").build());
    usersItem.put("name", AttributeValue.builder().s("John").build());
    
    Map<String, AttributeValue> ordersItem = new HashMap<>();
    ordersItem.put("orderId", AttributeValue.builder().s("order1").build());
    ordersItem.put("customerId", AttributeValue.builder().s("user1").build());
    
    when(mockClient.scan(any(ScanRequest.class)))
        .thenAnswer(invocation -> {
          ScanRequest request = invocation.getArgument(0);
          String tableName = request.tableName();
          if ("Users".equals(tableName)) {
            return ScanResponse.builder().items(Arrays.asList(usersItem)).build();
          } else if ("Orders".equals(tableName)) {
            return ScanResponse.builder().items(Arrays.asList(ordersItem)).build();
          }
          return ScanResponse.builder().items(Arrays.asList()).build();
        });

    Properties props = new Properties();
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");

    // Test with validation off - parser created with validateOnParse=false
    props.setProperty("validateForeignKeys", "false");
    ForeignKeyParser parser1 = new ForeignKeyParser(mockClient, false);
    List<ForeignKeyMetadata> fks1 = parser1.parseFromProperties(props);
    assertThat(fks1).hasSize(1);
    assertThat(fks1.get(0).isValidated()).isFalse();

    // Test with validation on
    props.setProperty("validateForeignKeys", "true");
    ForeignKeyParser parser2 = new ForeignKeyParser(mockClient, true);
    List<ForeignKeyMetadata> fks2 = parser2.parseFromProperties(props);
    assertThat(fks2).hasSize(1);
    assertThat(fks2.get(0).isValidated()).isTrue();
    assertThat(fks2.get(0).hasValidationErrors()).isFalse();
  }

  @Test
  void testCircularReferenceDetection() throws Exception {
    setupMockTablesExist("Table1", "Table2", "Table3");
    
    // Setup table-specific columns for circular reference
    Map<String, AttributeValue> table1Item = new HashMap<>();
    table1Item.put("col1", AttributeValue.builder().s("val1").build());
    
    Map<String, AttributeValue> table2Item = new HashMap<>();
    table2Item.put("col2", AttributeValue.builder().s("val2").build());
    
    Map<String, AttributeValue> table3Item = new HashMap<>();
    table3Item.put("col3", AttributeValue.builder().s("val3").build());
    
    when(mockClient.scan(any(ScanRequest.class)))
        .thenAnswer(invocation -> {
          ScanRequest request = invocation.getArgument(0);
          String tableName = request.tableName();
          if ("Table1".equals(tableName)) {
            return ScanResponse.builder().items(Arrays.asList(table1Item)).build();
          } else if ("Table2".equals(tableName)) {
            return ScanResponse.builder().items(Arrays.asList(table2Item)).build();
          } else if ("Table3".equals(tableName)) {
            return ScanResponse.builder().items(Arrays.asList(table3Item)).build();
          }
          return ScanResponse.builder().items(Arrays.asList()).build();
        });

    Properties props = new Properties();
    props.setProperty("foreignKey.FK1", "Table1.col1->Table2.col2");
    props.setProperty("foreignKey.FK2", "Table2.col2->Table3.col3");
    props.setProperty("foreignKey.FK3", "Table3.col3->Table1.col1");

    ForeignKeyParser parser = new ForeignKeyParser(mockClient, true);
    List<ForeignKeyMetadata> foreignKeys = parser.parseFromProperties(props);

    // All should be validated with circular reference warnings
    assertThat(foreignKeys).hasSize(3);
    assertThat(foreignKeys).allMatch(ForeignKeyMetadata::isValidated);
    assertThat(foreignKeys).allMatch(ForeignKeyMetadata::hasValidationErrors);
    assertThat(foreignKeys.get(0).getValidationErrors())
        .anyMatch(error -> error.contains("circular reference"));
  }

  @Test
  void testDuplicateConstraintHandling() throws Exception {
    setupMockTablesExist("Users", "Orders");
    setupMockColumnsExist();

    Properties props = new Properties();
    props.setProperty("foreignKey.FK_Dup", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK_Dup", "Orders.sellerId->Users.userId"); // Duplicate name

    ForeignKeyParser parser = new ForeignKeyParser(mockClient);
    List<ForeignKeyMetadata> foreignKeys = parser.parseFromProperties(props);

    // Properties with same key overwrite, so only one should exist
    assertThat(foreignKeys).hasSize(1);
    assertThat(foreignKeys.get(0).getForeignColumn()).isEqualTo("sellerId");
  }

  @Test
  void testParseRuleMapping() throws Exception {
    File propsFile = new File(tempDir, "rules.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("fk.1.name=FK_WithRules\n");
      writer.write("fk.1.foreign.table=Orders\n");
      writer.write("fk.1.foreign.column=customerId\n");
      writer.write("fk.1.primary.table=Users\n");
      writer.write("fk.1.primary.column=userId\n");
      writer.write("fk.1.updateRule=CASCADE\n");
      writer.write("fk.1.deleteRule=SET_NULL\n");
    }

    PropertiesFileForeignKeyLoader loader = new PropertiesFileForeignKeyLoader();
    List<ForeignKeyMetadata> foreignKeys = loader.load(propsFile.getAbsolutePath());

    assertThat(foreignKeys).hasSize(1);
    ForeignKeyMetadata fk = foreignKeys.get(0);
    assertThat(fk.getUpdateRule()).isEqualTo(DatabaseMetaData.importedKeyCascade);
    assertThat(fk.getDeleteRule()).isEqualTo(DatabaseMetaData.importedKeySetNull);
  }

  private void setupMockTablesExist(String... tableNames) {
    for (String tableName : tableNames) {
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenReturn(
              DescribeTableResponse.builder()
                  .table(TableDescription.builder().tableName(tableName).build())
                  .build());
    }
  }

  private void setupMockColumnsExist() {
    // Mock scan returns items with common columns
    Map<String, AttributeValue> sampleItem = new HashMap<>();
    sampleItem.put("id", AttributeValue.builder().s("123").build());
    sampleItem.put("userId", AttributeValue.builder().s("user1").build());
    sampleItem.put("customerId", AttributeValue.builder().s("cust1").build());
    sampleItem.put("orderId", AttributeValue.builder().s("order1").build());
    sampleItem.put("productId", AttributeValue.builder().s("prod1").build());
    sampleItem.put("categoryId", AttributeValue.builder().s("cat1").build());
    sampleItem.put("col1", AttributeValue.builder().s("val1").build());
    sampleItem.put("col2", AttributeValue.builder().s("val2").build());
    sampleItem.put("col3", AttributeValue.builder().s("val3").build());
    sampleItem.put("sellerId", AttributeValue.builder().s("seller1").build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(Arrays.asList(sampleItem)).build());
  }
}