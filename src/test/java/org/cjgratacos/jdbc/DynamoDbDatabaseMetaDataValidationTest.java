package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class DynamoDbDatabaseMetaDataValidationTest {

  @Test
  void testConstructorWithValidationDisabled() throws SQLException {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "false");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    // Should not throw even if tables don't exist because validation is disabled
    DynamoDbDatabaseMetaData metadata = new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    
    assertThat(metadata).isNotNull();
  }

  @Test
  void testConstructorWithValidationLenientMode() throws SQLException {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    // Mock table doesn't exist
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "lenient");
    props.setProperty("foreignKey.FK1", "Orders.customerId->NonExistentTable.id");
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    // Should not throw in lenient mode
    DynamoDbDatabaseMetaData metadata = new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    
    assertThat(metadata).isNotNull();
  }

  @Test
  void testConstructorWithValidationStrictMode() {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    // Mock table doesn't exist
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("Table not found").build());
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "strict");
    props.setProperty("foreignKey.FK1", "Orders.customerId->NonExistentTable.id");
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    // Should throw in strict mode
    SQLException exception = assertThrows(SQLException.class, () -> {
      new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    });
    
    assertThat(exception.getMessage()).contains("Failed to register foreign key");
  }

  @Test
  void testConstructorWithValidForeignKeys() throws SQLException {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    // Mock tables exist - return different table name based on request
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(invocation -> {
          // Just return a successful response for any table
          return DescribeTableResponse.builder()
              .table(TableDescription.builder()
                  .tableName("MockedTable")
                  .build())
              .build();
        });
    
    // Mock scan returns items with required columns
    when(mockClient.scan(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
            .items(java.util.Arrays.asList(
                java.util.Map.of(
                    "customerId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("123").build(),
                    "userId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("456").build()
                )
            ))
            .build());
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "strict");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    // Should not throw when foreign keys are valid
    DynamoDbDatabaseMetaData metadata = new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    
    assertThat(metadata).isNotNull();
  }

  @Test
  void testConstructorWithCacheEnabled() throws SQLException {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    // Mock tables exist
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(invocation -> {
          return DescribeTableResponse.builder()
              .table(TableDescription.builder()
                  .tableName("MockedTable")
                  .build())
              .build();
        });
    
    // Mock scan returns items with required columns
    when(mockClient.scan(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
            .items(java.util.Arrays.asList(
                java.util.Map.of(
                    "customerId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("123").build(),
                    "userId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("456").build()
                )
            ))
            .build());
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("cacheTableMetadata", "true");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK2", "Orders.customerId->Users.userId"); // Duplicate
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    // With caching enabled, table existence should be checked only once per table
    DynamoDbDatabaseMetaData metadata = new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    
    assertThat(metadata).isNotNull();
  }

  @Test
  void testGetImportedKeysWithValidation() throws SQLException {
    // Setup
    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    DynamoDbConnection mockConnection = mock(DynamoDbConnection.class);
    when(mockConnection.getDynamoDbClient()).thenReturn(mockClient);
    
    // Mock tables exist and have columns
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(invocation -> {
          return DescribeTableResponse.builder()
              .table(TableDescription.builder()
                  .tableName("MockedTable")
                  .keySchema(software.amazon.awssdk.services.dynamodb.model.KeySchemaElement.builder()
                      .attributeName("id")
                      .keyType(software.amazon.awssdk.services.dynamodb.model.KeyType.HASH)
                      .build())
                  .build())
              .build();
        });
    
    when(mockClient.scan(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
            .items(java.util.Arrays.asList(
                java.util.Map.of(
                    "customerId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("123").build(),
                    "userId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("456").build()
                )
            ))
            .build());
    
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    
    SchemaCache mockSchemaCache = mock(SchemaCache.class);
    
    DynamoDbDatabaseMetaData metadata = new DynamoDbDatabaseMetaData(mockConnection, props, mockSchemaCache);
    
    // Get imported keys
    java.sql.ResultSet rs = metadata.getImportedKeys(null, null, "Orders");
    
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString("FK_NAME")).isEqualTo("FK1");
    assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Users");
    assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("userId");
    assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("Orders");
    assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("customerId");
  }
}