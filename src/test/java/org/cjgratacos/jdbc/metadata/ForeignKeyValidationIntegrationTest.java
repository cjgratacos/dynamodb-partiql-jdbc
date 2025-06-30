package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.cjgratacos.jdbc.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class ForeignKeyValidationIntegrationTest extends BaseIntegrationTest {

  @Override
  protected void onSetup() throws SQLException {
    super.onSetup();

    // Create test tables
    createTable("Users", "userId", ScalarAttributeType.S);
    createTable("Orders", "orderId", ScalarAttributeType.S);
    createTable("OrderItems", "itemId", ScalarAttributeType.S);
  }

  @Test
  void testValidationDisabledByDefault() throws Exception {
    // By default, validation should be disabled
    Properties props = new Properties();
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK2", "NonExistentTable.col->Users.userId");

    String url = jdbcUrl + toPropertiesString(props);
    try (Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData meta = conn.getMetaData();

      // Should include both foreign keys even though one references non-existent table
      try (ResultSet rs = meta.getImportedKeys(null, null, "Orders")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("FK_NAME")).isEqualTo("FK1");
      }
    }
  }

  @Test
  void testValidationLenientMode() throws Exception {
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "lenient");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK2", "Orders.sellerId->NonExistentTable.id");

    String url = jdbcUrl + toPropertiesString(props);
    try (Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData meta = conn.getMetaData();

      // In lenient mode, both foreign keys should be registered
      // but validation errors should be logged
      try (ResultSet rs = meta.getImportedKeys(null, null, "Orders")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
    }
  }

  @Test
  void testValidationStrictMode() throws Exception {
    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "strict");
    props.setProperty("foreignKey.FK1", "Orders.customerId->NonExistentTable.id");

    String url = jdbcUrl + toPropertiesString(props);

    // In strict mode, connection should fail if foreign keys are invalid
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> {
              try (Connection conn = DriverManager.getConnection(url)) {
                // Force metadata initialization
                conn.getMetaData();
              }
            });

    assertThat(exception.getMessage()).contains("Failed to register foreign key");
  }

  @Test
  void testValidationWithValidForeignKeys() throws Exception {
    // First, add some sample data to ensure columns exist
    putItem("Users", "userId", "user1", "name", "John Doe");
    putItem("Orders", "orderId", "order1", "customerId", "user1");
    putItem("OrderItems", "itemId", "item1", "orderId", "order1");

    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeyValidationMode", "strict");
    props.setProperty("foreignKey.FK_Orders_Users", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK_OrderItems_Orders", "OrderItems.orderId->Orders.orderId");

    String url = jdbcUrl + toPropertiesString(props);
    try (Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData meta = conn.getMetaData();

      // Should successfully connect and retrieve foreign keys
      try (ResultSet rs = meta.getImportedKeys(null, null, "Orders")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("FK_NAME")).isEqualTo("FK_Orders_Users");
        assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Users");
        assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("userId");
        assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("Orders");
        assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("customerId");
      }
    }
  }

  @Test
  void testValidationWithCaching() throws Exception {
    // Add sample data
    putItem("Users", "userId", "user1");
    putItem("Orders", "orderId", "order1", "customerId", "user1");

    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("cacheTableMetadata", "true");
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK2", "Orders.customerId->Users.userId"); // Duplicate reference

    String url = jdbcUrl + toPropertiesString(props);
    try (Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData meta = conn.getMetaData();

      // Should use cache for second validation of same tables
      try (ResultSet rs = meta.getImportedKeys(null, null, "Orders")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
    }
  }

  @Test
  void testValidationFromFile() throws Exception {
    // Create a properties file with foreign keys
    java.io.File tempFile = java.io.File.createTempFile("foreign-keys", ".properties");
    tempFile.deleteOnExit();

    try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
      writer.write("foreignKey.FK1=Orders.customerId->Users.userId\n");
      writer.write("foreignKey.FK2=OrderItems.orderId->Orders.orderId\n");
    }

    // Add sample data
    putItem("Users", "userId", "user1");
    putItem("Orders", "orderId", "order1", "customerId", "user1");
    putItem("OrderItems", "itemId", "item1", "orderId", "order1");

    Properties props = new Properties();
    props.setProperty("validateForeignKeys", "true");
    props.setProperty("foreignKeysFile", tempFile.getAbsolutePath());

    String url = jdbcUrl + toPropertiesString(props);
    try (Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData meta = conn.getMetaData();

      // Should load and validate foreign keys from file
      try (ResultSet rs = meta.getImportedKeys(null, null, "Orders")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("FK_NAME")).isEqualTo("FK1");
      }

      try (ResultSet rs = meta.getImportedKeys(null, null, "OrderItems")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("FK_NAME")).isEqualTo("FK2");
      }
    }
  }

  private void createTable(String tableName, String keyAttribute, ScalarAttributeType keyType) {
    testContainer
        .getDynamoDbClient()
        .createTable(
            CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(keyAttribute)
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName(keyAttribute)
                        .attributeType(keyType)
                        .build())
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build());
  }

  private void putItem(
      String tableName, String keyAttribute, String keyValue, String... attributes) {
    java.util.Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item =
        new java.util.HashMap<>();
    item.put(
        keyAttribute,
        software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
            .s(keyValue)
            .build());

    for (int i = 0; i < attributes.length; i += 2) {
      item.put(
          attributes[i],
          software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
              .s(attributes[i + 1])
              .build());
    }

    testContainer.getDynamoDbClient().putItem(builder -> builder.tableName(tableName).item(item));
  }

  private String toPropertiesString(Properties props) {
    StringBuilder sb = new StringBuilder();
    for (String key : props.stringPropertyNames()) {
      sb.append(";").append(key).append("=").append(props.getProperty(key));
    }
    return sb.toString();
  }
}
