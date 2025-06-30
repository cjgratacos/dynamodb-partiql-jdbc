package org.cjgratacos.jdbc;

// TODO: This integration test is ready but needs to be enabled after fixing BaseIntegrationTest
// import
// The test validates foreign key functionality with a real DynamoDB instance
// Uncomment when ready to run integration tests

/*
@Testcontainers
class ForeignKeyIntegrationTest extends BaseIntegrationTest {

  private Connection connection;
  private DatabaseMetaData metadata;

  @Override
  protected void onSetup() throws SQLException {
    super.onSetup();
    try {
      createTestTables();
    } catch (Exception e) {
      throw new SQLException("Failed to create test tables", e);
    }

    // Create connection with foreign key definitions
    Properties props = new Properties(connectionProperties);
    props.setProperty("foreignKey.FK_Orders_Users", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK_OrderItems_Orders", "OrderItems.orderId->Orders.orderId");
    props.setProperty(
        "foreignKey.FK_OrderItems_Products", "OrderItems.productId->Products.productId");

    // Append foreign key properties to URL
    StringBuilder urlBuilder = new StringBuilder(jdbcUrl);
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith("foreignKey.")) {
        urlBuilder.append(";").append(key).append("=").append(props.getProperty(key));
      }
    }

    connection = java.sql.DriverManager.getConnection(urlBuilder.toString());
    metadata = connection.getMetaData();
  }

  @Override
  protected void onTeardown() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
      cleanupTestTables();
    } catch (Exception e) {
      logger.error("Error during teardown", e);
    }
    super.onTeardown();
  }

  @Test
  void testGetImportedKeys() throws Exception {
    // Test imported keys for Orders table
    try (ResultSet rs = metadata.getImportedKeys(null, null, "Orders")) {
      assertThat(rs.next()).isTrue();

      // Orders imports from Users
      assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Users");
      assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("userId");
      assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("Orders");
      assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("customerId");
      assertThat(rs.getString("FK_NAME")).isEqualTo("FK_Orders_Users");
      assertThat(rs.getInt("KEY_SEQ")).isEqualTo(1);

      assertThat(rs.next()).isFalse();
    }

    // Test imported keys for OrderItems table
    try (ResultSet rs = metadata.getImportedKeys(null, null, "OrderItems")) {
      int count = 0;
      while (rs.next()) {
        count++;
        String pkTable = rs.getString("PKTABLE_NAME");
        if ("Orders".equals(pkTable)) {
          assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("orderId");
          assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("orderId");
          assertThat(rs.getString("FK_NAME")).isEqualTo("FK_OrderItems_Orders");
        } else if ("Products".equals(pkTable)) {
          assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("productId");
          assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("productId");
          assertThat(rs.getString("FK_NAME")).isEqualTo("FK_OrderItems_Products");
        }
      }
      assertThat(count).isEqualTo(2);
    }
  }

  @Test
  void testGetExportedKeys() throws Exception {
    // Test exported keys for Users table
    try (ResultSet rs = metadata.getExportedKeys(null, null, "Users")) {
      assertThat(rs.next()).isTrue();

      // Users exports to Orders
      assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Users");
      assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("userId");
      assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("Orders");
      assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("customerId");
      assertThat(rs.getString("FK_NAME")).isEqualTo("FK_Orders_Users");

      assertThat(rs.next()).isFalse();
    }

    // Test exported keys for Orders table
    try (ResultSet rs = metadata.getExportedKeys(null, null, "Orders")) {
      assertThat(rs.next()).isTrue();

      // Orders exports to OrderItems
      assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Orders");
      assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("orderId");
      assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("OrderItems");
      assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("orderId");

      assertThat(rs.next()).isFalse();
    }
  }

  @Test
  void testGetCrossReference() throws Exception {
    // Test cross reference between Users and Orders
    try (ResultSet rs = metadata.getCrossReference(null, null, "Users", null, null, "Orders")) {
      assertThat(rs.next()).isTrue();

      assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Users");
      assertThat(rs.getString("PKCOLUMN_NAME")).isEqualTo("userId");
      assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("Orders");
      assertThat(rs.getString("FKCOLUMN_NAME")).isEqualTo("customerId");
      assertThat(rs.getString("FK_NAME")).isEqualTo("FK_Orders_Users");

      assertThat(rs.next()).isFalse();
    }

    // Test cross reference between Orders and OrderItems
    try (ResultSet rs =
        metadata.getCrossReference(null, null, "Orders", null, null, "OrderItems")) {
      assertThat(rs.next()).isTrue();

      assertThat(rs.getString("PKTABLE_NAME")).isEqualTo("Orders");
      assertThat(rs.getString("FKTABLE_NAME")).isEqualTo("OrderItems");

      assertThat(rs.next()).isFalse();
    }

    // Test no cross reference between unrelated tables
    try (ResultSet rs = metadata.getCrossReference(null, null, "Users", null, null, "Products")) {
      assertThat(rs.next()).isFalse();
    }
  }

  @Test
  void testForeignKeyMetadataColumns() throws Exception {
    // Verify all required columns are present
    try (ResultSet rs = metadata.getImportedKeys(null, null, "Orders")) {
      assertThat(rs.next()).isTrue();

      // Check all required columns exist
      assertThat(rs.getObject("PKTABLE_CAT")).isNull();
      assertThat(rs.getObject("PKTABLE_SCHEM")).isNull();
      assertThat(rs.getString("PKTABLE_NAME")).isNotNull();
      assertThat(rs.getString("PKCOLUMN_NAME")).isNotNull();
      assertThat(rs.getObject("FKTABLE_CAT")).isNull();
      assertThat(rs.getObject("FKTABLE_SCHEM")).isNull();
      assertThat(rs.getString("FKTABLE_NAME")).isNotNull();
      assertThat(rs.getString("FKCOLUMN_NAME")).isNotNull();
      assertThat(rs.getInt("KEY_SEQ")).isEqualTo(1);
      assertThat(rs.getInt("UPDATE_RULE")).isEqualTo(DatabaseMetaData.importedKeyNoAction);
      assertThat(rs.getInt("DELETE_RULE")).isEqualTo(DatabaseMetaData.importedKeyNoAction);
      assertThat(rs.getString("FK_NAME")).isNotNull();
      assertThat(rs.getObject("PK_NAME")).isNull();
      assertThat(rs.getInt("DEFERRABILITY")).isEqualTo(DatabaseMetaData.importedKeyNotDeferrable);
    }
  }

  private void createTestTables() throws Exception {
    // Create test tables using the container
    createTestTables("Users", "Products", "Orders", "OrderItems");
  }

  private void cleanupTestTables() throws Exception {
    // Tables will be cleaned up automatically by the test container
  }
}
*/
