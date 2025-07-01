package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@DisplayName("Batch Operations Integration Tests")
class BatchOperationsIntegrationTest extends BaseIntegrationTest {

  @Override
  protected void onSetup() throws SQLException {
    // Create test table
    try {
      testContainer.getDynamoDbClient().createTable(
          CreateTableRequest.builder()
              .tableName("BatchTestTable")
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("id")
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .keySchema(
                  KeySchemaElement.builder()
                      .attributeName("id")
                      .keyType(KeyType.HASH)
                      .build())
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(5L)
                      .writeCapacityUnits(5L)
                      .build())
              .build());
    } catch (Exception e) {
      // Table might already exist
      logger.warn("Failed to create table: {}", e.getMessage());
    }
  }

  @Override
  protected void onTeardown() {
    try {
      testContainer.getDynamoDbClient().deleteTable(
          DeleteTableRequest.builder().tableName("BatchTestTable").build());
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  @DisplayName("Should execute batch inserts with Statement")
  void testStatementBatchInserts() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      // Add multiple INSERT statements to batch
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'batch1', 'name': 'Item 1', 'value': 100}");
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'batch2', 'name': 'Item 2', 'value': 200}");
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'batch3', 'name': 'Item 3', 'value': 300}");
      
      // Execute batch
      int[] results = stmt.executeBatch();
      
      assertThat(results).hasSize(3);
      assertThat(results).containsExactly(0, 0, 0); // DynamoDB doesn't return update counts
      
      // Verify the data was inserted
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id IN ('batch1', 'batch2', 'batch3')")) {
        // Collect all results since order is not guaranteed
        java.util.Set<String> ids = new java.util.HashSet<>();
        java.util.Set<String> names = new java.util.HashSet<>();
        
        while (rs.next()) {
          ids.add(rs.getString("id"));
          names.add(rs.getString("name"));
        }
        
        assertThat(ids).containsExactlyInAnyOrder("batch1", "batch2", "batch3");
        assertThat(names).containsExactlyInAnyOrder("Item 1", "Item 2", "Item 3");
      }
    }
  }

  @Test
  @DisplayName("Should execute batch inserts with PreparedStatement")
  void testPreparedStatementBatchInserts() throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement pstmt = conn.prepareStatement(
             "INSERT INTO BatchTestTable VALUE {'id': ?, 'name': ?, 'value': ?}")) {
      
      // Add first batch
      pstmt.setString(1, "prep1");
      pstmt.setString(2, "Prepared Item 1");
      pstmt.setInt(3, 1000);
      pstmt.addBatch();
      
      // Add second batch
      pstmt.setString(1, "prep2");
      pstmt.setString(2, "Prepared Item 2");
      pstmt.setInt(3, 2000);
      pstmt.addBatch();
      
      // Add third batch
      pstmt.setString(1, "prep3");
      pstmt.setString(2, "Prepared Item 3");
      pstmt.setInt(3, 3000);
      pstmt.addBatch();
      
      // Execute batch
      int[] results = pstmt.executeBatch();
      
      assertThat(results).hasSize(3);
      assertThat(results).containsExactly(0, 0, 0);
      
      // Verify the data
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id IN ('prep1', 'prep2', 'prep3')")) {
        
        // Collect all results since order is not guaranteed
        java.util.Map<String, Integer> valueMap = new java.util.HashMap<>();
        
        while (rs.next()) {
          valueMap.put(rs.getString("id"), rs.getInt("value"));
        }
        
        assertThat(valueMap).hasSize(3);
        assertThat(valueMap.get("prep1")).isEqualTo(1000);
        assertThat(valueMap.get("prep2")).isEqualTo(2000);
        assertThat(valueMap.get("prep3")).isEqualTo(3000);
      }
    }
  }

  @Test
  @DisplayName("Should handle mixed DML operations in batch")
  void testBatchWithMixedOperations() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      // First insert some data
      stmt.executeUpdate("INSERT INTO BatchTestTable VALUE {'id': 'update1', 'name': 'Original', 'value': 1}");
      stmt.executeUpdate("INSERT INTO BatchTestTable VALUE {'id': 'delete1', 'name': 'To Delete', 'value': 2}");
      
      // Now create a batch with mixed operations
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'new1', 'name': 'New Item', 'value': 10}");
      stmt.addBatch("UPDATE BatchTestTable SET name = 'Updated' WHERE id = 'update1'");
      stmt.addBatch("DELETE FROM BatchTestTable WHERE id = 'delete1'");
      
      int[] results = stmt.executeBatch();
      assertThat(results).hasSize(3);
      
      // Verify the operations
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id = 'update1'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Updated");
      }
      
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id = 'delete1'")) {
        assertThat(rs.next()).isFalse(); // Should be deleted
      }
      
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id = 'new1'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("New Item");
      }
    }
  }

  @Test
  @DisplayName("Should handle partial batch failures correctly")
  void testBatchWithPartialFailure() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      // Insert an item first
      stmt.executeUpdate("INSERT INTO BatchTestTable VALUE {'id': 'existing', 'name': 'Existing', 'value': 999}");
      
      // Create batch with duplicate key that will fail
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'batch_ok1', 'name': 'OK 1', 'value': 1}");
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'existing', 'name': 'Duplicate', 'value': 2}"); // Will fail
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'batch_ok2', 'name': 'OK 2', 'value': 3}");
      
      // Execute batch - should throw BatchUpdateException
      assertThatThrownBy(() -> stmt.executeBatch())
          .isInstanceOf(BatchUpdateException.class)
          .hasMessageContaining("1 failures out of 3 commands");
      
      // Verify partial success - first and third should succeed
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id IN ('batch_ok1', 'batch_ok2')")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("id")).isEqualTo("batch_ok1");
        
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("id")).isEqualTo("batch_ok2");
        
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  @DisplayName("Should clear batch commands properly")
  void testClearBatch() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'clear1', 'name': 'Item 1'}");
      stmt.addBatch("INSERT INTO BatchTestTable VALUE {'id': 'clear2', 'name': 'Item 2'}");
      
      // Clear the batch
      stmt.clearBatch();
      
      // Execute should return empty array
      int[] results = stmt.executeBatch();
      assertThat(results).isEmpty();
      
      // Verify no data was inserted
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id IN ('clear1', 'clear2')")) {
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  @DisplayName("Should maintain parameter isolation between batches")
  void testPreparedStatementBatchParameterIsolation() throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement pstmt = conn.prepareStatement(
             "INSERT INTO BatchTestTable VALUE {'id': ?, 'name': ?, 'counter': ?}")) {
      
      // Use same parameter object but change values
      for (int i = 1; i <= 5; i++) {
        pstmt.setString(1, "iso" + i);
        pstmt.setString(2, "Isolation Test " + i);
        pstmt.setInt(3, i * 100);
        pstmt.addBatch();
      }
      
      int[] results = pstmt.executeBatch();
      assertThat(results).hasSize(5);
      
      // Verify each batch maintained its own parameters
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTestTable WHERE id IN ('iso1', 'iso2', 'iso3', 'iso4', 'iso5')")) {
        
        // Collect all results since order is not guaranteed
        java.util.Map<String, String> nameMap = new java.util.HashMap<>();
        java.util.Map<String, Integer> counterMap = new java.util.HashMap<>();
        
        while (rs.next()) {
          String id = rs.getString("id");
          nameMap.put(id, rs.getString("name"));
          counterMap.put(id, rs.getInt("counter"));
        }
        
        assertThat(nameMap).hasSize(5);
        for (int i = 1; i <= 5; i++) {
          String id = "iso" + i;
          assertThat(nameMap.get(id)).isEqualTo("Isolation Test " + i);
          assertThat(counterMap.get(id)).isEqualTo(i * 100);
        }
      }
    }
  }
}