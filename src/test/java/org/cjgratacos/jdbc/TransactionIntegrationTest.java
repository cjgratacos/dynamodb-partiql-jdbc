package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@DisplayName("Transaction Integration Tests")
class TransactionIntegrationTest extends BaseIntegrationTest {

  @Override
  protected void onSetup() throws SQLException {
    // Create test table
    try {
      testContainer.getDynamoDbClient().createTable(
          CreateTableRequest.builder()
              .tableName("TransactionTestTable")
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
          DeleteTableRequest.builder().tableName("TransactionTestTable").build());
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  @DisplayName("Should support basic transaction commit")
  void testBasicTransactionCommit() throws SQLException {
    try (Connection conn = getConnection()) {
      // Disable auto-commit to start a transaction
      conn.setAutoCommit(false);
      
      try (Statement stmt = conn.createStatement()) {
        // Insert multiple items in transaction
        stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'tx1', 'name': 'Item 1'}");
        stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'tx2', 'name': 'Item 2'}");
        stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'tx3', 'name': 'Item 3'}");
        
        // Commit the transaction
        conn.commit();
        
        // Verify all items were committed
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id IN ('tx1', 'tx2', 'tx3')")) {
          int count = 0;
          while (rs.next()) {
            count++;
            assertThat(rs.getString("id")).isIn("tx1", "tx2", "tx3");
          }
          assertThat(count).isEqualTo(3);
        }
      }
    }
  }

  @Test
  @DisplayName("Should support transaction rollback")
  void testTransactionRollback() throws SQLException {
    try (Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      
      try (Statement stmt = conn.createStatement()) {
        // Insert items in transaction
        stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'rollback1', 'name': 'Should not exist'}");
        stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'rollback2', 'name': 'Should not exist'}");
        
        // Rollback the transaction
        conn.rollback();
        
        // Verify items were not inserted
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id IN ('rollback1', 'rollback2')")) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  @DisplayName("Should handle mixed DML operations in transaction")
  void testMixedDMLOperationsInTransaction() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      // First insert a record to update/delete
      stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'existing', 'name': 'Original', 'value': 100}");
      
      // Start transaction
      conn.setAutoCommit(false);
      
      // Perform mixed operations
      stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'new_tx', 'name': 'New Item'}");
      stmt.executeUpdate("UPDATE TransactionTestTable SET name = 'Updated' WHERE id = 'existing'");
      stmt.executeUpdate("DELETE FROM TransactionTestTable WHERE id = 'to_delete'"); // Won't fail even if doesn't exist
      
      // Commit
      conn.commit();
      
      // Verify the operations
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id = 'existing'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Updated");
      }
      
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id = 'new_tx'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("New Item");
      }
    }
  }

  @Test
  @DisplayName("Should auto-commit when switching from manual to auto-commit mode")
  void testAutoCommitSwitch() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      // Start transaction
      conn.setAutoCommit(false);
      
      // Insert item in transaction
      stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'auto_switch', 'name': 'Should be committed'}");
      
      // Switch to auto-commit (should commit pending transaction)
      conn.setAutoCommit(true);
      
      // Verify item was committed
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id = 'auto_switch'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Should be committed");
      }
    }
  }

  @Test
  @DisplayName("Should throw exception when trying to commit in auto-commit mode")
  void testCommitInAutoCommitMode() throws SQLException {
    try (Connection conn = getConnection()) {
      assertThat(conn.getAutoCommit()).isTrue();
      
      assertThatThrownBy(() -> conn.commit())
          .isInstanceOf(SQLException.class)
          .hasMessage("Connection is in auto-commit mode");
    }
  }

  @Test
  @DisplayName("Should throw exception when trying to rollback in auto-commit mode")
  void testRollbackInAutoCommitMode() throws SQLException {
    try (Connection conn = getConnection()) {
      assertThat(conn.getAutoCommit()).isTrue();
      
      assertThatThrownBy(() -> conn.rollback())
          .isInstanceOf(SQLException.class)
          .hasMessage("Connection is in auto-commit mode");
    }
  }

  @Test
  @DisplayName("Should handle prepared statements in transactions")
  void testPreparedStatementsInTransaction() throws SQLException {
    try (Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      
      try (PreparedStatement pstmt = conn.prepareStatement(
              "INSERT INTO TransactionTestTable VALUE {'id': ?, 'name': ?, 'count': ?}")) {
        
        // Add multiple items
        for (int i = 1; i <= 5; i++) {
          pstmt.setString(1, "prep_tx_" + i);
          pstmt.setString(2, "Prepared Item " + i);
          pstmt.setInt(3, i * 100);
          pstmt.executeUpdate();
        }
        
        // Commit transaction
        conn.commit();
        
        // Verify all items were inserted
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id IN ('prep_tx_1', 'prep_tx_2', 'prep_tx_3', 'prep_tx_4', 'prep_tx_5')")) {
          
          int count = 0;
          while (rs.next()) {
            count++;
            assertThat(rs.getString("id")).startsWith("prep_tx_");
            assertThat(rs.getString("name")).startsWith("Prepared Item");
          }
          assertThat(count).isEqualTo(5);
        }
      }
    }
  }

  @Test
  @DisplayName("Should handle transaction size limit")
  void testTransactionSizeLimit() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      conn.setAutoCommit(false);
      
      // Try to add more than 100 operations (DynamoDB limit)
      assertThatThrownBy(() -> {
        for (int i = 0; i < 101; i++) {
          stmt.executeUpdate(String.format(
              "INSERT INTO TransactionTestTable VALUE {'id': 'limit_%d', 'name': 'Item %d'}", i, i));
        }
      })
      .isInstanceOf(SQLException.class)
      .hasMessageContaining("Transaction size limit");
    }
  }

  @Test
  @DisplayName("Should start new transaction after commit when auto-commit is false")
  void testNewTransactionAfterCommit() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      
      conn.setAutoCommit(false);
      
      // First transaction
      stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'first_tx', 'name': 'First'}");
      conn.commit();
      
      // Second transaction (should start automatically)
      stmt.executeUpdate("INSERT INTO TransactionTestTable VALUE {'id': 'second_tx', 'name': 'Second'}");
      conn.commit();
      
      // Verify both items exist
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM TransactionTestTable WHERE id IN ('first_tx', 'second_tx')")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
    }
  }

  @Test
  @DisplayName("Should handle transaction with no operations")
  void testEmptyTransaction() throws SQLException {
    try (Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      
      // Commit empty transaction - should not throw
      conn.commit();
      
      // Rollback empty transaction - should not throw
      conn.rollback();
    }
  }
}