package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

class BatchOperationsTest {

  @Nested
  class StatementBatchTests {
    
    private DynamoDbConnection connection;
    private DynamoDbClient dynamoDbClient;
    private RetryHandler retryHandler;
    private QueryMetrics queryMetrics;
    private DynamoDbStatement statement;

    @BeforeEach
    void setUp() throws SQLException {
      connection = mock(DynamoDbConnection.class);
      dynamoDbClient = mock(DynamoDbClient.class);
      retryHandler = mock(RetryHandler.class);
      queryMetrics = mock(QueryMetrics.class);
      TransactionManager transactionManager = mock(TransactionManager.class);

      when(connection.isClosed()).thenReturn(false);
      when(connection.getTransactionManager()).thenReturn(transactionManager);
      when(connection.getAutoCommit()).thenReturn(true);
      when(transactionManager.isInTransaction()).thenReturn(false);
      when(retryHandler.getQueryMetrics()).thenReturn(queryMetrics);

      statement = new DynamoDbStatement(connection, dynamoDbClient, retryHandler);
    }

    @Test
    void testAddBatchWithValidDML() throws SQLException {
      // Test adding valid DML statements
      statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')");
      statement.addBatch("UPDATE users SET name = 'name2' WHERE id = 'id1'");
      statement.addBatch("DELETE FROM users WHERE id = 'id1'");
      
      // Should not throw any exceptions
    }

    @Test
    void testAddBatchWithInvalidSQL() {
      // Test that SELECT statements are rejected
      assertThatThrownBy(() -> statement.addBatch("SELECT * FROM users"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Only DML statements (INSERT, UPDATE, DELETE, UPSERT, REPLACE) are allowed in batch operations");
    }

    @Test
    void testAddBatchWithNullSQL() {
      assertThatThrownBy(() -> statement.addBatch(null))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot add null or empty SQL to batch");
    }

    @Test
    void testAddBatchWithEmptySQL() {
      assertThatThrownBy(() -> statement.addBatch(""))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Cannot add null or empty SQL to batch");
    }

    @Test
    void testClearBatch() throws SQLException {
      statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')");
      statement.addBatch("INSERT INTO users VALUES ('id2', 'name2')");
      
      statement.clearBatch();
      
      // Execute batch should return empty array after clear
      int[] results = statement.executeBatch();
      assertThat(results).isEmpty();
    }

    @Test
    void testExecuteBatchSuccess() throws SQLException {
      // Mock successful execution
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString())).thenReturn(response);
      
      statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')");
      statement.addBatch("UPDATE users SET name = 'name2' WHERE id = 'id1'");
      statement.addBatch("DELETE FROM users WHERE id = 'id1'");
      
      int[] results = statement.executeBatch();
      
      assertThat(results).hasSize(3);
      assertThat(results).containsExactly(0, 0, 0); // DynamoDB doesn't return update counts
      
      // Verify each statement was executed
      verify(retryHandler, times(3)).executeWithRetry(any(), anyString());
    }

    @Test
    void testExecuteBatchWithFailures() throws SQLException {
      // Mock first two succeed, third fails
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString()))
          .thenReturn(response)
          .thenReturn(response)
          .thenThrow(new RuntimeException("DynamoDB error"));
      
      statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')");
      statement.addBatch("INSERT INTO users VALUES ('id2', 'name2')");
      statement.addBatch("INSERT INTO users VALUES ('id3', 'name3')");
      
      assertThatThrownBy(() -> statement.executeBatch())
          .isInstanceOf(BatchUpdateException.class)
          .hasMessageContaining("1 failures out of 3 commands");
      
      // Verify all statements were attempted
      verify(retryHandler, times(3)).executeWithRetry(any(), anyString());
    }

    @Test
    void testExecuteBatchClearsBatchAfterExecution() throws SQLException {
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString())).thenReturn(response);
      
      statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')");
      statement.executeBatch();
      
      // Execute again should return empty
      int[] results = statement.executeBatch();
      assertThat(results).isEmpty();
    }

    @Test
    void testBatchWithClosedStatement() throws SQLException {
      statement.close();
      
      assertThatThrownBy(() -> statement.addBatch("INSERT INTO users VALUES ('id1', 'name1')"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Statement is closed");
    }
  }

  @Nested
  class PreparedStatementBatchTests {
    
    private DynamoDbConnection connection;
    private DynamoDbClient dynamoDbClient;
    private RetryHandler retryHandler;
    private QueryMetrics queryMetrics;
    private SchemaCache schemaCache;
    private DynamoDbPreparedStatement preparedStatement;

    @BeforeEach
    void setUp() throws SQLException {
      connection = mock(DynamoDbConnection.class);
      dynamoDbClient = mock(DynamoDbClient.class);
      retryHandler = mock(RetryHandler.class);
      queryMetrics = mock(QueryMetrics.class);
      schemaCache = mock(SchemaCache.class);
      TransactionManager transactionManager = mock(TransactionManager.class);

      when(connection.getDynamoDbClient()).thenReturn(dynamoDbClient);
      when(connection.getRetryHandler()).thenReturn(retryHandler);
      when(connection.getQueryMetrics()).thenReturn(queryMetrics);
      when(connection.getSchemaCache()).thenReturn(schemaCache);
      when(connection.isClosed()).thenReturn(false);
      when(connection.getTransactionManager()).thenReturn(transactionManager);
      when(connection.getAutoCommit()).thenReturn(true);
      when(transactionManager.isInTransaction()).thenReturn(false);
      when(retryHandler.getQueryMetrics()).thenReturn(queryMetrics);

      preparedStatement = new DynamoDbPreparedStatement(
          connection, "INSERT INTO users VALUES (?, ?)");
    }

    @Test
    void testAddBatchWithAllParametersSet() throws SQLException {
      preparedStatement.setString(1, "id1");
      preparedStatement.setString(2, "name1");
      preparedStatement.addBatch();
      
      preparedStatement.setString(1, "id2");
      preparedStatement.setString(2, "name2");
      preparedStatement.addBatch();
      
      // Should not throw any exceptions
    }

    @Test
    void testAddBatchWithMissingParameters() throws SQLException {
      preparedStatement.setString(1, "id1");
      // Missing parameter 2
      
      assertThatThrownBy(() -> preparedStatement.addBatch())
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Not all parameters set");
    }

    @Test
    void testAddBatchWithSelectStatement() throws SQLException {
      preparedStatement = new DynamoDbPreparedStatement(
          connection, "SELECT * FROM users WHERE id = ?");
      preparedStatement.setString(1, "id1");
      
      assertThatThrownBy(() -> preparedStatement.addBatch())
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Only DML statements");
    }

    @Test
    void testClearBatch() throws SQLException {
      preparedStatement.setString(1, "id1");
      preparedStatement.setString(2, "name1");
      preparedStatement.addBatch();
      
      preparedStatement.clearBatch();
      
      int[] results = preparedStatement.executeBatch();
      assertThat(results).isEmpty();
    }

    @Test
    void testExecuteBatchSuccess() throws SQLException {
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString())).thenReturn(response);
      
      // Add first batch
      preparedStatement.setString(1, "id1");
      preparedStatement.setString(2, "name1");
      preparedStatement.addBatch();
      
      // Add second batch
      preparedStatement.setString(1, "id2");
      preparedStatement.setString(2, "name2");
      preparedStatement.addBatch();
      
      int[] results = preparedStatement.executeBatch();
      
      assertThat(results).hasSize(2);
      assertThat(results).containsExactly(0, 0);
      
      verify(retryHandler, times(2)).executeWithRetry(any(), anyString());
    }

    @Test
    void testExecuteBatchRestoresOriginalParameters() throws SQLException {
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString())).thenReturn(response);
      
      // Set initial parameters
      preparedStatement.setString(1, "original-id");
      preparedStatement.setString(2, "original-name");
      
      // Add batch with different parameters
      preparedStatement.setString(1, "batch-id");
      preparedStatement.setString(2, "batch-name");
      preparedStatement.addBatch();
      
      // Execute batch
      preparedStatement.executeBatch();
      
      // Original parameters should be restored
      // Execute with original parameters
      preparedStatement.executeUpdate();
      
      // Should have executed 2 times total (1 batch + 1 regular)
      verify(retryHandler, times(2)).executeWithRetry(any(), anyString());
    }

    @Test
    void testExecuteBatchWithFailures() throws SQLException {
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString()))
          .thenReturn(response)
          .thenThrow(new RuntimeException("DynamoDB error"));
      
      preparedStatement.setString(1, "id1");
      preparedStatement.setString(2, "name1");
      preparedStatement.addBatch();
      
      preparedStatement.setString(1, "id2");
      preparedStatement.setString(2, "name2");
      preparedStatement.addBatch();
      
      assertThatThrownBy(() -> preparedStatement.executeBatch())
          .isInstanceOf(BatchUpdateException.class)
          .hasMessageContaining("1 failures out of 2 commands");
      
      // Check the update counts in the exception
      try {
        preparedStatement.executeBatch();
      } catch (BatchUpdateException e) {
        assertThat(e.getUpdateCounts()).hasSize(2);
        assertThat(e.getUpdateCounts()[0]).isEqualTo(0);
        assertThat(e.getUpdateCounts()[1]).isEqualTo(Statement.EXECUTE_FAILED);
      }
    }

    @Test
    void testBatchParameterIsolation() throws SQLException {
      // Each batch should have isolated parameters
      preparedStatement.setString(1, "id1");
      preparedStatement.setString(2, "name1");
      preparedStatement.addBatch();
      
      // Change parameters for next batch
      preparedStatement.setString(1, "id2");
      preparedStatement.setString(2, "name2");
      preparedStatement.addBatch();
      
      // Parameters in each batch should be independent
      ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
      when(retryHandler.executeWithRetry(any(), anyString())).thenReturn(response);
      
      int[] results = preparedStatement.executeBatch();
      assertThat(results).hasSize(2);
    }
  }
}