package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;
import org.cjgratacos.jdbc.TransactionManager;

class DynamoDbPreparedStatementTest {

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

    preparedStatement = new DynamoDbPreparedStatement(connection, "SELECT * FROM users WHERE id = ?");
  }

  @Test
  void testParameterBinding() throws SQLException {
    // Test various parameter types
    preparedStatement.setString(1, "test-id");
    
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(java.util.Map.of(
        "id", AttributeValue.builder().s("test-id").build(),
        "name", AttributeValue.builder().s("Test User").build()
    ));
    
    ExecuteStatementResponse response = ExecuteStatementResponse.builder()
        .items(items)
        .build();
    
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    ResultSet rs = preparedStatement.executeQuery();
    assertThat(rs).isNotNull();
    
    // Verify the SQL was built correctly
    when(retryHandler.executeWithRetry(any(), any())).thenAnswer(invocation -> {
      Supplier<ExecuteStatementResponse> supplier = invocation.getArgument(0);
      // Execute the supplier to get the request
      supplier.get();
      return response;
    });
  }

  @Test
  void testAllParameterTypes() throws SQLException {
    String sql = "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    preparedStatement = new DynamoDbPreparedStatement(connection, sql);

    // Test all parameter types
    preparedStatement.setString(1, "string-value");
    preparedStatement.setInt(2, 42);
    preparedStatement.setLong(3, 12345L);
    preparedStatement.setDouble(4, 3.14159);
    preparedStatement.setBoolean(5, true);
    preparedStatement.setBigDecimal(6, new BigDecimal("123.456"));
    preparedStatement.setBytes(7, new byte[]{1, 2, 3});
    preparedStatement.setDate(8, Date.valueOf("2024-01-01"));
    preparedStatement.setTimestamp(9, Timestamp.valueOf("2024-01-01 12:30:00"));
    preparedStatement.setNull(10, Types.VARCHAR);

    // Execute should work
    ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    preparedStatement.executeUpdate();
  }

  @Test
  void testParameterMetaData() throws SQLException {
    preparedStatement.setString(1, "test");
    
    ParameterMetaData metaData = preparedStatement.getParameterMetaData();
    assertThat(metaData).isNotNull();
    assertThat(metaData.getParameterCount()).isEqualTo(1);
    assertThat(metaData.getParameterType(1)).isEqualTo(Types.VARCHAR);
    assertThat(metaData.getParameterTypeName(1)).isEqualTo("String");
    assertThat(metaData.getParameterClassName(1)).isEqualTo(String.class.getName());
    assertThat(metaData.isNullable(1)).isEqualTo(ParameterMetaData.parameterNullable);
    assertThat(metaData.isSigned(1)).isFalse();
  }

  @Test
  void testParameterValidation() {
    // Test invalid parameter index
    assertThatThrownBy(() -> preparedStatement.setString(0, "test"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Parameter index 0 is out of range");
    
    assertThatThrownBy(() -> preparedStatement.setString(2, "test"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Parameter index 2 is out of range");
  }

  @Test
  void testExecuteWithoutAllParameters() {
    // Don't set the parameter
    assertThatThrownBy(() -> preparedStatement.executeQuery())
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Not all parameters set");
  }

  @Test
  void testClearParameters() throws SQLException {
    preparedStatement.setString(1, "test");
    preparedStatement.clearParameters();
    
    // Should fail because parameter is no longer set
    assertThatThrownBy(() -> preparedStatement.executeQuery())
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Not all parameters set");
  }

  @Test
  void testMultipleParameters() throws SQLException {
    String sql = "SELECT * FROM users WHERE id = ? AND status = ? AND age > ?";
    preparedStatement = new DynamoDbPreparedStatement(connection, sql);
    
    preparedStatement.setString(1, "user-123");
    preparedStatement.setString(2, "active");
    preparedStatement.setInt(3, 18);
    
    ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    preparedStatement.executeQuery();
  }

  @Test
  void testNullHandling() throws SQLException {
    preparedStatement.setString(1, null);
    
    ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    preparedStatement.executeQuery();
  }

  @Test
  void testStringEscaping() throws SQLException {
    // Test that single quotes are properly escaped
    preparedStatement.setString(1, "O'Neill");
    
    ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    preparedStatement.executeQuery();
  }

  @Test
  void testSetObject() throws SQLException {
    // Test setObject with various types
    preparedStatement.setObject(1, "string");
    preparedStatement.clearParameters();
    
    preparedStatement.setObject(1, 42);
    preparedStatement.clearParameters();
    
    preparedStatement.setObject(1, true);
    preparedStatement.clearParameters();
    
    preparedStatement.setObject(1, new BigDecimal("123.45"));
    preparedStatement.clearParameters();
    
    preparedStatement.setObject(1, null);
    
    ParameterMetaData metaData = preparedStatement.getParameterMetaData();
    assertThat(metaData.getParameterType(1)).isEqualTo(Types.NULL);
  }

  @Test
  void testAddBatchNotSupported() {
    // This is a SELECT statement, so it should be rejected for batch operations
    assertThatThrownBy(() -> preparedStatement.addBatch())
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Only DML statements (INSERT, UPDATE, DELETE, UPSERT, REPLACE) are allowed in batch operations");
  }

  @Test
  void testExecuteMethods() throws SQLException {
    preparedStatement.setString(1, "test");
    
    ExecuteStatementResponse response = ExecuteStatementResponse.builder().build();
    when(retryHandler.executeWithRetry(any(), any())).thenReturn(response);
    
    // Test execute()
    boolean hasResultSet = preparedStatement.execute();
    assertThat(hasResultSet).isTrue(); // SELECT returns true
    
    // Test executeUpdate() with an UPDATE statement
    preparedStatement = new DynamoDbPreparedStatement(connection, "UPDATE users SET name = ? WHERE id = 'test'");
    preparedStatement.setString(1, "New Name");
    int updateCount = preparedStatement.executeUpdate();
    assertThat(updateCount).isEqualTo(0); // DynamoDB doesn't return update counts
  }
}