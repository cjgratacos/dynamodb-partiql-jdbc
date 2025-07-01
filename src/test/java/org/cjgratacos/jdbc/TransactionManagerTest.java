package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.core.exception.SdkServiceException;

@DisplayName("TransactionManager Tests")
class TransactionManagerTest {
    
    private DynamoDbClient mockClient;
    private TransactionManager transactionManager;
    
    @BeforeEach
    void setUp() {
        mockClient = mock(DynamoDbClient.class);
        transactionManager = new TransactionManager(mockClient);
    }
    
    @Test
    @DisplayName("Should begin transaction successfully")
    void testBeginTransaction() throws SQLException {
        assertThat(transactionManager.isInTransaction()).isFalse();
        
        transactionManager.beginTransaction();
        
        assertThat(transactionManager.isInTransaction()).isTrue();
        assertThat(transactionManager.getTransactionSize()).isEqualTo(0);
    }
    
    @Test
    @DisplayName("Should throw exception when beginning transaction while already in transaction")
    void testBeginTransactionWhileInTransaction() throws SQLException {
        transactionManager.beginTransaction();
        
        assertThatThrownBy(() -> transactionManager.beginTransaction())
            .isInstanceOf(SQLException.class)
            .hasMessage("Transaction already in progress");
    }
    
    @Test
    @DisplayName("Should add Put operation to transaction")
    void testAddPut() throws SQLException {
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        item.put("name", AttributeValue.builder().s("Test Item").build());
        
        transactionManager.addPut("TestTable", item);
        
        assertThat(transactionManager.getTransactionSize()).isEqualTo(1);
    }
    
    @Test
    @DisplayName("Should add Update operation to transaction")
    void testAddUpdate() throws SQLException {
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("123").build());
        
        Map<String, String> attributeNames = new HashMap<>();
        attributeNames.put("#n", "name");
        
        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.put(":val", AttributeValue.builder().s("Updated Name").build());
        
        transactionManager.addUpdate("TestTable", key, "SET #n = :val", attributeNames, attributeValues);
        
        assertThat(transactionManager.getTransactionSize()).isEqualTo(1);
    }
    
    @Test
    @DisplayName("Should add Delete operation to transaction")
    void testAddDelete() throws SQLException {
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("123").build());
        
        transactionManager.addDelete("TestTable", key);
        
        assertThat(transactionManager.getTransactionSize()).isEqualTo(1);
    }
    
    @Test
    @DisplayName("Should throw exception when adding operation without transaction")
    void testAddOperationWithoutTransaction() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        
        assertThatThrownBy(() -> transactionManager.addPut("TestTable", item))
            .isInstanceOf(SQLException.class)
            .hasMessage("Not in a transaction. Call beginTransaction() first.");
    }
    
    @Test
    @DisplayName("Should throw exception when transaction size exceeds limit")
    void testTransactionSizeLimit() throws SQLException {
        transactionManager.beginTransaction();
        
        // Add 100 operations (the maximum)
        for (int i = 0; i < 100; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s("id" + i).build());
            transactionManager.addPut("TestTable", item);
        }
        
        // Try to add one more
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("id100").build());
        
        assertThatThrownBy(() -> transactionManager.addPut("TestTable", item))
            .isInstanceOf(SQLException.class)
            .hasMessage("Transaction size limit reached. Maximum 100 items allowed per transaction.");
    }
    
    @Test
    @DisplayName("Should commit transaction successfully")
    void testCommitTransaction() throws SQLException {
        when(mockClient.transactWriteItems((TransactWriteItemsRequest) any()))
            .thenReturn(TransactWriteItemsResponse.builder().build());
        
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        transactionManager.addPut("TestTable", item);
        
        transactionManager.commit();
        
        assertThat(transactionManager.isInTransaction()).isFalse();
        assertThat(transactionManager.getTransactionSize()).isEqualTo(0);
        
        ArgumentCaptor<TransactWriteItemsRequest> requestCaptor = 
            ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(mockClient).transactWriteItems(requestCaptor.capture());
        
        TransactWriteItemsRequest request = requestCaptor.getValue();
        assertThat(request.transactItems()).hasSize(1);
    }
    
    @Test
    @DisplayName("Should handle empty transaction commit")
    void testCommitEmptyTransaction() throws SQLException {
        transactionManager.beginTransaction();
        transactionManager.commit();
        
        assertThat(transactionManager.isInTransaction()).isFalse();
        verify(mockClient, never()).transactWriteItems((TransactWriteItemsRequest) any());
    }
    
    @Test
    @DisplayName("Should handle transaction cancellation")
    void testTransactionCancellation() throws SQLException {
        TransactionCanceledException exception = TransactionCanceledException.builder()
                .message("Transaction cancelled")
                .build();
        when(mockClient.transactWriteItems((TransactWriteItemsRequest) any()))
            .thenThrow(exception);
        
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        transactionManager.addPut("TestTable", item);
        
        assertThatThrownBy(() -> transactionManager.commit())
            .isInstanceOf(SQLException.class)
            .hasMessageContaining("Transaction cancelled");
        
        assertThat(transactionManager.isInTransaction()).isFalse();
    }
    
    @Test
    @DisplayName("Should rollback transaction successfully")
    void testRollbackTransaction() throws SQLException {
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        transactionManager.addPut("TestTable", item);
        
        assertThat(transactionManager.getTransactionSize()).isEqualTo(1);
        
        transactionManager.rollback();
        
        assertThat(transactionManager.isInTransaction()).isFalse();
        assertThat(transactionManager.getTransactionSize()).isEqualTo(0);
        verify(mockClient, never()).transactWriteItems((TransactWriteItemsRequest) any());
    }
    
    @Test
    @DisplayName("Should throw exception when committing without transaction")
    void testCommitWithoutTransaction() {
        assertThatThrownBy(() -> transactionManager.commit())
            .isInstanceOf(SQLException.class)
            .hasMessage("Not in a transaction. Call beginTransaction() first.");
    }
    
    @Test
    @DisplayName("Should throw exception when rolling back without transaction")
    void testRollbackWithoutTransaction() {
        assertThatThrownBy(() -> transactionManager.rollback())
            .isInstanceOf(SQLException.class)
            .hasMessage("Not in a transaction. Call beginTransaction() first.");
    }
    
    @Test
    @DisplayName("Should handle DynamoDB exceptions during commit")
    void testCommitWithDynamoDbException() throws SQLException {
        SdkServiceException exception = SdkServiceException.builder()
                .message("Service error")
                .build();
        when(mockClient.transactWriteItems((TransactWriteItemsRequest) any()))
            .thenThrow(exception);
        
        transactionManager.beginTransaction();
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        transactionManager.addPut("TestTable", item);
        
        assertThatThrownBy(() -> transactionManager.commit())
            .isInstanceOf(SQLException.class)
            .hasMessageContaining("Unexpected error during transaction commit");
        
        assertThat(transactionManager.isInTransaction()).isFalse();
    }
}