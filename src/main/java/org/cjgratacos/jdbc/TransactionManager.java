package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Manages transactions for DynamoDB using TransactWriteItems API.
 * 
 * <p>This class provides transaction support for the JDBC driver by collecting
 * DML operations and executing them atomically using DynamoDB's TransactWriteItems.
 * 
 * <p>Features:
 * <ul>
 *   <li>Support for INSERT (Put), UPDATE, and DELETE operations</li>
 *   <li>Atomic execution of up to 100 operations</li>
 *   <li>Automatic rollback on failure</li>
 *   <li>Thread-safe implementation</li>
 * </ul>
 * 
 * <p>Limitations:
 * <ul>
 *   <li>Maximum 100 operations per transaction</li>
 *   <li>No support for SELECT within transactions</li>
 *   <li>All operations must be on different items (no duplicate keys)</li>
 *   <li>Maximum 4MB total transaction size</li>
 * </ul>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class TransactionManager {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);
    private static final int MAX_TRANSACTION_ITEMS = 100;
    
    private final DynamoDbClient client;
    private final List<TransactWriteItem> transactionItems;
    private final AtomicBoolean inTransaction;
    private final Object lock = new Object();
    
    /**
     * Creates a new transaction manager.
     * 
     * @param client the DynamoDB client
     */
    public TransactionManager(DynamoDbClient client) {
        this.client = client;
        this.transactionItems = new ArrayList<>();
        this.inTransaction = new AtomicBoolean(false);
    }
    
    /**
     * Begins a new transaction.
     * 
     * @throws SQLException if a transaction is already in progress
     */
    public void beginTransaction() throws SQLException {
        synchronized (lock) {
            if (inTransaction.get()) {
                throw new SQLException("Transaction already in progress");
            }
            transactionItems.clear();
            inTransaction.set(true);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction started");
            }
        }
    }
    
    /**
     * Adds a Put operation to the transaction.
     * 
     * @param tableName the table name
     * @param item the item to put
     * @throws SQLException if not in a transaction or transaction is full
     */
    public void addPut(String tableName, java.util.Map<String, AttributeValue> item) 
            throws SQLException {
        synchronized (lock) {
            validateInTransaction();
            validateTransactionSize();
            
            Put put = Put.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            
            TransactWriteItem writeItem = TransactWriteItem.builder()
                    .put(put)
                    .build();
            
            transactionItems.add(writeItem);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Added PUT to transaction for table: {}", tableName);
            }
        }
    }
    
    /**
     * Adds an Update operation to the transaction.
     * 
     * @param tableName the table name
     * @param key the item key
     * @param updateExpression the update expression
     * @param expressionAttributeNames attribute name mappings
     * @param expressionAttributeValues attribute value mappings
     * @throws SQLException if not in a transaction or transaction is full
     */
    public void addUpdate(String tableName, 
                         java.util.Map<String, AttributeValue> key,
                         String updateExpression,
                         java.util.Map<String, String> expressionAttributeNames,
                         java.util.Map<String, AttributeValue> expressionAttributeValues) 
            throws SQLException {
        synchronized (lock) {
            validateInTransaction();
            validateTransactionSize();
            
            Update.Builder updateBuilder = Update.builder()
                    .tableName(tableName)
                    .key(key)
                    .updateExpression(updateExpression);
            
            if (expressionAttributeNames != null && !expressionAttributeNames.isEmpty()) {
                updateBuilder.expressionAttributeNames(expressionAttributeNames);
            }
            
            if (expressionAttributeValues != null && !expressionAttributeValues.isEmpty()) {
                updateBuilder.expressionAttributeValues(expressionAttributeValues);
            }
            
            TransactWriteItem writeItem = TransactWriteItem.builder()
                    .update(updateBuilder.build())
                    .build();
            
            transactionItems.add(writeItem);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Added UPDATE to transaction for table: {}", tableName);
            }
        }
    }
    
    /**
     * Adds a Delete operation to the transaction.
     * 
     * @param tableName the table name
     * @param key the item key to delete
     * @throws SQLException if not in a transaction or transaction is full
     */
    public void addDelete(String tableName, java.util.Map<String, AttributeValue> key) 
            throws SQLException {
        synchronized (lock) {
            validateInTransaction();
            validateTransactionSize();
            
            Delete delete = Delete.builder()
                    .tableName(tableName)
                    .key(key)
                    .build();
            
            TransactWriteItem writeItem = TransactWriteItem.builder()
                    .delete(delete)
                    .build();
            
            transactionItems.add(writeItem);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Added DELETE to transaction for table: {}", tableName);
            }
        }
    }
    
    /**
     * Commits the current transaction.
     * 
     * @throws SQLException if not in a transaction or commit fails
     */
    public void commit() throws SQLException {
        synchronized (lock) {
            validateInTransaction();
            
            if (transactionItems.isEmpty()) {
                // Nothing to commit
                inTransaction.set(false);
                if (logger.isDebugEnabled()) {
                    logger.debug("Empty transaction committed");
                }
                return;
            }
            
            try {
                TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                        .transactItems(transactionItems)
                        .build();
                
                if (logger.isInfoEnabled()) {
                    logger.info("Committing transaction with {} items", transactionItems.size());
                }
                
                client.transactWriteItems(request);
                
                if (logger.isInfoEnabled()) {
                    logger.info("Transaction committed successfully");
                }
                
                // Clear transaction state
                transactionItems.clear();
                inTransaction.set(false);
                
            } catch (TransactionCanceledException e) {
                // Transaction was cancelled - convert to SQLException
                transactionItems.clear();
                inTransaction.set(false);
                
                String message = "Transaction cancelled: " + e.getMessage();
                if (e.cancellationReasons() != null && !e.cancellationReasons().isEmpty()) {
                    message += " Reasons: " + e.cancellationReasons();
                }
                
                logger.error("Transaction cancelled", e);
                throw new SQLException(message, e);
                
            } catch (DynamoDbException e) {
                // Other DynamoDB error
                transactionItems.clear();
                inTransaction.set(false);
                
                logger.error("Transaction failed", e);
                throw new SQLException("Transaction failed: " + e.getMessage(), e);
                
            } catch (Exception e) {
                // Unexpected error
                transactionItems.clear();
                inTransaction.set(false);
                
                logger.error("Unexpected error during transaction commit", e);
                throw new SQLException("Unexpected error during transaction commit", e);
            }
        }
    }
    
    /**
     * Rolls back the current transaction.
     * 
     * @throws SQLException if not in a transaction
     */
    public void rollback() throws SQLException {
        synchronized (lock) {
            validateInTransaction();
            
            transactionItems.clear();
            inTransaction.set(false);
            
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rolled back");
            }
        }
    }
    
    /**
     * Checks if currently in a transaction.
     * 
     * @return true if in transaction, false otherwise
     */
    public boolean isInTransaction() {
        return inTransaction.get();
    }
    
    /**
     * Gets the current number of operations in the transaction.
     * 
     * @return the number of operations
     */
    public int getTransactionSize() {
        synchronized (lock) {
            return transactionItems.size();
        }
    }
    
    /**
     * Validates that we are currently in a transaction.
     * 
     * @throws SQLException if not in a transaction
     */
    private void validateInTransaction() throws SQLException {
        if (!inTransaction.get()) {
            throw new SQLException("Not in a transaction. Call beginTransaction() first.");
        }
    }
    
    /**
     * Validates that the transaction hasn't exceeded the maximum size.
     * 
     * @throws SQLException if transaction is at maximum size
     */
    private void validateTransactionSize() throws SQLException {
        if (transactionItems.size() >= MAX_TRANSACTION_ITEMS) {
            throw new SQLException(
                    String.format("Transaction size limit reached. Maximum %d items allowed per transaction.",
                            MAX_TRANSACTION_ITEMS));
        }
    }
}