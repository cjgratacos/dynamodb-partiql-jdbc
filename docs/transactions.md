# Transaction Support

The DynamoDB PartiQL JDBC driver provides transaction support using DynamoDB's TransactWriteItems API, enabling ACID transactions across multiple items and tables.

## Overview

DynamoDB transactions provide atomicity, consistency, isolation, and durability (ACID) guarantees for multi-item operations. The JDBC driver implements standard transaction semantics mapping to DynamoDB's transaction capabilities.

## Basic Usage

### Simple Transaction Example

```java
Connection conn = DriverManager.getConnection("jdbc:dynamodb:partiql:", props);

// Disable auto-commit for transaction mode
conn.setAutoCommit(false);

try {
    // Multiple operations within transaction
    PreparedStatement ps1 = conn.prepareStatement(
        "INSERT INTO Orders VALUE {'orderId': ?, 'userId': ?, 'total': ?}"
    );
    ps1.setString(1, "order123");
    ps1.setString(2, "user456");
    ps1.setBigDecimal(3, new BigDecimal("99.99"));
    ps1.executeUpdate();
    
    PreparedStatement ps2 = conn.prepareStatement(
        "UPDATE Users SET balance = balance - ? WHERE userId = ?"
    );
    ps2.setBigDecimal(1, new BigDecimal("99.99"));
    ps2.setString(2, "user456");
    ps2.executeUpdate();
    
    PreparedStatement ps3 = conn.prepareStatement(
        "UPDATE Inventory SET quantity = quantity - ? WHERE productId = ?"
    );
    ps3.setInt(1, 1);
    ps3.setString(2, "prod789");
    ps3.executeUpdate();
    
    // Commit all changes atomically
    conn.commit();
    System.out.println("Transaction completed successfully");
    
} catch (SQLException e) {
    // Rollback on any error
    conn.rollback();
    System.err.println("Transaction failed: " + e.getMessage());
} finally {
    // Re-enable auto-commit
    conn.setAutoCommit(true);
}
```

### Transaction with Conditional Checks

```java
conn.setAutoCommit(false);

try {
    // Check balance before deduction
    PreparedStatement checkBalance = conn.prepareStatement(
        "SELECT balance FROM Users WHERE userId = ?"
    );
    checkBalance.setString(1, "user123");
    ResultSet rs = checkBalance.executeQuery();
    
    if (rs.next() && rs.getBigDecimal("balance").compareTo(new BigDecimal("100")) >= 0) {
        // Sufficient balance, proceed with transaction
        PreparedStatement deduct = conn.prepareStatement(
            "UPDATE Users SET balance = balance - ? WHERE userId = ?"
        );
        deduct.setBigDecimal(1, new BigDecimal("100"));
        deduct.setString(2, "user123");
        deduct.executeUpdate();
        
        PreparedStatement addPoints = conn.prepareStatement(
            "UPDATE Users SET points = points + ? WHERE userId = ?"
        );
        addPoints.setInt(1, 10);
        addPoints.setString(2, "user123");
        addPoints.executeUpdate();
        
        conn.commit();
    } else {
        throw new SQLException("Insufficient balance");
    }
} catch (SQLException e) {
    conn.rollback();
    throw e;
}
```

## Configuration

### Transaction Properties

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

// Transaction configuration
props.setProperty("transaction.maxItems", "25");              // Max items per transaction
props.setProperty("transaction.timeout", "30000");            // Transaction timeout in ms
props.setProperty("transaction.retryOnConflict", "true");    // Auto-retry on conflicts
props.setProperty("transaction.conflictRetries", "3");       // Number of retries
props.setProperty("transaction.idempotencyToken", "true");   // Enable idempotency

Connection conn = DriverManager.getConnection("jdbc:dynamodb:partiql:", props);
```

### Configuration Options

| Property | Description | Default | Range |
|----------|-------------|---------|-------|
| `transaction.maxItems` | Maximum items per transaction | `25` | 1-25 |
| `transaction.timeout` | Transaction timeout (ms) | `30000` | 1000-60000 |
| `transaction.retryOnConflict` | Auto-retry on transaction conflicts | `true` | true/false |
| `transaction.conflictRetries` | Number of automatic retries | `3` | 0-10 |
| `transaction.idempotencyToken` | Use idempotency tokens | `true` | true/false |
| `transaction.isolation` | Transaction isolation level | `READ_COMMITTED` | See levels below |

## Advanced Features

### Isolation Levels

```java
// Set transaction isolation level
conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

// Supported levels:
// - TRANSACTION_READ_UNCOMMITTED (mapped to READ_COMMITTED)
// - TRANSACTION_READ_COMMITTED (default)
// - TRANSACTION_REPEATABLE_READ (mapped to SERIALIZABLE)
// - TRANSACTION_SERIALIZABLE
```

### Conditional Transactions

```java
// Transaction with condition checks
conn.setAutoCommit(false);

try {
    // Update only if current value matches expected
    PreparedStatement ps = conn.prepareStatement(
        "UPDATE Products SET price = ? WHERE productId = ? AND price = ?"
    );
    ps.setBigDecimal(1, new BigDecimal("29.99"));  // New price
    ps.setString(2, "prod123");                     // Product ID
    ps.setBigDecimal(3, new BigDecimal("24.99"));  // Expected current price
    
    int updated = ps.executeUpdate();
    
    if (updated == 0) {
        throw new SQLException("Optimistic locking failure - price changed");
    }
    
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
    throw e;
}
```

### Multi-Table Transactions

```java
public void transferFunds(String fromAccount, String toAccount, BigDecimal amount) 
        throws SQLException {
    
    conn.setAutoCommit(false);
    
    try {
        // Debit from account
        PreparedStatement debit = conn.prepareStatement(
            "UPDATE Accounts SET balance = balance - ? WHERE accountId = ? AND balance >= ?"
        );
        debit.setBigDecimal(1, amount);
        debit.setString(2, fromAccount);
        debit.setBigDecimal(3, amount); // Ensure sufficient balance
        
        if (debit.executeUpdate() == 0) {
            throw new SQLException("Insufficient funds");
        }
        
        // Credit to account
        PreparedStatement credit = conn.prepareStatement(
            "UPDATE Accounts SET balance = balance + ? WHERE accountId = ?"
        );
        credit.setBigDecimal(1, amount);
        credit.setString(2, toAccount);
        credit.executeUpdate();
        
        // Record transaction
        PreparedStatement record = conn.prepareStatement(
            "INSERT INTO Transactions VALUE {" +
            "'transactionId': ?, 'from': ?, 'to': ?, 'amount': ?, 'timestamp': ?}"
        );
        record.setString(1, UUID.randomUUID().toString());
        record.setString(2, fromAccount);
        record.setString(3, toAccount);
        record.setBigDecimal(4, amount);
        record.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
        record.executeUpdate();
        
        conn.commit();
        System.out.println("Transfer completed successfully");
        
    } catch (SQLException e) {
        conn.rollback();
        System.err.println("Transfer failed: " + e.getMessage());
        throw e;
    } finally {
        conn.setAutoCommit(true);
    }
}
```

### Savepoints (Limited Support)

```java
// Note: DynamoDB doesn't support true savepoints
// The driver provides limited savepoint emulation
conn.setAutoCommit(false);

Savepoint sp1 = null;
Savepoint sp2 = null;

try {
    // First set of operations
    executeUpdate1(conn);
    sp1 = conn.setSavepoint("checkpoint1");
    
    // Second set of operations
    executeUpdate2(conn);
    sp2 = conn.setSavepoint("checkpoint2");
    
    // Third set of operations
    executeUpdate3(conn);
    
    // If third set fails, rollback to checkpoint2
} catch (SQLException e) {
    if (sp2 != null) {
        try {
            conn.rollback(sp2);
            conn.commit(); // Commit up to checkpoint2
        } catch (SQLException ex) {
            conn.rollback(); // Full rollback
        }
    }
}
```

## Error Handling

### Transaction Conflicts

```java
public void handleTransactionConflict() {
    int maxRetries = 3;
    int retryCount = 0;
    
    while (retryCount < maxRetries) {
        try {
            conn.setAutoCommit(false);
            
            // Perform transaction operations
            performTransactionalOperations();
            
            conn.commit();
            break; // Success
            
        } catch (SQLException e) {
            conn.rollback();
            
            if (e.getMessage().contains("TransactionConflict")) {
                retryCount++;
                if (retryCount < maxRetries) {
                    // Exponential backoff
                    try {
                        Thread.sleep((long) Math.pow(2, retryCount) * 100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
            }
            throw new RuntimeException("Transaction failed after " + retryCount + " retries", e);
        }
    }
}
```

### Validation Errors

```java
try {
    conn.setAutoCommit(false);
    
    // Multiple updates
    for (Update update : updates) {
        PreparedStatement ps = conn.prepareStatement(update.getSql());
        update.setParameters(ps);
        ps.executeUpdate();
    }
    
    conn.commit();
    
} catch (SQLException e) {
    conn.rollback();
    
    if (e.getMessage().contains("ValidationException")) {
        // Handle validation errors
        logger.error("Validation error in transaction: " + e.getMessage());
        
        // Check specific validation issues
        if (e.getMessage().contains("Item size")) {
            throw new DataValidationException("Item too large for DynamoDB", e);
        } else if (e.getMessage().contains("Attribute")) {
            throw new DataValidationException("Invalid attribute in transaction", e);
        }
    }
    throw e;
}
```

## Best Practices

### 1. Keep Transactions Small

```java
// GOOD: Small, focused transaction
conn.setAutoCommit(false);
try {
    updateUserBalance(userId, amount);
    recordTransaction(userId, transactionId, amount);
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
}

// BAD: Large transaction with many operations
conn.setAutoCommit(false);
try {
    for (int i = 0; i < 100; i++) {
        updateMultipleTables(i);  // Risk of hitting limits
    }
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
}
```

### 2. Use Idempotency Tokens

```java
// Enable idempotency for safe retries
props.setProperty("transaction.idempotencyToken", "true");

// The driver will automatically generate tokens
conn.setAutoCommit(false);
try {
    // Operations are idempotent
    performOperations();
    conn.commit();
} catch (SQLException e) {
    // Safe to retry entire transaction
    conn.rollback();
    retryTransaction();
}
```

### 3. Handle Partial Reads

```java
// Read data before transaction to minimize holding time
Map<String, Object> currentData = readCurrentState();

conn.setAutoCommit(false);
try {
    // Use pre-read data for calculations
    BigDecimal newBalance = calculateNewBalance(currentData);
    
    // Quick transactional update
    updateBalance(userId, newBalance);
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
}
```

### 4. Monitor Transaction Metrics

```java
// Enable transaction metrics
props.setProperty("transaction.metrics.enabled", "true");

// After transactions
if (conn instanceof TransactionCapable) {
    TransactionMetrics metrics = ((TransactionCapable) conn).getTransactionMetrics();
    
    System.out.println("Total transactions: " + metrics.getTotalTransactions());
    System.out.println("Successful commits: " + metrics.getSuccessfulCommits());
    System.out.println("Rollbacks: " + metrics.getRollbacks());
    System.out.println("Conflicts: " + metrics.getConflicts());
    System.out.println("Average duration: " + metrics.getAverageDuration() + "ms");
}
```

## Performance Considerations

### Batch Operations in Transactions

```java
conn.setAutoCommit(false);

try {
    // Batch inserts within transaction
    PreparedStatement ps = conn.prepareStatement(
        "INSERT INTO Orders VALUE {'orderId': ?, 'data': ?}"
    );
    
    for (Order order : orders) {
        ps.setString(1, order.getId());
        ps.setString(2, order.getData());
        ps.addBatch();
        
        // Execute in chunks to respect transaction limits
        if (orders.indexOf(order) % 10 == 0) {
            ps.executeBatch();
            ps.clearBatch();
        }
    }
    
    ps.executeBatch(); // Final batch
    conn.commit();
    
} catch (SQLException e) {
    conn.rollback();
}
```

### Transaction Isolation Impact

```java
// Higher isolation = more conflicts but stronger consistency
conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

// Lower isolation = fewer conflicts but weaker consistency  
conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

// Choose based on requirements:
// - Financial transactions: SERIALIZABLE
// - High-throughput updates: READ_COMMITTED
```

## Common Patterns

### Optimistic Locking Pattern

```java
public void updateWithOptimisticLock(String id, String newData, int expectedVersion) 
        throws SQLException {
    
    conn.setAutoCommit(false);
    
    try {
        PreparedStatement ps = conn.prepareStatement(
            "UPDATE Items SET data = ?, version = ? WHERE id = ? AND version = ?"
        );
        ps.setString(1, newData);
        ps.setInt(2, expectedVersion + 1);
        ps.setString(3, id);
        ps.setInt(4, expectedVersion);
        
        int updated = ps.executeUpdate();
        
        if (updated == 0) {
            throw new OptimisticLockException("Version mismatch");
        }
        
        conn.commit();
    } catch (SQLException e) {
        conn.rollback();
        throw e;
    }
}
```

### Saga Pattern

```java
public class OrderSaga {
    
    public void processOrder(Order order) throws SQLException {
        List<CompensationAction> compensations = new ArrayList<>();
        
        try {
            // Step 1: Reserve inventory
            reserveInventory(order);
            compensations.add(() -> releaseInventory(order));
            
            // Step 2: Charge payment
            chargePayment(order);
            compensations.add(() -> refundPayment(order));
            
            // Step 3: Create shipment
            createShipment(order);
            compensations.add(() -> cancelShipment(order));
            
            // All steps succeeded
            confirmOrder(order);
            
        } catch (Exception e) {
            // Compensate in reverse order
            Collections.reverse(compensations);
            for (CompensationAction action : compensations) {
                try {
                    action.compensate();
                } catch (Exception ce) {
                    logger.error("Compensation failed", ce);
                }
            }
            throw new SQLException("Order processing failed", e);
        }
    }
}
```

## Troubleshooting

### Transaction Size Limits

```java
// Error: Transaction request cannot include more than 25 items
// Solution: Split into multiple transactions

public void largeUpdate(List<Item> items) throws SQLException {
    int batchSize = 20; // Leave margin
    
    for (int i = 0; i < items.size(); i += batchSize) {
        List<Item> batch = items.subList(i, 
            Math.min(i + batchSize, items.size()));
        
        conn.setAutoCommit(false);
        try {
            processBatch(batch);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }
}
```

### Debugging Failed Transactions

```java
// Enable detailed transaction logging
props.setProperty("transaction.logging.enabled", "true");
props.setProperty("transaction.logging.level", "DEBUG");

// Logs will include:
// - Transaction ID
// - Items involved
// - Conflict details
// - Timing information
```

### Performance Monitoring

```java
public class TransactionMonitor {
    
    public void logTransactionPerformance(Connection conn) {
        long start = System.currentTimeMillis();
        
        try {
            conn.setAutoCommit(false);
            performTransactionalWork();
            conn.commit();
            
            long duration = System.currentTimeMillis() - start;
            
            if (duration > 1000) {
                logger.warn("Slow transaction detected: {}ms", duration);
            }
            
            updateMetrics("transaction.success", duration);
            
        } catch (SQLException e) {
            conn.rollback();
            updateMetrics("transaction.failure", System.currentTimeMillis() - start);
            throw e;
        }
    }
}
```
