# Batch Operations

The DynamoDB PartiQL JDBC driver supports batch operations for efficiently executing multiple SQL statements in a single request, reducing network overhead and improving performance.

## Overview

Batch operations allow you to group multiple SQL statements (INSERT, UPDATE, DELETE) and execute them together. The driver optimizes these operations using DynamoDB's BatchWriteItem API for better performance.

## Basic Usage

### Batch Insert Example

```java
String sql = "INSERT INTO Products VALUE {'productId': ?, 'name': ?, 'price': ?, 'category': ?}";

try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // Add multiple rows to batch
    ps.setString(1, "prod001");
    ps.setString(2, "Laptop");
    ps.setBigDecimal(3, new BigDecimal("999.99"));
    ps.setString(4, "Electronics");
    ps.addBatch();
    
    ps.setString(1, "prod002");
    ps.setString(2, "Mouse");
    ps.setBigDecimal(3, new BigDecimal("29.99"));
    ps.setString(4, "Electronics");
    ps.addBatch();
    
    ps.setString(1, "prod003");
    ps.setString(2, "Keyboard");
    ps.setBigDecimal(3, new BigDecimal("79.99"));
    ps.setString(4, "Electronics");
    ps.addBatch();
    
    // Execute all statements in batch
    int[] results = ps.executeBatch();
    
    System.out.println("Inserted " + results.length + " products");
}
```

### Batch Update Example

```java
String sql = "UPDATE Products SET price = price * ? WHERE category = ?";

try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // 10% discount on Electronics
    ps.setBigDecimal(1, new BigDecimal("0.9"));
    ps.setString(2, "Electronics");
    ps.addBatch();
    
    // 15% discount on Clothing
    ps.setBigDecimal(1, new BigDecimal("0.85"));
    ps.setString(2, "Clothing");
    ps.addBatch();
    
    // 5% discount on Books
    ps.setBigDecimal(1, new BigDecimal("0.95"));
    ps.setString(2, "Books");
    ps.addBatch();
    
    int[] results = ps.executeBatch();
    
    for (int i = 0; i < results.length; i++) {
        System.out.println("Update " + i + " affected " + results[i] + " rows");
    }
}
```

### Batch Delete Example

```java
String sql = "DELETE FROM Orders WHERE orderId = ? AND status = ?";

try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // Delete cancelled orders
    for (String orderId : cancelledOrderIds) {
        ps.setString(1, orderId);
        ps.setString(2, "cancelled");
        ps.addBatch();
    }
    
    int[] results = ps.executeBatch();
    System.out.println("Deleted " + Arrays.stream(results).sum() + " orders");
}
```

## Configuration

### Batch Size Configuration

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

// Batch operation configuration
props.setProperty("batch.size", "25");              // Max items per batch (default: 25)
props.setProperty("batch.retryAttempts", "3");      // Retry attempts for failed items
props.setProperty("batch.retryDelay", "100");       // Delay between retries in ms
props.setProperty("batch.failOnError", "false");    // Continue on partial failures

Connection conn = DriverManager.getConnection("jdbc:dynamodb:partiql:", props);
```

### Batch Properties

| Property | Description | Default | Range |
|----------|-------------|---------|-------|
| `batch.size` | Maximum items per DynamoDB batch request | `25` | 1-25 |
| `batch.retryAttempts` | Number of retry attempts for failed items | `3` | 0-10 |
| `batch.retryDelay` | Delay between retries (ms) | `100` | 0-5000 |
| `batch.failOnError` | Fail entire batch on any error | `false` | true/false |
| `batch.parallelism` | Number of parallel batch requests | `1` | 1-10 |

## Advanced Features

### Mixed Operation Batches

```java
try (Statement stmt = conn.createStatement()) {
    // Add different types of operations
    stmt.addBatch("INSERT INTO Users VALUE {'userId': 'user123', 'name': 'John Doe'}");
    stmt.addBatch("UPDATE Users SET status = 'active' WHERE userId = 'user456'");
    stmt.addBatch("DELETE FROM Users WHERE userId = 'user789' AND status = 'deleted'");
    
    int[] results = stmt.executeBatch();
    
    // Check individual results
    for (int i = 0; i < results.length; i++) {
        if (results[i] == Statement.SUCCESS_NO_INFO) {
            System.out.println("Operation " + i + " succeeded");
        } else if (results[i] == Statement.EXECUTE_FAILED) {
            System.out.println("Operation " + i + " failed");
        } else {
            System.out.println("Operation " + i + " affected " + results[i] + " rows");
        }
    }
}
```

### Batch with Error Handling

```java
String sql = "INSERT INTO Inventory VALUE {'itemId': ?, 'quantity': ?, 'warehouse': ?}";

try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // Add items to batch
    for (InventoryItem item : items) {
        ps.setString(1, item.getId());
        ps.setInt(2, item.getQuantity());
        ps.setString(3, item.getWarehouse());
        ps.addBatch();
    }
    
    try {
        int[] results = ps.executeBatch();
        System.out.println("All items inserted successfully");
    } catch (BatchUpdateException e) {
        int[] updateCounts = e.getUpdateCounts();
        
        // Process partial results
        int successful = 0;
        int failed = 0;
        
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] >= 0 || updateCounts[i] == Statement.SUCCESS_NO_INFO) {
                successful++;
            } else {
                failed++;
                System.err.println("Failed to insert item: " + items.get(i).getId());
            }
        }
        
        System.out.println("Batch results: " + successful + " successful, " + failed + " failed");
        
        // Get next exception in chain
        SQLException next = e.getNextException();
        while (next != null) {
            System.err.println("Error: " + next.getMessage());
            next = next.getNextException();
        }
    }
}
```

### Large Batch Processing

```java
public void proceseLargeBatch(List<Product> products, Connection conn) throws SQLException {
    String sql = "INSERT INTO Products VALUE {'productId': ?, 'name': ?, 'price': ?}";
    
    int batchSize = 25; // DynamoDB limit
    int totalProcessed = 0;
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        for (int i = 0; i < products.size(); i++) {
            Product product = products.get(i);
            
            ps.setString(1, product.getId());
            ps.setString(2, product.getName());
            ps.setBigDecimal(3, product.getPrice());
            ps.addBatch();
            
            // Execute batch when size limit reached
            if ((i + 1) % batchSize == 0 || i == products.size() - 1) {
                int[] results = ps.executeBatch();
                totalProcessed += results.length;
                
                System.out.println("Processed batch: " + results.length + " items");
                ps.clearBatch(); // Clear for next batch
            }
        }
    }
    
    System.out.println("Total items processed: " + totalProcessed);
}
```

## Performance Optimization

### Parallel Batch Execution

```java
// Enable parallel batch execution
props.setProperty("batch.parallelism", "4");

// Large batch will be split into parallel requests
try (PreparedStatement ps = conn.prepareStatement(sql)) {
    for (int i = 0; i < 100; i++) {
        ps.setString(1, "item" + i);
        ps.addBatch();
    }
    
    // Executes in 4 parallel threads
    int[] results = ps.executeBatch();
}
```

### Batch Metrics

```java
// Enable batch metrics
props.setProperty("batch.metrics.enabled", "true");

// After batch execution
if (ps instanceof MetricsAware) {
    BatchMetrics metrics = ((MetricsAware) ps).getBatchMetrics();
    
    System.out.println("Total batches: " + metrics.getTotalBatches());
    System.out.println("Failed items: " + metrics.getFailedItems());
    System.out.println("Retry count: " + metrics.getRetryCount());
    System.out.println("Average batch time: " + metrics.getAverageBatchTime() + "ms");
}
```

## Best Practices

### 1. Optimal Batch Size

```java
// DynamoDB BatchWriteItem limit is 25 items
// Using exactly 25 maximizes efficiency
int OPTIMAL_BATCH_SIZE = 25;

try (PreparedStatement ps = conn.prepareStatement(sql)) {
    int count = 0;
    
    for (Item item : items) {
        // Add to batch
        ps.setString(1, item.getId());
        ps.addBatch();
        count++;
        
        // Execute when batch is full
        if (count == OPTIMAL_BATCH_SIZE) {
            ps.executeBatch();
            ps.clearBatch();
            count = 0;
        }
    }
    
    // Execute remaining items
    if (count > 0) {
        ps.executeBatch();
    }
}
```

### 2. Error Recovery

```java
public void batchInsertWithRetry(List<Item> items, Connection conn) {
    String sql = "INSERT INTO Items VALUE {'id': ?, 'data': ?}";
    List<Item> failedItems = new ArrayList<>();
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        // First attempt
        for (Item item : items) {
            ps.setString(1, item.getId());
            ps.setString(2, item.getData());
            ps.addBatch();
        }
        
        try {
            ps.executeBatch();
        } catch (BatchUpdateException e) {
            // Collect failed items
            int[] results = e.getUpdateCounts();
            for (int i = 0; i < results.length; i++) {
                if (results[i] == Statement.EXECUTE_FAILED) {
                    failedItems.add(items.get(i));
                }
            }
        }
        
        // Retry failed items individually
        for (Item item : failedItems) {
            try {
                ps.setString(1, item.getId());
                ps.setString(2, item.getData());
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error("Failed to insert item: " + item.getId(), e);
            }
        }
    }
}
```

### 3. Transaction Support

```java
// Batch operations with transaction support
conn.setAutoCommit(false);

try {
    // Multiple batch operations
    try (PreparedStatement ps1 = conn.prepareStatement(insertSql)) {
        // Add inserts
        ps1.executeBatch();
    }
    
    try (PreparedStatement ps2 = conn.prepareStatement(updateSql)) {
        // Add updates
        ps2.executeBatch();
    }
    
    // Commit all changes
    conn.commit();
} catch (Exception e) {
    conn.rollback();
    throw e;
}
```

### 4. Memory Management

```java
// Process large datasets in chunks to manage memory
public void processLargeDataset(ResultSet source, Connection target) throws SQLException {
    String sql = "INSERT INTO TargetTable VALUE {'id': ?, 'data': ?}";
    
    try (PreparedStatement ps = target.prepareStatement(sql)) {
        int batchCount = 0;
        
        while (source.next()) {
            ps.setString(1, source.getString("id"));
            ps.setString(2, source.getString("data"));
            ps.addBatch();
            batchCount++;
            
            // Execute and clear every 1000 items
            if (batchCount % 1000 == 0) {
                ps.executeBatch();
                ps.clearBatch();
                System.gc(); // Suggest garbage collection
            }
        }
        
        // Final batch
        if (batchCount % 1000 != 0) {
            ps.executeBatch();
        }
    }
}
```

## Monitoring and Debugging

### Enable Batch Logging

```java
// Enable detailed batch logging
props.setProperty("batch.logging.enabled", "true");
props.setProperty("batch.logging.level", "DEBUG");

// Logs will show:
// - Batch composition
// - Execution times
// - Retry attempts
// - Failed items
```

### Batch Statistics

```java
public class BatchMonitor {
    private final Connection conn;
    
    public void monitorBatchOperations() {
        // Get connection-level batch stats
        if (conn instanceof BatchCapable) {
            BatchStatistics stats = ((BatchCapable) conn).getBatchStatistics();
            
            System.out.println("=== Batch Statistics ===");
            System.out.println("Total batches executed: " + stats.getTotalBatches());
            System.out.println("Total items processed: " + stats.getTotalItems());
            System.out.println("Failed items: " + stats.getFailedItems());
            System.out.println("Average items per batch: " + stats.getAverageItemsPerBatch());
            System.out.println("Success rate: " + stats.getSuccessRate() + "%");
        }
    }
}
```

## Common Issues and Solutions

### Issue: Batch Size Exceeded

```java
// Error: Batch request size exceeded
// Solution: Split into smaller batches
public void safeBatchInsert(List<Item> items) throws SQLException {
    int SAFE_BATCH_SIZE = 20; // Leave margin for large items
    
    for (int i = 0; i < items.size(); i += SAFE_BATCH_SIZE) {
        List<Item> batch = items.subList(i, 
            Math.min(i + SAFE_BATCH_SIZE, items.size()));
        
        executeBatch(batch);
    }
}
```

### Issue: Partial Batch Failures

```java
// Handle mixed success/failure in batch
try {
    int[] results = ps.executeBatch();
} catch (BatchUpdateException e) {
    // Some succeeded, some failed
    handlePartialSuccess(e.getUpdateCounts(), items);
}

private void handlePartialSuccess(int[] results, List<Item> items) {
    for (int i = 0; i < results.length; i++) {
        if (results[i] == Statement.EXECUTE_FAILED) {
            // Log or retry individual item
            retryItem(items.get(i));
        }
    }
}
```

### Issue: Memory Overhead

```java
// Reduce memory usage for large batches
ps.setFetchSize(100);  // Limit memory for result processing
ps.clearBatch();       // Clear after each execution

// Use streaming for very large operations
conn.setAutoCommit(false);
ps.setFetchSize(Integer.MIN_VALUE); // Enable streaming
```

## Integration Examples

### Spring Batch Integration

```java
@Component
public class DynamoDbBatchWriter implements ItemWriter<Product> {
    
    @Autowired
    private DataSource dataSource;
    
    @Override
    public void write(List<? extends Product> items) throws Exception {
        String sql = "INSERT INTO Products VALUE {'id': ?, 'name': ?, 'price': ?}";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            for (Product product : items) {
                ps.setString(1, product.getId());
                ps.setString(2, product.getName());
                ps.setBigDecimal(3, product.getPrice());
                ps.addBatch();
            }
            
            ps.executeBatch();
        }
    }
}
```

### Bulk Data Import

```java
public class BulkImporter {
    
    public void importCSV(String filename, Connection conn) throws Exception {
        String sql = "INSERT INTO ImportedData VALUE {'id': ?, 'col1': ?, 'col2': ?}";
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filename));
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            String line;
            int lineCount = 0;
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                ps.setString(3, parts[2]);
                ps.addBatch();
                
                if (++lineCount % 25 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            
            // Final batch
            ps.executeBatch();
            
            System.out.println("Imported " + lineCount + " records");
        }
    }
}
```
