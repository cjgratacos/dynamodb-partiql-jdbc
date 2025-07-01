# Performance Tuning Guide

This guide provides comprehensive performance tuning recommendations for the DynamoDB PartiQL JDBC driver to optimize query performance, reduce latency, and manage costs effectively.

## Connection Optimization

### Connection Pooling

Always use connection pooling for multi-threaded applications:

```java
Properties props = new Properties();
// Size pool based on concurrent threads
props.setProperty("connectionPool.maxSize", "20");
props.setProperty("connectionPool.minSize", "5");
props.setProperty("connectionPool.maxIdleTime", "300000"); // 5 minutes

// Fast connection validation
props.setProperty("connectionPool.validationQuery", "SELECT 1");
props.setProperty("connectionPool.validationTimeout", "2000");
```

### Connection Warming

Pre-warm connections for consistent performance:

```java
props.setProperty("connectionPool.initializeOnStartup", "true");
props.setProperty("connectionPool.warmupConnections", "5");
```

## Query Optimization

### Use Prepared Statements

Prepared statements are cached and reused:

```java
// Good: Reusable prepared statement
PreparedStatement ps = conn.prepareStatement(
    "SELECT * FROM Users WHERE userId = ?"
);

for (String userId : userIds) {
    ps.setString(1, userId);
    ResultSet rs = ps.executeQuery();
    // Process results
}

// Bad: Creating new statement each time
for (String userId : userIds) {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "SELECT * FROM Users WHERE userId = '" + userId + "'"
    );
}
```

### Optimize Statement Cache

```java
props.setProperty("preparedStatementCache.size", "500");
props.setProperty("preparedStatementCache.sqlLimit", "4096");
```

### Use Appropriate Fetch Size

Control memory usage and network calls:

```java
// For large result sets
Statement stmt = conn.createStatement();
stmt.setFetchSize(1000); // Fetch 1000 rows at a time

// For known small results
stmt.setFetchSize(10);
```

### Leverage LIMIT and OFFSET

Always use LIMIT to prevent unbounded queries:

```java
// Good: Limited results
ResultSet rs = stmt.executeQuery("SELECT * FROM Products LIMIT 100");

// Better: With pagination
int pageSize = 100;
int offset = 0;
ResultSet rs = stmt.executeQuery(
    "SELECT * FROM Products LIMIT " + pageSize + " OFFSET " + offset
);
```

## Batch Operations

### Optimize Batch Size

DynamoDB has a limit of 25 items per batch:

```java
PreparedStatement ps = conn.prepareStatement(
    "INSERT INTO Items VALUE {'id': ?, 'data': ?}"
);

int batchCount = 0;
for (Item item : items) {
    ps.setString(1, item.getId());
    ps.setString(2, item.getData());
    ps.addBatch();
    
    if (++batchCount % 25 == 0) {
        ps.executeBatch();
        ps.clearBatch();
        batchCount = 0;
    }
}

// Execute remaining
if (batchCount > 0) {
    ps.executeBatch();
}
```

### Parallel Batch Execution

Enable parallel processing for large batches:

```java
props.setProperty("batch.parallelism", "4");
props.setProperty("batch.parallelExecutor", "FORK_JOIN");
```

## Transaction Optimization

### Minimize Transaction Size

Keep transactions small for better performance:

```java
conn.setAutoCommit(false);

try {
    // Good: Small, focused transaction
    updateUserBalance(userId, amount);
    insertTransactionRecord(transactionId, userId, amount);
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
}
```

### Use Idempotency Tokens

Enable safe retries:

```java
props.setProperty("transaction.idempotencyToken", "true");
props.setProperty("transaction.retryOnConflict", "true");
props.setProperty("transaction.conflictRetries", "3");
```

## Schema and Metadata

### Cache Schema Information

Reduce metadata queries:

```java
props.setProperty("cacheSchemas", "true");
props.setProperty("schemaCache.ttl", "3600000"); // 1 hour
props.setProperty("schemaCache.maxSize", "1000");
```

### Disable Unnecessary Discovery

If schema is known:

```java
props.setProperty("schemaDiscovery", "DISABLED");
props.setProperty("metadataQueries", "MINIMAL");
```

## Index Optimization

### Query Using Indexes

Always query using indexed attributes:

```java
// Good: Uses index
SELECT * FROM Users WHERE email = 'user@example.com'

// Bad: Full table scan
SELECT * FROM Users WHERE age > 25
```

### Use Projection Indexes

Query specific indexes when appropriate:

```java
// Query GSI directly
SELECT * FROM Users."email-index" WHERE email = ?
```

## Network and Retry

### Configure Retry Strategy

Optimize retry behavior:

```java
props.setProperty("retry.maxAttempts", "3");
props.setProperty("retry.baseDelay", "100");
props.setProperty("retry.maxDelay", "2000");
props.setProperty("retry.backoffMultiplier", "2.0");
```

### Enable Request Compression

Reduce network overhead:

```java
props.setProperty("network.compression", "true");
props.setProperty("network.compressionThreshold", "1024"); // 1KB
```

## Monitoring and Metrics

### Enable Query Metrics

Track performance:

```java
props.setProperty("metrics.enabled", "true");
props.setProperty("metrics.detailedMetrics", "true");

// Access metrics
DynamoDbConnection conn = (DynamoDbConnection) connection;
QueryMetrics metrics = conn.getQueryMetrics();

System.out.println("Average latency: " + metrics.getAverageExecutionTimeMs());
System.out.println("Read capacity: " + metrics.getTotalReadCapacityUnits());
```

### Monitor Connection Pool

```java
if (conn instanceof PooledConnection) {
    PoolStatistics stats = ((PooledConnection) conn).getPoolStatistics();
    
    // Log if pool is exhausted
    if (stats.getWaitingThreads() > 0) {
        logger.warn("Connection pool exhausted: {} waiting", 
            stats.getWaitingThreads());
    }
}
```

## Cost Optimization

### Minimize Read Capacity

Use projections to reduce data transfer:

```java
// Good: Only fetch needed columns
SELECT userId, name, email FROM Users WHERE status = 'active'

// Bad: Fetching all columns
SELECT * FROM Users WHERE status = 'active'
```

### Batch Reads

Combine multiple gets:

```java
// Instead of multiple queries
for (String id : ids) {
    stmt.executeQuery("SELECT * FROM Items WHERE id = '" + id + "'");
}

// Use batch gets (if implementing custom logic)
String inClause = String.join(",", ids.stream()
    .map(id -> "'" + id + "'")
    .collect(Collectors.toList()));
stmt.executeQuery("SELECT * FROM Items WHERE id IN (" + inClause + ")");
```

## Lambda Performance

### Optimize Lambda Configuration

For Lambda integration:

```java
// Increase timeout for complex operations
props.setProperty("lambda.timeout", "60000");

// Enable function warming
props.setProperty("lambda.warmup", "true");
props.setProperty("lambda.warmupInterval", "300000"); // 5 minutes

// Use appropriate memory allocation
props.setProperty("lambda.memorySize", "512");
```

### Cache Lambda Results

For frequently called functions:

```java
props.setProperty("lambda.cache.enabled", "true");
props.setProperty("lambda.cache.ttl", "60000"); // 1 minute
props.setProperty("lambda.cache.maxSize", "100");
```

## JVM Tuning

### Memory Configuration

Optimize JVM settings:

```bash
# Increase heap for large result sets
-Xmx2g -Xms1g

# Use G1GC for better latency
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200

# Enable string deduplication
-XX:+UseStringDeduplication
```

### Thread Pool Tuning

For concurrent applications:

```java
// Configure thread pools
props.setProperty("executor.corePoolSize", "10");
props.setProperty("executor.maxPoolSize", "50");
props.setProperty("executor.keepAliveTime", "60000");
props.setProperty("executor.queueCapacity", "1000");
```

## Best Practices Summary

### Do's

1. **Always use connection pooling** for multi-threaded apps
2. **Use prepared statements** for repeated queries
3. **Set appropriate fetch sizes** based on result size
4. **Enable statement caching** for performance
5. **Use LIMIT clauses** to prevent unbounded queries
6. **Monitor metrics** to identify bottlenecks
7. **Configure retries** for resilience

### Don'ts

1. **Don't create connections per request** - use pooling
2. **Don't use SELECT *** when specific columns suffice
3. **Don't ignore indexes** - query on indexed attributes
4. **Don't use large fetch sizes** for small results
5. **Don't disable caching** unless necessary
6. **Don't use synchronous Lambda** for long operations

## Performance Checklist

- [ ] Connection pooling enabled and sized appropriately
- [ ] Prepared statement caching configured
- [ ] Fetch size optimized for query patterns
- [ ] Schema caching enabled
- [ ] Retry strategy configured
- [ ] Metrics collection enabled
- [ ] Indexes utilized effectively
- [ ] Batch operations using optimal size (25)
- [ ] Transactions kept small
- [ ] Lambda functions optimized
- [ ] JVM properly tuned

## Troubleshooting Performance Issues

### High Latency

1. Check connection pool exhaustion
2. Verify retry configuration
3. Analyze query patterns
4. Review network latency

### High Memory Usage

1. Reduce fetch size
2. Limit result set size
3. Clear statement cache
4. Review JVM heap settings

### Throttling

1. Implement exponential backoff
2. Use batch operations
3. Consider on-demand capacity
4. Review access patterns