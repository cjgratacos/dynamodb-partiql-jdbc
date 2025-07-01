# DynamoDB PartiQL JDBC Driver - Pagination Guide

## Understanding DynamoDB Pagination

DynamoDB uses **token-based pagination** rather than numeric offsets. This fundamental difference affects how OFFSET queries perform:

- **Token-based**: DynamoDB returns a `NextToken` to fetch the next page
- **OFFSET simulation**: The driver must fetch and skip rows to reach the desired offset
- **Performance impact**: Large OFFSET values require fetching all preceding rows

## OFFSET Performance Warning

When using OFFSET values larger than 1000 (configurable), you'll receive a warning:

```
Large OFFSET value (5000) may impact performance. DynamoDB uses token-based pagination, 
so rows must be fetched and discarded to reach the desired offset. 
Consider using cursor-based pagination with WHERE clauses instead.
```

## Optimization: Token Caching

The driver includes an intelligent token cache to optimize OFFSET performance:

### How It Works

1. **Token Storage**: As queries execute, NextToken values are cached at regular intervals
2. **Smart Resumption**: For subsequent queries with OFFSET, the driver starts from the nearest cached token
3. **Automatic Management**: Cache entries expire based on TTL and use LRU eviction

### Configuration

```properties
# Enable/disable token caching (default: true)
offsetCacheEnabled=true

# Maximum cached tokens per query pattern (default: 100)
offsetCacheSize=100

# Cache tokens every N rows (default: 100)
# E.g., tokens cached at offsets 100, 200, 300...
offsetCacheInterval=100

# Time-to-live for cached tokens in seconds (default: 3600)
offsetCacheTtlSeconds=3600

# Threshold for OFFSET warnings (default: 1000)
offsetWarningThreshold=1000
```

### Example URL

```
jdbc:dynamodb:partiql:region=us-east-1;offsetCacheInterval=50;offsetCacheSize=200
```

## Best Practices

### 1. Use WHERE Clauses Instead of OFFSET

**Bad**: Fetches and discards 10000 rows
```sql
SELECT * FROM Users 
LIMIT 100 OFFSET 10000;
```

**Good**: Uses index efficiently
```sql
SELECT * FROM Users 
WHERE userId > 'user-10000' 
LIMIT 100;
```

### 2. Timestamp-Based Pagination

**Bad**: Large offset for time-based data
```sql
SELECT * FROM Events 
ORDER BY eventTime 
LIMIT 50 OFFSET 5000;
```

**Good**: Direct time range query
```sql
SELECT * FROM Events 
WHERE eventTime > '2024-01-15T10:00:00Z' 
  AND eventTime < '2024-01-15T11:00:00Z'
LIMIT 50;
```

### 3. Cursor-Based Pagination

Maintain the last seen key from previous results:

```java
// First page
ResultSet rs = stmt.executeQuery(
    "SELECT * FROM Products LIMIT 100"
);

String lastProductId = null;
while (rs.next()) {
    lastProductId = rs.getString("productId");
    // Process row...
}

// Next page using cursor
rs = stmt.executeQuery(
    "SELECT * FROM Products " +
    "WHERE productId > '" + lastProductId + "' " +
    "LIMIT 100"
);
```

### 4. Sequential Pagination Pattern

For GUI tools that paginate sequentially, the token cache provides optimal performance:

```sql
-- First page (offset 0): No cache lookup needed
SELECT * FROM MyTable LIMIT 100;

-- Second page (offset 100): Token cached from first query
SELECT * FROM MyTable LIMIT 100 OFFSET 100;

-- Third page (offset 200): Token cached from second query
SELECT * FROM MyTable LIMIT 100 OFFSET 200;

-- Jump to page 10 (offset 900): Uses cached token from offset 900
SELECT * FROM MyTable LIMIT 100 OFFSET 900;
```

## Performance Comparison

| Offset | Without Cache | With Cache (interval=100) | Improvement |
|--------|--------------|---------------------------|-------------|
| 100    | Fetch 100 rows | Direct from cache | ~100% faster |
| 500    | Fetch 500 rows | Fetch from offset 500 cache | ~100% faster |
| 550    | Fetch 550 rows | Start from offset 500, fetch 50 | ~90% faster |
| 1000   | Fetch 1000 rows | Direct from cache | ~100% faster |
| 5250   | Fetch 5250 rows | Start from offset 5200, fetch 50 | ~99% faster |

## GUI Client Tips

### DbVisualizer

1. **Enable Max Rows**: Set a reasonable max rows limit
2. **Use SQL for Pagination**: Add LIMIT/OFFSET to queries
3. **Monitor Warnings**: Check for large OFFSET warnings in logs

### DBeaver

1. **Result Set Fetch Size**: Configure in connection properties
2. **Use Built-in Pagination**: Let DBeaver handle page navigation
3. **Custom Queries**: Use WHERE clauses for large datasets

## Troubleshooting

### Slow OFFSET Queries

**Symptoms**: Queries with large OFFSET values are slow

**Solutions**:
1. Enable token caching if disabled
2. Reduce `offsetCacheInterval` for more cache points
3. Use cursor-based pagination instead
4. Add appropriate indexes to your DynamoDB table

### Cache Not Working

**Symptoms**: No performance improvement with cache enabled

**Check**:
1. Verify `offsetCacheEnabled=true`
2. Check cache statistics in logs
3. Ensure queries are identical (cache is query-specific)
4. Verify cache TTL hasn't expired

### Memory Usage

**Symptoms**: High memory usage with large datasets

**Solutions**:
1. Reduce `offsetCacheSize` 
2. Lower `offsetCacheTtlSeconds`
3. Use streaming result sets
4. Implement proper result set closing

## Advanced Usage

### Monitoring Cache Performance

```java
DynamoDbConnection conn = (DynamoDbConnection) DriverManager.getConnection(url);
OffsetTokenCache cache = conn.getOffsetTokenCache();

// Get cache statistics
Map<String, Object> stats = cache.getStats();
System.out.println("Cache size: " + stats.get("totalEntries"));
System.out.println("Cache hit rate: " + calculateHitRate());
```

### Custom Cache Configuration

```java
Properties props = new Properties();
props.setProperty("offsetCacheEnabled", "true");
props.setProperty("offsetCacheInterval", "50");  // Cache every 50 rows
props.setProperty("offsetCacheSize", "500");     // Up to 500 entries per query
props.setProperty("offsetCacheTtlSeconds", "1800"); // 30 minute TTL

Connection conn = DriverManager.getConnection(jdbcUrl, props);
```

### Clearing Cache

```java
// Clear cache for specific query pattern
cache.clearQuery("SELECT * FROM users");

// Clear entire cache
cache.clearAll();
```

## Summary

- **OFFSET is expensive** in DynamoDB due to token-based pagination
- **Token caching** significantly improves performance for repeated queries
- **WHERE-based pagination** is always preferred over OFFSET
- **Configure appropriately** based on your access patterns
- **Monitor performance** and adjust cache settings as needed