# Prepared Statement Caching

The DynamoDB PartiQL JDBC driver includes automatic prepared statement caching to improve query performance by reusing parsed and compiled statements.

## Overview

Prepared statement caching stores compiled SQL statements in memory, avoiding the overhead of parsing and preparing the same queries repeatedly. This is especially beneficial for applications that execute the same queries with different parameters.

## How It Works

1. When a prepared statement is created, the driver checks if it's already in the cache
2. If found, the cached statement is reused
3. If not found, a new statement is prepared and added to the cache
4. The cache uses an LRU (Least Recently Used) eviction policy

## Configuration

### Connection-Level Configuration

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

// Prepared statement cache configuration
props.setProperty("preparedStatementCache.enabled", "true");     // Enable caching (default: true)
props.setProperty("preparedStatementCache.size", "250");         // Cache size (default: 250)
props.setProperty("preparedStatementCache.sqlLimit", "2048");    // Max SQL length (default: 2048)

String url = "jdbc:dynamodb:partiql:";
Connection conn = DriverManager.getConnection(url, props);
```

### Cache Properties

| Property | Description | Default | Range |
|----------|-------------|---------|-------|
| `preparedStatementCache.enabled` | Enable/disable statement caching | `true` | true/false |
| `preparedStatementCache.size` | Maximum number of cached statements | `250` | 0-5000 |
| `preparedStatementCache.sqlLimit` | Maximum SQL string length to cache | `2048` | 256-65536 |
| `preparedStatementCache.serverPrepare` | Use server-side prepare when available | `false` | true/false |

## Usage Examples

### Basic Usage

```java
// The driver automatically caches prepared statements
String sql = "SELECT * FROM Users WHERE userId = ? AND status = ?";

// First execution - statement is prepared and cached
try (PreparedStatement ps = conn.prepareStatement(sql)) {
    ps.setString(1, "user123");
    ps.setString(2, "active");
    ResultSet rs = ps.executeQuery();
    // Process results
}

// Subsequent executions - cached statement is reused
try (PreparedStatement ps = conn.prepareStatement(sql)) {
    ps.setString(1, "user456");  // Different parameters
    ps.setString(2, "active");
    ResultSet rs = ps.executeQuery();
    // Process results - faster due to caching
}
```

### Batch Operations with Caching

```java
String insertSql = "INSERT INTO Orders VALUE {'orderId': ?, 'userId': ?, 'amount': ?}";

// Prepare once, cache is used for all iterations
try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
    for (Order order : orders) {
        ps.setString(1, order.getId());
        ps.setString(2, order.getUserId());
        ps.setBigDecimal(3, order.getAmount());
        ps.addBatch();
    }
    ps.executeBatch(); // Efficient batch execution with cached statement
}
```

## Cache Key Composition

The cache key is composed of:

1. SQL query text (normalized)
2. Result set type
3. Result set concurrency
4. Result set holdability

```java
// These create different cache entries
PreparedStatement ps1 = conn.prepareStatement(sql);
PreparedStatement ps2 = conn.prepareStatement(sql, 
    ResultSet.TYPE_SCROLL_INSENSITIVE, 
    ResultSet.CONCUR_READ_ONLY);
```

## Performance Benefits

### Benchmark Results

```java
// Without caching
long start = System.currentTimeMillis();
for (int i = 0; i < 1000; i++) {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "user" + i);
        ps.executeQuery();
    }
}
long withoutCache = System.currentTimeMillis() - start;

// With caching (typical improvement: 30-50%)
props.setProperty("preparedStatementCache.enabled", "true");
// ... same loop ...
long withCache = System.currentTimeMillis() - start;
```

### Memory vs Performance Trade-off

```java
// High-performance configuration (more memory)
props.setProperty("preparedStatementCache.size", "500");
props.setProperty("preparedStatementCache.sqlLimit", "4096");

// Memory-conscious configuration
props.setProperty("preparedStatementCache.size", "100");
props.setProperty("preparedStatementCache.sqlLimit", "1024");
```

## Monitoring Cache Performance

### Cache Statistics

```java
// Get cache statistics
if (conn instanceof CacheableConnection) {
    CacheStatistics stats = ((CacheableConnection) conn).getCacheStatistics();
    
    System.out.println("Cache hit rate: " + stats.getHitRate() + "%");
    System.out.println("Cache size: " + stats.getCurrentSize());
    System.out.println("Cache hits: " + stats.getHits());
    System.out.println("Cache misses: " + stats.getMisses());
    System.out.println("Evictions: " + stats.getEvictions());
}
```

### JMX Monitoring

```java
// Enable JMX monitoring for cache
props.setProperty("preparedStatementCache.jmxEnabled", "true");
```

Access cache metrics via JMX:

- `CacheHitRate`: Percentage of cache hits
- `CacheSize`: Current number of cached statements
- `CacheMisses`: Number of cache misses
- `EvictionCount`: Number of evicted statements

## Best Practices

### 1. Size Cache Appropriately

```java
// Calculate based on unique queries
int uniqueQueries = 150;  // Estimate of unique SQL statements
int safetyFactor = 2;     // Allow for variations
props.setProperty("preparedStatementCache.size", 
    String.valueOf(uniqueQueries * safetyFactor));
```

### 2. Use Consistent SQL

```java
// GOOD: Consistent SQL that can be cached effectively
String sql = "SELECT * FROM Users WHERE userId = ?";

// BAD: Dynamic SQL that creates many cache entries
String sql = "SELECT * FROM Users WHERE userId = '" + userId + "'";
```

### 3. Close Statements Properly

```java
// Always close statements to return them to cache
try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // Use statement
} // Automatically closed and available for reuse
```

### 4. Monitor Cache Effectiveness

```java
// Periodic cache health check
public void monitorCache() {
    CacheStatistics stats = getCacheStatistics();
    
    if (stats.getHitRate() < 80) {
        logger.warn("Low cache hit rate: {}%", stats.getHitRate());
        // Consider increasing cache size
    }
    
    if (stats.getEvictions() > stats.getSize() * 0.1) {
        logger.warn("High eviction rate detected");
        // Cache might be too small
    }
}
```

## Advanced Configuration

### Per-Statement Cache Control

```java
// Disable caching for specific statement
Map<String, String> properties = new HashMap<>();
properties.put("cacheEnabled", "false");

PreparedStatement ps = conn.prepareStatement(sql, properties);
```

### Custom Cache Implementation

```java
// Use custom cache implementation
public class CustomStatementCache implements StatementCache {
    private final Cache<String, PreparedStatement> cache;
    
    public CustomStatementCache() {
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(500)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .recordStats()
            .build();
    }
    
    @Override
    public PreparedStatement get(String key) {
        return cache.getIfPresent(key);
    }
    
    @Override
    public void put(String key, PreparedStatement statement) {
        cache.put(key, statement);
    }
}

// Register custom cache
props.setProperty("preparedStatementCache.className", 
    "com.example.CustomStatementCache");
```

## Cache Invalidation

### Manual Invalidation

```java
// Clear entire cache
if (conn instanceof CacheableConnection) {
    ((CacheableConnection) conn).clearStatementCache();
}

// Invalidate specific statement
((CacheableConnection) conn).evictStatement(sql);
```

### Automatic Invalidation

The cache automatically invalidates entries when:

- Connection is closed
- Schema changes are detected
- Memory pressure requires eviction
- Statement hasn't been used within TTL

## Integration with ORMs

### Hibernate Configuration

```xml
<property name="hibernate.jdbc.batch_size">25</property>
<property name="hibernate.connection.provider_class">
    org.hibernate.hikaricp.internal.HikariCPConnectionProvider
</property>
<property name="hibernate.hikari.dataSource.preparedStatementCache.enabled">true</property>
<property name="hibernate.hikari.dataSource.preparedStatementCache.size">250</property>
```

### MyBatis Configuration

```xml
<configuration>
    <settings>
        <setting name="defaultStatementTimeout" value="30"/>
    </settings>
    <environments default="production">
        <environment id="production">
            <dataSource type="POOLED">
                <property name="driver" value="org.cjgratacos.jdbc.DynamoDbDriver"/>
                <property name="url" value="jdbc:dynamodb:partiql:"/>
                <property name="preparedStatementCache.enabled" value="true"/>
                <property name="preparedStatementCache.size" value="300"/>
            </dataSource>
        </environment>
    </environments>
</configuration>
```

## Troubleshooting

### High Memory Usage

If cache is consuming too much memory:

1. **Reduce cache size**:

   ```java
   props.setProperty("preparedStatementCache.size", "100");
   ```

2. **Limit SQL length**:

   ```java
   props.setProperty("preparedStatementCache.sqlLimit", "1024");
   ```

3. **Enable aggressive eviction**:

   ```java
   props.setProperty("preparedStatementCache.ttl", "300000"); // 5 minutes
   ```

### Low Hit Rate

If cache hit rate is low:

1. **Check for dynamic SQL** that changes frequently
2. **Increase cache size** if evictions are high
3. **Analyze query patterns** to identify variations
4. **Consolidate similar queries** to use same SQL with parameters

### Cache Corruption

If you suspect cache corruption:

```java
try {
    // Clear cache and retry
    ((CacheableConnection) conn).clearStatementCache();
    
    // Re-execute query
    PreparedStatement ps = conn.prepareStatement(sql);
    // ...
} catch (SQLException e) {
    logger.error("Cache corruption detected", e);
}
```

## Performance Tips

1. **Pre-warm cache** for critical queries:

   ```java
   public void prewarmCache(Connection conn, List<String> criticalQueries) {
       for (String sql : criticalQueries) {
           try (PreparedStatement ps = conn.prepareStatement(sql)) {
               // Just prepare, don't execute
           }
       }
   }
   ```

2. **Use batch operations** to maximize cache benefit
3. **Avoid inline parameters** in SQL strings
4. **Monitor and tune** cache size based on actual usage
5. **Consider query complexity** when setting cache limits
