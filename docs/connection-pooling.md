# Connection Pooling

The DynamoDB PartiQL JDBC driver includes built-in connection pooling to efficiently manage database connections and improve application performance.

## Overview

Connection pooling maintains a cache of database connections that can be reused, eliminating the overhead of creating new connections for each request. This is particularly beneficial for applications that make frequent database queries.

## Configuration

Connection pooling is configured through connection properties:

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

// Connection pool configuration
props.setProperty("connectionPool.enabled", "true");           // Enable pooling (default: true)
props.setProperty("connectionPool.maxSize", "20");            // Maximum pool size (default: 10)
props.setProperty("connectionPool.minSize", "5");             // Minimum pool size (default: 1)
props.setProperty("connectionPool.maxIdleTime", "300000");    // Max idle time in ms (default: 600000)
props.setProperty("connectionPool.validationQuery", "SELECT 1"); // Validation query
props.setProperty("connectionPool.validationTimeout", "5000"); // Validation timeout in ms

String url = "jdbc:dynamodb:partiql:";
Connection conn = DriverManager.getConnection(url, props);
```

## Connection Pool Properties

| Property | Description | Default | Range |
|----------|-------------|---------|-------|
| `connectionPool.enabled` | Enable/disable connection pooling | `true` | true/false |
| `connectionPool.maxSize` | Maximum number of connections in pool | `10` | 1-100 |
| `connectionPool.minSize` | Minimum number of connections to maintain | `1` | 0-maxSize |
| `connectionPool.maxIdleTime` | Time before idle connections are closed (ms) | `600000` (10 min) | 0-3600000 |
| `connectionPool.validationQuery` | Query to validate connections | `SELECT 1` | Any valid query |
| `connectionPool.validationTimeout` | Timeout for validation query (ms) | `5000` | 1000-30000 |
| `connectionPool.acquireTimeout` | Timeout to acquire connection (ms) | `30000` | 1000-60000 |
| `connectionPool.acquireIncrement` | Connections to create when pool exhausted | `3` | 1-10 |

## Usage Patterns

### Basic Usage

```java
// Connection automatically managed by pool
try (Connection conn = DriverManager.getConnection(url, props)) {
    PreparedStatement ps = conn.prepareStatement("SELECT * FROM Users WHERE id = ?");
    ps.setString(1, "user123");
    ResultSet rs = ps.executeQuery();
    // Process results
} // Connection returned to pool, not closed
```

### DataSource Configuration

For application servers and frameworks that support DataSource:

```java
DynamoDbDataSource ds = new DynamoDbDataSource();
ds.setRegion("us-east-1");
ds.setCredentialsType("DEFAULT");
ds.setConnectionPoolEnabled(true);
ds.setConnectionPoolMaxSize(20);
ds.setConnectionPoolMinSize(5);

// Use with dependency injection or JNDI
Connection conn = ds.getConnection();
```

### Spring Boot Configuration

```yaml
spring:
  datasource:
    url: jdbc:dynamodb:partiql:
    driver-class-name: org.cjgratacos.jdbc.DynamoDbDriver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      idle-timeout: 300000
      connection-test-query: SELECT 1
      validation-timeout: 5000
    properties:
      region: us-east-1
      credentialsType: DEFAULT
```

## Connection Validation

The pool automatically validates connections before use to ensure they're still active:

1. **Test on Borrow**: Validates connection when retrieved from pool
2. **Test While Idle**: Periodically validates idle connections
3. **Test on Return**: Validates when connection returned to pool (optional)

### Custom Validation Query

```java
// Use a lightweight query that touches your most-used table
props.setProperty("connectionPool.validationQuery", "SELECT 1 FROM Users LIMIT 1");
```

## Monitoring

### Pool Statistics

```java
// Get pool statistics (when using internal pool)
if (conn instanceof PooledConnection) {
    PoolStatistics stats = ((PooledConnection) conn).getPoolStatistics();
    System.out.println("Active connections: " + stats.getActiveConnections());
    System.out.println("Idle connections: " + stats.getIdleConnections());
    System.out.println("Total connections: " + stats.getTotalConnections());
    System.out.println("Waiting threads: " + stats.getWaitingThreads());
}
```

### JMX Monitoring

The connection pool exposes JMX MBeans for monitoring:

```java
// Enable JMX monitoring
props.setProperty("connectionPool.jmxEnabled", "true");
props.setProperty("connectionPool.jmxName", "DynamoDBPool");
```

Access via JConsole or your monitoring tool under:
`org.cjgratacos.jdbc:type=ConnectionPool,name=DynamoDBPool`

## Best Practices

### 1. Size Your Pool Appropriately

```java
// For web applications
props.setProperty("connectionPool.maxSize", "20");     // 2 * CPU cores + disk spindles
props.setProperty("connectionPool.minSize", "5");      // 25% of max size

// For batch processing
props.setProperty("connectionPool.maxSize", "50");     // Higher for parallel processing
props.setProperty("connectionPool.minSize", "10");     // Keep more connections ready
```

### 2. Configure Timeouts

```java
// Fast-fail for web applications
props.setProperty("connectionPool.acquireTimeout", "5000");    // 5 seconds
props.setProperty("connectionPool.validationTimeout", "2000");  // 2 seconds

// More patient for batch jobs
props.setProperty("connectionPool.acquireTimeout", "30000");   // 30 seconds
props.setProperty("connectionPool.validationTimeout", "5000");  // 5 seconds
```

### 3. Use Connection Pool with Try-With-Resources

```java
// Always use try-with-resources to ensure connections return to pool
try (Connection conn = dataSource.getConnection();
     PreparedStatement ps = conn.prepareStatement(sql)) {
    // Use connection
} // Automatically returns to pool
```

### 4. Avoid Connection Leaks

```java
// BAD: Connection leak
Connection conn = dataSource.getConnection();
// Forgot to close!

// GOOD: Proper cleanup
Connection conn = null;
try {
    conn = dataSource.getConnection();
    // Use connection
} finally {
    if (conn != null) {
        conn.close(); // Returns to pool
    }
}
```

## Troubleshooting

### Pool Exhaustion

If you see "Pool exhausted" errors:

1. **Increase pool size**:

   ```java
   props.setProperty("connectionPool.maxSize", "50");
   ```

2. **Check for connection leaks**:

   ```java
   props.setProperty("connectionPool.leakDetectionThreshold", "30000"); // 30 seconds
   ```

3. **Reduce connection hold time**:
   - Review long-running queries
   - Use connection only when needed
   - Release connections promptly

### Validation Failures

If connections fail validation:

1. **Check network connectivity** to DynamoDB
2. **Verify credentials** haven't expired
3. **Adjust validation query** to be more lightweight
4. **Increase validation timeout** for high-latency environments

### Performance Tuning

1. **Monitor pool metrics** to find optimal size
2. **Adjust min/max** based on actual usage patterns
3. **Use connection warming** for consistent performance:

   ```java
   props.setProperty("connectionPool.initializationFailFast", "true");
   props.setProperty("connectionPool.warmupConnections", "5");
   ```

## Integration with Connection Pool Libraries

The driver can work with external connection pools like HikariCP, Apache DBCP2, or C3P0:

### HikariCP Example

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:dynamodb:partiql:");
config.setDriverClassName("org.cjgratacos.jdbc.DynamoDbDriver");
config.addDataSourceProperty("region", "us-east-1");
config.addDataSourceProperty("credentialsType", "DEFAULT");
config.setMaximumPoolSize(20);
config.setMinimumIdle(5);

HikariDataSource ds = new HikariDataSource(config);
```

### Apache DBCP2 Example

```java
BasicDataSource ds = new BasicDataSource();
ds.setDriverClassName("org.cjgratacos.jdbc.DynamoDbDriver");
ds.setUrl("jdbc:dynamodb:partiql:");
ds.setConnectionProperties("region=us-east-1;credentialsType=DEFAULT");
ds.setInitialSize(5);
ds.setMaxTotal(20);
ds.setMaxIdle(10);
ds.setMinIdle(5);
```

## Security Considerations

1. **Credential Rotation**: Pool automatically handles credential refresh for IAM roles
2. **Encryption**: All connections use HTTPS/TLS
3. **Connection Security**: Each pooled connection maintains its security context
4. **Audit Logging**: Enable to track connection usage:

   ```java
   props.setProperty("connectionPool.auditEnabled", "true");
   ```
