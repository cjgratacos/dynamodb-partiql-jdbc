# Troubleshooting Guide

This guide helps diagnose and resolve common issues with the DynamoDB PartiQL JDBC driver.

## Connection Issues

### Cannot Connect to DynamoDB

**Symptoms:**
- Connection timeout
- "Unable to load AWS credentials" error
- "Region not specified" error

**Solutions:**

1. **Check AWS Credentials**:
   ```java
   // Verify credentials are available
   aws sts get-caller-identity
   
   // Use explicit credentials type
   props.setProperty("credentialsType", "PROFILE");
   props.setProperty("profileName", "your-profile");
   ```

2. **Specify Region Explicitly**:
   ```java
   props.setProperty("region", "us-east-1");
   // Or via environment
   export AWS_REGION=us-east-1
   ```

3. **Test with DynamoDB Local**:
   ```java
   props.setProperty("endpoint", "http://localhost:8000");
   ```

### Connection Pool Exhausted

**Symptoms:**
- "Pool exhausted" exceptions
- Long wait times for connections
- Application hangs

**Solutions:**

1. **Increase Pool Size**:
   ```java
   props.setProperty("connectionPool.maxSize", "50");
   props.setProperty("connectionPool.acquireTimeout", "10000");
   ```

2. **Check for Connection Leaks**:
   ```java
   // Always use try-with-resources
   try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
       // Use connection
   } // Automatically closed
   ```

3. **Monitor Pool Usage**:
   ```java
   PoolStatistics stats = getPoolStatistics();
   logger.info("Active: {}, Idle: {}, Waiting: {}", 
       stats.getActiveConnections(),
       stats.getIdleConnections(),
       stats.getWaitingThreads());
   ```

## Query Issues

### PartiQL Syntax Errors

**Symptoms:**
- "UNEXPECTED_TOKEN" errors
- "Syntax error" messages
- Query parsing failures

**Solutions:**

1. **Use DynamoDB VALUE Syntax**:
   ```sql
   -- Correct for single item
   INSERT INTO Users VALUE {'userId': 'user1', 'name': 'Alice'}
   
   -- Not VALUES (plural)
   INSERT INTO Users VALUES {'userId': 'user1', 'name': 'Alice'}
   ```

2. **Quote Table/Column Names if Needed**:
   ```sql
   -- For reserved words or special characters
   SELECT * FROM "order" WHERE "user-id" = ?
   ```

3. **Check PartiQL Limitations**:
   ```sql
   -- Not supported: LIKE operator
   SELECT * FROM Users WHERE name LIKE '%john%'
   
   -- Use CONTAINS instead
   SELECT * FROM Users WHERE CONTAINS(name, 'john')
   ```

### Query Performance Issues

**Symptoms:**
- Slow query execution
- High read capacity consumption
- Timeouts

**Solutions:**

1. **Use Indexes**:
   ```sql
   -- Query on indexed attributes
   SELECT * FROM Users WHERE email = 'user@example.com'
   
   -- Create GSI for non-key attributes
   ```

2. **Limit Result Size**:
   ```java
   stmt.setFetchSize(100);
   stmt.setMaxRows(1000);
   
   // Or in query
   SELECT * FROM LargeTable LIMIT 100
   ```

3. **Enable Query Metrics**:
   ```java
   QueryMetrics metrics = connection.getQueryMetrics();
   logger.info("Query took {} ms, consumed {} RCU",
       metrics.getLastExecutionTime(),
       metrics.getLastReadCapacityUnits());
   ```

### LIMIT/OFFSET Not Working

**Symptoms:**
- OFFSET ignored
- More rows returned than LIMIT
- Pagination issues

**Solutions:**

1. **Check Query Syntax**:
   ```sql
   -- Correct syntax
   SELECT * FROM Users LIMIT 20 OFFSET 100
   
   -- Also supported
   SELECT * FROM Users OFFSET 100 LIMIT 20
   ```

2. **Verify Driver Version**:
   ```xml
   <!-- Ensure latest version -->
   <dependency>
       <groupId>org.cjgratacos</groupId>
       <artifactId>dynamodb-partiql-jdbc</artifactId>
       <version>1.0.0</version>
   </dependency>
   ```

## Type Conversion Issues

### Data Type Mismatch

**Symptoms:**
- "Cannot convert" errors
- Null values where data expected
- NumberFormatException

**Solutions:**

1. **Check DynamoDB Types**:
   ```java
   // DynamoDB Number to Java
   BigDecimal value = rs.getBigDecimal("amount");
   
   // For integers
   int count = rs.getInt("count");
   
   // For dates stored as strings
   String dateStr = rs.getString("createdAt");
   LocalDateTime date = LocalDateTime.parse(dateStr);
   ```

2. **Handle NULL Values**:
   ```java
   String value = rs.getString("optional_field");
   if (rs.wasNull()) {
       // Handle null case
   }
   ```

## Transaction Issues

### Transaction Failures

**Symptoms:**
- TransactionCanceledException
- "Transaction request cannot include more than 25 items"
- Conflicts between transactions

**Solutions:**

1. **Limit Transaction Size**:
   ```java
   conn.setAutoCommit(false);
   int count = 0;
   
   for (Operation op : operations) {
       executeOperation(op);
       if (++count >= 25) {
           conn.commit();
           count = 0;
       }
   }
   if (count > 0) {
       conn.commit();
   }
   ```

2. **Handle Conflicts**:
   ```java
   props.setProperty("transaction.retryOnConflict", "true");
   props.setProperty("transaction.conflictRetries", "3");
   ```

## Lambda Integration Issues

### Lambda Function Not Found

**Symptoms:**
- "Function not found" errors
- "Not in allowed list" errors
- Permission denied

**Solutions:**

1. **Check Allowed Functions**:
   ```java
   props.setProperty("lambda.allowedFunctions", "func1,func2,func3");
   ```

2. **Verify IAM Permissions**:
   ```json
   {
       "Effect": "Allow",
       "Action": "lambda:InvokeFunction",
       "Resource": "arn:aws:lambda:*:*:function:func1"
   }
   ```

3. **Test Lambda Directly**:
   ```bash
   aws lambda invoke --function-name func1 \
       --payload '{"action":"test"}' response.json
   ```

### Lambda Timeout

**Symptoms:**
- Function timeout errors
- Incomplete results
- Connection reset

**Solutions:**

1. **Increase Timeout**:
   ```java
   props.setProperty("lambda.timeout", "60000"); // 60 seconds
   ```

2. **Use Async Invocation**:
   ```java
   props.setProperty("lambda.invocationType", "Event");
   ```

## Metadata Issues

### Missing Column Information

**Symptoms:**
- Empty ResultSetMetaData
- Unknown column types
- Schema not detected

**Solutions:**

1. **Enable Schema Discovery**:
   ```java
   props.setProperty("schemaDiscovery", "SAMPLE");
   props.setProperty("sampleSize", "100");
   ```

2. **Refresh Schema Cache**:
   ```java
   SchemaCache cache = connection.getSchemaCache();
   cache.refreshTableSchema("Users");
   ```

### Foreign Keys Not Working

**Symptoms:**
- getImportedKeys returns empty
- Relationships not shown in tools
- Foreign key validation fails

**Solutions:**

1. **Define Foreign Keys**:
   ```java
   props.setProperty("foreignKey.FK1", "Orders.userId->Users.userId");
   props.setProperty("validateForeignKeys", "true");
   ```

2. **Check Table/Column Names**:
   ```java
   // Verify exact names
   DatabaseMetaData meta = conn.getMetaData();
   ResultSet tables = meta.getTables(null, null, "%", null);
   while (tables.next()) {
       System.out.println(tables.getString("TABLE_NAME"));
   }
   ```

## Performance Issues

### High Memory Usage

**Symptoms:**
- OutOfMemoryError
- GC overhead limit exceeded
- Application slowdown

**Solutions:**

1. **Reduce Fetch Size**:
   ```java
   stmt.setFetchSize(100); // Instead of default
   ```

2. **Process Results Incrementally**:
   ```java
   ResultSet rs = stmt.executeQuery("SELECT * FROM LargeTable");
   while (rs.next()) {
       processRow(rs);
       // Don't accumulate in memory
   }
   ```

3. **Increase JVM Heap**:
   ```bash
   java -Xmx2g -Xms1g -XX:+UseG1GC YourApp
   ```

### Throttling

**Symptoms:**
- ProvisionedThroughputExceededException
- Increasing latency
- Retry storms

**Solutions:**

1. **Configure Retry Strategy**:
   ```java
   props.setProperty("retry.maxAttempts", "5");
   props.setProperty("retry.baseDelay", "100");
   props.setProperty("retry.maxDelay", "5000");
   ```

2. **Use Batch Operations**:
   ```java
   // Instead of individual inserts
   PreparedStatement ps = conn.prepareStatement(
       "INSERT INTO Items VALUE {'id': ?, 'data': ?}"
   );
   for (Item item : items) {
       ps.setString(1, item.getId());
       ps.setString(2, item.getData());
       ps.addBatch();
   }
   ps.executeBatch();
   ```

## Common Error Messages

### "Requested resource not found"

**Cause**: Table or index doesn't exist

**Solution**:
```sql
-- Check exact table name
SELECT * FROM "my-table"  -- With quotes if needed
```

### "ValidationException"

**Cause**: Invalid query or parameters

**Solution**:
```java
// Validate parameters before execution
if (userId == null || userId.isEmpty()) {
    throw new IllegalArgumentException("userId required");
}
```

### "Item size has exceeded the maximum allowed size"

**Cause**: Item larger than 400KB

**Solution**:
```java
// Compress large attributes
byte[] compressed = compress(largeData);
ps.setBytes(1, compressed);

// Or use S3 for large objects
String s3Key = uploadToS3(largeData);
ps.setString(1, s3Key);
```

## Debugging Tips

### Enable Debug Logging

```xml
<!-- logback.xml -->
<configuration>
    <logger name="org.cjgratacos.jdbc" level="DEBUG"/>
    <logger name="software.amazon.awssdk" level="DEBUG"/>
</configuration>
```

### Trace Queries

```java
props.setProperty("trace.enabled", "true");
props.setProperty("trace.includeParameters", "true");
```

### Use Correlation IDs

```java
MDC.put("correlationId", UUID.randomUUID().toString());
try {
    // Execute queries
} finally {
    MDC.clear();
}
```

## Getting Help

### Collect Diagnostic Information

```java
public void collectDiagnostics() {
    System.out.println("Driver version: " + driver.getVersion());
    System.out.println("Java version: " + System.getProperty("java.version"));
    System.out.println("AWS SDK version: " + AwsSdkVersion.SDK_VERSION);
    
    // Connection properties (sanitized)
    Properties props = connection.getClientInfo();
    props.forEach((k, v) -> {
        if (!k.toString().contains("secret")) {
            System.out.println(k + "=" + v);
        }
    });
}
```

### Report Issues

When reporting issues, include:
1. Driver version
2. Full stack trace
3. Minimal code to reproduce
4. Connection properties (without secrets)
5. AWS region and service limits

### Community Resources

- GitHub Issues: [Report bugs and feature requests](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)
- Stack Overflow: Tag with `dynamodb-jdbc`
- AWS Forums: DynamoDB section