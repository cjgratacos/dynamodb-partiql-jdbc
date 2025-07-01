# Schema Discovery

The DynamoDB PartiQL JDBC driver includes intelligent schema discovery capabilities to automatically detect table structures, making DynamoDB appear more like a traditional relational database to JDBC clients.

## Overview

Since DynamoDB is schema-less, the driver uses various strategies to discover and cache table schemas. This enables JDBC clients and tools to understand table structures, column types, and relationships.

## Discovery Strategies

### 1. Sample-Based Discovery (Default)

The driver samples a configurable number of items from each table to build a schema:

```java
Properties props = new Properties();
props.setProperty("schemaDiscovery", "SAMPLE");
props.setProperty("sampleSize", "100");  // Sample 100 items per table
```

#### How It Works

1. Queries the first N items from the table
2. Analyzes attribute names and types across all sampled items
3. Builds a composite schema including all discovered attributes
4. Caches the schema for future use

### 2. Hint-Based Discovery

Use table annotations or external configuration to define schemas:

```java
props.setProperty("schemaDiscovery", "HINTS");
props.setProperty("schemaHintsFile", "/path/to/schema-hints.json");
```

Schema hints file format:
```json
{
  "Users": {
    "columns": {
      "userId": "STRING",
      "name": "STRING",
      "age": "NUMBER",
      "email": "STRING",
      "active": "BOOLEAN",
      "metadata": "MAP"
    },
    "primaryKey": {
      "partitionKey": "userId"
    }
  }
}
```

### 3. Auto Discovery

Combines multiple strategies for comprehensive schema detection:

```java
props.setProperty("schemaDiscovery", "AUTO");
```

This mode:
- Starts with hint-based discovery if available
- Falls back to sample-based discovery
- Continuously updates schema as new attributes are discovered

### 4. Disabled Discovery

For performance-critical applications where schema is known:

```java
props.setProperty("schemaDiscovery", "DISABLED");
```

## Schema Caching

Discovered schemas are cached to improve performance:

```java
// Cache configuration
props.setProperty("cacheSchemas", "true");
props.setProperty("schemaCache.ttl", "300000");      // 5 minutes TTL
props.setProperty("schemaCache.maxSize", "1000");    // Max 1000 schemas
```

### Cache Management

```java
Connection conn = DriverManager.getConnection(url, props);
DynamoDbConnection dynamoConn = (DynamoDbConnection) conn;

// Get schema cache
SchemaCache cache = dynamoConn.getSchemaCache();

// Manually refresh a table's schema
cache.refreshTableSchema("Users");

// Clear entire cache
cache.clear();

// Get cache statistics
Map<String, Object> stats = cache.getCacheStats();
System.out.println("Hit rate: " + stats.get("hitRate"));
System.out.println("Cache size: " + stats.get("size"));
```

## Enhanced Schema Detection

The driver includes an enhanced schema detector with advanced features:

### Lazy Loading

Load schemas on-demand to reduce startup time:

```java
props.setProperty("lazyLoadingStrategy", "IMMEDIATE");  // Load immediately
// or
props.setProperty("lazyLoadingStrategy", "BACKGROUND"); // Load in background
// or
props.setProperty("lazyLoadingStrategy", "CACHED_ONLY"); // Only use cached schemas
```

### Schema Preloading

Preload frequently used schemas at startup:

```java
props.setProperty("preloadStrategy", "PATTERN_BASED");
props.setProperty("preloadPatterns", "User*,Order*,Product*");
```

Preload strategies:
- `STARTUP`: Load all schemas at connection time
- `PATTERN_BASED`: Load schemas matching patterns
- `SCHEDULED`: Load schemas on a schedule
- `REACTIVE`: Load based on query patterns
- `NONE`: No preloading

### Schema Optimization

Enable optimizations for better performance:

```java
props.setProperty("schemaOptimizations", "true");
```

Optimizations include:
- Attribute type inference
- Index-based column ordering
- Sparse attribute detection
- Access pattern analysis

## Column Type Mapping

DynamoDB types are mapped to JDBC SQL types:

| DynamoDB Type | JDBC Type | SQL Type Name |
|---------------|-----------|---------------|
| String (S) | VARCHAR | VARCHAR |
| Number (N) | NUMERIC | NUMERIC |
| Binary (B) | BINARY | BINARY |
| Boolean (BOOL) | BOOLEAN | BOOLEAN |
| Null (NULL) | NULL | NULL |
| List (L) | ARRAY | ARRAY |
| Map (M) | STRUCT | STRUCT |
| String Set (SS) | ARRAY | ARRAY |
| Number Set (NS) | ARRAY | ARRAY |
| Binary Set (BS) | ARRAY | ARRAY |

## Handling Schema Evolution

DynamoDB tables can evolve over time with new attributes. The driver handles this gracefully:

### 1. Automatic Detection

When new attributes are encountered:
```java
// Enable automatic schema updates
props.setProperty("autoUpdateSchema", "true");
props.setProperty("schemaUpdateThreshold", "10"); // Update after 10 new attributes
```

### 2. Manual Refresh

Force a schema refresh:
```java
Statement stmt = conn.createStatement();
stmt.execute("REFRESH SCHEMA FOR TABLE Users");
```

### 3. Schema Versioning

Track schema versions over time:
```java
SchemaCache cache = dynamoConn.getSchemaCache();
List<SchemaVersion> versions = cache.getSchemaHistory("Users");

for (SchemaVersion version : versions) {
    System.out.println("Version: " + version.getVersion());
    System.out.println("Timestamp: " + version.getTimestamp());
    System.out.println("Attributes: " + version.getAttributeCount());
}
```

## Best Practices

### 1. Choose the Right Strategy

- **Development**: Use AUTO or SAMPLE for flexibility
- **Production**: Use HINTS for consistency and performance
- **Testing**: Use SAMPLE with small sample sizes

### 2. Optimize Sample Size

```java
// Small tables: sample all
props.setProperty("sampleSize", "1000");

// Large tables: sample subset
props.setProperty("sampleSize", "100");
props.setProperty("sampleStrategy", "RANDOM"); // or "SEQUENTIAL"
```

### 3. Cache Management

```java
// Production settings
props.setProperty("schemaCache.ttl", "3600000");     // 1 hour
props.setProperty("schemaCache.maxSize", "5000");    // Large cache
props.setProperty("schemaCache.concurrency", "16");  // High concurrency
```

### 4. Monitor Schema Changes

```java
// Enable schema change notifications
props.setProperty("schemaChangeNotifications", "true");

// Set up listener
SchemaChangeListener listener = new SchemaChangeListener() {
    @Override
    public void onSchemaChange(String tableName, SchemaChange change) {
        logger.info("Schema changed for {}: {}", tableName, change);
    }
};

dynamoConn.addSchemaChangeListener(listener);
```

## Troubleshooting

### Incomplete Schema Detection

If not all columns are detected:

1. **Increase sample size**:
   ```java
   props.setProperty("sampleSize", "500");
   ```

2. **Use hint files** for complete schema definition

3. **Enable deep sampling**:
   ```java
   props.setProperty("deepSampling", "true");
   props.setProperty("deepSamplingDepth", "3");
   ```

### Performance Issues

If schema discovery is slow:

1. **Use cached schemas**:
   ```java
   props.setProperty("schemaCache.preload", "true");
   ```

2. **Disable auto-discovery**:
   ```java
   props.setProperty("schemaDiscovery", "HINTS");
   ```

3. **Optimize sampling**:
   ```java
   props.setProperty("parallelDiscovery", "true");
   props.setProperty("discoveryThreads", "4");
   ```

### Schema Conflicts

When schemas conflict between discovery methods:

```java
// Set priority order
props.setProperty("schemaPriority", "HINTS,CACHE,SAMPLE");

// Enable conflict resolution
props.setProperty("schemaConflictResolution", "MERGE"); // or "OVERRIDE"
```

## Integration with Tools

### DbVisualizer

The driver automatically provides schema information to DbVisualizer:
- Column names and types
- Primary keys
- Indexes
- Foreign keys (if configured)

### DataGrip

Enhanced support for JetBrains DataGrip:
- Automatic schema refresh
- Type mapping
- Index information

### DBeaver

Optimized for DBeaver compatibility:
- Table metadata
- Column details
- Constraint information