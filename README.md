# DynamoDB PartiQL JDBC Driver

A JDBC driver for Amazon DynamoDB that supports PartiQL queries, enabling SQL-like access to DynamoDB data through standard JDBC interfaces.

## ⚠️ Important Disclaimer

**This driver was created primarily for use with JDBC-compliant GUI clients (such as DBeaver, DataGrip, etc.) to browse and query DynamoDB data using familiar SQL tools.**

While the driver implements the necessary JDBC interfaces to support application-level usage, **we do not recommend using this driver in production applications** due to the fundamental differences between DynamoDB's NoSQL nature and traditional relational databases.

**Use at your own risk for application-level integration.** The driver is best suited for:
- Database exploration and visualization tools
- Data analysis and reporting tools  
- Development and testing environments
- One-off data queries and migrations

## 🚀 Features

- **PartiQL Support**: Execute SQL-like queries against DynamoDB using PartiQL
- **Standard JDBC Interface**: Compatible with JDBC 4.0+ specifications
- **GUI Client Support**: Designed for database visualization tools like DBeaver, DataGrip, DbVisualizer
- **Index Query Support**: Automatic normalization of index queries from `"table.index"` to proper DynamoDB PartiQL syntax
- **LIMIT/OFFSET Support**: Full support for SQL LIMIT and OFFSET clauses with client-side implementation
- **Logical Foreign Keys**: Define table relationships for tool compatibility via connection properties
- **Testcontainers Integration**: Full support for integration testing with DynamoDB Local
- **Comprehensive Configuration**: Extensive configuration options for performance tuning
- **Schema Discovery**: Automatic schema detection with multiple strategies
- **Information Schema Support**: Full support for information_schema queries including tables, columns, and index metadata
- **Enhanced Metadata**: Comprehensive TYPE_NAME fields for primary keys and index columns, KEY_NAME support, and information_schema integration
- **Performance Optimization**: Built-in caching, lazy loading, and concurrent operations
- **Connection Pooling**: Built-in connection pool with configurable sizing and validation
- **Observability**: Query metrics, retry handling, and correlation tracing

## 📦 Installation

### Maven Dependency

```xml
<dependency>
    <groupId>org.cjgratacos</groupId>
    <artifactId>dynamodb-partiql-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Note:** Maven/Gradle automatically handle dependencies, so use the regular JAR (not the fat JAR).

### Gradle Dependency

```gradle
implementation 'org.cjgratacos:dynamodb-partiql-jdbc:1.0.0'
```

### Direct JAR Download

Download the latest JAR from the [Releases](https://github.com/cjgratacos/dynamodb-partiql-jdbc/releases) page.

**Two versions available:**
- `dynamodb-partiql-*.jar` - Regular JAR for Maven/Gradle projects
- `dynamodb-partiql-*-with-dependencies.jar` - **Fat JAR for GUI clients (recommended)**

**For GUI Clients:** Always use the `*-with-dependencies.jar` file which includes all required dependencies.

## 🔧 Usage

### JDBC URL Format

```
jdbc:dynamodb:partiql:region=<region>;credentialsType=<type>;[additional_properties]
```

### Default Behavior Improvements

The driver includes optimizations for GUI clients like DbVisualizer:

- **Default Fetch Size**: Set to 100 rows (instead of unlimited) for better performance
- **Max Rows Enforcement**: The `setMaxRows()` JDBC method is now properly enforced
- **Efficient Pagination**: Only fetches what's needed based on LIMIT, maxRows, and fetchSize
- **Automatic Limiting**: When no explicit limit is set, fetchSize is used as a safety limit to prevent unbounded queries

#### Row Limiting Priority

The driver applies limits in the following priority order:
1. SQL `LIMIT` clause (highest priority)
2. JDBC `Statement.setMaxRows()` value
3. `defaultFetchSize` connection property (default: 100 rows)

This ensures queries never fetch all rows accidentally, even when GUI clients don't set explicit limits.

### Basic Connection Example

```java
import java.sql.*;

public class DynamoDBExample {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM MyTable WHERE id = '123'");
                
                while (rs.next()) {
                    System.out.println("ID: " + rs.getString("id"));
                    System.out.println("Name: " + rs.getString("name"));
                }
            }
        }
    }
}
```

### Index Query Support

The driver automatically handles index queries from GUI clients that use the common `"table.index"` syntax pattern. This is particularly useful for tools like DbVisualizer that may generate queries with this syntax.

#### Automatic Syntax Normalization

```java
// These queries are automatically normalized:
"SELECT * FROM \"users.email_index\""      → "SELECT * FROM \"users\".\"email_index\""
"SELECT * FROM \"orders.GSI1\""            → "SELECT * FROM \"orders\".\"GSI1\""
"SELECT * FROM \"products.PRIMARY\""       → "SELECT * FROM \"products\"" // PRIMARY is converted to table-only

// Queries with proper syntax are left unchanged:
"SELECT * FROM \"users\".\"email_index\""  → No change
```

#### Special Handling for PRIMARY Index

DynamoDB doesn't have a "PRIMARY" index - the main table is queried without an index specifier. Our driver automatically handles this:
```java
"SELECT * FROM \"users.PRIMARY\""          → "SELECT * FROM \"users\""
```

This feature ensures compatibility with various database tools while maintaining proper DynamoDB PartiQL syntax.

### Enhanced Metadata Support

The driver provides comprehensive metadata support with type information for better integration with database tools:

#### Primary Key Metadata
The `getPrimaryKeys()` method now includes detailed type information:
- **TYPE_NAME**: DynamoDB attribute type ("String", "Number", "Binary")
- Standard JDBC fields: TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME

```java
DatabaseMetaData meta = connection.getMetaData();
ResultSet keys = meta.getPrimaryKeys(null, null, "MyTable");
while (keys.next()) {
    System.out.println("Key: " + keys.getString("COLUMN_NAME"));
    System.out.println("Type: " + keys.getString("TYPE_NAME")); // "String", "Number", etc.
    System.out.println("Sequence: " + keys.getInt("KEY_SEQ"));
}
```

#### Index Column Metadata
New `getIndexColumns()` method provides detailed information about index keys:
- **KEY_NAME**: Same as COLUMN_NAME (the attribute name)
- **TYPE_NAME**: DynamoDB attribute type ("String", "Number", "Binary")  
- **KEY_TYPE**: Key role ("HASH" for partition key, "RANGE" for sort key)
- **INDEX_NAME**: Name of the index ("PRIMARY", GSI name, or LSI name)
- **ORDINAL_POSITION**: Position within the index (1-based)

**Information Schema Support**: The driver also provides full support for `information_schema.index_columns` queries, which is used by many database tools like DbVisualizer. This includes:
- **INDEX_KEY**: The attribute name of the index key
- **TYPE_NAME**: The data type (String, Number, Binary) - standard JDBC field used by GUI tools
- **INDEX_KEY_TYPE**: The data type (alternative field for compatibility)
- **KEY_TYPE**: Key role (HASH or RANGE)

```java
// Get detailed index column information
DynamoDbDatabaseMetaData dynaMeta = (DynamoDbDatabaseMetaData) connection.getMetaData();
ResultSet indexColumns = dynaMeta.getIndexColumns("MyTable", "MyGSI", "MyTable");
while (indexColumns.next()) {
    System.out.println("Column: " + indexColumns.getString("COLUMN_NAME"));
    System.out.println("Key Name: " + indexColumns.getString("KEY_NAME"));     // Same as COLUMN_NAME
    System.out.println("Type: " + indexColumns.getString("TYPE_NAME"));        // "String", "Number", etc.
    System.out.println("Key Type: " + indexColumns.getString("KEY_TYPE"));     // "HASH" or "RANGE"
    System.out.println("Index: " + indexColumns.getString("INDEX_NAME"));      // Index name
    System.out.println("Position: " + indexColumns.getInt("ORDINAL_POSITION")); // 1, 2, etc.
}
```

#### Benefits for Database Tools
These enhancements provide better integration with GUI database tools:
- **Type-aware column display**: Tools can show appropriate icons/formatting based on data type
- **Enhanced schema browsing**: Individual index columns are properly represented
- **Better query building**: Tools can generate appropriate type-specific queries
- **Improved metadata export**: Schema export tools get complete type information

### Comprehensive Type Metadata Support

The driver provides comprehensive type information across all JDBC metadata methods to ensure consistent and accurate data type reporting for database tools.

#### Type Information Methods

The following metadata methods now include complete type information:

**`getTypeInfo()`** - Returns all DynamoDB-supported data types:
```java
DatabaseMetaData meta = connection.getMetaData();
ResultSet typeInfo = meta.getTypeInfo();
while (typeInfo.next()) {
    System.out.println("Type: " + typeInfo.getString("TYPE_NAME"));        // "String", "Number", "Binary"
    System.out.println("SQL Type: " + typeInfo.getInt("DATA_TYPE"));       // VARCHAR, NUMERIC, BINARY
    System.out.println("Searchable: " + typeInfo.getInt("SEARCHABLE"));    // Search capability
    System.out.println("Nullable: " + typeInfo.getBoolean("NULLABLE"));    // Always true for DynamoDB
}
```

**`getBestRowIdentifier()`** - Returns primary key columns with type information:
```java
DatabaseMetaData meta = connection.getMetaData();
ResultSet bestRow = meta.getBestRowIdentifier(null, null, "MyTable", 
                                             DatabaseMetaData.bestRowSession, false);
while (bestRow.next()) {
    System.out.println("Column: " + bestRow.getString("COLUMN_NAME"));    // e.g., "userId"
    System.out.println("Type: " + bestRow.getString("TYPE_NAME"));        // "String", "Number", "Binary"
    System.out.println("SQL Type: " + bestRow.getInt("DATA_TYPE"));       // VARCHAR, NUMERIC, BINARY
    System.out.println("Scope: " + bestRow.getInt("SCOPE"));              // bestRowSession
}
```

**`getTableTypes()`** - Returns supported table types:
```java
DatabaseMetaData meta = connection.getMetaData();
ResultSet tableTypes = meta.getTableTypes();
while (tableTypes.next()) {
    System.out.println("Table Type: " + tableTypes.getString("TABLE_TYPE")); // "TABLE"
}
```

**`getColumns()`** - Enhanced with consistent type information:
```java
DatabaseMetaData meta = connection.getMetaData();
ResultSet columns = meta.getColumns(null, null, "MyTable", null);
while (columns.next()) {
    System.out.println("Column: " + columns.getString("COLUMN_NAME"));
    System.out.println("Type: " + columns.getString("TYPE_NAME"));        // "VARCHAR", "NUMERIC", "BINARY"
    System.out.println("SQL Type: " + columns.getInt("DATA_TYPE"));       // Types.VARCHAR, etc.
}
```

#### DynamoDB Type Mappings

The driver uses consistent type mappings across all metadata methods:

| DynamoDB Type | TYPE_NAME | SQL Type (DATA_TYPE) | Description |
|---------------|-----------|---------------------|-------------|
| **S** (String) | `"String"` or `"VARCHAR"` | `Types.VARCHAR` | Text and string data |
| **N** (Number) | `"Number"` or `"NUMERIC"` | `Types.NUMERIC` | Numeric data (integers, decimals) |
| **B** (Binary) | `"Binary"` or `"BINARY"` | `Types.BINARY` | Binary data and byte arrays |

**Note**: Some methods return the native DynamoDB type names ("String", "Number", "Binary") while others return SQL-standard type names ("VARCHAR", "NUMERIC", "BINARY") for better compatibility with standard JDBC tools.

#### Type Consistency Testing

The driver includes comprehensive tests (`DatabaseMetadataTypeTest`) that verify:
- All metadata methods return consistent type information
- Type names are properly populated and never null
- SQL type constants match expected values
- Edge cases like single-key tables are handled correctly
- All three DynamoDB types are properly represented

This ensures reliable type information for database tools and prevents issues like missing type hints or inconsistent data type reporting.

### PartiQL Keywords and Syntax Support

The driver includes a comprehensive `PartiQLKeywords` class that exposes all supported PartiQL keywords, functions, and syntax patterns. This is useful for:
- IDE autocomplete and type hinting
- Syntax highlighting in editors
- Query builders and tools
- Validating reserved keywords

```java
import org.cjgratacos.jdbc.PartiQLKeywords;

// Check if a word is reserved
boolean isReserved = PartiQLKeywords.isReservedKeyword("year"); // true

// Quote identifiers safely
String quoted = PartiQLKeywords.quoteIfNeeded("year"); // "year"

// Use constants for query building
String query = PartiQLKeywords.SELECT + " * " + 
               PartiQLKeywords.FROM + " users " + 
               PartiQLKeywords.WHERE + " age > 21";

// Access predefined query patterns
String pattern = PartiQLKeywords.SELECT_INDEX_PATTERN;
String indexQuery = String.format(pattern, "*", "users", "email_index", "email = ?");
```

### Logical Foreign Key Support

Since DynamoDB doesn't support foreign keys natively, the driver allows you to define logical relationships between tables. This enables database tools to display and understand table relationships.

#### Configuration Methods

The driver supports three methods for defining foreign keys:

##### Method 1: Inline in Connection URL

```
foreignKey.<constraint_name>=<foreign_table>.<foreign_column>-><primary_table>.<primary_column>
```

Example:
```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "foreignKey.FK_Orders_Users=Orders.customerId->Users.userId;" +
             "foreignKey.FK_OrderItems_Orders=OrderItems.orderId->Orders.orderId";

Connection conn = DriverManager.getConnection(url);
```

##### Method 2: Properties File

Specify a properties file containing foreign key definitions:

```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "foreignKeysFile=/path/to/foreign-keys.properties";
```

Properties file format (`foreign-keys.properties`):
```properties
# Simple format
foreignKey.FK_Orders_Users=Orders.customerId->Users.userId
foreignKey.FK_OrderItems_Orders=OrderItems.orderId->Orders.orderId

# Detailed format with additional properties
fk.1.name=FK_Orders_Users
fk.1.foreign.table=Orders
fk.1.foreign.column=customerId
fk.1.primary.table=Users
fk.1.primary.column=userId
fk.1.updateRule=CASCADE
fk.1.deleteRule=RESTRICT
```

##### Method 3: DynamoDB Table

Store foreign key definitions in a DynamoDB table:

```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "foreignKeysTable=MyAppForeignKeys";
```

Expected DynamoDB table schema:
```json
{
  "constraintName": "FK_Orders_Users",  // Primary key
  "foreignTable": "Orders",
  "foreignColumn": "customerId",
  "primaryTable": "Users",
  "primaryColumn": "userId",
  "updateRule": "CASCADE",    // Optional
  "deleteRule": "RESTRICT"    // Optional
}
```

Create the table using PartiQL:
```sql
CREATE TABLE MyAppForeignKeys (
  constraintName STRING,
  foreignTable STRING,
  foreignColumn STRING,
  primaryTable STRING,
  primaryColumn STRING,
  updateRule STRING,
  deleteRule STRING
)
```

##### Combining Methods

You can combine multiple methods - foreign keys from all sources will be merged:

```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "foreignKeysFile=/path/to/foreign-keys.properties;" +
             "foreignKeysTable=MyAppForeignKeys;" +
             "foreignKey.FK_Additional=Table1.col1->Table2.col2";
```

#### Using Foreign Key Metadata

Once defined, foreign keys are accessible through standard JDBC DatabaseMetaData methods:

```java
DatabaseMetaData meta = conn.getMetaData();

// Get foreign keys imported by Orders table
ResultSet imported = meta.getImportedKeys(null, null, "Orders");
while (imported.next()) {
    System.out.println("Orders." + imported.getString("FKCOLUMN_NAME") + 
                      " references " + 
                      imported.getString("PKTABLE_NAME") + "." + 
                      imported.getString("PKCOLUMN_NAME"));
}

// Get foreign keys exported by Users table
ResultSet exported = meta.getExportedKeys(null, null, "Users");
while (exported.next()) {
    System.out.println(exported.getString("FKTABLE_NAME") + "." + 
                      exported.getString("FKCOLUMN_NAME") + 
                      " references Users." + 
                      exported.getString("PKCOLUMN_NAME"));
}

// Get cross-reference between specific tables
ResultSet crossRef = meta.getCrossReference(null, null, "Users", 
                                          null, null, "Orders");
```

#### Benefits

- **Tool Compatibility**: Database visualization tools like DbVisualizer can display relationship diagrams
- **Documentation**: Relationships are documented in connection configuration
- **No DynamoDB Changes**: Purely client-side metadata, no changes to your DynamoDB tables
- **Standard JDBC**: Uses standard JDBC methods that tools already understand

### Foreign Key Validation

The driver includes comprehensive validation support to ensure foreign key definitions reference actual existing tables and columns in DynamoDB.

#### Validation Configuration

Control validation behavior using connection properties:

| Property | Default | Description |
|----------|---------|-------------|
| `validateForeignKeys` | `false` | Enable/disable foreign key validation |
| `foreignKeyValidationMode` | `lenient` | Validation mode: `strict`, `lenient`, or `off` |
| `cacheTableMetadata` | `true` | Cache table/column existence checks for performance |

#### Validation Modes

##### Strict Mode
In strict mode, the connection will fail if any foreign key definition is invalid:
```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "validateForeignKeys=true;" +
             "foreignKeyValidationMode=strict;" +
             "foreignKey.FK1=Orders.customerId->NonExistentTable.id";

// This will throw SQLException due to invalid foreign key
Connection conn = DriverManager.getConnection(url);
```

##### Lenient Mode (Default)
In lenient mode, invalid foreign keys are logged as warnings but don't prevent connection:
```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "validateForeignKeys=true;" +
             "foreignKeyValidationMode=lenient;" +
             "foreignKey.FK1=Orders.customerId->Users.userId;" +
             "foreignKey.FK2=Orders.sellerId->NonExistentTable.id";

// Connection succeeds, FK2 logged as warning
Connection conn = DriverManager.getConnection(url);
```

##### Off Mode
Validation is completely disabled (same as `validateForeignKeys=false`).

#### Validation Checks

When validation is enabled, the driver performs the following checks:

1. **Table Existence**: Verifies both primary and foreign tables exist
2. **Column Existence**: Verifies referenced columns exist in their respective tables
3. **Constraint Name**: Ensures constraint names are not empty
4. **Duplicate Names**: Detects duplicate constraint names
5. **Circular References**: Warns about circular foreign key relationships

#### Performance Considerations

- **Caching**: Table and column existence checks are cached (15-minute default)
- **Lazy Loading**: Validation occurs during connection initialization
- **Batch Validation**: Multiple foreign keys are validated together for efficiency

#### Example with All Features

```java
// Create properties file with foreign keys
// foreign-keys.properties:
// foreignKey.FK1=Orders.customerId->Users.userId
// foreignKey.FK2=OrderItems.orderId->Orders.orderId

Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("validateForeignKeys", "true");
props.setProperty("foreignKeyValidationMode", "strict");
props.setProperty("cacheTableMetadata", "true");
props.setProperty("foreignKeysFile", "/path/to/foreign-keys.properties");

String url = "jdbc:dynamodb:partiql:" + toPropertiesString(props);

try (Connection conn = DriverManager.getConnection(url)) {
    DatabaseMetaData meta = conn.getMetaData();
    
    // Only valid foreign keys are available
    ResultSet rs = meta.getImportedKeys(null, null, "Orders");
    while (rs.next()) {
        // All foreign keys have been validated
        System.out.println("Valid FK: " + rs.getString("FK_NAME"));
    }
} catch (SQLException e) {
    // In strict mode, connection fails if validation errors occur
    System.err.println("Connection failed: " + e.getMessage());
}
```

#### Validation API Access

For advanced use cases, you can access validation information programmatically:

```java
DynamoDbDatabaseMetaData meta = (DynamoDbDatabaseMetaData) conn.getMetaData();
// Note: This requires casting to the implementation class and accessing
// internal validation state - not recommended for production use
```

### GUI Client Configuration

**Important:** Use the `*-with-dependencies.jar` file for GUI clients to avoid dependency issues.

#### DBeaver
1. Add new connection → Other → Generic JDBC
2. **URL**: `jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT`
3. **Driver**: Upload the `*-with-dependencies.jar` file and select `org.cjgratacos.jdbc.DynamoDbDriver`
4. Configure AWS credentials as needed

#### DataGrip / IntelliJ
1. New Data Source → Generic JDBC
2. **URL**: `jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT`
3. **Driver**: Add the `*-with-dependencies.jar` and select `org.cjgratacos.jdbc.DynamoDbDriver`
4. Test connection

#### DbVisualizer
1. Tools → Driver Manager → Create Driver
2. **Name**: DynamoDB PartiQL
3. **URL Format**: `jdbc:dynamodb:partiql:region=<region>;credentialsType=<type>`
4. **Driver File Paths**: Add the `*-with-dependencies.jar`
5. **Driver Class**: `org.cjgratacos.jdbc.DynamoDbDriver`

**DbVisualizer Features:**
- ✅ Browse tables and indexes in the database tree
- ✅ View index columns (Hash and Sort keys) separately when expanding indexes
- ✅ Execute PartiQL queries with automatic index syntax normalization
- ✅ View table data and metadata through information_schema support
- ✅ Automatic row limiting (defaults to 100 rows) for better performance
- ✅ Respects Max Rows settings in DbVisualizer preferences
- ✅ Pagination support using LIMIT/OFFSET in queries

**Pagination in DbVisualizer:**

To view more than the default 100 rows, you have several options:

1. **Use SQL LIMIT and OFFSET**:
   ```sql
   -- First page (rows 1-100)
   SELECT * FROM MyTable LIMIT 100;
   
   -- Second page (rows 101-200)
   SELECT * FROM MyTable LIMIT 100 OFFSET 100;
   
   -- Third page (rows 201-300)
   SELECT * FROM MyTable LIMIT 100 OFFSET 200;
   ```

2. **Adjust Max Rows in DbVisualizer**:
   - Go to **Tools → Tool Properties → General → SQL Commander**
   - Set "Max Rows" to a higher value (e.g., 500 or 1000)
   - Or set to -1 to fetch all rows (use with caution on large tables!)

3. **Use connection properties**:
   ```
   jdbc:dynamodb:partiql:region=us-east-1;defaultMaxRows=500
   ```

#### Troubleshooting GUI Client Issues

If you encounter `ClassNotFoundException: org.slf4j.LoggerFactory` or similar dependency errors:

1. **Always use the fat JAR**: Download `dynamodb-partiql-*-with-dependencies.jar`
2. **Remove regular JAR**: Don't mix regular and fat JARs in the same client
3. **Clear driver cache**: Some clients cache drivers; restart the client after changing JARs

### Connection Pooling

The driver includes built-in connection pooling for efficient connection reuse in applications:

#### Basic Usage

```java
import org.cjgratacos.jdbc.pool.DynamoDbConnectionPoolDataSource;

// Create pooled data source
DynamoDbConnectionPoolDataSource dataSource = new DynamoDbConnectionPoolDataSource();
dataSource.setJdbcUrl("jdbc:dynamodb:partiql:region=us-east-1");
dataSource.setMinPoolSize(5);
dataSource.setMaxPoolSize(20);
dataSource.setInitialPoolSize(10);

// Use connections from pool
try (Connection conn = dataSource.getConnection()) {
    // Connection automatically returned to pool on close
    try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT * FROM MyTable");
        // Process results
    }
}

// Close pool when done
dataSource.close();
```

#### Pool Configuration Properties

Configure the pool via connection properties:

| Property | Description | Default |
|----------|-------------|---------|
| `pool.minSize` | Minimum pool size | 5 |
| `pool.maxSize` | Maximum pool size | 20 |
| `pool.initialSize` | Initial pool size | 5 |
| `pool.connectionTimeout` | Connection creation timeout (seconds) | 30 |
| `pool.idleTimeout` | Idle connection timeout (seconds) | 600 |
| `pool.maxLifetime` | Maximum connection lifetime (seconds) | 1800 |
| `pool.testOnBorrow` | Validate connection before use | true |
| `pool.testOnReturn` | Validate connection on return | false |
| `pool.testWhileIdle` | Validate idle connections | true |
| `pool.blockWhenExhausted` | Block when pool exhausted | true |
| `pool.maxWaitTime` | Max wait for connection (seconds) | 30 |

#### Using with Connection URL

```java
String url = "jdbc:dynamodb:partiql:region=us-east-1;" +
             "pool.minSize=10;pool.maxSize=50;pool.testOnBorrow=true";

DynamoDbConnectionPoolDataSource dataSource = new DynamoDbConnectionPoolDataSource(url);
```

#### Pool Monitoring

```java
// Get pool statistics
DynamoDbConnectionPoolDataSource.PoolStatistics stats = dataSource.getPoolStatistics();
System.out.println("Active connections: " + stats.getActiveConnections());
System.out.println("Idle connections: " + stats.getIdleConnections());
System.out.println("Total connections: " + stats.getTotalConnections());
System.out.println("Connections created: " + stats.getConnectionsCreated());
System.out.println("Wait timeouts: " + stats.getWaitTimeouts());
```

#### Best Practices

1. **Reuse DataSource**: Create one DataSource instance and share it across your application
2. **Always close connections**: Use try-with-resources to ensure connections return to pool
3. **Monitor pool metrics**: Track active connections and wait timeouts
4. **Size appropriately**: Set max pool size based on DynamoDB throttling limits
5. **Configure timeouts**: Set reasonable timeouts to prevent connection leaks

## ⚙️ Configuration Options

### Required Properties

| Property | Description | Example |
|----------|-------------|---------|
| `region` | AWS region | `us-east-1`, `eu-west-1` |

### AWS Credentials Configuration

#### Default Credentials (Recommended)
```
jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT
```
Uses the AWS SDK default credential provider chain.

#### Static Credentials
```
jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI...;secretKey=wJal...;sessionToken=optional
```

#### Profile-Based Credentials
```
jdbc:dynamodb:partiql:region=us-east-1;credentialsType=PROFILE;profileName=myprofile
```

### Optional Configuration Properties

#### Connection Settings

| Property | Default | Description |
|----------|---------|-------------|
| `endpoint` | AWS default | Custom DynamoDB endpoint URL |
| `apiCallTimeoutMs` | 30000 | Timeout for entire API call including retries |
| `apiCallAttemptTimeoutMs` | 5000 | Timeout for single API call attempt |
| `defaultFetchSize` | 100 | Default number of rows to fetch per page |
| `defaultMaxRows` | - | Default maximum rows limit for all queries |
| `offsetWarningThreshold` | 1000 | Threshold for large OFFSET warnings |
| `foreignKeysFile` | - | Path to properties file containing foreign key definitions |
| `foreignKeysTable` | - | DynamoDB table name containing foreign key definitions |

#### Schema Discovery

| Property | Default | Description |
|----------|---------|-------------|
| `schemaDiscovery` | `auto` | Schema discovery mode: `auto`, `hints`, `sampling`, `disabled` |
| `sampleSize` | 1000 | Number of items to sample for schema inference |
| `sampleStrategy` | `random` | Sampling strategy: `random`, `sequential`, `recent` |
| `schemaCache` | `true` | Enable/disable schema caching |
| `schemaCacheTTL` | 3600 | Schema cache TTL in seconds |

#### Performance Optimization

| Property | Default | Description |
|----------|---------|-------------|
| `schemaOptimizations` | `true` | Enable/disable all schema performance optimizations |
| `concurrentSchemaDiscovery` | `true` | Enable concurrent schema discovery for multiple tables |
| `maxConcurrentSchemaDiscoveries` | CPU cores × 2 | Maximum concurrent discovery operations |
| `lazyLoadingStrategy` | `IMMEDIATE` | Strategy: `IMMEDIATE`, `BACKGROUND`, `CACHED_ONLY`, `PREDICTIVE` |
| `lazyLoadingCacheTTL` | 3600 | Lazy loading cache TTL in seconds |
| `lazyLoadingMaxCacheSize` | 1000 | Maximum number of cached schemas |
| `predictiveSchemaLoading` | `true` | Enable predictive preloading based on access patterns |

#### Schema Preloading

| Property | Default | Description |
|----------|---------|-------------|
| `preloadStrategy` | `PATTERN_BASED` | Strategy: `STARTUP`, `PATTERN_BASED`, `SCHEDULED`, `REACTIVE`, `NONE` |
| `preloadStartupTables` | - | Comma-separated list of tables to preload at startup |
| `preloadScheduledIntervalMs` | 1800000 | Interval for scheduled preloading (30 minutes) |
| `preloadMaxBatchSize` | 10 | Maximum tables to preload in a single batch |
| `preloadPatternRecognition` | `true` | Enable pattern recognition for intelligent preloading |

#### Cache Optimization

| Property | Default | Description |
|----------|---------|-------------|
| `cacheWarmingIntervalMs` | 3600000 | Background cache warming interval (1 hour) |
| `schemaCacheRefreshIntervalMs` | 300000 | Schema cache refresh interval (5 minutes) |

#### Retry Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `retryMaxAttempts` | 3 | Maximum retry attempts for throttled operations |
| `retryBaseDelayMs` | 100 | Base delay for exponential backoff |
| `retryMaxDelayMs` | 20000 | Maximum delay between retries |
| `retryJitterEnabled` | `true` | Enable jitter to prevent thundering herd |

#### Table Filtering

| Property | Default | Description |
|----------|---------|-------------|
| `tableFilter` | - | Regular expression to filter tables in DatabaseMetaData |

#### Offset Token Cache (Performance Optimization)

| Property | Default | Description |
|----------|---------|-------------|
| `offsetCacheEnabled` | `true` | Enable token caching for OFFSET optimization |
| `offsetCacheSize` | 100 | Maximum cached tokens per query pattern |
| `offsetCacheInterval` | 100 | Cache tokens every N rows (e.g., at offsets 100, 200, 300...) |
| `offsetCacheTtlSeconds` | 3600 | Time-to-live for cached tokens in seconds |

### Example Configuration URLs

#### Basic Development Setup
```
jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000;retryMaxAttempts=1
```

#### Production with Optimizations
```
jdbc:dynamodb:partiql:region=us-east-1;schemaOptimizations=true;lazyLoadingStrategy=PREDICTIVE;preloadStrategy=STARTUP;preloadStartupTables=users,orders,products
```

#### High-Performance Configuration
```
jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=auto;sampleSize=1500;maxConcurrentSchemaDiscoveries=8;lazyLoadingStrategy=PREDICTIVE;preloadStrategy=PATTERN_BASED;cacheWarmingIntervalMs=1800000
```

## 📊 Monitoring and Observability

### Query Metrics

Access comprehensive metrics about your queries:

```java
DynamoDbConnection conn = (DynamoDbConnection) DriverManager.getConnection(url);

// Query performance metrics
QueryMetrics queryMetrics = conn.getQueryMetrics();
System.out.println("Total queries: " + queryMetrics.getTotalQueries());
System.out.println("Average execution time: " + queryMetrics.getAverageExecutionTimeMs() + "ms");
System.out.println("Error rate: " + (queryMetrics.getErrorRate() * 100) + "%");

// Capacity consumption
System.out.println("Read capacity consumed: " + queryMetrics.getTotalReadCapacityUnits());
System.out.println("Write capacity consumed: " + queryMetrics.getTotalWriteCapacityUnits());

// Throttling detection
System.out.println("Throttling events: " + queryMetrics.getThrottlingEvents());
```

### Schema Cache Information

```java
SchemaCache schemaCache = conn.getSchemaCache();

// Get schema for a specific table
Map<String, Integer> tableSchema = schemaCache.getTableSchema("MyTable");

// Cache statistics
Map<String, Object> cacheStats = schemaCache.getCacheStats();
System.out.println("Cache hit rate: " + cacheStats.get("hitRate"));
System.out.println("Cached schemas: " + cacheStats.get("size"));

// Manually refresh a table's schema
schemaCache.refreshTableSchema("MyTable");
```

### Correlation Tracing

The driver automatically generates correlation IDs for operation tracing and includes them in SLF4J MDC for consistent logging across multi-page queries.

## 🔍 PartiQL Query Examples

### Basic Queries

```sql
-- Select all items from a table
SELECT * FROM MyTable;

-- Select with condition
SELECT * FROM MyTable WHERE id = '12345';

-- Select specific attributes
SELECT id, name, email FROM Users WHERE status = 'active';

-- Select with LIKE operator
SELECT * FROM Products WHERE name LIKE '%widget%';
```

### Advanced Queries

```sql
-- Query with nested attributes
SELECT id, address.city, address.zipcode FROM Users WHERE address.state = 'CA';

-- Query with array contains
SELECT * FROM Orders WHERE tags CONTAINS 'priority';

-- Query with BETWEEN
SELECT * FROM Transactions WHERE amount BETWEEN 100 AND 1000;

-- Query with IN operator
SELECT * FROM Users WHERE status IN ('active', 'pending');
```

### Limitations

Due to DynamoDB's NoSQL nature, certain SQL operations are not supported:
- Complex JOINs between tables
- Aggregate functions (SUM, COUNT, AVG) across partitions
- ORDER BY on non-key attributes without filtering
- Transactions spanning multiple tables
- Foreign key constraints

## 🧪 Testing

### Integration Testing with Testcontainers

The driver fully supports Testcontainers for integration testing:

```java
@Test
void testWithDynamoDBLocal() {
    try (GenericContainer<?> dynamodb = new GenericContainer<>("amazon/dynamodb-local:1.20.0")
            .withExposedPorts(8000)) {
        
        dynamodb.start();
        
        String url = String.format(
            "jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:%d",
            dynamodb.getMappedPort(8000)
        );
        
        try (Connection conn = DriverManager.getConnection(url)) {
            // Your test code here
        }
    }
}
```

### Local Development

```bash
# Start DynamoDB Local
docker run -d -p 8000:8000 amazon/dynamodb-local:1.20.0

# Use local endpoint
jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000
```

## 🔒 Security Considerations

### AWS Credentials

- **Never hardcode credentials** in connection strings for production
- Use IAM roles, instance profiles, or credential files
- For GUI clients, consider using temporary credentials or profiles
- Regularly rotate access keys

### Permissions

Minimum required IAM permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PartiQLSelect",
                "dynamodb:DescribeTable",
                "dynamodb:ListTables"
            ],
            "Resource": "*"
        }
    ]
}
```

For write operations, add:
- `dynamodb:PartiQLInsert`
- `dynamodb:PartiQLUpdate`
- `dynamodb:PartiQLDelete`

## 📝 Logging Configuration

The driver uses SLF4J for structured logging. Configure your logging framework to include correlation IDs:

### Logback Example

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level [%X{correlationId}] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <!-- Set driver log level -->
    <logger name="org.cjgratacos.jdbc" level="INFO" />
    
    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

### Log Levels

- **TRACE**: Detailed execution flow
- **DEBUG**: Query details, timing, capacity consumption
- **INFO**: Query summaries, retry notifications
- **WARN**: Performance warnings, retry attempts
- **ERROR**: Query failures, connection errors

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) for details on how to submit pull requests, report issues, and contribute to the project.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/cjgratacos/dynamodb-partiql-jdbc.git
cd dynamodb-partiql-jdbc

# Build the project
./mvnw clean compile

# Run tests
./mvnw test

# Run integration tests (requires Docker)
./mvnw verify

# Format code
./mvnw spotless:apply
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🐛 Issues and Support

- **Bug Reports**: [GitHub Issues](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)

## 🔗 Related Projects

- [AWS SDK for Java](https://github.com/aws/aws-sdk-java-v2)
- [PartiQL](https://partiql.org/)
- [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
- [Testcontainers](https://www.testcontainers.org/)

---

**Disclaimer**: This is an unofficial JDBC driver for DynamoDB. It is not affiliated with or endorsed by Amazon Web Services (AWS). DynamoDB is a trademark of Amazon.com, Inc. or its affiliates.