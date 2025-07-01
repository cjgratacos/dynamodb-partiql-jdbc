# DynamoDB PartiQL JDBC Driver Documentation

Welcome to the comprehensive documentation for the DynamoDB PartiQL JDBC driver. This documentation provides in-depth guides, examples, and best practices for using all features of the driver.

## About This Documentation

This documentation is organized into two main sections:

- **Core Features**: Essential functionality that most users will need
- **Advanced Topics**: Specialized guides for performance, security, and troubleshooting

Each guide includes:
- 📋 Configuration examples
- 💻 Code samples in Java
- ✅ Best practices
- ⚠️ Common pitfalls to avoid
- 🔧 Troubleshooting tips

## Quick Navigation

### Core Features

1. **[Connection Pooling](connection-pooling.md)**
   - Built-in connection pool management
   - Configuration options and best practices
   - Integration with external pool libraries
   - Performance tuning and monitoring

2. **[Prepared Statement Caching](prepared-statement-caching.md)**
   - Automatic statement caching for improved performance
   - Cache configuration and management
   - Memory optimization strategies
   - Integration with ORMs

3. **[Batch Operations](batch-operations.md)**
   - Efficient batch INSERT, UPDATE, and DELETE
   - Error handling and partial failures
   - Performance optimization techniques
   - Large dataset processing

4. **[Transaction Support](transactions.md)**
   - ACID transactions with DynamoDB TransactWriteItems
   - Isolation levels and consistency
   - Multi-table transactions
   - Error handling and retry strategies

5. **[Updatable ResultSets](updatable-resultsets.md)**
   - Direct data modification through ResultSet
   - Insert, update, and delete operations
   - Scrollable and sensitive ResultSets
   - Best practices and limitations

6. **[Lambda Integration](lambda-integration.md)**
   - Execute AWS Lambda functions as stored procedures
   - Separate credentials for Lambda and DynamoDB
   - Environment variables and configuration
   - Security and performance considerations

### Advanced Topics

7. **[Schema Discovery](schema-discovery.md)**
   - Automatic schema detection strategies
   - Schema caching and management
   - Handling schema evolution
   - Integration with database tools

8. **[Performance Tuning](performance-tuning.md)**
   - Connection optimization techniques
   - Query performance best practices
   - Batch and transaction optimization
   - JVM tuning recommendations

9. **[Security Guide](security.md)**
   - Authentication and authorization
   - Encryption and credential management
   - Secure coding practices
   - Audit and compliance

10. **[Troubleshooting](troubleshooting.md)**
    - Common connection issues
    - Query and performance problems
    - Type conversion and transaction errors
    - Debugging tips and diagnostics

## Quick Start Examples

### Basic Connection

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

String url = "jdbc:dynamodb:partiql:";
Connection conn = DriverManager.getConnection(url, props);
```

### Query Execution

```java
PreparedStatement ps = conn.prepareStatement(
    "SELECT * FROM Users WHERE userId = ?"
);
ps.setString(1, "user123");

ResultSet rs = ps.executeQuery();
while (rs.next()) {
    System.out.println("Name: " + rs.getString("name"));
}
```

### Batch Operations

```java
PreparedStatement ps = conn.prepareStatement(
    "INSERT INTO Products VALUE {'id': ?, 'name': ?, 'price': ?}"
);

for (Product product : products) {
    ps.setString(1, product.getId());
    ps.setString(2, product.getName());
    ps.setBigDecimal(3, product.getPrice());
    ps.addBatch();
}

int[] results = ps.executeBatch();
```

### Transactions

```java
conn.setAutoCommit(false);

try {
    // Multiple operations
    executeUpdate1(conn);
    executeUpdate2(conn);
    executeUpdate3(conn);
    
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
    throw e;
}
```

### Lambda Functions

```java
CallableStatement cs = conn.prepareCall(
    "{call lambda:processOrder(?, ?, ?)}"
);
cs.setString(1, "order123");
cs.setDouble(2, 99.99);
cs.registerOutParameter(3, Types.VARCHAR);

cs.execute();
String result = cs.getString(3);
```

## Configuration Reference

### Essential Properties

| Property | Description | Default |
|----------|-------------|---------|
| `region` | AWS region | Required |
| `credentialsType` | Credential provider type | `DEFAULT` |
| `endpoint` | Custom DynamoDB endpoint | AWS default |
| `connectionPool.enabled` | Enable connection pooling | `true` |
| `preparedStatementCache.enabled` | Enable statement caching | `true` |
| `batch.size` | Max items per batch | `25` |
| `transaction.maxItems` | Max items per transaction | `25` |

### Advanced Properties

See individual feature documentation for comprehensive configuration options.

## Best Practices

1. **Connection Management**
   - Always use try-with-resources
   - Configure appropriate pool sizes
   - Monitor connection metrics

2. **Query Performance**
   - Use prepared statements
   - Enable statement caching
   - Batch similar operations

3. **Error Handling**
   - Implement retry logic
   - Handle partial failures
   - Use transactions appropriately

4. **Security**
   - Never hardcode credentials
   - Use IAM roles when possible
   - Apply least privilege principle

## Troubleshooting

### Common Issues

1. **Connection Pool Exhaustion**
   - Increase pool size
   - Check for connection leaks
   - Monitor pool metrics

2. **Performance Issues**
   - Enable statement caching
   - Use batch operations
   - Optimize fetch size

3. **Transaction Failures**
   - Check item limits
   - Handle conflicts properly
   - Monitor retry metrics

## Support

- **Bug Reports**: [GitHub Issues](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)
- **Documentation Updates**: Pull requests welcome!

## Version Compatibility

| Driver Version | DynamoDB API | Java Version | JDBC Version |
|----------------|--------------|--------------|--------------|
| 1.0.0 | 2.x | 11+ | 4.2 |

## Documentation Structure

```
docs/
├── README.md                    # This file - documentation index
├── connection-pooling.md        # Connection pool configuration and management
├── prepared-statement-caching.md # Statement caching for performance
├── batch-operations.md          # Batch INSERT, UPDATE, DELETE operations
├── transactions.md              # Transaction support with TransactWriteItems
├── updatable-resultsets.md      # Modifying data through ResultSet
├── lambda-integration.md        # AWS Lambda as stored procedures
├── schema-discovery.md          # Automatic schema detection
├── performance-tuning.md        # Performance optimization guide
├── security.md                  # Security best practices
└── troubleshooting.md          # Common issues and solutions
```

## Getting Started

If you're new to the driver, we recommend reading the documentation in this order:

1. Start with the main [README](../README.md) for installation and basic usage
2. Learn about [Connection Pooling](connection-pooling.md) for efficient resource management
3. Understand [Prepared Statement Caching](prepared-statement-caching.md) for better performance
4. Explore [Batch Operations](batch-operations.md) for bulk data operations
5. Read the [Performance Tuning](performance-tuning.md) guide for optimization
6. Review the [Security Guide](security.md) for production deployments

## Feature Matrix

| Feature | Documentation | Complexity | Performance Impact |
|---------|--------------|------------|-------------------|
| Connection Pooling | [Guide](connection-pooling.md) | Medium | High (Positive) |
| Statement Caching | [Guide](prepared-statement-caching.md) | Low | High (Positive) |
| Batch Operations | [Guide](batch-operations.md) | Medium | High (Positive) |
| Transactions | [Guide](transactions.md) | High | Medium |
| Updatable ResultSets | [Guide](updatable-resultsets.md) | Medium | Low |
| Lambda Integration | [Guide](lambda-integration.md) | High | Variable |
| Schema Discovery | [Guide](schema-discovery.md) | Low | Medium |

## Common Use Cases

### Web Applications
- [Connection Pooling](connection-pooling.md) - Essential for handling concurrent requests
- [Prepared Statement Caching](prepared-statement-caching.md) - Improves query performance
- [Security Guide](security.md) - Secure credential management

### Batch Processing
- [Batch Operations](batch-operations.md) - Efficient bulk data processing
- [Performance Tuning](performance-tuning.md) - Optimize for throughput
- [Transactions](transactions.md) - Ensure data consistency

### Data Migration
- [Schema Discovery](schema-discovery.md) - Understand source table structures
- [Batch Operations](batch-operations.md) - Efficient data transfer
- [Troubleshooting](troubleshooting.md) - Handle edge cases

### Business Logic
- [Lambda Integration](lambda-integration.md) - Complex calculations and workflows
- [Transactions](transactions.md) - ACID guarantees
- [Updatable ResultSets](updatable-resultsets.md) - Interactive data modification

## Need Help?

- 🐛 **Found a bug?** Check the [Troubleshooting Guide](troubleshooting.md) first
- 💡 **Have a question?** Review the relevant feature documentation
- 🚀 **Want to contribute?** See our [Contributing Guidelines](../CONTRIBUTING.md)
- 📧 **Still stuck?** Open an issue on [GitHub](https://github.com/cjgratacos/dynamodb-partiql-jdbc/issues)

## License

This project is licensed under the MIT License. See the [LICENSE](../LICENSE) file for details.
