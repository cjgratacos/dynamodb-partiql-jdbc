# Updatable ResultSets

The DynamoDB PartiQL JDBC driver supports updatable ResultSets, allowing you to modify data directly through the ResultSet interface using standard JDBC methods like `updateRow()`, `deleteRow()`, and `insertRow()`.

## Overview

Updatable ResultSets provide a cursor-based approach to data modification. Instead of writing separate UPDATE, DELETE, or INSERT statements, you can navigate through query results and modify them directly.

## Creating Updatable ResultSets

### Basic Configuration

```java
// Create an updatable ResultSet
Statement stmt = conn.createStatement(
    ResultSet.TYPE_SCROLL_INSENSITIVE,
    ResultSet.CONCUR_UPDATABLE
);

ResultSet rs = stmt.executeQuery("SELECT * FROM Products WHERE category = 'Electronics'");

// Navigate and update
while (rs.next()) {
    BigDecimal currentPrice = rs.getBigDecimal("price");
    
    // Apply 10% discount
    rs.updateBigDecimal("price", currentPrice.multiply(new BigDecimal("0.9")));
    rs.updateTimestamp("lastModified", new Timestamp(System.currentTimeMillis()));
    rs.updateRow(); // Apply changes to database
}
```

### PreparedStatement with Updatable ResultSet

```java
// Using PreparedStatement for updatable results
PreparedStatement ps = conn.prepareStatement(
    "SELECT * FROM Users WHERE status = ?",
    ResultSet.TYPE_SCROLL_SENSITIVE,
    ResultSet.CONCUR_UPDATABLE
);

ps.setString(1, "pending");
ResultSet rs = ps.executeQuery();

while (rs.next()) {
    // Activate pending users
    rs.updateString("status", "active");
    rs.updateTimestamp("activatedAt", new Timestamp(System.currentTimeMillis()));
    rs.updateRow();
}
```

## Update Operations

### Updating Existing Rows

```java
// Find and update specific row
ResultSet rs = stmt.executeQuery("SELECT * FROM Products WHERE productId = 'prod123'");

if (rs.next()) {
    // Update multiple fields
    rs.updateString("name", "Updated Product Name");
    rs.updateBigDecimal("price", new BigDecimal("49.99"));
    rs.updateInt("quantity", 100);
    rs.updateString("status", "in_stock");
    
    // Commit changes to database
    rs.updateRow();
    
    System.out.println("Product updated successfully");
}
```

### Conditional Updates

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM Inventory WHERE quantity < 10");

while (rs.next()) {
    String productId = rs.getString("productId");
    int currentQty = rs.getInt("quantity");
    
    // Restock low inventory
    rs.updateInt("quantity", currentQty + 50);
    rs.updateString("restockStatus", "completed");
    rs.updateTimestamp("restockDate", new Timestamp(System.currentTimeMillis()));
    
    try {
        rs.updateRow();
        System.out.println("Restocked product: " + productId);
    } catch (SQLException e) {
        System.err.println("Failed to restock: " + productId);
    }
}
```

## Insert Operations

### Inserting New Rows

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM Users");

// Move to insert row
rs.moveToInsertRow();

// Set values for new row
rs.updateString("userId", "user999");
rs.updateString("name", "New User");
rs.updateString("email", "newuser@example.com");
rs.updateString("status", "active");
rs.updateTimestamp("createdAt", new Timestamp(System.currentTimeMillis()));

// Insert the row
rs.insertRow();

// Return to current row
rs.moveToCurrentRow();

System.out.println("New user inserted");
```

### Bulk Inserts with ResultSet

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM Products LIMIT 1");

// Insert multiple products
List<Product> newProducts = getNewProducts();

for (Product product : newProducts) {
    rs.moveToInsertRow();
    
    rs.updateString("productId", product.getId());
    rs.updateString("name", product.getName());
    rs.updateBigDecimal("price", product.getPrice());
    rs.updateString("category", product.getCategory());
    rs.updateInt("quantity", product.getQuantity());
    
    rs.insertRow();
}

rs.moveToCurrentRow();
System.out.println("Inserted " + newProducts.size() + " products");
```

## Delete Operations

### Deleting Rows

```java
// Delete inactive users
ResultSet rs = stmt.executeQuery(
    "SELECT * FROM Users WHERE status = 'inactive' AND lastLogin < '2023-01-01'"
);

int deletedCount = 0;
while (rs.next()) {
    String userId = rs.getString("userId");
    
    try {
        rs.deleteRow();
        deletedCount++;
        System.out.println("Deleted user: " + userId);
    } catch (SQLException e) {
        System.err.println("Failed to delete user: " + userId);
    }
}

System.out.println("Total deleted: " + deletedCount);
```

### Conditional Deletes

```java
// Delete with business logic
ResultSet rs = stmt.executeQuery("SELECT * FROM Orders WHERE status = 'cancelled'");

while (rs.next()) {
    Timestamp cancelledAt = rs.getTimestamp("cancelledAt");
    
    // Only delete if cancelled more than 30 days ago
    long daysSinceCancelled = 
        (System.currentTimeMillis() - cancelledAt.getTime()) / (1000 * 60 * 60 * 24);
    
    if (daysSinceCancelled > 30) {
        rs.deleteRow();
    }
}
```

## Advanced Features

### Refreshing Row Data

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM Products WHERE productId = 'prod123'");

if (rs.next()) {
    // Display current price
    System.out.println("Current price: " + rs.getBigDecimal("price"));
    
    // Pause for potential external updates
    Thread.sleep(5000);
    
    // Refresh to get latest data
    rs.refreshRow();
    
    // Display potentially updated price
    System.out.println("Refreshed price: " + rs.getBigDecimal("price"));
}
```

### Detecting Changes

```java
// Check if row was updated
ResultSet rs = stmt.executeQuery("SELECT * FROM Orders");

while (rs.next()) {
    // Store original values
    String originalStatus = rs.getString("status");
    
    // Make changes
    rs.updateString("status", "processed");
    
    // Check if actually changed
    if (rs.rowUpdated()) {
        System.out.println("Row was updated");
    }
    
    // Commit changes
    rs.updateRow();
}
```

### Scrollable ResultSets

```java
// Create scrollable, updatable ResultSet
Statement stmt = conn.createStatement(
    ResultSet.TYPE_SCROLL_INSENSITIVE,
    ResultSet.CONCUR_UPDATABLE
);

ResultSet rs = stmt.executeQuery("SELECT * FROM Products");

// Navigate to last row
rs.last();
System.out.println("Last product: " + rs.getString("name"));

// Navigate to first row
rs.first();
System.out.println("First product: " + rs.getString("name"));

// Navigate to specific row
rs.absolute(5); // 5th row
rs.updateBigDecimal("price", new BigDecimal("99.99"));
rs.updateRow();

// Navigate relative
rs.relative(-2); // Move back 2 rows
rs.updateString("status", "featured");
rs.updateRow();
```

## Best Practices

### 1. Use Appropriate Cursor Types

```java
// For small result sets that need updating
Statement stmt1 = conn.createStatement(
    ResultSet.TYPE_SCROLL_INSENSITIVE,  // Can navigate freely
    ResultSet.CONCUR_UPDATABLE          // Can update
);

// For large result sets with forward-only updates
Statement stmt2 = conn.createStatement(
    ResultSet.TYPE_FORWARD_ONLY,        // Memory efficient
    ResultSet.CONCUR_UPDATABLE          // Can still update
);
```

### 2. Handle Primary Keys Properly

```java
// Ensure primary key columns are included in SELECT
// BAD: Missing primary key
ResultSet rs = stmt.executeQuery("SELECT name, price FROM Products");

// GOOD: Includes primary key
ResultSet rs = stmt.executeQuery("SELECT productId, name, price FROM Products");

// Primary key is required for updates/deletes
while (rs.next()) {
    rs.updateBigDecimal("price", newPrice);
    rs.updateRow(); // Needs productId to identify row
}
```

### 3. Validate Before Updates

```java
public void safeUpdate(ResultSet rs, String column, Object newValue) 
        throws SQLException {
    
    // Get current value
    Object currentValue = rs.getObject(column);
    
    // Validate change
    if (isValidUpdate(currentValue, newValue)) {
        // Update based on type
        if (newValue instanceof String) {
            rs.updateString(column, (String) newValue);
        } else if (newValue instanceof BigDecimal) {
            rs.updateBigDecimal(column, (BigDecimal) newValue);
        } else if (newValue instanceof Integer) {
            rs.updateInt(column, (Integer) newValue);
        }
        // Add more types as needed
        
        rs.updateRow();
    } else {
        throw new SQLException("Invalid update: " + currentValue + " -> " + newValue);
    }
}
```

### 4. Batch Updates for Performance

```java
// Group updates before applying
ResultSet rs = stmt.executeQuery("SELECT * FROM Products WHERE category = 'Electronics'");

int batchCount = 0;
while (rs.next()) {
    // Make multiple field updates
    rs.updateBigDecimal("price", calculateNewPrice(rs));
    rs.updateString("status", determineStatus(rs));
    rs.updateTimestamp("lastModified", new Timestamp(System.currentTimeMillis()));
    
    // Don't update immediately
    batchCount++;
    
    // Update in batches
    if (batchCount % 100 == 0) {
        rs.updateRow();
    }
}

// Final update
if (batchCount % 100 != 0) {
    rs.updateRow();
}
```

## Error Handling

### Handling Update Conflicts

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM Products");

while (rs.next()) {
    try {
        // Attempt update
        rs.updateBigDecimal("price", newPrice);
        rs.updateRow();
        
    } catch (SQLException e) {
        if (e.getMessage().contains("Concurrent modification")) {
            // Refresh and retry
            rs.refreshRow();
            BigDecimal currentPrice = rs.getBigDecimal("price");
            
            // Recalculate based on current value
            BigDecimal adjustedPrice = recalculatePrice(currentPrice);
            rs.updateBigDecimal("price", adjustedPrice);
            rs.updateRow();
            
        } else {
            throw e; // Re-throw other errors
        }
    }
}
```

### Validation Errors

```java
public void updateWithValidation(ResultSet rs) throws SQLException {
    while (rs.next()) {
        try {
            // Validate before update
            String newStatus = determineNewStatus(rs);
            
            if (isValidStatus(newStatus)) {
                rs.updateString("status", newStatus);
                rs.updateRow();
            } else {
                logInvalidUpdate(rs.getString("id"), newStatus);
            }
            
        } catch (SQLException e) {
            if (e.getSQLState().equals("22001")) {
                // Data too long
                System.err.println("Value too long for column");
            } else if (e.getSQLState().equals("23000")) {
                // Constraint violation
                System.err.println("Constraint violation");
            } else {
                throw e;
            }
        }
    }
}
```

## Performance Considerations

### Fetch Size Optimization

```java
// Set appropriate fetch size for large updates
Statement stmt = conn.createStatement(
    ResultSet.TYPE_FORWARD_ONLY,
    ResultSet.CONCUR_UPDATABLE
);

stmt.setFetchSize(100); // Fetch 100 rows at a time

ResultSet rs = stmt.executeQuery("SELECT * FROM LargeTable");

// Process in chunks
while (rs.next()) {
    processRow(rs);
    rs.updateRow();
}
```

### Memory Management

```java
// For very large result sets
public void processLargeDataSet(Connection conn) throws SQLException {
    // Use forward-only cursor to minimize memory
    Statement stmt = conn.createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE
    );
    
    // Enable streaming
    stmt.setFetchSize(Integer.MIN_VALUE);
    
    ResultSet rs = stmt.executeQuery("SELECT * FROM VeryLargeTable");
    
    int processed = 0;
    while (rs.next()) {
        updateRow(rs);
        
        if (++processed % 1000 == 0) {
            System.out.println("Processed: " + processed);
            System.gc(); // Suggest garbage collection
        }
    }
}
```

## Integration Examples

### Spring JDBC Template

```java
@Repository
public class ProductRepository {
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    public void updateProductPrices(final BigDecimal multiplier) {
        jdbcTemplate.execute((Connection conn) -> {
            Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE
            );
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM Products");
            
            while (rs.next()) {
                BigDecimal currentPrice = rs.getBigDecimal("price");
                rs.updateBigDecimal("price", currentPrice.multiply(multiplier));
                rs.updateRow();
            }
            
            return null;
        });
    }
}
```

### Hibernate ScrollableResults

```java
@Transactional
public void bulkUpdateWithHibernate(Session session) {
    String hql = "FROM Product WHERE category = :category";
    
    Query query = session.createQuery(hql);
    query.setParameter("category", "Electronics");
    
    ScrollableResults results = query.scroll(ScrollMode.FORWARD_ONLY);
    
    int count = 0;
    while (results.next()) {
        Product product = (Product) results.get(0);
        product.setPrice(product.getPrice().multiply(new BigDecimal("0.9")));
        
        if (++count % 50 == 0) {
            session.flush();
            session.clear();
        }
    }
    
    results.close();
}
```

## Limitations and Considerations

### DynamoDB-Specific Limitations

1. **No server-side cursors**: All data is fetched to client
2. **Primary key required**: Updates/deletes need full primary key
3. **No partial updates**: Entire item is replaced
4. **Optimistic locking**: Consider using version attributes

### Working with Limitations

```java
// Implement optimistic locking
public void updateWithVersion(ResultSet rs) throws SQLException {
    while (rs.next()) {
        int currentVersion = rs.getInt("version");
        
        // Make updates
        rs.updateString("data", newData);
        rs.updateInt("version", currentVersion + 1);
        
        // This will fail if version changed
        try {
            rs.updateRow();
        } catch (SQLException e) {
            if (e.getMessage().contains("ConditionalCheckFailed")) {
                throw new OptimisticLockException("Version mismatch");
            }
            throw e;
        }
    }
}
```

## Debugging and Monitoring

### Enable ResultSet Logging

```java
// Enable detailed logging
Properties props = new Properties();
props.setProperty("resultset.logging.enabled", "true");
props.setProperty("resultset.logging.updates", "true");

// Logs will show:
// - Update operations
// - Changed values
// - Row positions
// - Execution times
```

### Track Update Statistics

```java
public class UpdateTracker {
    private int updates = 0;
    private int inserts = 0;
    private int deletes = 0;
    
    public void trackResultSetOperations(ResultSet rs) throws SQLException {
        while (rs.next()) {
            // Perform operation
            
            if (rs.rowUpdated()) updates++;
            if (rs.rowInserted()) inserts++;
            if (rs.rowDeleted()) deletes++;
        }
        
        System.out.println("Operations - Updates: " + updates + 
                          ", Inserts: " + inserts + 
                          ", Deletes: " + deletes);
    }
}
```
