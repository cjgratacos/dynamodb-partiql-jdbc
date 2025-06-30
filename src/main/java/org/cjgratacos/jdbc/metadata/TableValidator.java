package org.cjgratacos.jdbc.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Utility class for validating table and column existence in DynamoDB. Provides caching to minimize
 * API calls for repeated validations.
 */
public class TableValidator {
  private static final Logger logger = LoggerFactory.getLogger(TableValidator.class);

  private final DynamoDbClient dynamoDbClient;
  private final boolean cacheEnabled;
  private final long cacheExpirationMinutes;

  // Cache for table existence checks
  private final Map<String, CachedResult<Boolean>> tableExistenceCache = new ConcurrentHashMap<>();

  // Cache for table columns
  private final Map<String, CachedResult<List<String>>> tableColumnsCache =
      new ConcurrentHashMap<>();

  /**
   * Creates a new TableValidator with caching enabled.
   *
   * @param dynamoDbClient the DynamoDB client
   */
  public TableValidator(DynamoDbClient dynamoDbClient) {
    this(dynamoDbClient, true, 15); // Default 15 minutes cache
  }

  /**
   * Creates a new TableValidator with configurable caching.
   *
   * @param dynamoDbClient the DynamoDB client
   * @param cacheEnabled whether to enable caching
   * @param cacheExpirationMinutes cache expiration time in minutes
   */
  public TableValidator(
      DynamoDbClient dynamoDbClient, boolean cacheEnabled, long cacheExpirationMinutes) {
    this.dynamoDbClient = dynamoDbClient;
    this.cacheEnabled = cacheEnabled;
    this.cacheExpirationMinutes = cacheExpirationMinutes;
  }

  /**
   * Checks if a table exists in DynamoDB.
   *
   * @param tableName the table name to check
   * @return true if the table exists, false otherwise
   */
  public boolean tableExists(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      return false;
    }

    // Check cache first
    if (cacheEnabled) {
      CachedResult<Boolean> cached = tableExistenceCache.get(tableName);
      if (cached != null && !cached.isExpired()) {
        return cached.value;
      }
    }

    // Perform actual check
    boolean exists = performTableExistenceCheck(tableName);

    // Update cache
    if (cacheEnabled) {
      tableExistenceCache.put(tableName, new CachedResult<>(exists, cacheExpirationMinutes));
    }

    return exists;
  }

  /**
   * Validates that a table exists, throwing an exception if it doesn't.
   *
   * @param tableName the table name to validate
   * @throws ForeignKeyValidationException if the table doesn't exist
   */
  public void validateTableExists(String tableName) throws ForeignKeyValidationException {
    if (!tableExists(tableName)) {
      throw new ForeignKeyValidationException(
          String.format("Table '%s' does not exist in DynamoDB", tableName));
    }
  }

  /**
   * Validates multiple tables exist in bulk.
   *
   * @param tableNames the table names to validate
   * @return map of table name to existence status
   */
  public Map<String, Boolean> validateTablesExist(List<String> tableNames) {
    Map<String, Boolean> results = new HashMap<>();

    for (String tableName : tableNames) {
      results.put(tableName, tableExists(tableName));
    }

    return results;
  }

  /**
   * Checks if a column exists in a table. Since DynamoDB is schemaless, this performs a scan with
   * limit 1 and checks if any items contain the specified attribute.
   *
   * @param tableName the table name
   * @param columnName the column (attribute) name
   * @return true if at least one item has the attribute, false otherwise
   */
  public boolean columnExists(String tableName, String columnName) {
    if (!tableExists(tableName)) {
      return false;
    }

    if (columnName == null || columnName.isEmpty()) {
      return false;
    }

    // Check cache first
    if (cacheEnabled) {
      CachedResult<List<String>> cached = tableColumnsCache.get(tableName);
      if (cached != null && !cached.isExpired()) {
        return cached.value.contains(columnName);
      }
    }

    // Get sample columns from table
    List<String> columns = getTableColumns(tableName);

    // Update cache
    if (cacheEnabled) {
      tableColumnsCache.put(tableName, new CachedResult<>(columns, cacheExpirationMinutes));
    }

    return columns.contains(columnName);
  }

  /**
   * Gets a list of columns (attributes) that exist in the table by sampling items.
   *
   * @param tableName the table name
   * @return list of column names found in the table
   */
  public List<String> getTableColumns(String tableName) {
    List<String> columns = new ArrayList<>();

    try {
      // First, get key attributes from table description
      DescribeTableResponse describeResponse =
          dynamoDbClient.describeTable(builder -> builder.tableName(tableName));

      // Add primary key attributes
      describeResponse
          .table()
          .keySchema()
          .forEach(keySchemaElement -> columns.add(keySchemaElement.attributeName()));

      // Sample a few items to find other attributes
      ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).limit(10).build();

      ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

      // Collect all unique attribute names from sampled items
      scanResponse
          .items()
          .forEach(
              item -> {
                item.keySet()
                    .forEach(
                        attr -> {
                          if (!columns.contains(attr)) {
                            columns.add(attr);
                          }
                        });
              });

    } catch (Exception e) {
      logger.warn("Failed to get columns for table {}: {}", tableName, e.getMessage());
    }

    return columns;
  }

  /** Clears all cached validation results. */
  public void clearCache() {
    tableExistenceCache.clear();
    tableColumnsCache.clear();
  }

  /**
   * Clears cached validation results for a specific table.
   *
   * @param tableName the table name to clear from cache
   */
  public void clearTableCache(String tableName) {
    tableExistenceCache.remove(tableName);
    tableColumnsCache.remove(tableName);
  }

  private boolean performTableExistenceCheck(String tableName) {
    try {
      dynamoDbClient.describeTable(builder -> builder.tableName(tableName));
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    } catch (Exception e) {
      logger.warn("Error checking table existence for {}: {}", tableName, e.getMessage());
      return false;
    }
  }

  /** Simple cache entry with expiration. */
  private static class CachedResult<T> {
    final T value;
    final long expirationTime;

    CachedResult(T value, long expirationMinutes) {
      this.value = value;
      this.expirationTime =
          System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(expirationMinutes);
    }

    boolean isExpired() {
      return System.currentTimeMillis() > expirationTime;
    }
  }
}
