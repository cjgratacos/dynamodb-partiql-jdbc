package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Concurrent schema discovery manager for DynamoDB tables with async operations and thread pooling.
 *
 * <p>This class manages concurrent schema discovery operations for multiple DynamoDB tables,
 * providing async discovery, thread pool management, and coordinated caching. It optimizes
 * performance by allowing parallel discovery of schema information from multiple tables.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Concurrent schema discovery for multiple tables using thread pools
 *   <li>Async operations with CompletableFuture for non-blocking calls
 *   <li>Coordinated caching to prevent duplicate discovery operations
 *   <li>Configurable thread pool sizing based on workload characteristics
 *   <li>Bulk discovery operations for efficient batch processing
 *   <li>Graceful shutdown and resource cleanup
 * </ul>
 *
 * <h2>Thread Pool Configuration:</h2>
 *
 * <p>The thread pool size is automatically configured based on:
 *
 * <ul>
 *   <li>Available CPU cores
 *   <li>Expected I/O wait times for DynamoDB operations
 *   <li>User-configurable limits via connection properties
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see EnhancedSchemaDetector
 * @see CachedSchemaEntry
 */
public class ConcurrentSchemaDiscovery {

  private static final Logger logger = LoggerFactory.getLogger(ConcurrentSchemaDiscovery.class);

  private final DynamoDbClient client;
  private final Properties properties;
  private final ExecutorService discoveryExecutor;
  private final Map<String, CompletableFuture<Map<String, ColumnMetadata>>> pendingDiscoveries;
  private final int maxConcurrentDiscoveries;
  private final boolean enabled;

  /**
   * Creates a new concurrent schema discovery manager.
   *
   * @param client the DynamoDB client
   * @param properties connection properties for configuration
   */
  public ConcurrentSchemaDiscovery(final DynamoDbClient client, final Properties properties) {
    this.client = client;
    this.properties = properties;
    this.enabled =
        Boolean.parseBoolean(properties.getProperty("concurrentSchemaDiscovery", "true"));
    this.pendingDiscoveries = new ConcurrentHashMap<>();

    if (this.enabled) {
      // Configure thread pool size: I/O bound operations benefit from more threads
      final var corePoolSize = Runtime.getRuntime().availableProcessors();
      final var defaultMaxConcurrent = corePoolSize * 2;
      this.maxConcurrentDiscoveries =
          parseIntegerProperty(properties, "maxConcurrentSchemaDiscoveries", defaultMaxConcurrent);

      this.discoveryExecutor =
          Executors.newFixedThreadPool(
              this.maxConcurrentDiscoveries,
              r -> {
                final var thread = new Thread(r, "dynamodb-schema-discovery");
                thread.setDaemon(true);
                thread.setUncaughtExceptionHandler(
                    (t, e) ->
                        logger.error(
                            "Uncaught exception in schema discovery thread {}", t.getName(), e));
                return thread;
              });

      if (logger.isInfoEnabled()) {
        logger.info(
            "ConcurrentSchemaDiscovery initialized: maxConcurrent={}, enabled={}",
            this.maxConcurrentDiscoveries,
            this.enabled);
      }
    } else {
      this.discoveryExecutor = null;
      this.maxConcurrentDiscoveries = 0;
      logger.info("Concurrent schema discovery disabled");
    }
  }

  /**
   * Discovers schema for a single table asynchronously.
   *
   * @param tableName the name of the table to discover
   * @return a CompletableFuture containing the discovered column metadata
   */
  public CompletableFuture<Map<String, ColumnMetadata>> discoverTableSchemaAsync(
      final String tableName) {
    if (!this.enabled) {
      // Fallback to synchronous discovery
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              final var detector = new EnhancedSchemaDetector(this.client, this.properties);
              return detector.detectTableColumnMetadata(tableName);
            } catch (final SQLException e) {
              throw new RuntimeException("Schema discovery failed for table " + tableName, e);
            }
          });
    }

    // Check if discovery is already in progress
    var existingFuture = this.pendingDiscoveries.get(tableName);
    if (existingFuture != null && !existingFuture.isDone()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Schema discovery already in progress for table: {}", tableName);
      }
      return existingFuture;
    }

    // Start new discovery
    final var future =
        CompletableFuture.supplyAsync(
            () -> {
              CorrelationContext.newOperation("concurrent-schema-discovery-" + tableName);
              try {
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "Starting concurrent schema discovery for DynamoDB table: {}", tableName);
                }

                final var startTime = System.currentTimeMillis();
                final var detector = new EnhancedSchemaDetector(this.client, this.properties);
                final var result = detector.detectTableColumnMetadata(tableName);
                final var duration = System.currentTimeMillis() - startTime;

                if (logger.isInfoEnabled()) {
                  logger.info(
                      "Concurrent schema discovery completed for table {}: {} attributes in {}ms",
                      tableName,
                      result.size(),
                      duration);
                }

                return result;
              } catch (final Exception e) {
                logger.error("Concurrent schema discovery failed for table: {}", tableName, e);
                throw new RuntimeException("Schema discovery failed for table " + tableName, e);
              } finally {
                CorrelationContext.clear();
              }
            },
            this.discoveryExecutor);

    // Clean up completed future when done
    future.whenComplete((result, throwable) -> this.pendingDiscoveries.remove(tableName));

    this.pendingDiscoveries.put(tableName, future);
    return future;
  }

  /**
   * Discovers schema for multiple tables concurrently.
   *
   * @param tableNames the list of table names to discover
   * @return a CompletableFuture containing a map of table names to their discovered column metadata
   */
  public CompletableFuture<Map<String, Map<String, ColumnMetadata>>> discoverMultipleTablesAsync(
      final List<String> tableNames) {
    if (tableNames == null || tableNames.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }

    if (logger.isInfoEnabled()) {
      logger.info("Starting concurrent schema discovery for {} DynamoDB tables", tableNames.size());
    }

    final var discoveryFutures =
        tableNames.stream()
            .collect(Collectors.toMap(tableName -> tableName, this::discoverTableSchemaAsync));

    // Combine all futures into a single result
    final var allFutures =
        CompletableFuture.allOf(discoveryFutures.values().toArray(new CompletableFuture[0]));

    return allFutures.thenApply(
        v -> {
          final var results = new ConcurrentHashMap<String, Map<String, ColumnMetadata>>();

          for (final var entry : discoveryFutures.entrySet()) {
            try {
              final var tableName = entry.getKey();
              final var future = entry.getValue();
              final var columnMetadata = future.get(); // Should be complete due to allOf()
              results.put(tableName, columnMetadata);
            } catch (final Exception e) {
              logger.warn("Failed to get discovery result for table {}", entry.getKey(), e);
              // Continue with other tables, don't fail the entire batch
            }
          }

          if (logger.isInfoEnabled()) {
            logger.info(
                "Concurrent schema discovery completed for {} out of {} tables",
                results.size(),
                tableNames.size());
          }

          return results;
        });
  }

  /**
   * Discovers schema for a table with timeout.
   *
   * @param tableName the name of the table to discover
   * @param timeoutMs the timeout in milliseconds
   * @return the discovered column metadata
   * @throws SQLException if discovery fails or times out
   */
  public Map<String, ColumnMetadata> discoverTableSchemaWithTimeout(
      final String tableName, final long timeoutMs) throws SQLException {
    try {
      final var future = this.discoverTableSchemaAsync(tableName);
      return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      throw new SQLException("Schema discovery timed out or failed for table " + tableName, e);
    }
  }

  /**
   * Cancels pending discovery for a specific table.
   *
   * @param tableName the name of the table to cancel discovery for
   * @return true if a pending discovery was cancelled, false otherwise
   */
  public boolean cancelDiscovery(final String tableName) {
    final var future = this.pendingDiscoveries.get(tableName);
    if (future != null && !future.isDone()) {
      final var cancelled = future.cancel(true);
      if (cancelled) {
        this.pendingDiscoveries.remove(tableName);
        if (logger.isDebugEnabled()) {
          logger.debug("Cancelled schema discovery for table: {}", tableName);
        }
      }
      return cancelled;
    }
    return false;
  }

  /**
   * Gets the number of currently pending discoveries.
   *
   * @return the number of pending discoveries
   */
  public int getPendingDiscoveryCount() {
    return (int)
        this.pendingDiscoveries.values().stream().filter(future -> !future.isDone()).count();
  }

  /**
   * Gets statistics about concurrent discovery operations.
   *
   * @return a map containing discovery statistics
   */
  public Map<String, Object> getStatistics() {
    return Map.of(
        "enabled",
        this.enabled,
        "maxConcurrentDiscoveries",
        this.maxConcurrentDiscoveries,
        "pendingDiscoveries",
        this.getPendingDiscoveryCount(),
        "totalPendingEntries",
        this.pendingDiscoveries.size(),
        "activeThreads",
        this.enabled && this.discoveryExecutor instanceof java.util.concurrent.ThreadPoolExecutor
            ? ((java.util.concurrent.ThreadPoolExecutor) this.discoveryExecutor).getActiveCount()
            : 0);
  }

  /** Shuts down the concurrent discovery service. */
  public void shutdown() {
    if (this.discoveryExecutor != null) {
      if (logger.isInfoEnabled()) {
        logger.info(
            "Shutting down concurrent schema discovery service with {} pending discoveries",
            this.getPendingDiscoveryCount());
      }

      // Cancel all pending discoveries
      this.pendingDiscoveries.values().forEach(future -> future.cancel(true));
      this.pendingDiscoveries.clear();

      // Shutdown executor
      this.discoveryExecutor.shutdown();
      try {
        if (!this.discoveryExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          this.discoveryExecutor.shutdownNow();
          if (!this.discoveryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.warn("Schema discovery executor did not terminate within timeout");
          }
        }
      } catch (final InterruptedException e) {
        this.discoveryExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      logger.info("Concurrent schema discovery service shutdown completed");
    }
  }

  /**
   * Checks if the concurrent discovery service is enabled.
   *
   * @return true if enabled, false otherwise
   */
  public boolean isEnabled() {
    return this.enabled;
  }

  /**
   * Safely parses an integer property with a fallback default value.
   *
   * @param properties the properties object
   * @param propertyName the name of the property
   * @param defaultValue the default value to use if parsing fails
   * @return the parsed integer or default value
   */
  private int parseIntegerProperty(Properties properties, String propertyName, int defaultValue) {
    try {
      return Integer.parseInt(properties.getProperty(propertyName, String.valueOf(defaultValue)));
    } catch (NumberFormatException e) {
      logger.warn("Invalid value for property '{}', using default: {}", propertyName, defaultValue);
      return defaultValue;
    }
  }
}
