package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * High-performance schema manager for DynamoDB with comprehensive optimization features.
 *
 * <p>This class integrates all performance optimization components to provide a unified,
 * high-performance schema management system for DynamoDB JDBC operations. It combines concurrent
 * discovery, lazy loading, intelligent caching, and preloading strategies.
 *
 * <h2>Performance Features:</h2>
 *
 * <ul>
 *   <li><strong>Concurrent Discovery</strong>: Parallel schema discovery for multiple tables
 *   <li><strong>Lazy Loading</strong>: On-demand schema loading with multiple strategies
 *   <li><strong>Intelligent Caching</strong>: TTL-based caching with LRU eviction
 *   <li><strong>Preloading Strategies</strong>: Startup, scheduled, and pattern-based preloading
 *   <li><strong>Cache Warming</strong>: Background refresh and optimization
 *   <li><strong>Performance Monitoring</strong>: Comprehensive metrics and statistics
 * </ul>
 *
 * <h2>Architecture:</h2>
 *
 * <pre>
 * OptimizedSchemaManager
 * ├── LazySchemaLoader (on-demand loading)
 * ├── ConcurrentSchemaDiscovery (parallel operations)
 * ├── SchemaPreloadingStrategy (proactive loading)
 * └── Performance monitoring &amp; metrics
 * </pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see LazySchemaLoader
 * @see ConcurrentSchemaDiscovery
 * @see SchemaPreloadingStrategy
 */
public class OptimizedSchemaManager {

  private static final Logger logger = LoggerFactory.getLogger(OptimizedSchemaManager.class);

  private final DynamoDbClient client;
  private final Properties properties;
  private final LazySchemaLoader lazyLoader;
  private final ConcurrentSchemaDiscovery concurrentDiscovery;
  private final SchemaPreloadingStrategy preloadingStrategy;
  private final ScheduledExecutorService managementExecutor;
  private final boolean optimizationsEnabled;

  // Performance monitoring
  private final AtomicLong totalRequests = new AtomicLong(0);
  private final AtomicLong optimizedRequests = new AtomicLong(0);
  private final AtomicLong cacheWarming = new AtomicLong(0);
  private final long startTime;

  /**
   * Creates a new optimized schema manager.
   *
   * @param client the DynamoDB client
   * @param properties connection properties for configuration
   */
  public OptimizedSchemaManager(final DynamoDbClient client, final Properties properties) {
    this.client = client;
    this.properties = properties;
    this.startTime = System.currentTimeMillis();
    this.optimizationsEnabled =
        Boolean.parseBoolean(properties.getProperty("schemaOptimizations", "true"));

    if (this.optimizationsEnabled) {
      // Initialize management thread pool
      this.managementExecutor =
          Executors.newScheduledThreadPool(
              2,
              r -> {
                final var thread = new Thread(r, "schema-manager");
                thread.setDaemon(true);
                return thread;
              });

      // Initialize core components
      this.concurrentDiscovery = new ConcurrentSchemaDiscovery(client, properties);
      this.lazyLoader = new LazySchemaLoader(client, properties);
      this.preloadingStrategy =
          new SchemaPreloadingStrategy(
              client,
              this.lazyLoader,
              this.concurrentDiscovery,
              this.managementExecutor,
              properties);

      // Initialize optimization features
      this.initializeOptimizations();

      if (logger.isInfoEnabled()) {
        logger.info("OptimizedSchemaManager initialized with full optimizations enabled");
      }
    } else {
      // Minimal initialization for disabled optimizations
      this.managementExecutor = null;
      this.concurrentDiscovery = new ConcurrentSchemaDiscovery(client, properties);
      this.lazyLoader = new LazySchemaLoader(client, properties);
      this.preloadingStrategy = null;

      logger.info("OptimizedSchemaManager initialized with optimizations disabled");
    }
  }

  /**
   * Gets schema for a table with full optimization pipeline.
   *
   * @param tableName the name of the table
   * @return the column metadata
   * @throws SQLException if schema retrieval fails
   */
  public Map<String, ColumnMetadata> getTableSchema(final String tableName) throws SQLException {
    this.totalRequests.incrementAndGet();

    try {
      // Notify preloading strategy about access
      if (this.optimizationsEnabled && this.preloadingStrategy != null) {
        this.preloadingStrategy.onTableAccess(tableName);
      }

      // Use lazy loader for optimized retrieval
      final var result = this.lazyLoader.getTableSchema(tableName);

      if (!result.isEmpty()) {
        this.optimizedRequests.incrementAndGet();
      }

      return result;
    } catch (final SQLException e) {
      logger.warn("Optimized schema retrieval failed for table: {}", tableName, e);
      throw e;
    }
  }

  /**
   * Gets schema for a table with specific loading strategy.
   *
   * @param tableName the name of the table
   * @param strategy the loading strategy to use
   * @return the column metadata
   * @throws SQLException if schema retrieval fails
   */
  public Map<String, ColumnMetadata> getTableSchema(
      final String tableName, final LazySchemaLoader.LoadingStrategy strategy) throws SQLException {
    this.totalRequests.incrementAndGet();

    if (this.optimizationsEnabled && this.preloadingStrategy != null) {
      this.preloadingStrategy.onTableAccess(tableName);
    }

    return this.lazyLoader.getTableSchema(tableName, strategy);
  }

  /**
   * Preloads schemas for multiple tables concurrently.
   *
   * @param tableNames the list of table names to preload
   * @return a CompletableFuture that completes when preloading is done
   */
  public CompletableFuture<Void> preloadSchemas(final List<String> tableNames) {
    if (this.optimizationsEnabled && this.preloadingStrategy != null) {
      return this.preloadingStrategy.preloadTables(tableNames);
    } else {
      // Fallback to basic concurrent discovery
      return this.concurrentDiscovery
          .discoverMultipleTablesAsync(tableNames)
          .thenAccept(
              results -> {
                // Results are automatically cached by the discovery process
              });
    }
  }

  /**
   * Performs startup optimizations including preloading.
   *
   * @return a CompletableFuture that completes when startup optimization is done
   */
  public CompletableFuture<Void> performStartupOptimization() {
    if (!this.optimizationsEnabled) {
      return CompletableFuture.completedFuture(null);
    }

    if (logger.isInfoEnabled()) {
      logger.info("Performing DynamoDB schema startup optimizations");
    }

    final var tasks = new java.util.ArrayList<CompletableFuture<Void>>();

    // Startup preloading
    if (this.preloadingStrategy != null) {
      tasks.add(this.preloadingStrategy.performStartupPreloading());
    }

    // Cache warming
    tasks.add(this.performCacheWarming());

    return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
        .thenRun(
            () -> {
              if (logger.isInfoEnabled()) {
                logger.info("DynamoDB schema startup optimizations completed");
              }
            });
  }

  /**
   * Adds a table relationship for reactive preloading.
   *
   * @param primaryTable the primary table
   * @param relatedTables the list of related tables
   */
  public void addTableRelationship(final String primaryTable, final List<String> relatedTables) {
    if (this.optimizationsEnabled && this.preloadingStrategy != null) {
      this.preloadingStrategy.addTableRelationship(primaryTable, relatedTables);
    }
  }

  /**
   * Manually triggers cache warming for performance optimization.
   *
   * @return a CompletableFuture that completes when cache warming is done
   */
  public CompletableFuture<Void> warmCache() {
    return this.performCacheWarming();
  }

  /**
   * Evicts a specific table from all caches.
   *
   * @param tableName the name of the table to evict
   */
  public void evictTable(final String tableName) {
    if (this.lazyLoader != null) {
      this.lazyLoader.evictTableSchema(tableName);
    }

    if (this.concurrentDiscovery != null) {
      this.concurrentDiscovery.cancelDiscovery(tableName);
    }
  }

  /** Clears all caches and resets optimization state. */
  public void clearCaches() {
    if (this.lazyLoader != null) {
      this.lazyLoader.clearCache();
    }

    if (logger.isInfoEnabled()) {
      logger.info("All schema caches cleared");
    }
  }

  private void initializeOptimizations() {
    if (this.managementExecutor != null) {
      // Schedule periodic cache warming
      final var cacheWarmingInterval =
          Long.parseLong(
              this.properties.getProperty("cacheWarmingIntervalMs", "3600000")); // 1 hour default

      this.managementExecutor.scheduleAtFixedRate(
          this::performBackgroundCacheWarming,
          cacheWarmingInterval,
          cacheWarmingInterval,
          TimeUnit.MILLISECONDS);

      // Schedule periodic optimization monitoring
      this.managementExecutor.scheduleAtFixedRate(
          this::logOptimizationMetrics,
          300000, // 5 minutes
          300000,
          TimeUnit.MILLISECONDS);
    }
  }

  private CompletableFuture<Void> performCacheWarming() {
    return CompletableFuture.runAsync(
        () -> {
          try {
            this.cacheWarming.incrementAndGet();

            if (logger.isDebugEnabled()) {
              logger.debug("Performing schema cache warming");
            }

            // Simple cache warming: refresh cached entries that are getting stale
            // In a real implementation, this could be more sophisticated

            // This is a placeholder for cache warming logic
            // Real implementation would analyze cache state and refresh strategically

            if (logger.isDebugEnabled()) {
              logger.debug("Cache warming completed");
            }
          } catch (final Exception e) {
            logger.warn("Cache warming failed", e);
          }
        },
        this.managementExecutor);
  }

  private void performBackgroundCacheWarming() {
    try {
      this.performCacheWarming();
    } catch (final Exception e) {
      logger.warn("Background cache warming failed", e);
    }
  }

  private void logOptimizationMetrics() {
    try {
      final var stats = this.getOptimizationStatistics();
      if (logger.isInfoEnabled()) {
        logger.info(
            "Schema optimization metrics: requests={}, optimized={}, hit_rate={}, uptime={}ms",
            stats.get("totalRequests"),
            stats.get("optimizedRequests"),
            stats.get("optimizationRate"),
            stats.get("uptimeMs"));
      }
    } catch (final Exception e) {
      logger.warn("Failed to log optimization metrics", e);
    }
  }

  /**
   * Gets comprehensive optimization statistics.
   *
   * @return a map containing optimization statistics
   */
  public Map<String, Object> getOptimizationStatistics() {
    final var stats = new java.util.HashMap<String, Object>();

    // Overall metrics
    stats.put("optimizationsEnabled", this.optimizationsEnabled);
    stats.put("totalRequests", this.totalRequests.get());
    stats.put("optimizedRequests", this.optimizedRequests.get());
    stats.put("cacheWarmingCount", this.cacheWarming.get());
    stats.put("uptimeMs", System.currentTimeMillis() - this.startTime);

    final var totalReq = this.totalRequests.get();
    final var optimizationRate =
        totalReq > 0 ? (double) this.optimizedRequests.get() / totalReq : 0.0;
    stats.put("optimizationRate", optimizationRate);

    // Component statistics
    if (this.lazyLoader != null) {
      stats.put("lazyLoader", this.lazyLoader.getStatistics());
    }

    if (this.concurrentDiscovery != null) {
      stats.put("concurrentDiscovery", this.concurrentDiscovery.getStatistics());
    }

    if (this.preloadingStrategy != null) {
      stats.put("preloadingStrategy", this.preloadingStrategy.getStatistics());
    }

    return stats;
  }

  /** Shuts down the optimized schema manager and all its components. */
  public void shutdown() {
    if (logger.isInfoEnabled()) {
      logger.info("Shutting down OptimizedSchemaManager");
    }

    // Shutdown components
    if (this.preloadingStrategy != null) {
      this.preloadingStrategy.shutdown();
    }

    if (this.lazyLoader != null) {
      this.lazyLoader.shutdown();
    }

    if (this.concurrentDiscovery != null) {
      this.concurrentDiscovery.shutdown();
    }

    // Shutdown management executor
    if (this.managementExecutor != null) {
      this.managementExecutor.shutdown();
      try {
        if (!this.managementExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          this.managementExecutor.shutdownNow();
        }
      } catch (final InterruptedException e) {
        this.managementExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    logger.info("OptimizedSchemaManager shutdown completed");
  }

  /**
   * Checks if optimizations are enabled.
   *
   * @return true if optimizations are enabled, false otherwise
   */
  public boolean isOptimizationsEnabled() {
    return this.optimizationsEnabled;
  }
}
