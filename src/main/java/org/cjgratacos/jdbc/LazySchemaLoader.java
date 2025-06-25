package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Lazy loading schema manager for DynamoDB with on-demand discovery and intelligent caching.
 *
 * <p>This class provides lazy loading of DynamoDB schema information, only discovering schema when
 * actually needed. It combines intelligent caching, concurrent discovery, and on-demand loading to
 * optimize performance while minimizing unnecessary DynamoDB API calls.
 *
 * <h2>Key Features:</h2>
 *
 * <ul>
 *   <li>On-demand schema discovery triggered by actual usage
 *   <li>Intelligent caching with TTL and LRU eviction
 *   <li>Async loading with CompletableFuture for non-blocking operations
 *   <li>Predictive preloading based on access patterns
 *   <li>Graceful degradation when discovery fails
 *   <li>Memory-efficient storage with configurable limits
 * </ul>
 *
 * <h2>Loading Strategies:</h2>
 *
 * <ul>
 *   <li><strong>IMMEDIATE</strong>: Load schema immediately when requested
 *   <li><strong>BACKGROUND</strong>: Load schema in background thread, return placeholder
 *   <li><strong>CACHED_ONLY</strong>: Only return cached data, no discovery
 *   <li><strong>PREDICTIVE</strong>: Preload based on usage patterns
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see ConcurrentSchemaDiscovery
 * @see CachedSchemaEntry
 */
public class LazySchemaLoader {

  private static final Logger logger = LoggerFactory.getLogger(LazySchemaLoader.class);

  /** Strategy for lazy loading of schema information. */
  public enum LoadingStrategy {
    /** Load schema immediately when requested */
    IMMEDIATE,
    /** Load schema in background threads */
    BACKGROUND,
    /** Only use cached schema, don't load new */
    CACHED_ONLY,
    /** Use predictive algorithms to preload likely schemas */
    PREDICTIVE
  }

  private final DynamoDbClient client;
  private final Properties properties;
  private final ConcurrentSchemaDiscovery concurrentDiscovery;
  private final Map<String, CachedSchemaEntry<ColumnMetadata>> schemaCache;
  private final Map<String, AtomicLong> accessCounts;
  private final LoadingStrategy defaultStrategy;
  private final long cacheTtlMs;
  private final int maxCacheSize;
  private final boolean predictiveLoading;

  // Statistics
  private final AtomicLong cacheHits = new AtomicLong(0);
  private final AtomicLong cacheMisses = new AtomicLong(0);
  private final AtomicLong lazyLoads = new AtomicLong(0);
  private final AtomicLong predictiveLoads = new AtomicLong(0);

  /**
   * Creates a new lazy schema loader.
   *
   * @param client the DynamoDB client
   * @param properties connection properties for configuration
   */
  public LazySchemaLoader(final DynamoDbClient client, final Properties properties) {
    this.client = client;
    this.properties = properties;
    this.concurrentDiscovery = new ConcurrentSchemaDiscovery(client, properties);
    this.schemaCache = new ConcurrentHashMap<>();
    this.accessCounts = new ConcurrentHashMap<>();

    // Configuration
    this.defaultStrategy =
        LoadingStrategy.valueOf(
            properties.getProperty("lazyLoadingStrategy", "IMMEDIATE").toUpperCase());
    this.cacheTtlMs =
        Long.parseLong(properties.getProperty("lazyLoadingCacheTTL", "3600"))
            * 1000; // Convert to ms
    this.maxCacheSize = Integer.parseInt(properties.getProperty("lazyLoadingMaxCacheSize", "1000"));
    this.predictiveLoading =
        Boolean.parseBoolean(properties.getProperty("predictiveSchemaLoading", "true"));

    if (logger.isInfoEnabled()) {
      logger.info(
          "LazySchemaLoader initialized: strategy={}, cacheTTL={}ms, maxCacheSize={}, predictive={}",
          this.defaultStrategy,
          this.cacheTtlMs,
          this.maxCacheSize,
          this.predictiveLoading);
    }
  }

  /**
   * Gets schema for a table using lazy loading with the default strategy.
   *
   * @param tableName the name of the table
   * @return the column metadata, or empty map if not available
   * @throws SQLException if immediate loading fails
   */
  public Map<String, ColumnMetadata> getTableSchema(final String tableName) throws SQLException {
    return this.getTableSchema(tableName, this.defaultStrategy);
  }

  /**
   * Gets schema for a table using the specified loading strategy.
   *
   * @param tableName the name of the table
   * @param strategy the loading strategy to use
   * @return the column metadata, or empty map if not available
   * @throws SQLException if immediate loading fails
   */
  public Map<String, ColumnMetadata> getTableSchema(
      final String tableName, final LoadingStrategy strategy) throws SQLException {
    // Record access for predictive loading
    this.recordAccess(tableName);

    // Check cache first
    final var cachedEntry = this.schemaCache.get(tableName);
    if (cachedEntry != null && cachedEntry.isValid()) {
      final var schemaData = cachedEntry.getSchemaData();
      if (schemaData != null) {
        this.cacheHits.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug("Lazy loading cache hit for DynamoDB table: {}", tableName);
        }
        return schemaData;
      }
    }

    this.cacheMisses.incrementAndGet();

    // Handle based on strategy
    return switch (strategy) {
      case IMMEDIATE -> this.loadImmediate(tableName);
      case BACKGROUND -> this.loadBackground(tableName);
      case CACHED_ONLY -> Map.of(); // Return empty if not cached
      case PREDICTIVE -> this.loadPredictive(tableName);
    };
  }

  /**
   * Preloads schema for a table in the background.
   *
   * @param tableName the name of the table to preload
   * @return a CompletableFuture that completes when preloading is done
   */
  public CompletableFuture<Void> preloadTableSchema(final String tableName) {
    if (logger.isDebugEnabled()) {
      logger.debug("Preloading schema for DynamoDB table: {}", tableName);
    }

    return this.concurrentDiscovery
        .discoverTableSchemaAsync(tableName)
        .thenAccept(
            columnMetadata -> {
              this.cacheSchema(tableName, columnMetadata);
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Preloaded schema for table {}: {} attributes",
                    tableName,
                    columnMetadata.size());
              }
            })
        .exceptionally(
            throwable -> {
              logger.warn("Failed to preload schema for table: {}", tableName, throwable);
              return null;
            });
  }

  /**
   * Checks if schema is cached for a table.
   *
   * @param tableName the name of the table
   * @return true if cached and valid, false otherwise
   */
  public boolean isTableSchemaCached(final String tableName) {
    final var cachedEntry = this.schemaCache.get(tableName);
    return cachedEntry != null && cachedEntry.isValid() && cachedEntry.getSchemaData() != null;
  }

  /**
   * Evicts a specific table from the cache.
   *
   * @param tableName the name of the table to evict
   * @return true if the table was evicted, false if not found
   */
  public boolean evictTableSchema(final String tableName) {
    final var removed = this.schemaCache.remove(tableName);
    this.accessCounts.remove(tableName);

    if (removed != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Evicted schema for table: {}", tableName);
      }
      return true;
    }
    return false;
  }

  /** Clears all cached schemas. */
  public void clearCache() {
    final var clearedCount = this.schemaCache.size();
    this.schemaCache.clear();
    this.accessCounts.clear();

    if (logger.isInfoEnabled()) {
      logger.info("Cleared lazy loading cache: {} entries removed", clearedCount);
    }
  }

  private Map<String, ColumnMetadata> loadImmediate(final String tableName) throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug("Immediate lazy loading for DynamoDB table: {}", tableName);
    }

    this.lazyLoads.incrementAndGet();

    try {
      final var columnMetadata =
          this.concurrentDiscovery.discoverTableSchemaWithTimeout(tableName, 30000); // 30s timeout
      this.cacheSchema(tableName, columnMetadata);
      return columnMetadata;
    } catch (final SQLException e) {
      logger.warn("Immediate lazy loading failed for table: {}", tableName, e);
      throw e;
    }
  }

  private Map<String, ColumnMetadata> loadBackground(final String tableName) {
    if (logger.isDebugEnabled()) {
      logger.debug("Background lazy loading for DynamoDB table: {}", tableName);
    }

    this.lazyLoads.incrementAndGet();

    // Start background loading
    this.concurrentDiscovery
        .discoverTableSchemaAsync(tableName)
        .thenAccept(columnMetadata -> this.cacheSchema(tableName, columnMetadata))
        .exceptionally(
            throwable -> {
              logger.warn("Background lazy loading failed for table: {}", tableName, throwable);
              return null;
            });

    // Return empty for now, will be available in cache later
    return Map.of();
  }

  private Map<String, ColumnMetadata> loadPredictive(final String tableName) throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug("Predictive lazy loading for DynamoDB table: {}", tableName);
    }

    // For predictive mode, start with immediate load but also trigger predictive preloading
    final var result = this.loadImmediate(tableName);

    if (this.predictiveLoading) {
      this.triggerPredictivePreloading(tableName);
    }

    return result;
  }

  private void triggerPredictivePreloading(final String tableName) {
    // Simple predictive logic: preload tables with similar names or high access counts
    this.accessCounts.entrySet().stream()
        .filter(entry -> entry.getValue().get() > 5) // High access threshold
        .filter(entry -> !entry.getKey().equals(tableName))
        .filter(entry -> !this.isTableSchemaCached(entry.getKey()))
        .limit(3) // Preload up to 3 additional tables
        .forEach(
            entry -> {
              final var targetTable = entry.getKey();
              this.predictiveLoads.incrementAndGet();
              this.preloadTableSchema(targetTable);
              if (logger.isDebugEnabled()) {
                logger.debug("Predictive preloading triggered for table: {}", targetTable);
              }
            });
  }

  private void recordAccess(final String tableName) {
    this.accessCounts.computeIfAbsent(tableName, k -> new AtomicLong(0)).incrementAndGet();
  }

  private void cacheSchema(
      final String tableName, final Map<String, ColumnMetadata> columnMetadata) {
    // Check cache size limits
    if (this.schemaCache.size() >= this.maxCacheSize) {
      this.evictLeastRecentlyUsed();
    }

    final var cacheEntry = new CachedSchemaEntry<>(tableName, columnMetadata, this.cacheTtlMs);
    this.schemaCache.put(tableName, cacheEntry);

    if (logger.isDebugEnabled()) {
      logger.debug("Cached schema for table {}: {} attributes", tableName, columnMetadata.size());
    }
  }

  private void evictLeastRecentlyUsed() {
    // Simple LRU eviction: find entry with oldest last access time
    this.schemaCache.entrySet().stream()
        .min(
            (e1, e2) ->
                Long.compare(e1.getValue().getLastAccessTime(), e2.getValue().getLastAccessTime()))
        .ifPresent(
            entry -> {
              final var tableName = entry.getKey();
              this.schemaCache.remove(tableName);
              this.accessCounts.remove(tableName);
              if (logger.isDebugEnabled()) {
                logger.debug("Evicted LRU table from lazy loading cache: {}", tableName);
              }
            });
  }

  /**
   * Gets lazy loading statistics.
   *
   * @return a map containing loading statistics
   */
  public Map<String, Object> getStatistics() {
    final var totalRequests = this.cacheHits.get() + this.cacheMisses.get();
    final var hitRate = totalRequests > 0 ? (double) this.cacheHits.get() / totalRequests : 0.0;

    final var stats = new java.util.HashMap<String, Object>();
    stats.put("defaultStrategy", this.defaultStrategy.toString());
    stats.put("cacheSize", this.schemaCache.size());
    stats.put("maxCacheSize", this.maxCacheSize);
    stats.put("cacheHits", this.cacheHits.get());
    stats.put("cacheMisses", this.cacheMisses.get());
    stats.put("hitRate", hitRate);
    stats.put("lazyLoads", this.lazyLoads.get());
    stats.put("predictiveLoads", this.predictiveLoads.get());
    stats.put("trackedTables", this.accessCounts.size());
    stats.put("cacheTtlMs", this.cacheTtlMs);
    stats.put("predictiveLoading", this.predictiveLoading);

    return stats;
  }

  /** Shuts down the lazy schema loader. */
  public void shutdown() {
    if (logger.isInfoEnabled()) {
      logger.info(
          "Shutting down lazy schema loader with {} cached schemas", this.schemaCache.size());
    }

    this.concurrentDiscovery.shutdown();
    this.clearCache();

    logger.info("Lazy schema loader shutdown completed");
  }

  /**
   * Gets the default loading strategy.
   *
   * @return the default loading strategy
   */
  public LoadingStrategy getDefaultStrategy() {
    return this.defaultStrategy;
  }
}
