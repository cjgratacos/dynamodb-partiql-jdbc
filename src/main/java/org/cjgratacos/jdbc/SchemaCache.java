package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background schema cache that periodically refreshes schema information for DynamoDB tables.
 *
 * <p>This class provides a thread-safe caching mechanism for table schema information, with
 * configurable background refresh to keep schema data current. The cache helps reduce the overhead
 * of schema detection by avoiding repeated sampling of the same tables.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Thread-safe concurrent access to cached schemas
 *   <li>Configurable background refresh intervals
 *   <li>Automatic cache expiration and cleanup
 *   <li>Metrics tracking for cache hit/miss ratios
 *   <li>Graceful handling of schema detection failures
 * </ul>
 *
 * <h2>Cache Lifecycle:</h2>
 *
 * <ol>
 *   <li>On first access, schema is detected and cached
 *   <li>Background thread periodically refreshes cached schemas
 *   <li>Failed refresh attempts retain previous schema until successful
 *   <li>Cache can be disabled via configuration for development/testing
 * </ol>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class SchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(SchemaCache.class);

  private final SchemaDetector schemaDetector;
  private final boolean cacheEnabled;
  private final long refreshIntervalMs;
  private final long cacheTtlMs;
  private final Map<String, CachedSchema> schemaCache;
  private final Map<String, Map<String, ColumnMetadata>> enhancedSchemaCache;
  private final ScheduledExecutorService refreshExecutor;
  private final AtomicLong cacheHits = new AtomicLong(0);
  private final AtomicLong cacheMisses = new AtomicLong(0);
  private final AtomicLong refreshCount = new AtomicLong(0);
  private final AtomicLong refreshErrors = new AtomicLong(0);

  /** Cached schema entry with timestamp and type information. */
  private static class CachedSchema {
    final Map<String, Integer> columnTypes;
    final long timestamp;
    volatile boolean refreshing = false;

    CachedSchema(Map<String, Integer> columnTypes) {
      this.columnTypes = columnTypes;
      this.timestamp = System.currentTimeMillis();
    }
  }

  /**
   * Creates a new SchemaCache with the specified configuration.
   *
   * @param schemaDetector the schema detector to use for cache population
   * @param properties connection properties containing cache configuration
   */
  public SchemaCache(SchemaDetector schemaDetector, Properties properties) {
    this.schemaDetector = schemaDetector;
    this.cacheEnabled = Boolean.parseBoolean(properties.getProperty("schemaCache", "true"));
    this.refreshIntervalMs =
        Long.parseLong(
            properties.getProperty("schemaCacheRefreshIntervalMs", "300000")); // 5 minutes default
    this.cacheTtlMs =
        Long.parseLong(properties.getProperty("schemaCacheTTL", "3600"))
            * 1000; // Convert seconds to ms, default 1 hour

    this.schemaCache = new ConcurrentHashMap<>();
    this.enhancedSchemaCache = new ConcurrentHashMap<>();

    if (cacheEnabled) {
      this.refreshExecutor =
          Executors.newScheduledThreadPool(
              1,
              r -> {
                var thread = new Thread(r, "schema-cache-refresh");
                thread.setDaemon(true);
                return thread;
              });

      // Schedule periodic refresh
      this.refreshExecutor.scheduleAtFixedRate(
          this::refreshAllSchemas, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);

      // Schedule TTL cleanup
      this.refreshExecutor.scheduleAtFixedRate(
          this::cleanupExpiredEntries,
          cacheTtlMs / 4, // Clean up every quarter of TTL period
          cacheTtlMs / 4,
          TimeUnit.MILLISECONDS);

      if (logger.isInfoEnabled()) {
        logger.info(
            "SchemaCache initialized: refreshInterval={}ms, ttl={}ms",
            refreshIntervalMs,
            cacheTtlMs);
      }
    } else {
      this.refreshExecutor = null;
      logger.info("SchemaCache disabled by configuration");
    }
  }

  /**
   * Gets the schema for a table, using cache if available or detecting if not.
   *
   * @param tableName the name of the table
   * @return a map of column names to their SQL types
   * @throws SQLException if schema detection fails
   */
  public Map<String, Integer> getTableSchema(String tableName) throws SQLException {
    if (!cacheEnabled) {
      // Cache disabled, always detect fresh
      cacheMisses.incrementAndGet();
      return schemaDetector.detectTableSchema(tableName);
    }

    var cachedSchema = schemaCache.get(tableName);

    if (cachedSchema != null) {
      // Check if entry has expired
      var currentTime = System.currentTimeMillis();
      if (currentTime - cachedSchema.timestamp > cacheTtlMs) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Schema cache entry expired for table: {}, age={}ms",
              tableName,
              currentTime - cachedSchema.timestamp);
        }
        schemaCache.remove(tableName);
        cachedSchema = null;
      } else {
        cacheHits.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug("Schema cache hit for table: {}", tableName);
        }
        return cachedSchema.columnTypes;
      }
    }

    // Cache miss or expired - detect and cache
    cacheMisses.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("Schema cache miss for table: {}, detecting schema", tableName);
    }

    var detectedSchema = schemaDetector.detectTableSchema(tableName);
    schemaCache.put(tableName, new CachedSchema(detectedSchema));

    return detectedSchema;
  }

  /**
   * Manually refreshes the schema for a specific table.
   *
   * @param tableName the name of the table to refresh
   * @throws SQLException if schema detection fails
   */
  public void refreshTableSchema(String tableName) throws SQLException {
    if (!cacheEnabled) {
      return;
    }

    var cachedSchema = schemaCache.get(tableName);
    if (cachedSchema != null && cachedSchema.refreshing) {
      if (logger.isDebugEnabled()) {
        logger.debug("Schema refresh already in progress for table: {}", tableName);
      }
      return;
    }

    try {
      if (cachedSchema != null) {
        cachedSchema.refreshing = true;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Manually refreshing schema for table: {}", tableName);
      }

      var detectedSchema = schemaDetector.detectTableSchema(tableName);
      schemaCache.put(tableName, new CachedSchema(detectedSchema));
      refreshCount.incrementAndGet();

    } catch (Exception e) {
      refreshErrors.incrementAndGet();
      logger.warn("Failed to refresh schema for table: {}", tableName, e);
      throw e;
    } finally {
      if (cachedSchema != null) {
        cachedSchema.refreshing = false;
      }
    }
  }

  /**
   * Gets enhanced column metadata for a table, using cache if available.
   *
   * @param tableName the name of the table
   * @param enhancedDetector the enhanced schema detector to use if cache miss
   * @return a map of column names to their detailed metadata
   * @throws SQLException if schema detection fails
   */
  public Map<String, ColumnMetadata> getTableColumnMetadata(
      String tableName, EnhancedSchemaDetector enhancedDetector) throws SQLException {
    if (!cacheEnabled || enhancedDetector == null) {
      // Cache disabled or no detector, always detect fresh
      return enhancedDetector != null
          ? enhancedDetector.detectTableColumnMetadata(tableName)
          : new HashMap<>();
    }

    var cachedMetadata = enhancedSchemaCache.get(tableName);

    if (cachedMetadata != null) {
      // Check if cached metadata exists (no TTL checking for enhanced cache yet)
      if (!cachedMetadata.isEmpty()) {
        // Since ColumnMetadata doesn't have timestamp, we'll use a simple approach
        // In a real implementation, we'd add timestamp to cached entries
        cacheHits.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug("Enhanced schema cache hit for table: {}", tableName);
        }
        return cachedMetadata;
      }
    }

    // Cache miss - detect and cache
    cacheMisses.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("Enhanced schema cache miss for table: {}, detecting metadata", tableName);
    }

    var detectedMetadata = enhancedDetector.detectTableColumnMetadata(tableName);
    enhancedSchemaCache.put(tableName, detectedMetadata);

    return detectedMetadata;
  }

  /**
   * Manually caches column metadata for a specific table.
   *
   * @param tableName the name of the table
   * @param columnMetadata the column metadata to cache
   */
  public void cacheTableColumnMetadata(
      String tableName, Map<String, ColumnMetadata> columnMetadata) {
    if (cacheEnabled && columnMetadata != null) {
      enhancedSchemaCache.put(tableName, columnMetadata);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Cached enhanced metadata for table {}: {} columns", tableName, columnMetadata.size());
      }
    }
  }

  /** Clears the entire schema cache including enhanced metadata. */
  public void clearCache() {
    if (cacheEnabled) {
      var clearedCount = schemaCache.size();
      var enhancedClearedCount = enhancedSchemaCache.size();
      schemaCache.clear();
      enhancedSchemaCache.clear();
      if (logger.isInfoEnabled()) {
        logger.info(
            "Schema cache cleared: {} basic entries + {} enhanced entries removed",
            clearedCount,
            enhancedClearedCount);
      }
    }
  }

  /**
   * Removes a specific table from both basic and enhanced caches.
   *
   * @param tableName the name of the table to remove
   */
  public void evictTable(String tableName) {
    if (cacheEnabled) {
      var removed = schemaCache.remove(tableName);
      var enhancedRemoved = enhancedSchemaCache.remove(tableName);
      if ((removed != null || enhancedRemoved != null) && logger.isDebugEnabled()) {
        logger.debug(
            "Evicted table from schema cache: {} (basic: {}, enhanced: {})",
            tableName,
            removed != null,
            enhancedRemoved != null);
      }
    }
  }

  /**
   * Gets cache statistics for monitoring including enhanced cache metrics.
   *
   * @return a map containing cache metrics
   */
  public Map<String, Object> getCacheStats() {
    var stats = new java.util.HashMap<String, Object>();
    stats.put("enabled", cacheEnabled);
    stats.put("basicCacheSize", cacheEnabled ? schemaCache.size() : 0);
    stats.put("enhancedCacheSize", cacheEnabled ? enhancedSchemaCache.size() : 0);
    stats.put(
        "totalCacheSize", cacheEnabled ? (schemaCache.size() + enhancedSchemaCache.size()) : 0);
    stats.put("hits", cacheHits.get());
    stats.put("misses", cacheMisses.get());
    stats.put("hitRate", calculateHitRate());
    stats.put("refreshCount", refreshCount.get());
    stats.put("refreshErrors", refreshErrors.get());
    stats.put("refreshIntervalMs", refreshIntervalMs);
    stats.put("ttlMs", cacheTtlMs);
    return stats;
  }

  private double calculateHitRate() {
    var totalRequests = cacheHits.get() + cacheMisses.get();
    return totalRequests > 0 ? (double) cacheHits.get() / totalRequests : 0.0;
  }

  private void refreshAllSchemas() {
    if (!cacheEnabled || schemaCache.isEmpty()) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Starting background refresh of {} cached schemas", schemaCache.size());
    }

    var startTime = System.currentTimeMillis();
    var refreshedCount = 0;
    var errorCount = 0;

    for (var tableName : schemaCache.keySet()) {
      try {
        var cachedSchema = schemaCache.get(tableName);
        if (cachedSchema != null && !cachedSchema.refreshing) {
          cachedSchema.refreshing = true;

          try {
            var detectedSchema = schemaDetector.detectTableSchema(tableName);
            schemaCache.put(tableName, new CachedSchema(detectedSchema));
            refreshedCount++;
          } finally {
            cachedSchema.refreshing = false;
          }
        }
      } catch (Exception e) {
        errorCount++;
        refreshErrors.incrementAndGet();
        logger.warn("Background refresh failed for table: {}", tableName, e);
      }
    }

    refreshCount.addAndGet(refreshedCount);
    var executionTime = System.currentTimeMillis() - startTime;

    if (logger.isInfoEnabled()) {
      logger.info(
          "Background schema refresh completed: {} refreshed, {} errors, {}ms",
          refreshedCount,
          errorCount,
          executionTime);
    }
  }

  private void cleanupExpiredEntries() {
    if (!cacheEnabled || (schemaCache.isEmpty() && enhancedSchemaCache.isEmpty())) {
      return;
    }

    var currentTime = System.currentTimeMillis();
    var initialBasicSize = schemaCache.size();

    // Clean up basic schema cache
    var hadExpiredEntries =
        schemaCache
            .entrySet()
            .removeIf(entry -> currentTime - entry.getValue().timestamp > cacheTtlMs);

    // For enhanced cache, we don't have timestamps in ColumnMetadata yet
    // So we'll implement a simple age-based cleanup later or rely on manual eviction
    // For now, we'll keep enhanced cache entries until manual eviction

    if (hadExpiredEntries) {
      var finalBasicSize = schemaCache.size();
      var removedBasicCount = initialBasicSize - finalBasicSize;
      if (logger.isDebugEnabled()) {
        logger.debug("Cleaned up {} expired basic cache entries", removedBasicCount);
      }
    }
  }

  /** Shuts down the cache and stops background refresh threads. */
  public void shutdown() {
    if (refreshExecutor != null) {
      refreshExecutor.shutdown();
      try {
        if (!refreshExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          refreshExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        refreshExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    if (cacheEnabled) {
      clearCache();
      logger.info("SchemaCache shutdown completed");
    }
  }
}
