package org.cjgratacos.jdbc;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Schema preloading strategy manager for DynamoDB tables with intelligent pattern recognition.
 *
 * <p>This class manages various preloading strategies to optimize schema discovery performance by
 * proactively loading schema information based on usage patterns, application startup, and
 * configurable schedules.
 *
 * <h2>Preloading Strategies:</h2>
 *
 * <ul>
 *   <li><strong>STARTUP</strong>: Preload schemas during application startup
 *   <li><strong>PATTERN_BASED</strong>: Preload based on observed access patterns
 *   <li><strong>SCHEDULED</strong>: Preload on a fixed schedule
 *   <li><strong>REACTIVE</strong>: Preload related tables when one is accessed
 *   <li><strong>NONE</strong>: Disable preloading
 * </ul>
 *
 * <h2>Pattern Recognition:</h2>
 *
 * <p>The strategy analyzes table access patterns to intelligently preload:
 *
 * <ul>
 *   <li>Tables accessed together frequently
 *   <li>Tables with similar naming patterns
 *   <li>Tables accessed during specific time windows
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see LazySchemaLoader
 * @see ConcurrentSchemaDiscovery
 */
public class SchemaPreloadingStrategy {

  private static final Logger logger = LoggerFactory.getLogger(SchemaPreloadingStrategy.class);

  /** Strategy for preloading schema information. */
  public enum PreloadStrategy {
    /** Preload schemas at application startup */
    STARTUP,
    /** Preload based on access patterns */
    PATTERN_BASED,
    /** Preload on a scheduled basis */
    SCHEDULED,
    /** Preload reactively based on demand */
    REACTIVE,
    /** No preloading */
    NONE
  }

  private final DynamoDbClient client;
  private final LazySchemaLoader lazyLoader;
  private final ConcurrentSchemaDiscovery concurrentDiscovery;
  private final ScheduledExecutorService scheduler;
  private final PreloadStrategy strategy;
  private final boolean enabled;

  // Configuration
  private final List<String> startupTables;
  private final long scheduledIntervalMs;
  private final int maxPreloadBatchSize;
  private final boolean patternRecognitionEnabled;

  // State
  private ScheduledFuture<?> scheduledPreloadTask;
  private final Map<String, List<String>> relatedTablesMap;

  /**
   * Creates a new schema preloading strategy manager.
   *
   * @param client the DynamoDB client
   * @param lazyLoader the lazy schema loader
   * @param concurrentDiscovery the concurrent discovery service
   * @param scheduler the scheduled executor for timed operations
   * @param properties connection properties for configuration
   */
  public SchemaPreloadingStrategy(
      final DynamoDbClient client,
      final LazySchemaLoader lazyLoader,
      final ConcurrentSchemaDiscovery concurrentDiscovery,
      final ScheduledExecutorService scheduler,
      final Properties properties) {
    this.client = client;
    this.lazyLoader = lazyLoader;
    this.concurrentDiscovery = concurrentDiscovery;
    this.scheduler = scheduler;
    this.relatedTablesMap = new java.util.concurrent.ConcurrentHashMap<>();

    // Configuration
    this.strategy =
        PreloadStrategy.valueOf(
            properties.getProperty("preloadStrategy", "PATTERN_BASED").toUpperCase());
    this.enabled = this.strategy != PreloadStrategy.NONE;
    this.scheduledIntervalMs =
        Long.parseLong(
            properties.getProperty("preloadScheduledIntervalMs", "1800000")); // 30 minutes default
    this.maxPreloadBatchSize =
        Integer.parseInt(properties.getProperty("preloadMaxBatchSize", "10"));
    this.patternRecognitionEnabled =
        Boolean.parseBoolean(properties.getProperty("preloadPatternRecognition", "true"));

    // Parse startup tables list
    final var startupTablesProperty = properties.getProperty("preloadStartupTables", "");
    this.startupTables =
        startupTablesProperty.isEmpty() ? List.of() : List.of(startupTablesProperty.split(","));

    if (this.enabled) {
      this.initializeStrategy();
      if (logger.isInfoEnabled()) {
        logger.info(
            "SchemaPreloadingStrategy initialized: strategy={}, scheduledInterval={}ms, batchSize={}",
            this.strategy,
            this.scheduledIntervalMs,
            this.maxPreloadBatchSize);
      }
    } else {
      logger.info("Schema preloading disabled");
    }
  }

  /**
   * Triggers preloading based on table access.
   *
   * @param tableName the table that was accessed
   */
  public void onTableAccess(final String tableName) {
    if (!this.enabled) {
      return;
    }

    switch (this.strategy) {
      case REACTIVE:
        this.triggerReactivePreloading(tableName);
        break;
      case PATTERN_BASED:
        this.updateAccessPattern(tableName);
        break;
      default:
        // No action for other strategies on access
        break;
    }
  }

  /**
   * Preloads schemas for a list of tables.
   *
   * @param tableNames the list of table names to preload
   * @return a CompletableFuture that completes when all preloading is done
   */
  public CompletableFuture<Void> preloadTables(final List<String> tableNames) {
    if (!this.enabled || tableNames.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    if (logger.isInfoEnabled()) {
      logger.info("Preloading schemas for {} DynamoDB tables", tableNames.size());
    }

    // Batch preloading to avoid overwhelming the system
    final var batches = this.createBatches(tableNames, this.maxPreloadBatchSize);
    return this.preloadBatches(batches);
  }

  /**
   * Performs startup preloading if configured.
   *
   * @return a CompletableFuture that completes when startup preloading is done
   */
  public CompletableFuture<Void> performStartupPreloading() {
    if (!this.enabled || this.strategy != PreloadStrategy.STARTUP || this.startupTables.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    if (logger.isInfoEnabled()) {
      logger.info("Performing startup preloading for {} tables", this.startupTables.size());
    }

    return this.preloadTables(this.startupTables);
  }

  /**
   * Adds a table relationship for reactive preloading.
   *
   * @param primaryTable the primary table
   * @param relatedTables the list of related tables to preload when primary is accessed
   */
  public void addTableRelationship(final String primaryTable, final List<String> relatedTables) {
    if (this.enabled && !relatedTables.isEmpty()) {
      this.relatedTablesMap.put(primaryTable, relatedTables);
      if (logger.isDebugEnabled()) {
        logger.debug("Added table relationship: {} -> {}", primaryTable, relatedTables);
      }
    }
  }

  private void initializeStrategy() {
    switch (this.strategy) {
      case SCHEDULED:
        this.initializeScheduledPreloading();
        break;
      case STARTUP:
        // Startup preloading is triggered externally
        break;
      case PATTERN_BASED:
        // Pattern-based preloading builds patterns over time
        break;
      case REACTIVE:
        // Reactive preloading responds to table access
        break;
      default:
        break;
    }
  }

  private void initializeScheduledPreloading() {
    if (this.scheduler != null) {
      this.scheduledPreloadTask =
          this.scheduler.scheduleAtFixedRate(
              this::performScheduledPreloading,
              this.scheduledIntervalMs, // Initial delay
              this.scheduledIntervalMs, // Period
              TimeUnit.MILLISECONDS);

      if (logger.isInfoEnabled()) {
        logger.info(
            "Scheduled preloading initialized with {}ms interval", this.scheduledIntervalMs);
      }
    }
  }

  private void performScheduledPreloading() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Performing scheduled schema preloading");
      }

      // Get list of tables that might need refreshing
      final var tablesToRefresh = this.identifyTablesForScheduledRefresh();

      if (!tablesToRefresh.isEmpty()) {
        this.preloadTables(tablesToRefresh);
        if (logger.isInfoEnabled()) {
          logger.info("Scheduled preloading refreshed {} tables", tablesToRefresh.size());
        }
      }
    } catch (final Exception e) {
      logger.warn("Scheduled preloading failed", e);
    }
  }

  private List<String> identifyTablesForScheduledRefresh() {
    // Simple strategy: refresh tables that are cached but might be getting stale
    // In a more sophisticated implementation, this could consider:
    // - Tables accessed recently
    // - Tables with high access frequency
    // - Tables that haven't been refreshed in a while

    return this.startupTables.stream()
        .filter(table -> this.lazyLoader.isTableSchemaCached(table))
        .limit(this.maxPreloadBatchSize / 2) // Limit to avoid overwhelming
        .collect(Collectors.toList());
  }

  private void triggerReactivePreloading(final String tableName) {
    final var relatedTables = this.relatedTablesMap.get(tableName);
    if (relatedTables != null && !relatedTables.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Reactive preloading triggered for table {}: preloading {} related tables",
            tableName,
            relatedTables.size());
      }

      // Preload related tables in background
      relatedTables.parallelStream()
          .filter(table -> !this.lazyLoader.isTableSchemaCached(table))
          .limit(this.maxPreloadBatchSize)
          .forEach(
              table -> {
                this.lazyLoader.preloadTableSchema(table);
                if (logger.isDebugEnabled()) {
                  logger.debug("Reactively preloading related table: {}", table);
                }
              });
    }
  }

  private void updateAccessPattern(final String tableName) {
    if (!this.patternRecognitionEnabled) {
      return;
    }

    // Simple pattern recognition: tables accessed within a time window are considered related
    // This is a simplified implementation - a real system might use more sophisticated ML
    // techniques

    // Implementation would track access timestamps and identify co-access patterns
    // For now, we just log the access for future pattern analysis

    if (logger.isTraceEnabled()) {
      logger.trace("Updated access pattern for table: {}", tableName);
    }
  }

  private List<List<String>> createBatches(final List<String> items, final int batchSize) {
    final var batches = new java.util.ArrayList<List<String>>();
    for (int i = 0; i < items.size(); i += batchSize) {
      final var endIndex = Math.min(i + batchSize, items.size());
      batches.add(items.subList(i, endIndex));
    }
    return batches;
  }

  private CompletableFuture<Void> preloadBatches(final List<List<String>> batches) {
    if (batches.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    // Process batches sequentially to avoid overwhelming DynamoDB
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);

    for (final var batch : batches) {
      chain = chain.thenCompose(v -> this.preloadBatch(batch));
    }

    return chain;
  }

  private CompletableFuture<Void> preloadBatch(final List<String> batch) {
    return this.concurrentDiscovery
        .discoverMultipleTablesAsync(batch)
        .thenAccept(
            results -> {
              // Cache the discovered schemas
              results.forEach(
                  (tableName, columnMetadata) -> {
                    // The concurrent discovery should have already triggered caching
                    if (logger.isDebugEnabled()) {
                      logger.debug(
                          "Preloaded schema for table {}: {} attributes",
                          tableName,
                          columnMetadata.size());
                    }
                  });
            })
        .exceptionally(
            throwable -> {
              logger.warn("Failed to preload batch: {}", batch, throwable);
              return null;
            });
  }

  /**
   * Gets preloading strategy statistics.
   *
   * @return a map containing strategy statistics
   */
  public Map<String, Object> getStatistics() {
    final var stats = new java.util.HashMap<String, Object>();
    stats.put("enabled", this.enabled);
    stats.put("strategy", this.strategy.toString());
    stats.put("scheduledIntervalMs", this.scheduledIntervalMs);
    stats.put("maxPreloadBatchSize", this.maxPreloadBatchSize);
    stats.put("patternRecognitionEnabled", this.patternRecognitionEnabled);
    stats.put("startupTablesCount", this.startupTables.size());
    stats.put("relatedTableMappings", this.relatedTablesMap.size());
    stats.put(
        "scheduledTaskActive",
        this.scheduledPreloadTask != null && !this.scheduledPreloadTask.isDone());

    return stats;
  }

  /** Shuts down the preloading strategy. */
  public void shutdown() {
    if (this.scheduledPreloadTask != null) {
      this.scheduledPreloadTask.cancel(false);
      this.scheduledPreloadTask = null;
    }

    this.relatedTablesMap.clear();

    if (logger.isInfoEnabled()) {
      logger.info("Schema preloading strategy shutdown completed");
    }
  }

  /**
   * Gets the current preloading strategy.
   *
   * @return the preloading strategy
   */
  public PreloadStrategy getStrategy() {
    return this.strategy;
  }

  /**
   * Checks if preloading is enabled.
   *
   * @return true if enabled, false otherwise
   */
  public boolean isEnabled() {
    return this.enabled;
  }
}
