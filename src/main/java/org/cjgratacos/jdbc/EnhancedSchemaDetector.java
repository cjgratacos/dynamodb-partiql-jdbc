package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Enhanced schema detection utility for DynamoDB with intelligent type inference and fallback
 * strategies.
 *
 * <p>This class provides advanced schema detection capabilities specifically designed for
 * DynamoDB's NoSQL characteristics, including intelligent type inference from sampled items,
 * conflict resolution, and multiple discovery modes with fallback logic. It builds detailed column
 * metadata with confidence scoring.
 *
 * <h2>DynamoDB-Specific Considerations:</h2>
 *
 * <ul>
 *   <li><strong>Schemaless Nature</strong>: Items can have different attribute sets - sampling
 *       discovers common patterns
 *   <li><strong>Type Flexibility</strong>: Same attribute can have different types across items -
 *       conflict resolution handles this
 *   <li><strong>Key Attributes Only</strong>: DynamoDB only defines types for partition/sort keys -
 *       other types inferred from data
 *   <li><strong>Complex Types</strong>: Maps, Lists, and Sets are common - proper SQL type mapping
 *       provided
 * </ul>
 *
 * <h2>Discovery Modes:</h2>
 *
 * <ul>
 *   <li><strong>AUTO</strong>: Automatically determines the best strategy based on DynamoDB table
 *       characteristics (GSI count, etc.)
 *   <li><strong>HINTS</strong>: Uses table metadata and lightweight item sampling for fast
 *       discovery
 *   <li><strong>SAMPLING</strong>: Performs comprehensive item analysis for detailed type inference
 *   <li><strong>DISABLED</strong>: Returns minimal metadata based on partition/sort key attributes
 *       only
 * </ul>
 *
 * <h2>Fallback Strategy:</h2>
 *
 * <ol>
 *   <li>Try specified discovery mode
 *   <li>If insufficient data, fall back to hints mode (lightweight sampling)
 *   <li>If hints fail, use DynamoDB key attributes only (partition/sort keys)
 *   <li>Always provide basic metadata as last resort
 * </ol>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see ColumnMetadata
 * @see SchemaDiscoveryMode
 */
public class EnhancedSchemaDetector {

  private static final Logger logger = LoggerFactory.getLogger(EnhancedSchemaDetector.class);

  private final DynamoDbClient client;
  private final int sampleSize;
  private final SchemaDetector.SampleStrategy sampleStrategy;
  private final SchemaDiscoveryMode discoveryMode;
  private final TypeResolver typeResolver;
  private final Map<String, Map<String, ColumnMetadata>> tableMetadataCache;

  /**
   * Creates a new EnhancedSchemaDetector with the specified configuration.
   *
   * @param client the DynamoDB client
   * @param properties connection properties containing detection configuration
   */
  public EnhancedSchemaDetector(final DynamoDbClient client, final Properties properties) {
    this.client = client;
    this.sampleSize = parseSampleSize(properties.getProperty("sampleSize", "1000"));
    this.sampleStrategy = parseSampleStrategy(properties.getProperty("sampleStrategy", "RANDOM"));
    this.discoveryMode =
        SchemaDiscoveryMode.fromString(properties.getProperty("schemaDiscovery", "auto"));
    this.typeResolver = new TypeResolver();
    this.tableMetadataCache = new ConcurrentHashMap<>();

    if (logger.isInfoEnabled()) {
      logger.info(
          "EnhancedSchemaDetector initialized: sampleSize={}, strategy={}, discoveryMode={}",
          this.sampleSize,
          this.sampleStrategy,
          this.discoveryMode);
    }
  }

  /**
   * Detects detailed column metadata for a specific table using intelligent discovery.
   *
   * @param tableName the name of the table to analyze
   * @return a map of column names to their detailed metadata
   * @throws SQLException if schema detection fails
   */
  public Map<String, ColumnMetadata> detectTableColumnMetadata(final String tableName)
      throws SQLException {
    CorrelationContext.newOperation("enhanced-schema-detection");
    try {
      if (!this.discoveryMode.isEnabled()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Schema discovery disabled for table: {}", tableName);
        }
        return this.getKeyAttributesOnly(tableName);
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Starting enhanced schema detection for table: {} with mode: {}",
            tableName,
            this.discoveryMode);
      }

      // Check cache first
      var cachedMetadata = this.tableMetadataCache.get(tableName);
      if (cachedMetadata != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Using cached column metadata for table: {}", tableName);
        }
        return cachedMetadata;
      }

      var startTime = System.currentTimeMillis();
      Map<String, ColumnMetadata> columnMetadata;

      try {
        columnMetadata =
            switch (this.discoveryMode) {
              case HINTS -> this.detectUsingHints(tableName);
              case SAMPLING -> this.detectUsingSampling(tableName);
              case AUTO -> this.detectUsingAuto(tableName);
              case DISABLED -> this.getKeyAttributesOnly(tableName);
            };
      } catch (final Exception e) {
        logger.warn(
            "Primary schema detection failed for table {}, attempting fallback", tableName, e);
        columnMetadata = this.performFallbackDetection(tableName);
      }

      // Cache the results
      this.tableMetadataCache.put(tableName, columnMetadata);

      var executionTime = System.currentTimeMillis() - startTime;
      if (logger.isInfoEnabled()) {
        logger.info(
            "Enhanced schema detection completed for {}: {} columns detected in {}ms (mode: {})",
            tableName,
            columnMetadata.size(),
            executionTime,
            this.discoveryMode);
      }

      return columnMetadata;

    } catch (final Exception e) {
      logger.error("Enhanced schema detection failed for table: {}", tableName, e);
      throw new SQLException("Failed to detect column metadata for table " + tableName, e);
    } finally {
      CorrelationContext.clear();
    }
  }

  private Map<String, ColumnMetadata> detectUsingHints(final String tableName) throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug("Using hints-based detection for DynamoDB table: {}", tableName);
    }

    // Start with key attributes (partition and sort keys)
    var columnMetadata = this.getKeyAttributesOnly(tableName);

    // DynamoDB hint: Do lightweight sampling to discover common attributes
    // This is important because DynamoDB items can have different attribute sets
    var lightSampleSize = Math.min(this.sampleSize / 10, 100);
    var sampledMetadata =
        this.performDataSampling(tableName, lightSampleSize, "DynamoDB Hints Discovery");

    // Merge with key attributes (sampled data provides actual type usage patterns)
    for (var entry : sampledMetadata.entrySet()) {
      columnMetadata.put(entry.getKey(), entry.getValue());
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "DynamoDB hints detection found {} total attributes for table {}",
          columnMetadata.size(),
          tableName);
    }

    return columnMetadata;
  }

  private Map<String, ColumnMetadata> detectUsingSampling(final String tableName)
      throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug("Using full sampling detection for DynamoDB table: {}", tableName);
    }

    return this.performDataSampling(tableName, this.sampleSize, "DynamoDB Full Sampling");
  }

  private Map<String, ColumnMetadata> detectUsingAuto(final String tableName) throws SQLException {
    if (logger.isDebugEnabled()) {
      logger.debug("Using auto detection for DynamoDB table: {}", tableName);
    }

    // DynamoDB Auto mode: intelligent selection based on table characteristics
    try {
      // First, get DynamoDB table information
      var describeResponse =
          this.client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      var table = describeResponse.table();
      var tableStatus = table.tableStatus().toString();

      if (!"ACTIVE".equals(tableStatus)) {
        logger.warn(
            "DynamoDB table {} is not ACTIVE ({}), using hints mode", tableName, tableStatus);
        return this.detectUsingHints(tableName);
      }

      // DynamoDB-specific auto logic: Consider table size, GSI count, etc.
      var hasGSI =
          table.globalSecondaryIndexes() != null && !table.globalSecondaryIndexes().isEmpty();
      var hasLSI =
          table.localSecondaryIndexes() != null && !table.localSecondaryIndexes().isEmpty();

      // For tables with many indexes, use more sampling to discover projection patterns
      var autoSampleSize = this.sampleSize / 2;
      if (hasGSI || hasLSI) {
        autoSampleSize = Math.min(this.sampleSize * 3 / 4, 750); // More sampling for indexed tables
        if (logger.isDebugEnabled()) {
          logger.debug(
              "DynamoDB table {} has indexes, using enhanced sampling: {} items",
              tableName,
              autoSampleSize);
        }
      }

      return this.performDataSampling(tableName, autoSampleSize, "DynamoDB Auto Discovery");

    } catch (final Exception e) {
      logger.warn(
          "DynamoDB auto detection failed for table {}, falling back to hints mode", tableName, e);
      return this.detectUsingHints(tableName);
    }
  }

  private Map<String, ColumnMetadata> performFallbackDetection(final String tableName) {
    logger.info("Performing DynamoDB fallback detection for table: {}", tableName);

    try {
      // Try hints mode as fallback (minimal sampling)
      return this.detectUsingHints(tableName);
    } catch (final Exception e) {
      logger.warn(
          "DynamoDB hints fallback failed for table {}, using partition/sort keys only",
          tableName,
          e);
      try {
        return this.getKeyAttributesOnly(tableName);
      } catch (final Exception keyEx) {
        logger.error(
            "DynamoDB key attributes fallback failed for table {}, returning empty metadata",
            tableName,
            keyEx);
        return new HashMap<>();
      }
    }
  }

  private Map<String, ColumnMetadata> getKeyAttributesOnly(final String tableName)
      throws SQLException {
    var columnMetadata = new HashMap<String, ColumnMetadata>();

    try {
      var describeResponse =
          this.client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      var tableDesc = describeResponse.table();

      // DynamoDB only defines types for key attributes (partition key, sort key)
      // Other attributes are discovered at runtime due to NoSQL flexibility
      for (var attr : tableDesc.attributeDefinitions()) {
        var attrName = attr.attributeName();
        var metadata = new ColumnMetadata(tableName, attrName);

        var sqlType = this.mapDynamoDbTypeToSql(attr.attributeType());
        metadata.recordTypeObservation(sqlType, false);

        // Determine if this is partition key or sort key for better context
        var keyType = "Key Attribute";
        for (var keyElement : tableDesc.keySchema()) {
          if (keyElement.attributeName().equals(attrName)) {
            keyType =
                keyElement.keyType().toString().equals("HASH")
                    ? "DynamoDB Partition Key"
                    : "DynamoDB Sort Key";
            break;
          }
        }
        metadata.setDiscoverySource(keyType);

        columnMetadata.put(attrName, metadata);
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Extracted {} DynamoDB key attributes for table: {}", columnMetadata.size(), tableName);
      }

    } catch (final Exception e) {
      throw new SQLException("Failed to get DynamoDB key attributes for table " + tableName, e);
    }

    return columnMetadata;
  }

  private Map<String, ColumnMetadata> performDataSampling(
      final String tableName, final int effectiveSampleSize, final String discoverySource)
      throws SQLException {
    var columnMetadata = new HashMap<String, ColumnMetadata>();
    var itemsScanned = 0;

    var scanRequest = this.buildScanRequest(tableName, effectiveSampleSize);
    var paginator = this.client.scanPaginator(scanRequest);

    for (var page : paginator) {
      for (var item : page.items()) {
        itemsScanned++;
        this.analyzeItemForColumnMetadata(item, columnMetadata, tableName, discoverySource);

        if (itemsScanned >= effectiveSampleSize) {
          break;
        }
      }

      if (itemsScanned >= effectiveSampleSize) {
        break;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Sampled {} items for table {}, discovered {} columns",
          itemsScanned,
          tableName,
          columnMetadata.size());
    }

    return columnMetadata;
  }

  private void analyzeItemForColumnMetadata(
      final Map<String, AttributeValue> item,
      final Map<String, ColumnMetadata> columnMetadata,
      final String tableName,
      final String discoverySource) {
    for (var entry : item.entrySet()) {
      var columnName = entry.getKey();
      var attributeValue = entry.getValue();

      var metadata =
          columnMetadata.computeIfAbsent(
              columnName,
              name -> {
                var newMetadata = new ColumnMetadata(tableName, name);
                newMetadata.setDiscoverySource(discoverySource);
                return newMetadata;
              });

      var sqlType = this.mapAttributeValueToSqlType(attributeValue);
      var isNull = (attributeValue.nul() != null && attributeValue.nul());

      metadata.recordTypeObservation(sqlType, isNull);
    }
  }

  private ScanRequest buildScanRequest(final String tableName, final int effectiveSampleSize) {
    var requestBuilder = ScanRequest.builder().tableName(tableName).select(Select.ALL_ATTRIBUTES);

    switch (this.sampleStrategy) {
      case RANDOM:
        var totalSegments = Math.max(1, Math.min(effectiveSampleSize / 100, 4096));
        var segment = ThreadLocalRandom.current().nextInt(totalSegments);
        requestBuilder.segment(segment).totalSegments(totalSegments);
        break;

      case SEQUENTIAL:
        requestBuilder.limit(effectiveSampleSize);
        break;

      case RECENT:
        requestBuilder.limit(effectiveSampleSize);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "RECENT sampling strategy uses sequential scan (timestamp filtering not implemented)");
        }
        break;
    }

    return requestBuilder.build();
  }

  private int mapAttributeValueToSqlType(final AttributeValue attributeValue) {
    if (attributeValue.s() != null) {
      return Types.VARCHAR;
    }
    if (attributeValue.n() != null) {
      return Types.NUMERIC;
    }
    if (attributeValue.b() != null) {
      return Types.BINARY;
    }
    if (attributeValue.bool() != null) {
      return Types.BOOLEAN;
    }
    if (attributeValue.ss() != null
        || attributeValue.ns() != null
        || attributeValue.bs() != null
        || attributeValue.l() != null) {
      return Types.ARRAY;
    }
    if (attributeValue.m() != null) {
      return Types.STRUCT;
    }
    if (attributeValue.nul() != null && attributeValue.nul()) {
      return Types.NULL;
    }
    return Types.OTHER;
  }

  private int mapDynamoDbTypeToSql(
      software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType type) {
    return switch (type) {
      case S -> Types.VARCHAR;
      case N -> Types.NUMERIC;
      case B -> Types.BINARY;
      default -> Types.OTHER;
    };
  }

  /** Clears the internal metadata cache. */
  public void clearCache() {
    var clearedCount = this.tableMetadataCache.size();
    this.tableMetadataCache.clear();
    if (logger.isDebugEnabled()) {
      logger.debug("Enhanced schema detector cache cleared: {} entries removed", clearedCount);
    }
  }

  /**
   * Gets the configured discovery mode.
   *
   * @return the schema discovery mode
   */
  public SchemaDiscoveryMode getDiscoveryMode() {
    return this.discoveryMode;
  }

  /**
   * Gets the configured sample size.
   *
   * @return the number of items to sample per table
   */
  public int getSampleSize() {
    return this.sampleSize;
  }

  /**
   * Gets cache statistics.
   *
   * @return a map containing cache statistics
   */
  public Map<String, Object> getCacheStats() {
    var stats = new HashMap<String, Object>();
    stats.put("cachedTables", this.tableMetadataCache.size());
    stats.put("discoveryMode", this.discoveryMode.getValue());
    stats.put("sampleSize", this.sampleSize);
    stats.put("sampleStrategy", this.sampleStrategy.toString());
    return stats;
  }

  private int parseSampleSize(final String value) {
    try {
      final var parsed = Integer.parseInt(value);
      return Math.max(1, Math.min(parsed, 10000)); // Clamp between 1 and 10000
    } catch (NumberFormatException e) {
      logger.warn("Invalid sampleSize '{}', using default: 1000", value);
      return 1000;
    }
  }

  private SchemaDetector.SampleStrategy parseSampleStrategy(final String value) {
    try {
      return SchemaDetector.SampleStrategy.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      logger.warn("Invalid sampleStrategy '{}', using default: RANDOM", value);
      return SchemaDetector.SampleStrategy.RANDOM;
    }
  }
}
