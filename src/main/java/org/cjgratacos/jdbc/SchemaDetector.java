package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Schema detection utility for DynamoDB tables using configurable sampling strategies.
 *
 * <p>This class provides sampling-based schema inference by analyzing a configurable number of
 * items from DynamoDB tables. It supports different sampling strategies and handles type conflicts
 * gracefully to provide the best possible schema information for JDBC operations.
 *
 * <h2>Sampling Strategies:</h2>
 *
 * <ul>
 *   <li><strong>RANDOM</strong>: Uses DynamoDB's random scan with segment-based sampling
 *   <li><strong>SEQUENTIAL</strong>: Scans items sequentially from the beginning of the table
 *   <li><strong>RECENT</strong>: Attempts to get recent items (requires timestamp attribute)
 * </ul>
 *
 * <h2>Type Resolution:</h2>
 *
 * <p>When multiple types are detected for the same attribute across different items, the detector
 * uses a priority-based resolution:
 *
 * <ol>
 *   <li>String (most flexible)
 *   <li>Number (can handle numeric strings)
 *   <li>Binary (specific data type)
 *   <li>Boolean (least flexible)
 * </ol>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class SchemaDetector {

  private static final Logger logger = LoggerFactory.getLogger(SchemaDetector.class);

  private final DynamoDbClient client;
  private final int sampleSize;
  private final SampleStrategy sampleStrategy;
  private final SchemaDiscoveryMode discoveryMode;
  private final TypeResolver typeResolver;

  /** Sampling strategy enumeration. */
  public enum SampleStrategy {
    /** Random sampling across the table */
    RANDOM,
    /** Sequential sampling from start */
    SEQUENTIAL,
    /** Sample most recently added items */
    RECENT
  }

  /**
   * Creates a new SchemaDetector with the specified configuration.
   *
   * @param client the DynamoDB client
   * @param properties connection properties containing sampling configuration
   */
  public SchemaDetector(final DynamoDbClient client, final Properties properties) {
    this.client = client;
    this.sampleSize = Integer.parseInt(properties.getProperty("sampleSize", "1000"));
    this.sampleStrategy =
        SampleStrategy.valueOf(properties.getProperty("sampleStrategy", "RANDOM").toUpperCase());
    this.discoveryMode =
        SchemaDiscoveryMode.fromString(properties.getProperty("schemaDiscovery", "auto"));
    this.typeResolver = new TypeResolver();

    if (SchemaDetector.logger.isInfoEnabled()) {
      SchemaDetector.logger.info(
          "SchemaDetector initialized: sampleSize={}, strategy={}, discoveryMode={}",
          this.sampleSize,
          this.sampleStrategy,
          this.discoveryMode);
    }
  }

  /**
   * Detects the schema for a specific table by sampling items.
   *
   * @param tableName the name of the table to analyze
   * @return a map of column names to their detected SQL types
   * @throws SQLException if schema detection fails
   */
  public Map<String, Integer> detectTableSchema(final String tableName) throws SQLException {
    CorrelationContext.newOperation("schema-detection");
    try {
      if (!this.discoveryMode.isEnabled()) {
        if (SchemaDetector.logger.isDebugEnabled()) {
          SchemaDetector.logger.debug("Schema discovery disabled for table: {}", tableName);
        }
        return new HashMap<>();
      }

      if (SchemaDetector.logger.isDebugEnabled()) {
        SchemaDetector.logger.debug(
            "Starting schema detection for table: {} with mode: {}", tableName, this.discoveryMode);
      }

      return switch (this.discoveryMode) {
        case HINTS -> this.detectSchemaUsingHints(tableName);
        case SAMPLING -> this.detectSchemaUsingSampling(tableName);
        case AUTO -> this.detectSchemaAuto(tableName);
        case DISABLED -> new HashMap<>();
      };

    } catch (final Exception e) {
      SchemaDetector.logger.error("Schema detection failed for table: {}", tableName, e);
      throw new SQLException("Failed to detect schema for table " + tableName, e);
    } finally {
      CorrelationContext.clear();
    }
  }

  private Map<String, Integer> detectSchemaUsingHints(final String tableName) throws SQLException {
    if (SchemaDetector.logger.isDebugEnabled()) {
      SchemaDetector.logger.debug("Using hints-based schema detection for table: {}", tableName);
    }

    // For hints mode, we'll do a lightweight sampling with reduced sample size
    return this.performSampling(tableName, Math.min(this.sampleSize / 10, 100));
  }

  private Map<String, Integer> detectSchemaUsingSampling(final String tableName)
      throws SQLException {
    if (SchemaDetector.logger.isDebugEnabled()) {
      SchemaDetector.logger.debug("Using full sampling schema detection for table: {}", tableName);
    }

    return this.performSampling(tableName, this.sampleSize);
  }

  private Map<String, Integer> detectSchemaAuto(final String tableName) throws SQLException {
    if (SchemaDetector.logger.isDebugEnabled()) {
      SchemaDetector.logger.debug("Using auto schema detection for table: {}", tableName);
    }

    // Auto mode: start with hints, fallback to sampling if needed
    // For now, use a moderate sample size that balances accuracy and performance
    final var autoSampleSize = Math.min(this.sampleSize / 2, 500);
    return this.performSampling(tableName, autoSampleSize);
  }

  private Map<String, Integer> performSampling(
      final String tableName, final int effectiveSampleSize) throws SQLException {
    final var startTime = System.currentTimeMillis();
    final var columnTypes = new HashMap<String, Map<String, Integer>>();
    var itemsScanned = 0;

    final var scanRequest = this.buildScanRequest(tableName, effectiveSampleSize);
    final var paginator = this.client.scanPaginator(scanRequest);

    for (final var page : paginator) {
      for (final var item : page.items()) {
        itemsScanned++;
        this.analyzeItem(item, columnTypes);

        if (itemsScanned >= effectiveSampleSize) {
          break;
        }
      }

      if (itemsScanned >= effectiveSampleSize) {
        break;
      }
    }

    final var detectedTypes = this.resolveColumnTypes(columnTypes);
    final var executionTime = System.currentTimeMillis() - startTime;

    if (SchemaDetector.logger.isInfoEnabled()) {
      SchemaDetector.logger.info(
          "Schema detection completed for {}: {} columns detected from {} items in {}ms (mode: {})",
          tableName,
          detectedTypes.size(),
          itemsScanned,
          executionTime,
          this.discoveryMode);
    }

    if (SchemaDetector.logger.isDebugEnabled()) {
      SchemaDetector.logger.debug("Detected schema for {}: {}", tableName, detectedTypes);
    }

    return detectedTypes;
  }

  private ScanRequest buildScanRequest(final String tableName, final int effectiveSampleSize) {
    final var requestBuilder =
        ScanRequest.builder().tableName(tableName).select(Select.ALL_ATTRIBUTES);

    switch (this.sampleStrategy) {
      case RANDOM:
        // Use segment-based scanning for random sampling
        final var totalSegments =
            Math.max(1, Math.min(effectiveSampleSize / 100, 4096)); // DynamoDB limit
        final var segment = ThreadLocalRandom.current().nextInt(totalSegments);
        requestBuilder.segment(segment).totalSegments(totalSegments);
        break;

      case SEQUENTIAL:
        // Use regular scan (default behavior)
        requestBuilder.limit(effectiveSampleSize);
        break;

      case RECENT:
        // For RECENT, we'll scan normally but could be enhanced to use
        // timestamp-based filtering if a timestamp attribute is known
        requestBuilder.limit(effectiveSampleSize);
        // TODO: Add timestamp-based filtering when timestamp attribute detection is implemented
        if (SchemaDetector.logger.isDebugEnabled()) {
          SchemaDetector.logger.debug(
              "RECENT sampling strategy not fully implemented, using sequential scan");
        }
        break;
    }

    return requestBuilder.build();
  }

  private void analyzeItem(
      final Map<String, AttributeValue> item, final Map<String, Map<String, Integer>> columnTypes) {
    for (final var entry : item.entrySet()) {
      final var columnName = entry.getKey();
      final var attributeValue = entry.getValue();
      final var sqlType = this.mapAttributeValueToSqlType(attributeValue);

      columnTypes
          .computeIfAbsent(columnName, k -> new HashMap<>())
          .merge(this.getTypeName(sqlType), 1, Integer::sum);
    }
  }

  private int mapAttributeValueToSqlType(final AttributeValue attributeValue) {
    if (attributeValue.s() != null) {
      return java.sql.Types.VARCHAR;
    }

    if (attributeValue.n() != null) {
      return java.sql.Types.NUMERIC;
    }

    if (attributeValue.b() != null) {
      return java.sql.Types.BINARY;
    }

    if (attributeValue.bool() != null) {
      return java.sql.Types.BOOLEAN;
    }

    if (attributeValue.ss() != null) {
      return java.sql.Types.ARRAY;
    }

    if (attributeValue.ns() != null) {
      return java.sql.Types.ARRAY;
    }

    if (attributeValue.bs() != null) {
      return java.sql.Types.ARRAY;
    }

    if (attributeValue.m() != null) {
      return java.sql.Types.STRUCT;
    }

    if (attributeValue.l() != null) {
      return java.sql.Types.ARRAY;
    }

    if (attributeValue.nul() != null && attributeValue.nul()) {
      return java.sql.Types.NULL;
    }

    return java.sql.Types.OTHER;
  }

  private String getTypeName(final int sqlType) {
    return switch (sqlType) {
      case java.sql.Types.VARCHAR -> "STRING";
      case java.sql.Types.NUMERIC -> "NUMBER";
      case java.sql.Types.BINARY -> "BINARY";
      case java.sql.Types.BOOLEAN -> "BOOLEAN";
      case java.sql.Types.ARRAY -> "ARRAY";
      case java.sql.Types.STRUCT -> "STRUCT";
      case java.sql.Types.NULL -> "NULL";
      default -> "OTHER";
    };
  }

  private Map<String, Integer> resolveColumnTypes(
      final Map<String, Map<String, Integer>> columnTypes) {
    final var resolvedTypes = new HashMap<String, Integer>();

    for (final var entry : columnTypes.entrySet()) {
      final var columnName = entry.getKey();
      final var typeCounts = entry.getValue();
      final var resolvedType = this.typeResolver.resolveConflictingTypes(typeCounts);
      resolvedTypes.put(columnName, resolvedType);

      if (SchemaDetector.logger.isDebugEnabled() && typeCounts.size() > 1) {
        SchemaDetector.logger.debug(
            "Type conflict resolved for column {}: {} -> {}",
            columnName,
            typeCounts,
            this.getTypeName(resolvedType));
      }
    }

    return resolvedTypes;
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
   * Gets the configured sample strategy.
   *
   * @return the sampling strategy
   */
  public SampleStrategy getSampleStrategy() {
    return this.sampleStrategy;
  }

  /**
   * Gets the configured schema discovery mode.
   *
   * @return the schema discovery mode
   */
  public SchemaDiscoveryMode getDiscoveryMode() {
    return this.discoveryMode;
  }
}
