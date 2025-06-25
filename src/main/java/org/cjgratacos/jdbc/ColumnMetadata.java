package org.cjgratacos.jdbc;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enhanced metadata for database columns with intelligent type inference and conflict resolution.
 *
 * <p>This class provides comprehensive column metadata including type inference from sampled data,
 * nullable detection, and conflict resolution when multiple types are observed for the same column.
 * It supports multiple discovery modes and maintains statistics about type observations.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Intelligent type inference with confidence scoring
 *   <li>Nullable detection based on observed null values
 *   <li>Type conflict resolution with prioritization
 *   <li>Support for complex DynamoDB types (Maps, Lists, Sets)
 *   <li>Size estimation and precision detection
 *   <li>Statistics collection for type observations
 * </ul>
 *
 * <h2>Type Resolution Strategy:</h2>
 *
 * <p>When multiple types are observed for the same column, the resolver uses the following
 * priority:
 *
 * <ol>
 *   <li>STRING (most flexible, can represent any value)
 *   <li>NUMBER (can handle numeric strings in many cases)
 *   <li>BINARY (specific binary data)
 *   <li>BOOLEAN (most restrictive)
 *   <li>Complex types (ARRAY, STRUCT) based on frequency
 * </ol>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see SchemaDetector
 * @see TypeResolver
 */
public class ColumnMetadata {

  private static final Logger logger = LoggerFactory.getLogger(ColumnMetadata.class);

  private final String columnName;
  private final String tableName;
  private final Map<Integer, AtomicInteger> typeObservations;
  private final AtomicLong totalObservations;
  private final AtomicLong nullObservations;
  private volatile int resolvedSqlType;
  private volatile String typeName;
  private volatile boolean nullable;
  private volatile int columnSize;
  private volatile int decimalDigits;
  private volatile double typeConfidence;
  private volatile boolean hasTypeConflict;
  private volatile String discoverySource;

  /**
   * Creates a new ColumnMetadata instance for the specified column.
   *
   * @param tableName the name of the table containing this column
   * @param columnName the name of the column
   */
  public ColumnMetadata(final String tableName, final String columnName) {
    this.tableName = tableName;
    this.columnName = columnName;
    this.typeObservations = new HashMap<>();
    this.totalObservations = new AtomicLong(0);
    this.nullObservations = new AtomicLong(0);
    this.resolvedSqlType = Types.OTHER;
    this.typeName = "OTHER";
    this.nullable = true; // Default to nullable until proven otherwise
    this.columnSize = 0;
    this.decimalDigits = 0;
    this.typeConfidence = 0.0;
    this.hasTypeConflict = false;
    this.discoverySource = "Unknown";
  }

  /**
   * Records an observation of a specific SQL type for this column.
   *
   * @param sqlType the SQL type that was observed
   * @param isNull whether the observed value was null
   */
  public void recordTypeObservation(final int sqlType, final boolean isNull) {
    this.totalObservations.incrementAndGet();

    if (isNull) {
      this.nullObservations.incrementAndGet();
    } else {
      this.typeObservations.computeIfAbsent(sqlType, k -> new AtomicInteger(0)).incrementAndGet();
    }

    // Update resolved type and check for conflicts
    this.updateResolvedType();
  }

  /**
   * Records a batch of type observations for performance.
   *
   * @param typeCounts a map of SQL types to their observation counts
   * @param nullCount the number of null observations
   */
  public void recordBatchObservations(
      final Map<Integer, Integer> typeCounts, final long nullCount) {
    var totalNewObservations = nullCount;
    for (final var count : typeCounts.values()) {
      totalNewObservations += count;
    }

    this.totalObservations.addAndGet(totalNewObservations);
    this.nullObservations.addAndGet(nullCount);

    for (final var entry : typeCounts.entrySet()) {
      final var sqlType = entry.getKey();
      final var count = entry.getValue();
      this.typeObservations.computeIfAbsent(sqlType, k -> new AtomicInteger(0)).addAndGet(count);
    }

    this.updateResolvedType();
  }

  private void updateResolvedType() {
    if (this.typeObservations.isEmpty()) {
      this.resolvedSqlType = Types.NULL;
      this.typeName = "NULL";
      this.typeConfidence = 1.0;
      this.hasTypeConflict = false;
      return;
    }

    // Check for type conflicts
    this.hasTypeConflict = this.typeObservations.size() > 1;

    if (this.hasTypeConflict) {
      this.resolvedSqlType = this.resolveTypeConflict();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Type conflict resolved for column {}.{}: {} types observed, resolved to {}",
            this.tableName,
            this.columnName,
            this.typeObservations.size(),
            this.getSqlTypeName(this.resolvedSqlType));
      }
    } else {
      // Single type observed
      this.resolvedSqlType = this.typeObservations.keySet().iterator().next();
      this.typeConfidence = 1.0;
    }

    this.typeName = this.getSqlTypeName(this.resolvedSqlType);
    this.updateNullable();
    this.updateSizeInformation();
    this.calculateTypeConfidence();
  }

  private int resolveTypeConflict() {
    // Type resolution priority (from most to least flexible)
    final var typePriority = new HashMap<Integer, Integer>();
    typePriority.put(Types.VARCHAR, 1); // Most flexible
    typePriority.put(Types.CLOB, 2);
    typePriority.put(Types.LONGVARCHAR, 3);
    typePriority.put(Types.NUMERIC, 4); // Can handle numbers
    typePriority.put(Types.DECIMAL, 5);
    typePriority.put(Types.DOUBLE, 6);
    typePriority.put(Types.FLOAT, 7);
    typePriority.put(Types.BIGINT, 8);
    typePriority.put(Types.INTEGER, 9);
    typePriority.put(Types.ARRAY, 10); // Complex types
    typePriority.put(Types.STRUCT, 11);
    typePriority.put(Types.BINARY, 12); // Specific types
    typePriority.put(Types.VARBINARY, 13);
    typePriority.put(Types.BOOLEAN, 14); // Most restrictive
    typePriority.put(Types.BIT, 15);

    return this.typeObservations.keySet().stream()
        .min(
            (type1, type2) -> {
              final var priority1 = typePriority.getOrDefault(type1, 99);
              final var priority2 = typePriority.getOrDefault(type2, 99);

              if (!priority1.equals(priority2)) {
                return Integer.compare(priority1, priority2);
              }

              // If same priority, choose the one with more observations
              final var count1 = this.typeObservations.get(type1).get();
              final var count2 = this.typeObservations.get(type2).get();
              return Integer.compare(count2, count1); // Descending order
            })
        .orElse(Types.OTHER);
  }

  private void updateNullable() {
    final var totalNonNullObservations = this.totalObservations.get() - this.nullObservations.get();
    this.nullable = this.nullObservations.get() > 0 || totalNonNullObservations == 0;
  }

  private void updateSizeInformation() {
    // DynamoDB-specific size information based on actual constraints
    switch (this.resolvedSqlType) {
      case Types.VARCHAR:
      case Types.CLOB:
      case Types.LONGVARCHAR:
        // DynamoDB string attributes can be up to 400KB (entire item limit)
        // But practically much smaller for individual attributes
        this.columnSize = 2048; // More realistic for individual string attributes
        this.decimalDigits = 0;
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        // DynamoDB numbers are stored as strings with up to 38 digits of precision
        this.columnSize = 38; // DynamoDB number precision limit
        this.decimalDigits = 0; // DynamoDB doesn't distinguish decimal places
        break;
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DOUBLE:
      case Types.FLOAT:
        // All numbers in DynamoDB are stored as strings internally
        this.columnSize = 38;
        this.decimalDigits = 0;
        break;
      case Types.BOOLEAN:
      case Types.BIT:
        this.columnSize = 1;
        this.decimalDigits = 0;
        break;
      case Types.BINARY:
      case Types.VARBINARY:
        // DynamoDB binary attributes - limited by total item size (400KB)
        this.columnSize = 1024; // More realistic for individual binary attributes
        this.decimalDigits = 0;
        break;
      case Types.ARRAY:
        // DynamoDB Lists - size varies, limited by total item size
        this.columnSize = 0; // Variable size
        this.decimalDigits = 0;
        break;
      case Types.STRUCT:
        // DynamoDB Maps - size varies, limited by total item size
        this.columnSize = 0; // Variable size
        this.decimalDigits = 0;
        break;
      default:
        this.columnSize = 0;
        this.decimalDigits = 0;
    }
  }

  private void calculateTypeConfidence() {
    if (this.typeObservations.isEmpty()) {
      this.typeConfidence = 0.0;
      return;
    }

    final var totalTypeObservations = this.totalObservations.get() - this.nullObservations.get();
    if (totalTypeObservations == 0) {
      this.typeConfidence = 0.0;
      return;
    }

    final var dominantTypeCount = this.typeObservations.get(this.resolvedSqlType).get();
    this.typeConfidence = (double) dominantTypeCount / totalTypeObservations;
  }

  private String getSqlTypeName(final int sqlType) {
    return switch (sqlType) {
      case Types.VARCHAR -> "VARCHAR";
      case Types.CLOB -> "CLOB";
      case Types.LONGVARCHAR -> "LONGVARCHAR";
      case Types.NUMERIC -> "NUMERIC";
      case Types.DECIMAL -> "DECIMAL";
      case Types.INTEGER -> "INTEGER";
      case Types.BIGINT -> "BIGINT";
      case Types.DOUBLE -> "DOUBLE";
      case Types.FLOAT -> "FLOAT";
      case Types.BOOLEAN -> "BOOLEAN";
      case Types.BIT -> "BIT";
      case Types.BINARY -> "BINARY";
      case Types.VARBINARY -> "VARBINARY";
      case Types.ARRAY -> "ARRAY";
      case Types.STRUCT -> "STRUCT";
      case Types.NULL -> "NULL";
      default -> "OTHER";
    };
  }

  // Getter methods

  /**
   * Gets the name of the column.
   *
   * @return the column name
   */
  public String getColumnName() {
    return this.columnName;
  }

  /**
   * Gets the name of the table this column belongs to.
   *
   * @return the table name
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Gets the resolved SQL type for this column.
   *
   * @return the SQL type constant from {@link java.sql.Types}
   */
  public int getResolvedSqlType() {
    return this.resolvedSqlType;
  }

  /**
   * Gets the type name for this column.
   *
   * @return the type name (e.g., "VARCHAR", "INTEGER")
   */
  public String getTypeName() {
    return this.typeName;
  }

  /**
   * Checks if this column can contain null values.
   *
   * @return true if the column is nullable, false otherwise
   */
  public boolean isNullable() {
    return this.nullable;
  }

  /**
   * Gets the column size (precision for numeric types, length for string types).
   *
   * @return the column size
   */
  public int getColumnSize() {
    return this.columnSize;
  }

  /**
   * Gets the number of decimal digits for numeric types.
   *
   * @return the decimal digits (scale)
   */
  public int getDecimalDigits() {
    return this.decimalDigits;
  }

  /**
   * Gets the confidence level of the type resolution (0.0 to 1.0).
   *
   * @return the type confidence as a decimal between 0.0 and 1.0
   */
  public double getTypeConfidence() {
    return this.typeConfidence;
  }

  /**
   * Checks if there were conflicting types observed for this column.
   *
   * @return true if type conflicts were detected, false otherwise
   */
  public boolean hasTypeConflict() {
    return this.hasTypeConflict;
  }

  /**
   * Gets the total number of observations for this column.
   *
   * @return the total observation count
   */
  public long getTotalObservations() {
    return this.totalObservations.get();
  }

  /**
   * Gets the number of null observations for this column.
   *
   * @return the null observation count
   */
  public long getNullObservations() {
    return this.nullObservations.get();
  }

  /**
   * Gets the null rate for this column (percentage of null values).
   *
   * @return the null rate as a decimal between 0.0 and 1.0
   */
  public double getNullRate() {
    final var total = this.totalObservations.get();
    return total > 0 ? (double) this.nullObservations.get() / total : 0.0;
  }

  /**
   * Gets the source of this column's discovery (e.g., "sampling", "hints").
   *
   * @return the discovery source identifier
   */
  public String getDiscoverySource() {
    return this.discoverySource;
  }

  /**
   * Sets the source of this column's discovery.
   *
   * @param source the discovery source identifier
   */
  public void setDiscoverySource(final String source) {
    this.discoverySource = source;
  }

  /**
   * Gets detailed statistics about type observations.
   *
   * @return a map containing detailed statistics
   */
  public Map<String, Object> getStatistics() {
    final var stats = new HashMap<String, Object>();
    stats.put("columnName", this.columnName);
    stats.put("tableName", this.tableName);
    stats.put("resolvedSqlType", this.resolvedSqlType);
    stats.put("typeName", this.typeName);
    stats.put("nullable", this.nullable);
    stats.put("columnSize", this.columnSize);
    stats.put("decimalDigits", this.decimalDigits);
    stats.put("typeConfidence", this.typeConfidence);
    stats.put("hasTypeConflict", this.hasTypeConflict);
    stats.put("totalObservations", this.totalObservations.get());
    stats.put("nullObservations", this.nullObservations.get());
    stats.put("nullRate", this.getNullRate());
    stats.put("discoverySource", this.discoverySource);

    // Type distribution
    final var typeDistribution = new HashMap<String, Integer>();
    for (final var entry : this.typeObservations.entrySet()) {
      typeDistribution.put(this.getSqlTypeName(entry.getKey()), entry.getValue().get());
    }
    stats.put("typeDistribution", typeDistribution);

    return stats;
  }

  @Override
  public String toString() {
    return String.format(
        "ColumnMetadata{table=%s, column=%s, type=%s, nullable=%s, confidence=%.2f, observations=%d}",
        this.tableName,
        this.columnName,
        this.typeName,
        this.nullable,
        this.typeConfidence,
        this.totalObservations.get());
  }
}
