package org.cjgratacos.jdbc;

/**
 * Enumeration of schema discovery modes supported by the DynamoDB JDBC driver.
 *
 * <p>This enum defines the different strategies available for discovering and inferring the schema
 * of DynamoDB tables when using the JDBC driver with PartiQL queries.
 *
 * <h2>Discovery Modes:</h2>
 *
 * <ul>
 *   <li><strong>AUTO</strong>: Automatically determines the best discovery strategy based on table
 *       characteristics
 *   <li><strong>HINTS</strong>: Uses metadata hints and query patterns to infer schema
 *   <li><strong>SAMPLING</strong>: Actively samples table data to build comprehensive schema
 *       information
 *   <li><strong>DISABLED</strong>: Disables schema discovery, returning minimal type information
 * </ul>
 *
 * <h2>Configuration:</h2>
 *
 * <p>The discovery mode is configured via the JDBC URL using the {@code schemaDiscovery} property:
 *
 * <pre>
 * jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=auto;
 * </pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see SchemaDetector
 * @see SchemaCache
 */
public enum SchemaDiscoveryMode {

  /**
   * Automatically determines the optimal discovery strategy.
   *
   * <p>This mode analyzes table characteristics such as size, access patterns, and available
   * metadata to choose between hints-based and sampling-based discovery. It provides a balance
   * between performance and accuracy.
   */
  AUTO("auto"),

  /**
   * Uses metadata hints and query patterns for schema inference.
   *
   * <p>This lightweight mode relies on DynamoDB table metadata, GSI definitions, and observed query
   * patterns to infer likely column types and structures. It's fast but may miss complex nested
   * structures or rarely-used attributes.
   */
  HINTS("hints"),

  /**
   * Actively samples table data to build comprehensive schema information.
   *
   * <p>This mode performs targeted scans of table data to analyze actual item structures and infer
   * detailed type information. It provides the most accurate schema discovery but has higher
   * latency and cost implications.
   */
  SAMPLING("sampling"),

  /**
   * Disables schema discovery completely.
   *
   * <p>When disabled, the driver returns minimal type information and relies on DynamoDB's flexible
   * schema model. This is the fastest option but provides limited metadata support for tools and
   * applications that depend on schema information.
   */
  DISABLED("disabled");

  private final String value;

  /**
   * Constructs a SchemaDiscoveryMode with the specified string value.
   *
   * @param value the string representation of this discovery mode
   */
  SchemaDiscoveryMode(final String value) {
    this.value = value;
  }

  /**
   * Returns the string representation of this discovery mode.
   *
   * @return the string value used in JDBC URL configuration
   */
  public String getValue() {
    return value;
  }

  /**
   * Parses a string value into a SchemaDiscoveryMode enum constant.
   *
   * <p>This method performs case-insensitive matching against the enum values. If no match is
   * found, it returns {@link #AUTO} as the default.
   *
   * @param value the string value to parse (may be null or empty)
   * @return the corresponding SchemaDiscoveryMode, or AUTO if no match found
   */
  public static SchemaDiscoveryMode fromString(final String value) {
    if (value == null || value.trim().isEmpty()) {
      return AUTO;
    }

    final var normalizedValue = value.trim().toLowerCase();
    for (final var mode : values()) {
      if (mode.value.equals(normalizedValue)) {
        return mode;
      }
    }

    return AUTO; // Default fallback
  }

  /**
   * Returns whether this discovery mode requires data sampling.
   *
   * @return true if this mode involves sampling table data, false otherwise
   */
  public boolean requiresSampling() {
    return this == SAMPLING || this == AUTO;
  }

  /**
   * Returns whether this discovery mode is enabled.
   *
   * @return true if schema discovery is enabled, false if disabled
   */
  public boolean isEnabled() {
    return this != DISABLED;
  }
}
