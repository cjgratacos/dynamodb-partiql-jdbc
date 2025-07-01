package org.cjgratacos.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for Amazon DynamoDB that supports PartiQL queries.
 *
 * <p>This driver implements the standard JDBC API to allow Java applications to connect to DynamoDB
 * using PartiQL (SQL-compatible query language for DynamoDB).
 *
 * <h2>Supported JDBC URL Format:</h2>
 *
 * <pre>
 * jdbc:dynamodb:partiql:region=&lt;region&gt;;credentialsType=&lt;type&gt;;[additional_properties];
 * </pre>
 *
 * <h2>Example URLs:</h2>
 * <pre>
 * // Basic connection
 * jdbc:dynamodb:partiql:region=us-east-1
 * 
 * // With custom endpoint (DynamoDB Local)
 * jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000
 * 
 * // With IAM role assumption
 * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=ASSUME_ROLE;assumeRoleArn=arn:aws:iam::123456789012:role/MyRole
 * 
 * // With Lambda support
 * jdbc:dynamodb:partiql:region=us-east-1;lambda.region=us-east-1;lambda.allowedFunctions=func1,func2
 * </pre>
 *
 * <h2>Core Properties:</h2>
 *
 * <ul>
 *   <li><strong>region</strong>: AWS region (required). Can also be set via {@code
 *       AWS_DEFAULT_REGION} or {@code AWS_REGION} environment variables.
 *   <li><strong>endpoint</strong>: Custom endpoint URL (optional). If not provided, AWS SDK will
 *       use the default endpoint.
 *   <li><strong>credentialsType</strong>: Type of AWS credentials provider (DEFAULT, STATIC,
 *       PROFILE, ASSUME_ROLE). Defaults to DEFAULT.
 * </ul>
 *
 * <h2>Connection Pool Properties:</h2>
 *
 * <ul>
 *   <li><strong>connectionPool.enabled</strong>: Enable connection pooling. Defaults to true.
 *   <li><strong>connectionPool.maxSize</strong>: Maximum pool size. Defaults to 10.
 *   <li><strong>connectionPool.minSize</strong>: Minimum pool size. Defaults to 1.
 * </ul>
 *
 * <h2>Schema Discovery Properties:</h2>
 *
 * <ul>
 *   <li><strong>schemaDiscovery</strong>: Schema discovery mode (auto, hints, sampling, disabled).
 *       Defaults to auto.
 *   <li><strong>schemaOptimizations</strong>: Enable/disable performance optimizations. Defaults to
 *       true.
 *   <li><strong>lazyLoadingStrategy</strong>: Lazy loading strategy (IMMEDIATE, BACKGROUND,
 *       CACHED_ONLY, PREDICTIVE). Defaults to IMMEDIATE.
 *   <li><strong>preloadStrategy</strong>: Schema preloading strategy (STARTUP, PATTERN_BASED,
 *       SCHEDULED, REACTIVE, NONE). Defaults to PATTERN_BASED.
 * </ul>
 *
 * <h2>Foreign Key Properties:</h2>
 *
 * <ul>
 *   <li><strong>foreignKey.&lt;name&gt;</strong>: Inline foreign key definition.
 *   <li><strong>foreignKeysFile</strong>: Path to properties file containing foreign key
 *       definitions.
 *   <li><strong>foreignKeysTable</strong>: DynamoDB table name containing foreign key definitions.
 *   <li><strong>validateForeignKeys</strong>: Enable/disable foreign key validation. Defaults to
 *       false.
 *   <li><strong>foreignKeyValidationMode</strong>: Validation mode (strict, lenient, off). Defaults
 *       to lenient.
 *   <li><strong>cacheTableMetadata</strong>: Cache table/column existence checks. Defaults to true.
 * </ul>
 *
 * <p>For a complete list of supported properties, call {@link #getPropertyInfo(String,
 * Properties)}.
 *
 * <h2>Credential Provider Types:</h2>
 *
 * <ol>
 *   <li><strong>DEFAULT</strong> (default): Uses AWS DefaultCredentialsProvider chain
 *       <pre>jdbc:dynamodb:partiql:region=us-east-1;</pre>
 *   <li><strong>STATIC</strong>: Uses explicit access key and secret key
 *       <pre>
 *       jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI...;secretKey=wJal...;sessionToken=optional_token;
 *       </pre>
 *   <li><strong>PROFILE</strong>: Uses AWS profile from credentials file
 *       <pre>jdbc:dynamodb:partiql:region=us-east-1;credentialsType=PROFILE;profileName=myprofile;
 *       </pre>
 * </ol>
 *
 * <h2>Supported PartiQL Operations:</h2>
 *
 * <ul>
 *   <li>SELECT queries for data retrieval
 *   <li>INSERT, UPDATE, DELETE, UPSERT, REPLACE for data modification
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Basic usage
 * String url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;";
 * Connection connection = DriverManager.getConnection(url);
 *
 * try (Statement stmt = connection.createStatement()) {
 *     ResultSet rs = stmt.executeQuery("SELECT * FROM MyTable WHERE id = '123'");
 *     while (rs.next()) {
 *         System.out.println(rs.getString("name"));
 *     }
 * }
 *
 * // High-performance configuration with optimizations
 * String optimizedUrl = "jdbc:dynamodb:partiql:region=us-east-1;" +
 *     "schemaOptimizations=true;lazyLoadingStrategy=PREDICTIVE;" +
 *     "preloadStrategy=STARTUP;preloadStartupTables=users,orders,products;";
 * Connection optimizedConnection = DriverManager.getConnection(optimizedUrl);
 *
 * // Configuration with foreign keys and validation
 * String fkUrl = "jdbc:dynamodb:partiql:region=us-east-1;" +
 *     "foreignKey.FK_Orders_Users=Orders.customerId->Users.userId;" +
 *     "foreignKey.FK_OrderItems_Orders=OrderItems.orderId->Orders.orderId;" +
 *     "validateForeignKeys=true;foreignKeyValidationMode=strict;";
 * Connection fkConnection = DriverManager.getConnection(fkUrl);
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class DynamoDbDriver implements Driver {

  private static final String URL_PREFIX = "jdbc:dynamodb:partiql:";
  private static final int MAJOR_VERSION = 1;
  private static final int MINOR_VERSION = 0;

  static {
    try {
      DriverManager.registerDriver(new DynamoDbDriver());
    } catch (final SQLException e) {
      throw new RuntimeException("Failed to register DynamoDB driver", e);
    }
  }

  /**
   * Default constructor for the DynamoDB JDBC driver.
   *
   * <p>This constructor is called automatically when the driver is loaded. The driver registers
   * itself with the DriverManager in the static block.
   */
  public DynamoDbDriver() {
    // Default constructor
  }

  /**
   * Attempts to create a connection to the DynamoDB database using the given URL and properties.
   *
   * <p>This method is the primary entry point for establishing connections to DynamoDB. It parses
   * the JDBC URL to extract connection properties and creates a new DynamoDB connection if the URL
   * is valid for this driver.
   *
   * @param url the JDBC URL in the format: jdbc:dynamodb:partiql:region=&lt;region&gt;;[properties]
   * @param info additional connection properties (currently not used, properties are extracted from
   *     URL)
   * @return a new DynamoDbConnection instance, or null if the URL is not for this driver
   * @throws SQLException if the URL is malformed or connection cannot be established
   * @see DynamoDbConnection
   * @see JdbcParser#extractProperties(String)
   */
  @Override
  public Connection connect(final String url, final Properties info) throws SQLException {
    if (!this.acceptsURL(url)) {
      return null;
    }

    return new DynamoDbConnection(url, info);
  }

  /**
   * Determines whether this driver can handle the given JDBC URL.
   *
   * <p>This method checks if the URL starts with the DynamoDB PartiQL JDBC URL prefix. It is called
   * by the DriverManager to determine which driver should handle a given URL.
   *
   * @param url the JDBC URL to test
   * @return true if this driver can handle the URL, false otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    return url != null && url.startsWith(DynamoDbDriver.URL_PREFIX);
  }

  /**
   * Gets information about the possible properties for this driver.
   *
   * <p>Returns detailed information about all supported connection properties including required
   * and optional properties, their descriptions, and possible values.
   *
   * @param url the JDBC URL
   * @param info properties proposed for the connection
   * @return an array of DriverPropertyInfo objects describing supported properties
   * @throws SQLException if a database access error occurs
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info)
      throws SQLException {

    // Parse existing properties from URL and info
    Properties existingProps = new Properties();
    if (url != null && acceptsURL(url)) {
      existingProps.putAll(JdbcParser.extractProperties(url));
    }
    if (info != null) {
      existingProps.putAll(info);
    }

    // Define all supported properties (basic + schema discovery + performance optimizations +
    // foreign keys)
    DriverPropertyInfo[] properties = new DriverPropertyInfo[38];

    // Required property: region
    properties[0] = new DriverPropertyInfo("region", existingProps.getProperty("region"));
    properties[0].required = true;
    properties[0].description =
        "AWS region (required). Can also be set via AWS_DEFAULT_REGION or AWS_REGION environment variables.";

    // Optional property: endpoint
    properties[1] = new DriverPropertyInfo("endpoint", existingProps.getProperty("endpoint"));
    properties[1].required = false;
    properties[1].description =
        "Custom endpoint URL (optional). If not provided, AWS SDK will use the default endpoint.";

    // Credentials type
    properties[2] =
        new DriverPropertyInfo(
            "credentialsType", existingProps.getProperty("credentialsType", "DEFAULT"));
    properties[2].required = false;
    properties[2].description =
        "Type of AWS credentials provider (DEFAULT, STATIC, PROFILE). Defaults to DEFAULT.";
    properties[2].choices = new String[] {"DEFAULT", "STATIC", "PROFILE"};

    // STATIC credentials properties
    properties[3] = new DriverPropertyInfo("accessKey", existingProps.getProperty("accessKey"));
    properties[3].required = false;
    properties[3].description = "AWS access key ID (required when credentialsType=STATIC).";

    properties[4] = new DriverPropertyInfo("secretKey", existingProps.getProperty("secretKey"));
    properties[4].required = false;
    properties[4].description = "AWS secret access key (required when credentialsType=STATIC).";

    properties[5] =
        new DriverPropertyInfo("sessionToken", existingProps.getProperty("sessionToken"));
    properties[5].required = false;
    properties[5].description = "AWS session token (optional when credentialsType=STATIC).";

    // PROFILE credentials property
    properties[6] = new DriverPropertyInfo("profileName", existingProps.getProperty("profileName"));
    properties[6].required = false;
    properties[6].description = "AWS profile name (required when credentialsType=PROFILE).";

    // Retry configuration properties
    properties[7] =
        new DriverPropertyInfo(
            "retryMaxAttempts", existingProps.getProperty("retryMaxAttempts", "3"));
    properties[7].required = false;
    properties[7].description = "Maximum retry attempts for throttled operations (default: 3).";

    properties[8] =
        new DriverPropertyInfo(
            "retryBaseDelayMs", existingProps.getProperty("retryBaseDelayMs", "100"));
    properties[8].required = false;
    properties[8].description =
        "Base delay in milliseconds for exponential backoff (default: 100).";

    properties[9] =
        new DriverPropertyInfo(
            "retryMaxDelayMs", existingProps.getProperty("retryMaxDelayMs", "20000"));
    properties[9].required = false;
    properties[9].description = "Maximum delay in milliseconds between retries (default: 20000).";

    properties[10] =
        new DriverPropertyInfo(
            "retryJitterEnabled", existingProps.getProperty("retryJitterEnabled", "true"));
    properties[10].required = false;
    properties[10].description = "Enable jitter to prevent thundering herd (default: true).";
    properties[10].choices = new String[] {"true", "false"};

    // AWS SDK configuration properties
    properties[11] =
        new DriverPropertyInfo("apiCallTimeoutMs", existingProps.getProperty("apiCallTimeoutMs"));
    properties[11].required = false;
    properties[11].description = "Timeout for entire API call including retries (optional).";

    properties[12] =
        new DriverPropertyInfo(
            "apiCallAttemptTimeoutMs", existingProps.getProperty("apiCallAttemptTimeoutMs"));
    properties[12].required = false;
    properties[12].description = "Timeout for single API call attempt (optional).";

    // Table filter property
    properties[13] =
        new DriverPropertyInfo("tableFilter", existingProps.getProperty("tableFilter"));
    properties[13].required = false;
    properties[13].description =
        "Regular expression pattern to filter which tables are returned by DatabaseMetaData.getTables() (optional). Supports SQL LIKE patterns with % and _ wildcards.";

    // Schema Discovery Properties
    properties[14] =
        new DriverPropertyInfo(
            "schemaDiscovery", existingProps.getProperty("schemaDiscovery", "auto"));
    properties[14].required = false;
    properties[14].description = "Schema discovery mode for DynamoDB tables (default: auto).";
    properties[14].choices = new String[] {"auto", "hints", "sampling", "disabled"};

    properties[15] =
        new DriverPropertyInfo("sampleSize", existingProps.getProperty("sampleSize", "1000"));
    properties[15].required = false;
    properties[15].description = "Number of items to sample for schema inference (default: 1000).";

    properties[16] =
        new DriverPropertyInfo(
            "sampleStrategy", existingProps.getProperty("sampleStrategy", "random"));
    properties[16].required = false;
    properties[16].description = "Sampling strategy for schema discovery (default: random).";
    properties[16].choices = new String[] {"random", "sequential", "recent"};

    properties[17] =
        new DriverPropertyInfo("schemaCache", existingProps.getProperty("schemaCache", "true"));
    properties[17].required = false;
    properties[17].description = "Enable/disable schema caching (default: true).";
    properties[17].choices = new String[] {"true", "false"};

    properties[18] =
        new DriverPropertyInfo(
            "schemaCacheTTL", existingProps.getProperty("schemaCacheTTL", "3600"));
    properties[18].required = false;
    properties[18].description = "Schema cache TTL in seconds (default: 3600).";

    // Performance Optimization Properties
    properties[19] =
        new DriverPropertyInfo(
            "schemaOptimizations", existingProps.getProperty("schemaOptimizations", "true"));
    properties[19].required = false;
    properties[19].description =
        "Enable/disable all schema performance optimizations (default: true).";
    properties[19].choices = new String[] {"true", "false"};

    properties[20] =
        new DriverPropertyInfo(
            "concurrentSchemaDiscovery",
            existingProps.getProperty("concurrentSchemaDiscovery", "true"));
    properties[20].required = false;
    properties[20].description =
        "Enable concurrent schema discovery for multiple tables (default: true).";
    properties[20].choices = new String[] {"true", "false"};

    properties[21] =
        new DriverPropertyInfo(
            "maxConcurrentSchemaDiscoveries",
            existingProps.getProperty(
                "maxConcurrentSchemaDiscoveries",
                String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
    properties[21].required = false;
    properties[21].description =
        "Maximum concurrent discovery operations (default: CPU cores * 2).";

    properties[22] =
        new DriverPropertyInfo(
            "lazyLoadingStrategy", existingProps.getProperty("lazyLoadingStrategy", "IMMEDIATE"));
    properties[22].required = false;
    properties[22].description =
        "Strategy for lazy loading schema information (default: IMMEDIATE).";
    properties[22].choices = new String[] {"IMMEDIATE", "BACKGROUND", "CACHED_ONLY", "PREDICTIVE"};

    properties[23] =
        new DriverPropertyInfo(
            "lazyLoadingCacheTTL", existingProps.getProperty("lazyLoadingCacheTTL", "3600"));
    properties[23].required = false;
    properties[23].description = "Lazy loading cache TTL in seconds (default: 3600).";

    properties[24] =
        new DriverPropertyInfo(
            "lazyLoadingMaxCacheSize",
            existingProps.getProperty("lazyLoadingMaxCacheSize", "1000"));
    properties[24].required = false;
    properties[24].description = "Maximum number of cached schemas in lazy loader (default: 1000).";

    properties[25] =
        new DriverPropertyInfo(
            "predictiveSchemaLoading",
            existingProps.getProperty("predictiveSchemaLoading", "true"));
    properties[25].required = false;
    properties[25].description =
        "Enable predictive preloading based on access patterns (default: true).";
    properties[25].choices = new String[] {"true", "false"};

    // Schema Preloading Properties
    properties[26] =
        new DriverPropertyInfo(
            "preloadStrategy", existingProps.getProperty("preloadStrategy", "PATTERN_BASED"));
    properties[26].required = false;
    properties[26].description =
        "Preloading strategy for schema information (default: PATTERN_BASED).";
    properties[26].choices =
        new String[] {"STARTUP", "PATTERN_BASED", "SCHEDULED", "REACTIVE", "NONE"};

    properties[27] =
        new DriverPropertyInfo(
            "preloadStartupTables", existingProps.getProperty("preloadStartupTables"));
    properties[27].required = false;
    properties[27].description = "Comma-separated list of tables to preload at startup (optional).";

    properties[28] =
        new DriverPropertyInfo(
            "preloadScheduledIntervalMs",
            existingProps.getProperty("preloadScheduledIntervalMs", "1800000"));
    properties[28].required = false;
    properties[28].description =
        "Interval for scheduled preloading in milliseconds (default: 1800000 = 30 minutes).";

    properties[29] =
        new DriverPropertyInfo(
            "preloadMaxBatchSize", existingProps.getProperty("preloadMaxBatchSize", "10"));
    properties[29].required = false;
    properties[29].description =
        "Maximum number of tables to preload in a single batch (default: 10).";

    properties[30] =
        new DriverPropertyInfo(
            "preloadPatternRecognition",
            existingProps.getProperty("preloadPatternRecognition", "true"));
    properties[30].required = false;
    properties[30].description =
        "Enable pattern recognition for intelligent preloading (default: true).";
    properties[30].choices = new String[] {"true", "false"};

    // Cache Optimization Properties
    properties[31] =
        new DriverPropertyInfo(
            "cacheWarmingIntervalMs",
            existingProps.getProperty("cacheWarmingIntervalMs", "3600000"));
    properties[31].required = false;
    properties[31].description =
        "Interval for background cache warming in milliseconds (default: 3600000 = 1 hour).";

    // Foreign Key Properties
    properties[32] =
        new DriverPropertyInfo("foreignKeysFile", existingProps.getProperty("foreignKeysFile"));
    properties[32].required = false;
    properties[32].description =
        "Path to properties file containing foreign key definitions (optional).";

    properties[33] =
        new DriverPropertyInfo("foreignKeysTable", existingProps.getProperty("foreignKeysTable"));
    properties[33].required = false;
    properties[33].description =
        "DynamoDB table name containing foreign key definitions (optional).";

    // Foreign Key Validation Properties
    properties[34] =
        new DriverPropertyInfo(
            "validateForeignKeys", existingProps.getProperty("validateForeignKeys", "false"));
    properties[34].required = false;
    properties[34].description = "Enable/disable foreign key validation (default: false).";
    properties[34].choices = new String[] {"true", "false"};

    properties[35] =
        new DriverPropertyInfo(
            "foreignKeyValidationMode",
            existingProps.getProperty("foreignKeyValidationMode", "lenient"));
    properties[35].required = false;
    properties[35].description =
        "Foreign key validation mode (default: lenient). "
            + "strict: fail on invalid foreign keys, lenient: log warnings, off: no validation.";
    properties[35].choices = new String[] {"strict", "lenient", "off"};

    properties[36] =
        new DriverPropertyInfo(
            "cacheTableMetadata", existingProps.getProperty("cacheTableMetadata", "true"));
    properties[36].required = false;
    properties[36].description =
        "Cache table/column existence checks for foreign key validation (default: true).";
    properties[36].choices = new String[] {"true", "false"};

    // Default fetch size and max rows properties
    properties[37] =
        new DriverPropertyInfo(
            "defaultFetchSize", existingProps.getProperty("defaultFetchSize", "100"));
    properties[37].required = false;
    properties[37].description = "Default number of rows to fetch per page (default: 100).";

    return properties;
  }

  /**
   * Gets the major version number of this driver.
   *
   * @return the major version number (1)
   */
  @Override
  public int getMajorVersion() {
    return DynamoDbDriver.MAJOR_VERSION;
  }

  /**
   * Gets the minor version number of this driver.
   *
   * @return the minor version number (0)
   */
  @Override
  public int getMinorVersion() {
    return DynamoDbDriver.MINOR_VERSION;
  }

  /**
   * Reports whether this driver is JDBC compliant.
   *
   * <p>Returns false because DynamoDB is a NoSQL database and doesn't support full SQL compliance
   * required for JDBC compliance. The driver supports PartiQL which is SQL-compatible but not fully
   * compliant.
   *
   * @return false - this driver is not JDBC compliant
   */
  @Override
  public boolean jdbcCompliant() {
    return false; // DynamoDB doesn't support full JDBC compliance
  }

  /**
   * Gets the parent logger for this driver.
   *
   * <p>This feature is not supported by the DynamoDB driver.
   *
   * @return never returns normally
   * @throws SQLFeatureNotSupportedException always thrown as this feature is not supported
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }
}
