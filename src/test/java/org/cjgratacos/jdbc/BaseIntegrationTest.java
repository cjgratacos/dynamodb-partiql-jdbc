package org.cjgratacos.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for integration tests that provides common setup and teardown for DynamoDB Local
 * testing.
 *
 * <p>This class handles:
 *
 * <ul>
 *   <li>DynamoDB Local container lifecycle management
 *   <li>Driver registration
 *   <li>Connection string generation
 *   <li>Common test data setup utilities
 * </ul>
 */
public abstract class BaseIntegrationTest {

  protected static final Logger logger = LoggerFactory.getLogger(BaseIntegrationTest.class);

  protected DynamoDbTestContainer testContainer;
  protected String jdbcUrl;
  protected Properties connectionProperties;

  @BeforeEach
  void setUpBase() throws SQLException {
    logger.info("Setting up integration test environment");

    // Register the driver explicitly
    DriverManager.registerDriver(new DynamoDbDriver());

    // Start test container
    testContainer = new DynamoDbTestContainer();
    testContainer.start();

    // Setup connection details
    jdbcUrl = testContainer.getJdbcUrl();
    connectionProperties = testContainer.getConnectionProperties();

    logger.info("Integration test environment ready. Endpoint: {}", testContainer.getEndpoint());

    // Allow subclasses to perform additional setup
    onSetup();
  }

  @AfterEach
  void tearDownBase() {
    logger.info("Tearing down integration test environment");

    // Allow subclasses to clean up
    onTeardown();

    // Stop test container
    if (testContainer != null) {
      testContainer.stop();
    }

    logger.info("Integration test environment cleaned up");
  }

  /**
   * Hook for subclasses to perform additional setup after the base setup is complete.
   *
   * @throws SQLException if setup fails
   */
  protected void onSetup() throws SQLException {
    // Default: no additional setup
  }

  /** Hook for subclasses to perform cleanup before the base teardown. */
  protected void onTeardown() {
    // Default: no additional cleanup
  }

  /**
   * Creates a test table with the given name.
   *
   * @param tableName the name of the table to create
   */
  protected void createTestTable(final String tableName) {
    testContainer.createTable(tableName);
    logger.debug("Created test table: {}", tableName);
  }

  /**
   * Creates multiple test tables.
   *
   * @param tableNames the names of the tables to create
   */
  protected void createTestTables(final String... tableNames) {
    testContainer.createTestTables(tableNames);
    logger.debug("Created {} test tables", tableNames.length);
  }

  /**
   * Populates a table with test data.
   *
   * @param tableName the name of the table
   * @param itemCount the number of items to create
   */
  protected void populateTestData(final String tableName, final int itemCount) {
    testContainer.populateTestData(tableName, itemCount);
    logger.debug("Populated table {} with {} items", tableName, itemCount);
  }

  /**
   * Gets a JDBC connection using the test container.
   *
   * @return a JDBC connection
   * @throws SQLException if connection fails
   */
  protected DynamoDbConnection getConnection() throws SQLException {
    return (DynamoDbConnection) DriverManager.getConnection(jdbcUrl);
  }

  /**
   * Gets a JDBC connection with custom properties.
   *
   * @param customProperties additional properties to merge with defaults
   * @return a JDBC connection
   * @throws SQLException if connection fails
   */
  protected DynamoDbConnection getConnection(final Properties customProperties)
      throws SQLException {
    final var mergedProperties = new Properties();
    mergedProperties.putAll(connectionProperties);
    mergedProperties.putAll(customProperties);

    return (DynamoDbConnection) DriverManager.getConnection(jdbcUrl, mergedProperties);
  }
}
