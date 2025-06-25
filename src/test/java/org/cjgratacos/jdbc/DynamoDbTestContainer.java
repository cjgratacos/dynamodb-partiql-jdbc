package org.cjgratacos.jdbc;

import java.util.Map;
import java.util.Properties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Test utility for creating and managing DynamoDB Local containers for testing.
 *
 * <p>This class provides a convenient way to set up DynamoDB Local using TestContainers for
 * integration testing with the JDBC driver. It handles container lifecycle, client creation, and
 * test data setup.
 */
public class DynamoDbTestContainer {

  private static final String DYNAMODB_LOCAL_IMAGE = "amazon/dynamodb-local:1.20.0";
  private static final int DYNAMODB_PORT = 8000;

  // Test credentials - DynamoDB Local typically accepts any credentials
  private static final String TEST_ACCESS_KEY = "fakeMyKeyId";
  private static final String TEST_SECRET_KEY = "fakeSecretAccessKey";
  private static final Region TEST_REGION = Region.US_EAST_1;

  private final GenericContainer<?> container;
  private DynamoDbClient client;

  /** Creates a new DynamoDB test container. */
  public DynamoDbTestContainer() {
    this.container =
        new GenericContainer<>(DockerImageName.parse(DYNAMODB_LOCAL_IMAGE))
            .withExposedPorts(DYNAMODB_PORT)
            .withCommand(
                "-jar",
                "DynamoDBLocal.jar",
                "-inMemory",
                "-sharedDb",
                "-cors",
                "*",
                "-port",
                "8000")
            .waitingFor(Wait.forListeningPort());
  }

  /** Starts the DynamoDB Local container. */
  public void start() {
    container.start();
    initializeClient();
  }

  /** Stops the DynamoDB Local container. */
  public void stop() {
    if (client != null) {
      client.close();
    }
    container.stop();
  }

  /**
   * Gets the DynamoDB client connected to the test container.
   *
   * @return the DynamoDB client
   */
  public DynamoDbClient getClient() {
    return client;
  }

  /**
   * Gets the endpoint URL for the test container.
   *
   * @return the endpoint URL
   */
  public String getEndpoint() {
    return String.format(
        "http://%s:%d", container.getHost(), container.getMappedPort(DYNAMODB_PORT));
  }

  /**
   * Creates connection properties for the test container.
   *
   * @return properties configured for the test container
   */
  public Properties getConnectionProperties() {
    final var properties = new Properties();
    properties.setProperty("region", TEST_REGION.id());
    properties.setProperty("endpoint", getEndpoint());
    properties.setProperty("credentialsType", "STATIC");
    properties.setProperty("accessKey", TEST_ACCESS_KEY);
    properties.setProperty("secretKey", TEST_SECRET_KEY);
    return properties;
  }

  /**
   * Creates a JDBC URL for the test container.
   *
   * @return the JDBC URL
   */
  public String getJdbcUrl() {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;endpoint=%s;credentialsType=STATIC;accessKey=%s;secretKey=%s",
        TEST_REGION.id(), getEndpoint(), TEST_ACCESS_KEY, TEST_SECRET_KEY);
  }

  /**
   * Creates test tables with specified names.
   *
   * @param tableNames the names of tables to create
   */
  public void createTestTables(String... tableNames) {
    for (String tableName : tableNames) {
      createTable(tableName);
    }
  }

  /**
   * Creates a simple test table with a hash key.
   *
   * @param tableName the name of the table to create
   */
  public void createTable(String tableName) {
    createTable(tableName, "id", ScalarAttributeType.S);
  }

  /**
   * Creates a test table with specified key schema.
   *
   * @param tableName the name of the table
   * @param keyName the name of the hash key
   * @param keyType the type of the hash key
   */
  public void createTable(String tableName, String keyName, ScalarAttributeType keyType) {
    final var createTableRequest =
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(
                KeySchemaElement.builder().attributeName(keyName).keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName(keyName).attributeType(keyType).build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    client.createTable(createTableRequest);

    // Wait for table to be active
    client.waiter().waitUntilTableExists(builder -> builder.tableName(tableName));
  }

  /**
   * Populates a table with test data.
   *
   * @param tableName the name of the table
   * @param itemCount the number of items to create
   */
  public void populateTestData(String tableName, int itemCount) {
    for (int i = 0; i < itemCount; i++) {
      final var item = TestDataGenerator.generateTestItem(tableName, i);

      final var putRequest = PutItemRequest.builder().tableName(tableName).item(item).build();

      client.putItem(putRequest);
    }
  }

  /**
   * Populates a table with custom test data.
   *
   * @param tableName the name of the table
   * @param items the items to insert
   */
  @SafeVarargs
  public final void populateCustomData(String tableName, Map<String, AttributeValue>... items) {
    for (final var item : items) {
      final var putRequest = PutItemRequest.builder().tableName(tableName).item(item).build();

      client.putItem(putRequest);
    }
  }

  private void initializeClient() {
    final var credentials = AwsBasicCredentials.create(TEST_ACCESS_KEY, TEST_SECRET_KEY);

    this.client =
        DynamoDbClient.builder()
            .region(TEST_REGION)
            .endpointOverride(java.net.URI.create(getEndpoint()))
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            // Force the SDK to use the local endpoint
            .overrideConfiguration(
                builder ->
                    builder.putAdvancedOption(
                        software.amazon.awssdk.core.client.config.SdkAdvancedClientOption
                            .DISABLE_HOST_PREFIX_INJECTION,
                        true))
            .build();

    // Wait a bit for the container to be fully ready
    try {
      Thread.sleep(1000); // Wait 1 second
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Test the connection
    try {
      client.listTables();
    } catch (Exception e) {
      System.err.println("Failed to connect to DynamoDB Local: " + e.getMessage());
      System.err.println("Endpoint: " + getEndpoint());
      throw new RuntimeException("Container startup failed", e);
    }
  }
}
