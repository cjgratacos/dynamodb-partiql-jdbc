package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Basic Integration Tests")
class BasicIntegrationTest {

  private DynamoDbTestContainer container;

  @BeforeEach
  void setUp() {
    container = new DynamoDbTestContainer();
    container.start();
  }

  @AfterEach
  void tearDown() {
    if (container != null) {
      container.stop();
    }
  }

  @Test
  @DisplayName("Can connect to DynamoDB Local container")
  void canConnectToDynamoDbLocalContainer() throws Exception {
    // Given: DynamoDB Local container is running
    final var jdbcUrl = container.getJdbcUrl();

    // When: Creating connection
    final var connection = DriverManager.getConnection(jdbcUrl);

    // Then: Connection should be successful
    assertThat(connection).isNotNull();
    assertThat(connection.isClosed()).isFalse();

    // Cleanup
    connection.close();
    assertThat(connection.isClosed()).isTrue();
  }

  @Test
  @DisplayName("Can access DynamoDB client directly")
  void canAccessDynamoDbClientDirectly() {
    // Given: Container is running
    final var client = container.getClient();

    // When: Listing tables (should be empty initially)
    final var tablesResponse = client.listTables();

    // Then: Should get valid response
    assertThat(tablesResponse.tableNames()).isEmpty();
  }

  @Test
  @DisplayName("Can create table and verify it exists")
  void canCreateTableAndVerifyItExists() {
    // Given: Container is running
    container.createTable("test_table");

    // When: Listing tables
    final var tablesResponse = container.getClient().listTables();

    // Then: Should contain our table
    assertThat(tablesResponse.tableNames()).contains("test_table");
  }

  @Test
  @DisplayName("Can populate table with test data")
  void canPopulateTableWithTestData() {
    // Given: Table with test data
    container.createTable("users");
    container.populateTestData("users", 5);

    // When: Scanning table
    final var scanResponse =
        container
            .getClient()
            .scan(
                software.amazon.awssdk.services.dynamodb.model.ScanRequest.builder()
                    .tableName("users")
                    .build());

    // Then: Should contain 5 items
    assertThat(scanResponse.items()).hasSize(5);
    assertThat(scanResponse.count()).isEqualTo(5);
  }

  @Test
  @DisplayName("Connection properties are correctly configured")
  void connectionPropertiesAreCorrectlyConfigured() {
    // Given: Container properties
    final var properties = container.getConnectionProperties();

    // When: Checking properties
    final var region = properties.getProperty("region");
    final var credentialsType = properties.getProperty("credentialsType");
    final var endpoint = properties.getProperty("endpoint");

    // Then: Should have expected values
    assertThat(region).isEqualTo("us-east-1");
    assertThat(credentialsType).isEqualTo("STATIC");
    assertThat(endpoint).startsWith("http://");
    assertThat(endpoint).contains("localhost");
  }
}
