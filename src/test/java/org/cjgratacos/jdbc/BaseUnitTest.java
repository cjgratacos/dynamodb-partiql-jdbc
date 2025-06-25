package org.cjgratacos.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 * Base class for unit tests that provides common mock setup and utilities.
 *
 * <p>This class provides:
 *
 * <ul>
 *   <li>Consistent Mockito setup with lenient stubbing
 *   <li>Common mock factory methods for DynamoDB responses
 *   <li>Test data generation utilities
 *   <li>Property builders for different test scenarios
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public abstract class BaseUnitTest {

  @Mock protected DynamoDbClient mockClient;

  protected Properties defaultProperties;

  @BeforeEach
  void setUpBase() {
    // Setup default properties
    defaultProperties = new Properties();
    defaultProperties.setProperty("region", "us-east-1");

    // Allow subclasses to customize setup
    onSetup();
  }

  /** Hook for subclasses to perform additional setup. */
  protected void onSetup() {
    // Default: no additional setup
  }

  /**
   * Creates a Properties object with schema discovery disabled.
   *
   * @return properties with disabled schema discovery
   */
  protected Properties createDisabledSchemaProperties() {
    final var properties = new Properties();
    properties.putAll(defaultProperties);
    properties.setProperty("schemaDiscovery", "disabled");
    return properties;
  }

  /**
   * Creates a Properties object with sampling schema discovery.
   *
   * @return properties with sampling schema discovery
   */
  protected Properties createSamplingSchemaProperties() {
    final var properties = new Properties();
    properties.putAll(defaultProperties);
    properties.setProperty("schemaDiscovery", "sampling");
    properties.setProperty("sampleSize", "100");
    return properties;
  }

  /**
   * Creates a Properties object with hints schema discovery.
   *
   * @return properties with hints schema discovery
   */
  protected Properties createHintsSchemaProperties() {
    final var properties = new Properties();
    properties.putAll(defaultProperties);
    properties.setProperty("schemaDiscovery", "hints");
    return properties;
  }

  /**
   * Creates a mock DynamoDB table with basic configuration.
   *
   * @param tableName the name of the table
   * @param itemCount estimated item count
   * @param hasGSI whether the table has GSI
   */
  protected void setupMockTable(
      final String tableName, final long itemCount, final boolean hasGSI) {
    final var attributeDefinitions =
        List.of(
            AttributeDefinition.builder()
                .attributeName("id")
                .attributeType(ScalarAttributeType.S)
                .build());

    final var tableBuilder =
        TableDescription.builder()
            .tableName(tableName)
            .tableStatus(TableStatus.ACTIVE)
            .itemCount(itemCount)
            .attributeDefinitions(attributeDefinitions);

    if (hasGSI) {
      final var gsi =
          GlobalSecondaryIndexDescription.builder()
              .indexName("test-gsi")
              .itemCount(itemCount)
              .build();
      tableBuilder.globalSecondaryIndexes(gsi);
    }

    final var response = DescribeTableResponse.builder().table(tableBuilder.build()).build();

    lenient()
        .when(mockClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build()))
        .thenReturn(response);
  }

  /**
   * Creates a mock DynamoDB table with no attribute definitions (for disabled schema tests).
   *
   * @param tableName the name of the table to create
   */
  protected void setupMockTableNoAttributes(final String tableName) {
    final var tableDescription =
        TableDescription.builder()
            .tableName(tableName)
            .tableStatus(TableStatus.ACTIVE)
            .attributeDefinitions(List.of())
            .build();

    final var response = DescribeTableResponse.builder().table(tableDescription).build();

    lenient()
        .when(mockClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build()))
        .thenReturn(response);
  }

  /**
   * Creates a mock scan response with the provided items.
   *
   * @param tableName the table name (for logging)
   * @param items the items to return
   */
  protected void setupMockScanResponse(
      final String tableName, final List<Map<String, AttributeValue>> items) {
    final var response = ScanResponse.builder().items(items).count(items.size()).build();

    lenient().when(mockClient.scan(any(ScanRequest.class))).thenReturn(response);

    // Also set up scan paginator to return a single page with the items
    final var mockPaginator =
        org.mockito.Mockito.mock(
            software.amazon.awssdk.services.dynamodb.paginators.ScanIterable.class);
    final var singlePage = java.util.List.of(response);
    lenient().when(mockPaginator.iterator()).thenReturn(singlePage.iterator());
    lenient().when(mockClient.scanPaginator(any(ScanRequest.class))).thenReturn(mockPaginator);
  }

  /**
   * Creates test items with various DynamoDB types.
   *
   * @param count the number of items to create
   * @return list of test items
   */
  protected List<Map<String, AttributeValue>> createTestItems(final int count) {
    return TestDataGenerator.generateTestItems("test", count);
  }

  /**
   * Creates a single test item with specified ID.
   *
   * @param tableName the table name
   * @param id the item ID
   * @return a test item
   */
  protected Map<String, AttributeValue> createTestItem(final String tableName, final int id) {
    return TestDataGenerator.generateTestItem(tableName, id);
  }

  /**
   * Creates a test item with mixed data types for type resolution testing.
   *
   * @return a complex test item
   */
  protected Map<String, AttributeValue> createMixedTypeItem() {
    return Map.of(
        "id", AttributeValue.builder().s("test-id").build(),
        "string_field", AttributeValue.builder().s("text").build(),
        "number_field", AttributeValue.builder().n("123.45").build(),
        "boolean_field", AttributeValue.builder().bool(true).build(),
        "binary_field",
            AttributeValue.builder()
                .b(software.amazon.awssdk.core.SdkBytes.fromByteArray("data".getBytes()))
                .build(),
        "null_field", AttributeValue.builder().nul(true).build());
  }
}
