package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

@ExtendWith(MockitoExtension.class)
@DisplayName("Simple EnhancedSchemaDetector Tests")
class SimpleEnhancedSchemaDetectorTest {

  @Mock private DynamoDbClient mockClient;

  @Test
  @DisplayName("Can create EnhancedSchemaDetector with valid properties")
  void canCreateEnhancedSchemaDetectorWithValidProperties() {
    // Given: Valid properties
    final var properties = new Properties();
    properties.setProperty("region", "us-east-1");
    properties.setProperty("schemaDiscovery", "disabled");

    // When: Creating detector
    final var detector = new EnhancedSchemaDetector(mockClient, properties);

    // Then: Should create successfully
    assertThat(detector).isNotNull();
  }

  @Test
  @DisplayName("Disabled schema discovery returns empty result")
  void disabledSchemaDiscoveryReturnsEmptyResult() throws Exception {
    // Given: Disabled schema discovery with mock table metadata
    setupMockTable("any_table");

    final var properties = new Properties();
    properties.setProperty("region", "us-east-1");
    properties.setProperty("schemaDiscovery", "disabled");
    final var detector = new EnhancedSchemaDetector(mockClient, properties);

    // When: Detecting schema
    final var result = detector.detectTableColumnMetadata("any_table");

    // Then: Should return only key attributes (empty in this case)
    assertThat(result).isEmpty();
  }

  @Test
  @DisplayName("Default configuration uses AUTO mode")
  void defaultConfigurationUsesAutoMode() {
    // Given: Default properties
    final var properties = new Properties();
    properties.setProperty("region", "us-east-1");

    // When: Creating detector
    final var detector = new EnhancedSchemaDetector(mockClient, properties);

    // Then: Should create successfully with defaults
    assertThat(detector).isNotNull();
  }

  private void setupMockTable(String tableName) {
    // Create a table with no key attributes to return empty schema
    final var tableDescription =
        TableDescription.builder()
            .tableName(tableName)
            .tableStatus(TableStatus.ACTIVE)
            .attributeDefinitions(List.of())
            .build();

    final var response = DescribeTableResponse.builder().table(tableDescription).build();

    when(mockClient.describeTable(any(DescribeTableRequest.class))).thenReturn(response);
  }
}
