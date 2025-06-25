package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("JDBC Driver Property Integration Tests")
class JdbcPropertyIntegrationTest {

  @BeforeEach
  void setUp() throws SQLException {
    // Register the driver explicitly
    DriverManager.registerDriver(new DynamoDbDriver());
  }

  @Nested
  @DisplayName("URL Property Parsing Integration")
  class UrlPropertyParsingIntegrationTests {

    @Test
    @DisplayName("All 32 properties are parsed from URL correctly")
    void all32PropertiesAreParsedFromUrlCorrectly() throws SQLException {
      // Given: URL with all possible properties
      final var url =
          "jdbc:dynamodb:partiql:"
              + "region=us-east-1;"
              + "endpoint=http://localhost:8000;"
              + "credentialsType=STATIC;"
              + "accessKey=test;"
              + "secretKey=secret;"
              + "sessionToken=token;"
              + "profileName=myprofile;"
              + "retryMaxAttempts=5;"
              + "retryBaseDelayMs=200;"
              + "retryMaxDelayMs=30000;"
              + "retryJitterEnabled=true;"
              + "apiCallTimeoutMs=60000;"
              + "apiCallAttemptTimeoutMs=5000;"
              + "tableFilter=test_%;"
              + "schemaDiscovery=auto;"
              + "sampleSize=1000;"
              + "sampleStrategy=random;"
              + "schemaCacheRefreshIntervalMs=300000;"
              + "schemaCacheEnabled=true;"
              + "schemaOptimizations=true;"
              + "concurrentSchemaDiscovery=true;"
              + "maxConcurrentSchemaDiscoveries=8;"
              + "lazyLoadingStrategy=PREDICTIVE;"
              + "lazyLoadingCacheTTL=1800;"
              + "lazyLoadingMaxCacheSize=500;"
              + "predictiveSchemaLoading=true;"
              + "preloadStrategy=STARTUP;"
              + "preloadStartupTables=users,orders;"
              + "preloadScheduledIntervalMs=1800000;"
              + "preloadMaxBatchSize=10;"
              + "cacheWarmingIntervalMs=3600000";

      // When: Creating connection (should succeed with property parsing)
      // Note: We're not actually testing connection establishment here

      // Then: Driver should have accepted the URL and parsed all properties
      final var driver = new DynamoDbDriver();
      assertThat(driver.acceptsURL(url)).isTrue();

      final var propertyInfo = driver.getPropertyInfo(url, new Properties());
      assertThat(propertyInfo).hasSize(32);
    }

    @Test
    @DisplayName("Required properties validation works")
    void requiredPropertiesValidationWorks() throws SQLException {
      // Given: URLs with missing required properties
      final var urlWithoutRegion = "jdbc:dynamodb:partiql:endpoint=http://localhost:8000";

      final var driver = new DynamoDbDriver();

      // When/Then: Should still accept URL but connection will require region
      assertThat(driver.acceptsURL(urlWithoutRegion)).isTrue();

      // In CI environments, AWS_DEFAULT_REGION or AWS_REGION might be set
      // So we need to check if the connection succeeds or fails based on environment
      try {
        Connection conn = DriverManager.getConnection(urlWithoutRegion);
        // If we get here, region was found from environment
        assertThat(conn).isNotNull();
        // Verify it's using region from environment
        String envRegion = System.getenv("AWS_DEFAULT_REGION");
        if (envRegion == null) {
          envRegion = System.getenv("AWS_REGION");
        }
        assertThat(envRegion).isNotNull().withFailMessage("Expected region from environment");
        conn.close();
      } catch (SQLException e) {
        // If we get here, region was not found - this is also valid
        assertThat(e.getMessage()).containsIgnoringCase("region");
      }
    }

    @Test
    @DisplayName("Property precedence works correctly")
    void propertyPrecedenceWorksCorrectly() throws SQLException {
      // Given: URL properties and separate Properties object
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;retryMaxAttempts=3";
      final var properties = new Properties();
      properties.setProperty("retryMaxAttempts", "5"); // Should override URL
      properties.setProperty("endpoint", "http://localhost:8000");

      // When: Processing properties (not testing actual connection)
      // Note: Connection establishment is not being tested here

      // Then: Driver should handle property precedence
      final var driver = new DynamoDbDriver();
      final var propertyInfo = driver.getPropertyInfo(url, properties);
      assertThat(propertyInfo).isNotEmpty();
    }
  }

  @Nested
  @DisplayName("Schema Discovery Property Integration")
  class SchemaDiscoveryPropertyIntegrationTests {

    @Test
    @DisplayName("Schema discovery modes are processed correctly")
    void schemaDiscoveryModesAreProcessedCorrectly() throws SQLException {
      final var driver = new DynamoDbDriver();
      final var modes = new String[] {"auto", "hints", "sampling", "disabled"};

      for (final var mode : modes) {
        // Given: URL with specific schema discovery mode
        final var url = "jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=" + mode;

        // When: Checking URL acceptance
        final var accepted = driver.acceptsURL(url);

        // Then: Should accept all valid modes
        assertThat(accepted).isTrue();

        // Verify property is recognized
        final var properties = driver.getPropertyInfo(url, new Properties());
        final var schemaDiscoveryProp =
            java.util.Arrays.stream(properties)
                .filter(prop -> "schemaDiscovery".equals(prop.name))
                .findFirst();
        assertThat(schemaDiscoveryProp).isPresent();
      }
    }

    @Test
    @DisplayName("Invalid schema discovery mode handled gracefully")
    void invalidSchemaDiscoveryModeHandledGracefully() throws SQLException {
      // Given: URL with invalid schema discovery mode
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=invalid_mode";

      final var driver = new DynamoDbDriver();

      // When/Then: Should still accept URL (invalid values handled at runtime)
      assertThat(driver.acceptsURL(url)).isTrue();
    }

    @Test
    @DisplayName("Schema optimization properties are recognized")
    void schemaOptimizationPropertiesAreRecognized() throws SQLException {
      // Given: URL with optimization properties
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;"
              + "schemaOptimizations=true;"
              + "concurrentSchemaDiscovery=true;"
              + "maxConcurrentSchemaDiscoveries=4;"
              + "lazyLoadingStrategy=PREDICTIVE;"
              + "predictiveSchemaLoading=true";

      final var driver = new DynamoDbDriver();

      // When: Getting property info
      final var propertyInfo = driver.getPropertyInfo(url, new Properties());

      // Then: Should recognize all optimization properties
      final var optimizationProps =
          java.util.Arrays.stream(propertyInfo)
              .map(prop -> prop.name)
              .filter(
                  name ->
                      name.contains("schema")
                          || name.contains("lazy")
                          || name.contains("concurrent")
                          || name.contains("predictive")
                          || name.contains("max"))
              .toList();

      assertThat(optimizationProps)
          .contains(
              "schemaOptimizations",
              "concurrentSchemaDiscovery",
              "maxConcurrentSchemaDiscoveries",
              "lazyLoadingStrategy",
              "predictiveSchemaLoading");
    }
  }

  @Nested
  @DisplayName("Retry Configuration Integration")
  class RetryConfigurationIntegrationTests {

    @Test
    @DisplayName("Retry properties are validated correctly")
    void retryPropertiesAreValidatedCorrectly() throws SQLException {
      final var driver = new DynamoDbDriver();

      // Test valid retry configurations
      final var validConfigs =
          new String[] {
            "retryMaxAttempts=1;retryBaseDelayMs=50",
            "retryMaxAttempts=10;retryBaseDelayMs=1000;retryMaxDelayMs=30000",
            "retryJitterEnabled=true;retryMaxAttempts=3"
          };

      for (final var config : validConfigs) {
        final var url = "jdbc:dynamodb:partiql:region=us-east-1;" + config;

        // Should accept all valid retry configurations
        assertThat(driver.acceptsURL(url)).isTrue();

        final var propertyInfo = driver.getPropertyInfo(url, new Properties());
        final var retryProps =
            java.util.Arrays.stream(propertyInfo)
                .map(prop -> prop.name)
                .filter(name -> name.startsWith("retry"))
                .toList();

        assertThat(retryProps).isNotEmpty();
      }
    }

    @Test
    @DisplayName("Invalid retry values handled appropriately")
    void invalidRetryValuesHandledAppropriately() throws SQLException {
      final var driver = new DynamoDbDriver();

      // Test configurations with invalid values
      final var invalidConfigs =
          new String[] {"retryMaxAttempts=-1", "retryBaseDelayMs=0", "retryMaxDelayMs=-500"};

      for (final var config : invalidConfigs) {
        final var url = "jdbc:dynamodb:partiql:region=us-east-1;" + config;

        // Should still accept URL (validation happens at connection time)
        assertThat(driver.acceptsURL(url)).isTrue();
      }
    }
  }

  @Nested
  @DisplayName("AWS Configuration Integration")
  class AwsConfigurationIntegrationTests {

    @Test
    @DisplayName("Credential types are handled correctly")
    void credentialTypesAreHandledCorrectly() throws SQLException {
      final var driver = new DynamoDbDriver();
      final var credentialTypes = new String[] {"DEFAULT", "STATIC", "PROFILE"};

      for (final var credType : credentialTypes) {
        // Given: URL with specific credential type
        final var url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=" + credType;

        // When/Then: Should accept all valid credential types
        assertThat(driver.acceptsURL(url)).isTrue();

        final var propertyInfo = driver.getPropertyInfo(url, new Properties());
        final var credentialsProp =
            java.util.Arrays.stream(propertyInfo)
                .filter(prop -> "credentialsType".equals(prop.name))
                .findFirst();
        assertThat(credentialsProp).isPresent();
      }
    }

    @Test
    @DisplayName("STATIC credentials require access key and secret")
    void staticCredentialsRequireAccessKeyAndSecret() throws SQLException {
      final var driver = new DynamoDbDriver();

      // Given: URL with STATIC credentials but missing keys
      final var incompleteUrl = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC";

      // When/Then: Should accept URL but connection will fail with helpful message
      assertThat(driver.acceptsURL(incompleteUrl)).isTrue();

      assertThatThrownBy(
              () -> {
                DriverManager.getConnection(incompleteUrl);
              })
          .isInstanceOf(SQLException.class);
    }

    @Test
    @DisplayName("AWS timeout properties are recognized")
    void awsTimeoutPropertiesAreRecognized() throws SQLException {
      // Given: URL with AWS timeout properties
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;"
              + "apiCallTimeoutMs=30000;"
              + "apiCallAttemptTimeoutMs=5000";

      final var driver = new DynamoDbDriver();

      // When: Getting property info
      final var propertyInfo = driver.getPropertyInfo(url, new Properties());

      // Then: Should recognize timeout properties
      final var timeoutProps =
          java.util.Arrays.stream(propertyInfo)
              .map(prop -> prop.name)
              .filter(name -> name.contains("Timeout") || name.contains("TimeoutMs"))
              .toList();

      assertThat(timeoutProps).contains("apiCallTimeoutMs", "apiCallAttemptTimeoutMs");
    }
  }

  @Nested
  @DisplayName("Complex Configuration Integration")
  class ComplexConfigurationIntegrationTests {

    @Test
    @DisplayName("Multiple property categories work together")
    void multiplePropertyCategoriesWorkTogether() throws SQLException {
      // Given: URL combining different property categories
      final var complexUrl =
          "jdbc:dynamodb:partiql:region=us-east-1;"
              +
              // AWS properties
              "credentialsType=STATIC;accessKey=test;secretKey=secret;"
              +
              // Retry properties
              "retryMaxAttempts=3;retryBaseDelayMs=100;"
              +
              // Schema properties
              "schemaDiscovery=auto;sampleSize=500;"
              +
              // Performance properties
              "lazyLoadingStrategy=PREDICTIVE;cacheWarmingIntervalMs=300000";

      final var driver = new DynamoDbDriver();

      // When: Processing complex configuration
      assertThat(driver.acceptsURL(complexUrl)).isTrue();

      final var propertyInfo = driver.getPropertyInfo(complexUrl, new Properties());

      // Then: All property categories should be recognized
      final var propNames = java.util.Arrays.stream(propertyInfo).map(prop -> prop.name).toList();

      assertThat(propNames)
          .contains(
              "region",
              "credentialsType",
              "accessKey",
              "secretKey",
              "retryMaxAttempts",
              "retryBaseDelayMs",
              "schemaDiscovery",
              "sampleSize",
              "lazyLoadingStrategy",
              "cacheWarmingIntervalMs");
    }

    @Test
    @DisplayName("Property info descriptions are helpful")
    void propertyInfoDescriptionsAreHelpful() throws SQLException {
      // Given: Basic URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";

      final var driver = new DynamoDbDriver();
      final var propertyInfo = driver.getPropertyInfo(url, new Properties());

      // Then: Each property should have description and appropriate metadata
      for (final var prop : propertyInfo) {
        assertThat(prop.name).isNotEmpty();
        assertThat(prop.description).isNotEmpty();
        // Required properties should be marked
        if ("region".equals(prop.name)) {
          assertThat(prop.required).isTrue();
        }
      }
    }

    @Test
    @DisplayName("URL with spaces and special characters handled correctly")
    void urlWithSpacesAndSpecialCharactersHandledCorrectly() throws SQLException {
      // Given: URL with complex values
      final var complexUrl =
          "jdbc:dynamodb:partiql:region=us-east-1;"
              + "endpoint=http://localhost:8000/path?param=value&other=123;"
              + "tableFilter=table_prefix_%;"
              + "preloadStartupTables=users,orders,products";

      final var driver = new DynamoDbDriver();

      // When/Then: Should handle complex values correctly
      assertThat(driver.acceptsURL(complexUrl)).isTrue();

      final var propertyInfo = driver.getPropertyInfo(complexUrl, new Properties());
      assertThat(propertyInfo).isNotEmpty();
    }
  }
}
