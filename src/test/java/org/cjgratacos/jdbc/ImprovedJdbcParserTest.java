package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Comprehensive unit tests for JdbcParser with improved coverage and edge case handling.
 *
 * <p>This test class provides extensive validation of URL parsing logic, including:
 *
 * <ul>
 *   <li>Basic property extraction
 *   <li>Complex URL formats with multiple properties
 *   <li>Edge cases and malformed URLs
 *   <li>Special characters and encoding
 *   <li>Performance characteristics
 * </ul>
 */
@DisplayName("Improved JDBC Parser Tests")
class ImprovedJdbcParserTest extends BaseUnitTest {

  @Nested
  @DisplayName("Basic Property Extraction")
  class BasicPropertyExtractionTests {

    @Test
    @DisplayName("Extracts single property correctly")
    void extractsSinglePropertyCorrectly() {
      // Given: URL with single property
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain the property
      assertThat(properties).hasSize(1);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
    }

    @Test
    @DisplayName("Extracts multiple properties correctly")
    void extractsMultiplePropertiesCorrectly() {
      // Given: URL with multiple properties
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;endpoint=http://localhost:8000";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain all properties
      assertThat(properties).hasSize(3);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
      assertThat(properties.getProperty("endpoint")).isEqualTo("http://localhost:8000");
    }

    @Test
    @DisplayName("Handles trailing semicolon gracefully")
    void handlesTrailingSemicolonGracefully() {
      // Given: URL with trailing semicolon
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract properties without issues
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }
  }

  @Nested
  @DisplayName("Schema Discovery Properties")
  class SchemaDiscoveryPropertiesTests {

    @Test
    @DisplayName("Extracts all schema discovery properties")
    void extractsAllSchemaDiscoveryProperties() {
      // Given: URL with comprehensive schema discovery configuration
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=auto;sampleSize=1000;sampleStrategy=random;schemaCache=true;schemaCacheTTL=3600";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain all schema discovery properties
      assertThat(properties).hasSize(6);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("schemaDiscovery")).isEqualTo("auto");
      assertThat(properties.getProperty("sampleSize")).isEqualTo("1000");
      assertThat(properties.getProperty("sampleStrategy")).isEqualTo("random");
      assertThat(properties.getProperty("schemaCache")).isEqualTo("true");
      assertThat(properties.getProperty("schemaCacheTTL")).isEqualTo("3600");
    }

    @Test
    @DisplayName("Extracts performance optimization properties")
    void extractsPerformanceOptimizationProperties() {
      // Given: URL with performance optimization properties
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;schemaOptimizations=true;maxConcurrentSchemaDiscoveries=8;lazyLoadingStrategy=PREDICTIVE";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain performance properties
      assertThat(properties).hasSize(4);
      assertThat(properties.getProperty("schemaOptimizations")).isEqualTo("true");
      assertThat(properties.getProperty("maxConcurrentSchemaDiscoveries")).isEqualTo("8");
      assertThat(properties.getProperty("lazyLoadingStrategy")).isEqualTo("PREDICTIVE");
    }
  }

  @Nested
  @DisplayName("Credentials Configuration")
  class CredentialsConfigurationTests {

    @Test
    @DisplayName("Extracts static credentials properties")
    void extractsStaticCredentialsProperties() {
      // Given: URL with static credentials
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI123;secretKey=secretValue123;sessionToken=token123";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain all credential properties
      assertThat(properties).hasSize(5);
      assertThat(properties.getProperty("credentialsType")).isEqualTo("STATIC");
      assertThat(properties.getProperty("accessKey")).isEqualTo("AKIAI123");
      assertThat(properties.getProperty("secretKey")).isEqualTo("secretValue123");
      assertThat(properties.getProperty("sessionToken")).isEqualTo("token123");
    }

    @Test
    @DisplayName("Extracts profile credentials properties")
    void extractsProfileCredentialsProperties() {
      // Given: URL with profile credentials
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=PROFILE;profileName=myprofile";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should contain profile properties
      assertThat(properties).hasSize(3);
      assertThat(properties.getProperty("credentialsType")).isEqualTo("PROFILE");
      assertThat(properties.getProperty("profileName")).isEqualTo("myprofile");
    }
  }

  @Nested
  @DisplayName("Edge Cases and Error Handling")
  class EdgeCasesAndErrorHandlingTests {

    @ParameterizedTest
    @DisplayName("Handles malformed URLs gracefully")
    @ValueSource(
        strings = {
          "not-a-jdbc-url",
          "jdbc:dynamodb:partiql:",
          "jdbc:dynamodb:partiql",
          "jdbc:dynamodb:partiql:=invalid",
          "jdbc:dynamodb:partiql:key=",
          "jdbc:dynamodb:partiql:=value"
        })
    void handlesMalformedUrlsGracefully(final String malformedUrl) {
      // When: Extracting properties from malformed URL
      final var properties = JdbcParser.extractProperties(malformedUrl);

      // Then: Should return empty properties without throwing exception
      assertThat(properties).isNotNull();
    }

    @Test
    @DisplayName("Handles null URL")
    void handlesNullUrl() {
      // When: Extracting properties from null URL
      final var properties = JdbcParser.extractProperties(null);

      // Then: Should return empty properties
      assertThat(properties).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("Handles empty URL")
    void handlesEmptyUrl() {
      // When: Extracting properties from empty URL
      final var properties = JdbcParser.extractProperties("");

      // Then: Should return empty properties
      assertThat(properties).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("Handles URL with special characters")
    void handlesUrlWithSpecialCharacters() {
      // Given: URL with special characters (spaces, encoded characters)
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;tableFilter=user%;endpoint=http://localhost:8000";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should handle special characters
      assertThat(properties).hasSize(3);
      assertThat(properties.getProperty("tableFilter")).isEqualTo("user%");
      assertThat(properties.getProperty("endpoint")).isEqualTo("http://localhost:8000");
    }

    @Test
    @DisplayName("Handles duplicate property keys")
    void handlesDuplicatePropertyKeys() {
      // Given: URL with duplicate keys (last one should win)
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;region=us-west-2;credentialsType=DEFAULT";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should use the last value
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-west-2");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }
  }

  @Nested
  @DisplayName("Real-world URL Examples")
  class RealWorldUrlExamplesTests {

    @ParameterizedTest
    @DisplayName("Parses real-world URL examples correctly")
    @MethodSource("realWorldUrlExamples")
    void parsesRealWorldUrlExamplesCorrectly(
        final String url, final String expectedRegion, final int expectedPropertyCount) {
      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract expected properties
      assertThat(properties).hasSize(expectedPropertyCount);
      assertThat(properties.getProperty("region")).isEqualTo(expectedRegion);
    }

    static Stream<Arguments> realWorldUrlExamples() {
      return Stream.of(
          Arguments.of(
              "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT", "us-east-1", 2),
          Arguments.of(
              "jdbc:dynamodb:partiql:region=eu-west-1;endpoint=http://localhost:8000;credentialsType=STATIC;accessKey=test;secretKey=test",
              "eu-west-1",
              5),
          Arguments.of(
              "jdbc:dynamodb:partiql:region=ap-southeast-1;schemaDiscovery=auto;sampleSize=1500;schemaCache=true;schemaCacheTTL=7200",
              "ap-southeast-1",
              5),
          Arguments.of(
              "jdbc:dynamodb:partiql:region=us-west-2;retryMaxAttempts=5;retryBaseDelayMs=200;retryMaxDelayMs=30000;retryJitterEnabled=true",
              "us-west-2",
              5));
    }
  }

  @Nested
  @DisplayName("Property Value Validation")
  class PropertyValueValidationTests {

    @Test
    @DisplayName("Preserves boolean values as strings")
    void preservesBooleanValuesAsStrings() {
      // Given: URL with boolean-like values
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;schemaCache=true;retryJitterEnabled=false";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should preserve as string values
      assertThat(properties.getProperty("schemaCache")).isEqualTo("true");
      assertThat(properties.getProperty("retryJitterEnabled")).isEqualTo("false");
    }

    @Test
    @DisplayName("Preserves numeric values as strings")
    void preservesNumericValuesAsStrings() {
      // Given: URL with numeric values
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;sampleSize=1000;schemaCacheTTL=3600;retryMaxAttempts=5";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should preserve as string values
      assertThat(properties.getProperty("sampleSize")).isEqualTo("1000");
      assertThat(properties.getProperty("schemaCacheTTL")).isEqualTo("3600");
      assertThat(properties.getProperty("retryMaxAttempts")).isEqualTo("5");
    }

    @Test
    @DisplayName("Handles empty property values")
    void handlesEmptyPropertyValues() {
      // Given: URL with empty values
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;emptyProperty=;sessionToken=";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should include empty string values
      assertThat(properties).hasSize(3);
      assertThat(properties.getProperty("emptyProperty")).isEqualTo("");
      assertThat(properties.getProperty("sessionToken")).isEqualTo("");
    }
  }

  @Nested
  @DisplayName("Performance and Boundary Tests")
  class PerformanceAndBoundaryTests {

    @Test
    @DisplayName("Handles URL with many properties efficiently")
    void handlesUrlWithManyPropertiesEfficiently() {
      // Given: URL with many properties
      final var urlBuilder = new StringBuilder("jdbc:dynamodb:partiql:region=us-east-1");
      for (int i = 0; i < 50; i++) {
        urlBuilder.append(";property").append(i).append("=value").append(i);
      }

      final var url = urlBuilder.toString();
      final var startTime = System.currentTimeMillis();

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      final var duration = System.currentTimeMillis() - startTime;

      // Then: Should complete quickly and extract all properties
      assertThat(properties).hasSize(51); // region + 50 generated properties
      assertThat(duration).isLessThan(100); // Should complete in less than 100ms
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("property0")).isEqualTo("value0");
      assertThat(properties.getProperty("property49")).isEqualTo("value49");
    }

    @Test
    @DisplayName("Handles very long property values")
    void handlesVeryLongPropertyValues() {
      // Given: URL with very long property value
      final var longValue = "x".repeat(1000);
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;longProperty=" + longValue;

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should handle long values correctly
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("longProperty")).isEqualTo(longValue);
    }
  }
}
