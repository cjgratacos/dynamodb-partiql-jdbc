package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("JdbcParser Unit Tests")
class JdbcParserUnitTest {

  @Nested
  @DisplayName("Basic Property Extraction Tests")
  class BasicPropertyExtractionTests {

    @Test
    @DisplayName("Extracts basic properties from valid URL")
    void extractsBasicPropertiesFromValidUrl() {
      // Given: Basic JDBC URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract correctly
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }

    @Test
    @DisplayName("Extracts complex URL with many properties")
    void extractsComplexUrlWithManyProperties() {
      // Given: Complex JDBC URL with optimization properties
      final var url =
          "jdbc:dynamodb:partiql:region=us-west-2;endpoint=http://localhost:8000;"
              + "credentialsType=STATIC;accessKey=test123;secretKey=secret456;"
              + "schemaDiscovery=auto;schemaOptimizations=true;lazyLoadingStrategy=PREDICTIVE;"
              + "preloadStrategy=STARTUP;sampleSize=2000";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract all properties
      assertThat(properties).hasSizeGreaterThanOrEqualTo(9);
      assertThat(properties.getProperty("region")).isEqualTo("us-west-2");
      assertThat(properties.getProperty("endpoint")).isEqualTo("http://localhost:8000");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("STATIC");
      assertThat(properties.getProperty("accessKey")).isEqualTo("test123");
      assertThat(properties.getProperty("secretKey")).isEqualTo("secret456");
      assertThat(properties.getProperty("schemaDiscovery")).isEqualTo("auto");
      assertThat(properties.getProperty("schemaOptimizations")).isEqualTo("true");
      assertThat(properties.getProperty("lazyLoadingStrategy")).isEqualTo("PREDICTIVE");
      assertThat(properties.getProperty("preloadStrategy")).isEqualTo("STARTUP");
      assertThat(properties.getProperty("sampleSize")).isEqualTo("2000");
    }

    @Test
    @DisplayName("Handles empty property values")
    void handlesEmptyPropertyValues() {
      // Given: URL with empty values
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;endpoint=;credentialsType=DEFAULT";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should handle empty values
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("endpoint")).isEqualTo("");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }

    @Test
    @DisplayName("Handles URLs with special characters in values")
    void handlesUrlsWithSpecialCharactersInValues() {
      // Given: URL with special characters
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;"
              + "endpoint=http://localhost:8000/path?param=value&other=123;"
              + "tableFilter=table_prefix_%";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should handle special characters
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("endpoint"))
          .isEqualTo("http://localhost:8000/path?param=value&other=123");
      assertThat(properties.getProperty("tableFilter")).isEqualTo("table_prefix_%");
    }
  }

  @Nested
  @DisplayName("Edge Cases Tests")
  class EdgeCasesTests {

    @Test
    @DisplayName("Handles URL with only prefix")
    void handlesUrlWithOnlyPrefix() {
      // Given: URL with only prefix
      final var url = "jdbc:dynamodb:partiql:";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should return empty properties
      assertThat(properties).isEmpty();
    }

    @Test
    @DisplayName("Handles URL with trailing semicolon")
    void handlesUrlWithTrailingSemicolon() {
      // Given: URL with trailing semicolon
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract properties correctly
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }

    @Test
    @DisplayName("Handles URL with multiple consecutive semicolons")
    void handlesUrlWithMultipleConsecutiveSemicolons() {
      // Given: URL with multiple semicolons
      final var url = "jdbc:dynamodb:partiql:region=us-east-1;;credentialsType=DEFAULT;;;";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract valid properties and ignore empty segments
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }

    @Test
    @DisplayName("Handles malformed property without equals sign")
    void handlesMalformedPropertyWithoutEqualsSign() {
      // Given: URL with malformed property
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;invalidproperty;credentialsType=DEFAULT";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract valid properties and ignore malformed ones
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }

    @Test
    @DisplayName("Handles property with multiple equals signs")
    void handlesPropertyWithMultipleEqualsSign() {
      // Given: URL with property containing equals in value
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;query=SELECT * FROM table WHERE id='value=123'";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should handle equals in value correctly
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("query"))
          .isEqualTo("SELECT * FROM table WHERE id='value=123'");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "",
          "invalid-url",
          "jdbc:mysql:localhost",
          "not-a-jdbc-url",
          "jdbc:dynamodb:partiql" // Missing colon
        })
    @DisplayName("Invalid URLs return empty properties")
    void invalidUrlsReturnEmptyProperties(String invalidUrl) {
      // When: Extracting properties from invalid URL
      final var properties = JdbcParser.extractProperties(invalidUrl);

      // Then: Should return empty properties
      assertThat(properties).isEmpty();
    }

    @Test
    @DisplayName("Null URL returns empty properties")
    void nullUrlReturnsEmptyProperties() {
      // When: Parsing null URL
      final var properties = JdbcParser.extractProperties(null);

      // Then: Should return empty properties (graceful handling)
      assertThat(properties).isNotNull().isEmpty();
    }
  }

  @Nested
  @DisplayName("DynamoDB Specific Property Tests")
  class DynamoDbSpecificPropertyTests {

    @Test
    @DisplayName("Extracts all standard DynamoDB properties")
    void extractsAllStandardDynamoDbProperties() {
      // Given: URL with all standard properties
      final var url =
          "jdbc:dynamodb:partiql:"
              + "region=us-east-1;"
              + "endpoint=http://localhost:8000;"
              + "credentialsType=STATIC;"
              + "accessKey=AKIAI...;"
              + "secretKey=wJal...;"
              + "sessionToken=token123;"
              + "profileName=myprofile;"
              + "retryMaxAttempts=5;"
              + "retryBaseDelayMs=200;"
              + "retryMaxDelayMs=30000;"
              + "retryJitterEnabled=true;"
              + "apiCallTimeoutMs=60000;"
              + "tableFilter=prefix_%";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract all properties
      assertThat(properties).hasSize(13);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("retryMaxAttempts")).isEqualTo("5");
      assertThat(properties.getProperty("retryJitterEnabled")).isEqualTo("true");
      assertThat(properties.getProperty("tableFilter")).isEqualTo("prefix_%");
    }

    @Test
    @DisplayName("Extracts schema optimization properties")
    void extractsSchemaOptimizationProperties() {
      // Given: URL with optimization properties
      final var url =
          "jdbc:dynamodb:partiql:"
              + "region=us-east-1;"
              + "schemaDiscovery=auto;"
              + "sampleSize=1000;"
              + "sampleStrategy=random;"
              + "schemaCache=true;"
              + "schemaCacheTTL=3600;"
              + "schemaOptimizations=true;"
              + "concurrentSchemaDiscovery=true;"
              + "maxConcurrentSchemaDiscoveries=8;"
              + "lazyLoadingStrategy=PREDICTIVE;"
              + "lazyLoadingCacheTTL=1800;"
              + "lazyLoadingMaxCacheSize=500;"
              + "predictiveSchemaLoading=true;"
              + "preloadStrategy=STARTUP;"
              + "preloadStartupTables=users,orders,products;"
              + "preloadScheduledIntervalMs=1800000;"
              + "preloadMaxBatchSize=10;"
              + "preloadPatternRecognition=true;"
              + "cacheWarmingIntervalMs=3600000";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract all optimization properties
      assertThat(properties).hasSize(19);
      assertThat(properties.getProperty("schemaDiscovery")).isEqualTo("auto");
      assertThat(properties.getProperty("lazyLoadingStrategy")).isEqualTo("PREDICTIVE");
      assertThat(properties.getProperty("preloadStrategy")).isEqualTo("STARTUP");
      assertThat(properties.getProperty("preloadStartupTables")).isEqualTo("users,orders,products");
      assertThat(properties.getProperty("cacheWarmingIntervalMs")).isEqualTo("3600000");
    }

    @Test
    @DisplayName("Handles boolean property values correctly")
    void handlesBooleanPropertyValuesCorrectly() {
      // Given: URL with various boolean representations
      final var url =
          "jdbc:dynamodb:partiql:"
              + "region=us-east-1;"
              + "retryJitterEnabled=true;"
              + "schemaOptimizations=false;"
              + "concurrentSchemaDiscovery=TRUE;"
              + "predictiveSchemaLoading=FALSE;"
              + "preloadPatternRecognition=1;"
              + "schemaCache=0";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should preserve string values (parsing happens later)
      assertThat(properties.getProperty("retryJitterEnabled")).isEqualTo("true");
      assertThat(properties.getProperty("schemaOptimizations")).isEqualTo("false");
      assertThat(properties.getProperty("concurrentSchemaDiscovery")).isEqualTo("TRUE");
      assertThat(properties.getProperty("predictiveSchemaLoading")).isEqualTo("FALSE");
      assertThat(properties.getProperty("preloadPatternRecognition")).isEqualTo("1");
      assertThat(properties.getProperty("schemaCache")).isEqualTo("0");
    }
  }

  @Nested
  @DisplayName("URL Format Tests")
  class UrlFormatTests {

    @Test
    @DisplayName("Handles minimal valid URL")
    void handlesMinimalValidUrl() {
      // Given: Minimal valid URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should extract single property
      assertThat(properties).hasSize(1);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
    }

    @Test
    @DisplayName("Correctly identifies property section of URL")
    void correctlyIdentifiesPropertySectionOfUrl() {
      // Given: URL structure tests
      final var url1 = "jdbc:dynamodb:partiql:region=us-east-1";
      final var url2 = "jdbc:dynamodb:partiql:some:other:stuff:region=us-east-1";

      // When: Extracting properties
      final var props1 = JdbcParser.extractProperties(url1);
      final var props2 = JdbcParser.extractProperties(url2);

      // Then: Should find properties after last colon before first equals
      assertThat(props1.getProperty("region")).isEqualTo("us-east-1");
      assertThat(props2.getProperty("region")).isEqualTo("us-east-1");
    }

    @Test
    @DisplayName("Handles whitespace around properties")
    void handlesWhitespaceAroundProperties() {
      // Given: URL with whitespace
      final var url = "jdbc:dynamodb:partiql: region = us-east-1 ; credentialsType = DEFAULT ";

      // When: Extracting properties
      final var properties = JdbcParser.extractProperties(url);

      // Then: Should trim whitespace
      assertThat(properties).hasSize(2);
      assertThat(properties.getProperty("region")).isEqualTo("us-east-1");
      assertThat(properties.getProperty("credentialsType")).isEqualTo("DEFAULT");
    }
  }
}
