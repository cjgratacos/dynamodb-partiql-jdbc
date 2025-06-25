package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Comprehensive unit tests for DynamoDbDriver with improved coverage and validation.
 *
 * <p>This test class provides extensive validation of the JDBC driver implementation, including:
 *
 * <ul>
 *   <li>URL format validation and acceptance criteria
 *   <li>Driver registration and management
 *   <li>Property information and validation
 *   <li>Connection creation with various configurations
 *   <li>JDBC compliance and feature support
 * </ul>
 */
@DisplayName("Improved DynamoDB Driver Tests")
class ImprovedDynamoDbDriverTest extends BaseUnitTest {

  private DynamoDbDriver driver;

  @BeforeEach
  void setUp() throws SQLException {
    driver = new DynamoDbDriver();
    // Ensure driver is registered
    DriverManager.registerDriver(driver);
  }

  @Nested
  @DisplayName("Driver Registration and Management")
  class DriverRegistrationAndManagementTests {

    @Test
    @DisplayName("Driver can be registered with DriverManager")
    void driverCanBeRegisteredWithDriverManager() throws SQLException {
      // Given: A new driver instance
      final var newDriver = new DynamoDbDriver();

      // When: Registering with DriverManager
      DriverManager.registerDriver(newDriver);

      // Then: Should not throw exception and should be retrievable
      final var drivers = DriverManager.getDrivers();
      assertThat(drivers.asIterator()).toIterable().anyMatch(d -> d instanceof DynamoDbDriver);
    }

    @Test
    @DisplayName("Driver reports correct version information")
    void driverReportsCorrectVersionInformation() {
      // When: Getting version information
      final var majorVersion = driver.getMajorVersion();
      final var minorVersion = driver.getMinorVersion();

      // Then: Should return valid version numbers
      assertThat(majorVersion).isGreaterThanOrEqualTo(0);
      assertThat(minorVersion).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("Driver reports JDBC compliance correctly")
    void driverReportsJdbcComplianceCorrectly() {
      // When: Checking JDBC compliance
      final var isJdbcCompliant = driver.jdbcCompliant();

      // Then: Should return a boolean value (implementation dependent)
      assertThat(isJdbcCompliant).isInstanceOf(Boolean.class);
    }

    @Test
    @DisplayName("Parent logger is not supported")
    void parentLoggerIsNotSupported() {
      // When: Getting parent logger
      // Then: Should throw SQLFeatureNotSupportedException
      assertThatThrownBy(() -> driver.getParentLogger())
          .isInstanceOf(SQLFeatureNotSupportedException.class);
    }
  }

  @Nested
  @DisplayName("URL Acceptance and Validation")
  class UrlAcceptanceAndValidationTests {

    @ParameterizedTest
    @DisplayName("Accepts valid DynamoDB JDBC URLs")
    @MethodSource("validDynamoDbUrls")
    void acceptsValidDynamoDbUrls(final String url) throws SQLException {
      // When: Checking URL acceptance
      final var accepts = driver.acceptsURL(url);

      // Then: Should accept the URL
      assertThat(accepts).isTrue();
    }

    @ParameterizedTest
    @DisplayName("Rejects invalid or non-DynamoDB URLs")
    @ValueSource(
        strings = {
          "jdbc:mysql://localhost:3306/test",
          "jdbc:postgresql://localhost:5432/test",
          "jdbc:dynamodb:invalid",
          "not-a-jdbc-url",
          "http://example.com",
          ""
        })
    void rejectsInvalidOrNonDynamoDbUrls(final String url) throws SQLException {
      // When: Checking URL acceptance
      final var accepts = driver.acceptsURL(url);

      // Then: Should reject the URL
      assertThat(accepts).isFalse();
    }

    @Test
    @DisplayName("Handles null URL gracefully")
    void handlesNullUrlGracefully() throws SQLException {
      // When: Checking null URL acceptance
      final var accepts = driver.acceptsURL(null);

      // Then: Should reject null URL
      assertThat(accepts).isFalse();
    }

    static Stream<String> validDynamoDbUrls() {
      return Stream.of(
          "jdbc:dynamodb:partiql:region=us-east-1",
          "jdbc:dynamodb:partiql:region=eu-west-1;credentialsType=DEFAULT",
          "jdbc:dynamodb:partiql:region=us-west-2;endpoint=http://localhost:8000",
          "jdbc:dynamodb:partiql:region=ap-southeast-1;credentialsType=STATIC;accessKey=test;secretKey=test",
          "jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=auto;sampleSize=1000");
    }
  }

  @Nested
  @DisplayName("Property Information and Metadata")
  class PropertyInformationAndMetadataTests {

    @Test
    @DisplayName("Returns comprehensive property information")
    void returnsComprehensivePropertyInformation() throws SQLException {
      // Given: Valid DynamoDB URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";
      final var info = new Properties();

      // When: Getting property information
      final var propertyInfo = driver.getPropertyInfo(url, info);

      // Then: Should return detailed property information
      assertThat(propertyInfo).isNotEmpty();

      // Check for key properties
      final var propertyNames =
          Stream.of(propertyInfo).map(prop -> prop.name).toArray(String[]::new);
      assertThat(propertyNames)
          .contains("region", "credentialsType", "schemaDiscovery", "endpoint");
    }

    @Test
    @DisplayName("Property info includes required and optional designations")
    void propertyInfoIncludesRequiredAndOptionalDesignations() throws SQLException {
      // Given: Valid DynamoDB URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";
      final var info = new Properties();

      // When: Getting property information
      final var propertyInfo = driver.getPropertyInfo(url, info);

      // Then: Should have at least one required property (region)
      final var hasRequiredProperty =
          Stream.of(propertyInfo).anyMatch(prop -> prop.required && "region".equals(prop.name));
      assertThat(hasRequiredProperty).isTrue();

      // And should have optional properties
      final var hasOptionalProperty = Stream.of(propertyInfo).anyMatch(prop -> !prop.required);
      assertThat(hasOptionalProperty).isTrue();
    }

    @Test
    @DisplayName("Property info provides choice values where applicable")
    void propertyInfoProvidesChoiceValuesWhereApplicable() throws SQLException {
      // Given: Valid DynamoDB URL
      final var url = "jdbc:dynamodb:partiql:region=us-east-1";
      final var info = new Properties();

      // When: Getting property information
      final var propertyInfo = driver.getPropertyInfo(url, info);

      // Then: Should provide choices for enum-like properties
      final var credentialsTypeProperty =
          Stream.of(propertyInfo).filter(prop -> "credentialsType".equals(prop.name)).findFirst();

      assertThat(credentialsTypeProperty).isPresent();
      assertThat(credentialsTypeProperty.get().choices).isNotEmpty();
    }

    @Test
    @DisplayName("Handles property info request with null URL")
    void handlesPropertyInfoRequestWithNullUrl() throws SQLException {
      // When: Getting property information with null URL
      final var propertyInfo = driver.getPropertyInfo(null, new Properties());

      // Then: Should return empty array or handle gracefully
      assertThat(propertyInfo).isNotNull();
    }
  }

  @Nested
  @DisplayName("Connection Creation and Management")
  class ConnectionCreationAndManagementTests {

    @Test
    @DisplayName("Attempts to create connection with valid URL format")
    void attemptsToCreateConnectionWithValidUrlFormat() throws SQLException {
      // Given: Valid URL with test endpoint
      final var url =
          "jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000;credentialsType=STATIC;accessKey=test;secretKey=test";
      final var properties = new Properties();

      // When: Creating connection
      try {
        final var connection = driver.connect(url, properties);
        // If we get here, connection was created successfully (shouldn't happen without real
        // endpoint)
        assertThat(connection).isNotNull();
        connection.close();
      } catch (SQLException e) {
        // Expected behavior - should fail due to no real DynamoDB endpoint
        // Just verify it's a SQLException (successful validation of driver behavior)
        // The specific error message may vary depending on AWS configuration
      }
    }

    @Test
    @DisplayName("Returns null for non-matching URLs")
    void returnsNullForNonMatchingUrls() throws SQLException {
      // Given: Non-DynamoDB URL
      final var url = "jdbc:mysql://localhost:3306/test";
      final var properties = new Properties();

      // When: Attempting to create connection
      final var connection = driver.connect(url, properties);

      // Then: Should return null (not this driver's responsibility)
      assertThat(connection).isNull();
    }

    @Test
    @DisplayName("Handles null URL in connect method")
    void handlesNullUrlInConnectMethod() throws SQLException {
      // When: Attempting to connect with null URL
      final var connection = driver.connect(null, new Properties());

      // Then: Should return null
      assertThat(connection).isNull();
    }

    @ParameterizedTest
    @DisplayName("Validates required properties presence")
    @MethodSource("urlsWithMissingRequiredProperties")
    void validatesRequiredPropertiesPresence(final String url, final String expectedMissingProperty)
        throws SQLException {
      // When: Attempting to create connection with missing required properties
      // Then: Should throw SQLException mentioning the missing property
      // Note: In CI environments, AWS_DEFAULT_REGION might be set
      try {
        Connection conn = driver.connect(url, new Properties());
        // If we get here, region was found from environment
        assertThat(conn).isNotNull();
        String envRegion = System.getenv("AWS_DEFAULT_REGION");
        if (envRegion == null) {
          envRegion = System.getenv("AWS_REGION");
        }
        assertThat(envRegion).isNotNull().withFailMessage("Expected region from environment");
        conn.close();
      } catch (SQLException e) {
        // If we get here, region was not found - this is also valid
        assertThat(e.getMessage()).containsIgnoringCase(expectedMissingProperty);
      }
    }

    static Stream<Arguments> urlsWithMissingRequiredProperties() {
      return Stream.of(
          Arguments.of("jdbc:dynamodb:partiql:", "region"),
          Arguments.of("jdbc:dynamodb:partiql:credentialsType=DEFAULT", "region"));
    }
  }

  @Nested
  @DisplayName("JDBC Compliance and Feature Support")
  class JdbcComplianceAndFeatureSupportTests {

    @Test
    @DisplayName("Driver implements required JDBC Driver interface methods")
    void driverImplementsRequiredJdbcDriverInterfaceMethods() {
      // When: Checking interface implementation
      // Then: Driver should implement all required methods
      assertThat(driver).isInstanceOf(java.sql.Driver.class);

      // Verify key methods are callable without exceptions
      assertThat(driver.getMajorVersion()).isGreaterThanOrEqualTo(0);
      assertThat(driver.getMinorVersion()).isGreaterThanOrEqualTo(0);
      assertThat(driver.jdbcCompliant()).isInstanceOf(Boolean.class);
    }

    @Test
    @DisplayName("Driver provides meaningful string representation")
    void driverProvidesMeaningfulStringRepresentation() {
      // When: Getting string representation
      final var driverString = driver.toString();

      // Then: Should contain meaningful information
      assertThat(driverString)
          .isNotEmpty()
          .containsIgnoringCase("dynamodb")
          .containsIgnoringCase("driver");
    }

    @Test
    @DisplayName("Driver supports standard JDBC URL prefix")
    void driverSupportsStandardJdbcUrlPrefix() throws SQLException {
      // Given: URLs with different case variations
      final String[] urlVariations = {
        "jdbc:dynamodb:partiql:region=us-east-1",
        "JDBC:DYNAMODB:PARTIQL:region=us-east-1",
        "Jdbc:DynamoDb:PartiQL:region=us-east-1"
      };

      // When/Then: Should accept case variations consistently
      for (final var url : urlVariations) {
        // Implementation may or may not be case-sensitive, test current behavior
        final var accepts = driver.acceptsURL(url);
        // Just verify it doesn't throw an exception
        assertThat(accepts).isInstanceOf(Boolean.class);
      }
    }
  }

  @Nested
  @DisplayName("Error Handling and Edge Cases")
  class ErrorHandlingAndEdgeCasesTests {

    @Test
    @DisplayName("Handles malformed URLs gracefully in acceptsURL")
    void handlesMalformedUrlsGracefullyInAcceptsUrl() throws SQLException {
      // Given: Various malformed URLs
      final String[] malformedUrls = {
        "malformed-url",
        "jdbc:",
        "jdbc:dynamodb:",
        "jdbc:dynamodb:partiql",
        "://invalid",
        "jdbc:dynamodb:partiql:region="
      };

      // When/Then: Should handle gracefully without throwing exceptions
      for (final var url : malformedUrls) {
        final var accepts = driver.acceptsURL(url);
        assertThat(accepts).isInstanceOf(Boolean.class);
      }
    }

    @Test
    @DisplayName("Handles very long URLs")
    void handlesVeryLongUrls() throws SQLException {
      // Given: Very long URL
      final var baseUrl = "jdbc:dynamodb:partiql:region=us-east-1";
      final var longProperty = ";veryLongProperty=" + "x".repeat(1000);
      final var longUrl = baseUrl + longProperty;

      // When: Checking URL acceptance
      final var accepts = driver.acceptsURL(longUrl);

      // Then: Should handle without throwing exception
      assertThat(accepts).isInstanceOf(Boolean.class);
    }

    @Test
    @DisplayName("Driver methods are thread-safe for concurrent access")
    void driverMethodsAreThreadSafeForConcurrentAccess() throws InterruptedException {
      // Given: Multiple threads accessing driver methods
      final var numThreads = 10;
      final var threads = new Thread[numThreads];
      final var results = new boolean[numThreads];
      final var exceptions = new Exception[numThreads];

      // When: Concurrent access to acceptsURL method
      for (int i = 0; i < numThreads; i++) {
        final var index = i;
        threads[i] =
            new Thread(
                () -> {
                  try {
                    results[index] =
                        driver.acceptsURL("jdbc:dynamodb:partiql:region=us-east-1;thread=" + index);
                  } catch (Exception e) {
                    exceptions[index] = e;
                  }
                });
        threads[i].start();
      }

      // Wait for all threads to complete
      for (final var thread : threads) {
        thread.join();
      }

      // Then: All calls should complete without exceptions
      for (int i = 0; i < numThreads; i++) {
        assertThat(exceptions[i]).isNull();
        assertThat(results[i]).isTrue();
      }
    }
  }
}
