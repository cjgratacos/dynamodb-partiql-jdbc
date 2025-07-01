package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Simple DynamoDbDriver Tests")
class SimpleDynamoDbDriverTest {

  @Test
  @DisplayName("Driver can be registered and found")
  void driverCanBeRegisteredAndFound() throws SQLException {
    // Given: JDBC URL for DynamoDB
    final var url = "jdbc:dynamodb:partiql:region=us-east-1";

    // When: Getting driver
    final var driver = DriverManager.getDriver(url);

    // Then: Should return DynamoDB driver
    assertThat(driver).isInstanceOf(DynamoDbDriver.class);
  }

  @Test
  @DisplayName("Driver accepts valid URLs")
  void driverAcceptsValidUrls() throws SQLException {
    // Given: DynamoDB driver and various URLs
    final var driver = new DynamoDbDriver();

    // When/Then: Should accept valid URLs
    assertThat(driver.acceptsURL("jdbc:dynamodb:partiql:region=us-east-1")).isTrue();
    assertThat(
            driver.acceptsURL(
                "jdbc:dynamodb:partiql:region=us-west-2;endpoint=http://localhost:8000"))
        .isTrue();

    // Should reject invalid URLs
    assertThat(driver.acceptsURL("jdbc:mysql:localhost")).isFalse();
    assertThat(driver.acceptsURL(null)).isFalse();
    assertThat(driver.acceptsURL("")).isFalse();
  }

  @Test
  @DisplayName("Driver returns property info with all 38 properties")
  void driverReturnsPropertyInfoWithAll38Properties() throws SQLException {
    // Given: DynamoDB driver
    final var driver = new DynamoDbDriver();
    final var url = "jdbc:dynamodb:partiql:region=us-east-1";
    final var properties = new Properties();

    // When: Getting property info
    final var propertyInfo = driver.getPropertyInfo(url, properties);

    // Then: Should return all 38 properties
    assertThat(propertyInfo).hasSize(38);

    // Check some key properties exist
    final var propertyNames = java.util.Arrays.stream(propertyInfo).map(prop -> prop.name).toList();

    assertThat(propertyNames)
        .contains(
            "region",
            "endpoint",
            "credentialsType",
            "schemaDiscovery",
            "schemaOptimizations",
            "lazyLoadingStrategy",
            "preloadStrategy");
  }

  @Test
  @DisplayName("Driver version is correct")
  void driverVersionIsCorrect() {
    // Given: DynamoDB driver
    final var driver = new DynamoDbDriver();

    // When: Getting version
    final var majorVersion = driver.getMajorVersion();
    final var minorVersion = driver.getMinorVersion();

    // Then: Should return expected version
    assertThat(majorVersion).isEqualTo(1);
    assertThat(minorVersion).isEqualTo(0);
  }

  @Test
  @DisplayName("Driver is not JDBC compliant")
  void driverIsNotJdbcCompliant() {
    // Given: DynamoDB driver
    final var driver = new DynamoDbDriver();

    // When: Checking JDBC compliance
    final var isCompliant = driver.jdbcCompliant();

    // Then: Should not be compliant (DynamoDB is NoSQL)
    assertThat(isCompliant).isFalse();
  }

  @Test
  @DisplayName("Required property is marked correctly")
  void requiredPropertyIsMarkedCorrectly() throws SQLException {
    // Given: DynamoDB driver
    final var driver = new DynamoDbDriver();
    final var url = "jdbc:dynamodb:partiql:region=us-east-1";

    // When: Getting property info
    final var propertyInfo = driver.getPropertyInfo(url, null);

    // Then: Region should be required, others optional
    final var regionProperty =
        java.util.Arrays.stream(propertyInfo)
            .filter(prop -> "region".equals(prop.name))
            .findFirst()
            .orElseThrow();

    assertThat(regionProperty.required).isTrue();

    final var endpointProperty =
        java.util.Arrays.stream(propertyInfo)
            .filter(prop -> "endpoint".equals(prop.name))
            .findFirst()
            .orElseThrow();

    assertThat(endpointProperty.required).isFalse();
  }
}
