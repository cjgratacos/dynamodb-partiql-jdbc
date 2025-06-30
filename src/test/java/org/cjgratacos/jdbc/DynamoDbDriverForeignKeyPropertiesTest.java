package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamoDbDriverForeignKeyPropertiesTest {

  private DynamoDbDriver driver;

  @BeforeEach
  void setUp() {
    driver = new DynamoDbDriver();
  }

  @Test
  void testDriverExposesAllForeignKeyProperties() throws SQLException {
    String url = "jdbc:dynamodb:partiql:region=us-east-1";
    Properties info = new Properties();

    DriverPropertyInfo[] properties = driver.getPropertyInfo(url, info);

    // Convert to set of property names for easier checking
    Set<String> propertyNames =
        Arrays.stream(properties).map(p -> p.name).collect(Collectors.toSet());

    // Verify foreign key properties are exposed
    assertThat(propertyNames).contains("foreignKeysFile");
    assertThat(propertyNames).contains("foreignKeysTable");
    assertThat(propertyNames).contains("validateForeignKeys");
    assertThat(propertyNames).contains("foreignKeyValidationMode");
    assertThat(propertyNames).contains("cacheTableMetadata");
  }

  @Test
  void testForeignKeyPropertiesHaveCorrectDefaults() throws SQLException {
    String url = "jdbc:dynamodb:partiql:region=us-east-1";
    Properties info = new Properties();

    DriverPropertyInfo[] properties = driver.getPropertyInfo(url, info);

    // Find specific properties and check their defaults
    for (DriverPropertyInfo prop : properties) {
      switch (prop.name) {
        case "validateForeignKeys":
          assertThat(prop.value).isEqualTo("false");
          assertThat(prop.choices).containsExactly("true", "false");
          assertThat(prop.required).isFalse();
          assertThat(prop.description).contains("Enable/disable foreign key validation");
          break;
        case "foreignKeyValidationMode":
          assertThat(prop.value).isEqualTo("lenient");
          assertThat(prop.choices).containsExactly("strict", "lenient", "off");
          assertThat(prop.required).isFalse();
          assertThat(prop.description).contains("Foreign key validation mode");
          break;
        case "cacheTableMetadata":
          assertThat(prop.value).isEqualTo("true");
          assertThat(prop.choices).containsExactly("true", "false");
          assertThat(prop.required).isFalse();
          assertThat(prop.description).contains("Cache table/column existence checks");
          break;
        case "foreignKeysFile":
          assertThat(prop.value).isNull();
          assertThat(prop.required).isFalse();
          assertThat(prop.description).contains("Path to properties file");
          break;
        case "foreignKeysTable":
          assertThat(prop.value).isNull();
          assertThat(prop.required).isFalse();
          assertThat(prop.description).contains("DynamoDB table name");
          break;
      }
    }
  }

  @Test
  void testForeignKeyPropertiesInheritFromUrl() throws SQLException {
    String url =
        "jdbc:dynamodb:partiql:region=us-east-1;"
            + "validateForeignKeys=true;"
            + "foreignKeyValidationMode=strict;"
            + "foreignKeysFile=/path/to/keys.properties";
    Properties info = new Properties();

    DriverPropertyInfo[] properties = driver.getPropertyInfo(url, info);

    // Properties should inherit values from URL
    for (DriverPropertyInfo prop : properties) {
      switch (prop.name) {
        case "validateForeignKeys":
          assertThat(prop.value).isEqualTo("true");
          break;
        case "foreignKeyValidationMode":
          assertThat(prop.value).isEqualTo("strict");
          break;
        case "foreignKeysFile":
          assertThat(prop.value).isEqualTo("/path/to/keys.properties");
          break;
      }
    }
  }

  @Test
  void testPropertyCount() throws SQLException {
    String url = "jdbc:dynamodb:partiql:region=us-east-1";
    Properties info = new Properties();

    DriverPropertyInfo[] properties = driver.getPropertyInfo(url, info);

    // Should have exactly 38 properties (including the new foreign key properties)
    assertThat(properties).hasSize(38);
  }

  @Test
  void testAllPropertiesHaveDescriptions() throws SQLException {
    String url = "jdbc:dynamodb:partiql:region=us-east-1";
    Properties info = new Properties();

    DriverPropertyInfo[] properties = driver.getPropertyInfo(url, info);

    // All properties should have descriptions
    for (DriverPropertyInfo prop : properties) {
      assertThat(prop.description)
          .as("Property %s should have a description", prop.name)
          .isNotNull()
          .isNotEmpty();
    }
  }
}
