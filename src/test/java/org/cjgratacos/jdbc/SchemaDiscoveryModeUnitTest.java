package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("SchemaDiscoveryMode Unit Tests")
class SchemaDiscoveryModeUnitTest {

  @Nested
  @DisplayName("Basic Enum Tests")
  class BasicEnumTests {

    @Test
    @DisplayName("All enum values have correct string representations")
    void allEnumValuesHaveCorrectStringRepresentations() {
      // When/Then: Each enum should have expected string value
      assertThat(SchemaDiscoveryMode.AUTO.getValue()).isEqualTo("auto");
      assertThat(SchemaDiscoveryMode.HINTS.getValue()).isEqualTo("hints");
      assertThat(SchemaDiscoveryMode.SAMPLING.getValue()).isEqualTo("sampling");
      assertThat(SchemaDiscoveryMode.DISABLED.getValue()).isEqualTo("disabled");
    }

    @Test
    @DisplayName("All enum values exist and are accessible")
    void allEnumValuesExistAndAreAccessible() {
      // Given: Expected enum values
      final var values = SchemaDiscoveryMode.values();

      // Then: Should have exactly 4 values
      assertThat(values).hasSize(4);
      assertThat(values)
          .containsExactly(
              SchemaDiscoveryMode.AUTO,
              SchemaDiscoveryMode.HINTS,
              SchemaDiscoveryMode.SAMPLING,
              SchemaDiscoveryMode.DISABLED);
    }
  }

  @Nested
  @DisplayName("String Parsing Tests")
  class StringParsingTests {

    @ParameterizedTest
    @CsvSource({"auto, AUTO", "hints, HINTS", "sampling, SAMPLING", "disabled, DISABLED"})
    @DisplayName("Valid strings parse to correct enum values")
    void validStringsParsesToCorrectEnumValues(String input, SchemaDiscoveryMode expected) {
      // When: Parsing valid string
      final var result = SchemaDiscoveryMode.fromString(input);

      // Then: Should return correct enum
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"AUTO, AUTO", "HINTS, HINTS", "SAMPLING, SAMPLING", "DISABLED, DISABLED"})
    @DisplayName("Uppercase strings parse correctly")
    void uppercaseStringsParsesCorrectly(String input, SchemaDiscoveryMode expected) {
      // When: Parsing uppercase string
      final var result = SchemaDiscoveryMode.fromString(input);

      // Then: Should return correct enum (case insensitive)
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"Auto, AUTO", "Hints, HINTS", "Sampling, SAMPLING", "Disabled, DISABLED"})
    @DisplayName("Mixed case strings parse correctly")
    void mixedCaseStringsParsesCorrectly(String input, SchemaDiscoveryMode expected) {
      // When: Parsing mixed case string
      final var result = SchemaDiscoveryMode.fromString(input);

      // Then: Should return correct enum
      assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"  auto  ", " hints ", "\tsampling\t", "\ndisabled\n"})
    @DisplayName("Strings with whitespace are trimmed correctly")
    void stringsWithWhitespaceAreTrimmedCorrectly(String input) {
      // When: Parsing string with whitespace
      final var result = SchemaDiscoveryMode.fromString(input);

      // Then: Should parse successfully (not default to AUTO)
      assertThat(result).isNotNull();
      assertThat(result)
          .isIn(
              SchemaDiscoveryMode.AUTO,
              SchemaDiscoveryMode.HINTS,
              SchemaDiscoveryMode.SAMPLING,
              SchemaDiscoveryMode.DISABLED);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "unknown", "xyz", "123", "auto_mode"})
    @DisplayName("Invalid strings default to AUTO")
    void invalidStringsDefaultToAuto(String input) {
      // When: Parsing invalid string
      final var result = SchemaDiscoveryMode.fromString(input);

      // Then: Should default to AUTO
      assertThat(result).isEqualTo(SchemaDiscoveryMode.AUTO);
    }

    @Test
    @DisplayName("Null string defaults to AUTO")
    void nullStringDefaultsToAuto() {
      // When: Parsing null
      final var result = SchemaDiscoveryMode.fromString(null);

      // Then: Should default to AUTO
      assertThat(result).isEqualTo(SchemaDiscoveryMode.AUTO);
    }

    @Test
    @DisplayName("Empty string defaults to AUTO")
    void emptyStringDefaultsToAuto() {
      // When: Parsing empty string
      final var result = SchemaDiscoveryMode.fromString("");

      // Then: Should default to AUTO
      assertThat(result).isEqualTo(SchemaDiscoveryMode.AUTO);
    }

    @Test
    @DisplayName("Whitespace-only string defaults to AUTO")
    void whitespaceOnlyStringDefaultsToAuto() {
      // When: Parsing whitespace-only strings
      assertThat(SchemaDiscoveryMode.fromString("   ")).isEqualTo(SchemaDiscoveryMode.AUTO);
      assertThat(SchemaDiscoveryMode.fromString("\t\t")).isEqualTo(SchemaDiscoveryMode.AUTO);
      assertThat(SchemaDiscoveryMode.fromString("\n\n")).isEqualTo(SchemaDiscoveryMode.AUTO);
    }
  }

  @Nested
  @DisplayName("Behavior Tests")
  class BehaviorTests {

    @Test
    @DisplayName("requiresSampling returns correct values")
    void requiresSamplingReturnsCorrectValues() {
      // Then: Only SAMPLING and AUTO should require sampling
      assertThat(SchemaDiscoveryMode.AUTO.requiresSampling()).isTrue();
      assertThat(SchemaDiscoveryMode.SAMPLING.requiresSampling()).isTrue();

      assertThat(SchemaDiscoveryMode.HINTS.requiresSampling()).isFalse();
      assertThat(SchemaDiscoveryMode.DISABLED.requiresSampling()).isFalse();
    }

    @Test
    @DisplayName("isEnabled returns correct values")
    void isEnabledReturnsCorrectValues() {
      // Then: Only DISABLED should be disabled
      assertThat(SchemaDiscoveryMode.AUTO.isEnabled()).isTrue();
      assertThat(SchemaDiscoveryMode.HINTS.isEnabled()).isTrue();
      assertThat(SchemaDiscoveryMode.SAMPLING.isEnabled()).isTrue();

      assertThat(SchemaDiscoveryMode.DISABLED.isEnabled()).isFalse();
    }

    @Test
    @DisplayName("getValue returns consistent values")
    void getValueReturnsConsistentValues() {
      // When: Getting values and parsing them back
      for (SchemaDiscoveryMode mode : SchemaDiscoveryMode.values()) {
        final var value = mode.getValue();
        final var parsedBack = SchemaDiscoveryMode.fromString(value);

        // Then: Should round-trip correctly
        assertThat(parsedBack).isEqualTo(mode);
      }
    }
  }

  @Nested
  @DisplayName("DynamoDB Context Tests")
  class DynamoDbContextTests {

    @Test
    @DisplayName("AUTO mode is appropriate for DynamoDB flexibility")
    void autoModeIsAppropriateForDynamoDbFlexibility() {
      // Given: AUTO mode (default)
      final var mode = SchemaDiscoveryMode.AUTO;

      // Then: Should be enabled and support sampling for schemaless nature
      assertThat(mode.isEnabled()).isTrue();
      assertThat(mode.requiresSampling()).isTrue();
    }

    @Test
    @DisplayName("HINTS mode is efficient for metadata-rich scenarios")
    void hintsModeIsEfficientForMetadataRichScenarios() {
      // Given: HINTS mode
      final var mode = SchemaDiscoveryMode.HINTS;

      // Then: Should be enabled but not require expensive sampling
      assertThat(mode.isEnabled()).isTrue();
      assertThat(mode.requiresSampling()).isFalse();
    }

    @Test
    @DisplayName("SAMPLING mode provides comprehensive discovery")
    void samplingModeProvisionComprehensiveDiscovery() {
      // Given: SAMPLING mode
      final var mode = SchemaDiscoveryMode.SAMPLING;

      // Then: Should be enabled and require sampling for accuracy
      assertThat(mode.isEnabled()).isTrue();
      assertThat(mode.requiresSampling()).isTrue();
    }

    @Test
    @DisplayName("DISABLED mode minimizes overhead")
    void disabledModeMinimizesOverhead() {
      // Given: DISABLED mode
      final var mode = SchemaDiscoveryMode.DISABLED;

      // Then: Should be disabled and not require sampling
      assertThat(mode.isEnabled()).isFalse();
      assertThat(mode.requiresSampling()).isFalse();
    }
  }

  @Nested
  @DisplayName("Configuration Integration Tests")
  class ConfigurationIntegrationTests {

    @Test
    @DisplayName("Default parsing behavior supports JDBC URL configuration")
    void defaultParsingBehaviorSupportsJdbcUrlConfiguration() {
      // Given: Typical JDBC URL parameter values
      final var jdbcStyleValues =
          new String[] {
            "auto", "hints", "sampling", "disabled",
            "AUTO", "HINTS", "SAMPLING", "DISABLED"
          };

      // When/Then: All should parse without defaulting to AUTO
      for (String value : jdbcStyleValues) {
        final var parsed = SchemaDiscoveryMode.fromString(value);
        assertThat(parsed).isNotNull();
        // Verify round-trip consistency
        assertThat(SchemaDiscoveryMode.fromString(parsed.getValue())).isEqualTo(parsed);
      }
    }

    @Test
    @DisplayName("Robust error handling for user input")
    void robustErrorHandlingForUserInput() {
      // Given: Various invalid user inputs
      final var invalidInputs =
          new String[] {
            "autoMode", "hint", "sample", "disable",
            "0", "1", "true", "false",
            "auto-mode", "auto_mode", "auto.mode"
          };

      // When: Parsing invalid inputs
      for (String invalidInput : invalidInputs) {
        final var result = SchemaDiscoveryMode.fromString(invalidInput);

        // Then: Should gracefully default to AUTO
        assertThat(result).isEqualTo(SchemaDiscoveryMode.AUTO);
      }
    }
  }
}
