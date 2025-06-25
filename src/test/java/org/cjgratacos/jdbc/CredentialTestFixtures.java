package org.cjgratacos.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test fixtures and utilities for generating AWS credential test data.
 *
 * <p>This class provides utilities for creating test credentials, AWS profile files, environment
 * variables, and other test data needed for comprehensive credential provider testing.
 */
public final class CredentialTestFixtures {

  // Test credential values - safe for testing since they're fake
  public static final String TEST_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";
  public static final String TEST_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  public static final String TEST_SESSION_TOKEN =
      "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE";
  public static final String TEST_REGION = "us-east-1";
  public static final String TEST_ENDPOINT = "http://localhost:8000";

  private CredentialTestFixtures() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates environment variables for DEFAULT credential provider testing.
   *
   * @return Map of environment variables for AWS credentials
   */
  public static Map<String, String> createDefaultCredentialEnvironment() {
    Map<String, String> env = new HashMap<>();
    env.put("AWS_ACCESS_KEY_ID", TEST_ACCESS_KEY);
    env.put("AWS_SECRET_ACCESS_KEY", TEST_SECRET_KEY);
    env.put("AWS_DEFAULT_REGION", TEST_REGION);
    return env;
  }

  /**
   * Creates environment variables for DEFAULT credential provider with session token.
   *
   * @return Map of environment variables for temporary AWS credentials
   */
  public static Map<String, String> createDefaultCredentialWithSessionEnvironment() {
    Map<String, String> env = createDefaultCredentialEnvironment();
    env.put("AWS_SESSION_TOKEN", TEST_SESSION_TOKEN);
    return env;
  }

  /**
   * Creates system properties for DEFAULT credential provider testing.
   *
   * @return Properties object with AWS credential system properties
   */
  public static Properties createDefaultCredentialSystemProperties() {
    Properties props = new Properties();
    props.setProperty("aws.accessKeyId", TEST_ACCESS_KEY);
    props.setProperty("aws.secretAccessKey", TEST_SECRET_KEY);
    props.setProperty("aws.region", TEST_REGION);
    return props;
  }

  /**
   * Creates JDBC URL for STATIC credential provider testing.
   *
   * @param endpoint DynamoDB endpoint URL
   * @return JDBC URL with static credentials
   */
  public static String createStaticCredentialJdbcUrl(String endpoint) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=STATIC;accessKey=%s;secretKey=%s;endpoint=%s",
        TEST_REGION, TEST_ACCESS_KEY, TEST_SECRET_KEY, endpoint);
  }

  /**
   * Creates JDBC URL for STATIC credential provider testing with session token.
   *
   * @param endpoint DynamoDB endpoint URL
   * @return JDBC URL with static credentials including session token
   */
  public static String createStaticCredentialWithSessionJdbcUrl(String endpoint) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=STATIC;accessKey=%s;secretKey=%s;sessionToken=%s;endpoint=%s",
        TEST_REGION, TEST_ACCESS_KEY, TEST_SECRET_KEY, TEST_SESSION_TOKEN, endpoint);
  }

  /**
   * Creates JDBC URL for PROFILE credential provider testing.
   *
   * @param endpoint DynamoDB endpoint URL
   * @param profileName AWS profile name
   * @return JDBC URL with profile credentials
   */
  public static String createProfileCredentialJdbcUrl(String endpoint, String profileName) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=PROFILE;profileName=%s;endpoint=%s",
        TEST_REGION, profileName, endpoint);
  }

  /**
   * Creates JDBC URL for DEFAULT credential provider testing.
   *
   * @param endpoint DynamoDB endpoint URL
   * @return JDBC URL with default credentials
   */
  public static String createDefaultCredentialJdbcUrl(String endpoint) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=DEFAULT;endpoint=%s",
        TEST_REGION, endpoint);
  }

  /**
   * Creates an AWS credentials file with basic profile.
   *
   * @param credentialsFile Path to credentials file
   * @param profileName Profile name
   * @throws IOException if file creation fails
   */
  public static void createBasicCredentialsFile(Path credentialsFile, String profileName)
      throws IOException {
    String content =
        String.format(
            "[%s]%n"
                + "aws_access_key_id = %s%n"
                + "aws_secret_access_key = %s%n"
                + "region = %s%n",
            profileName, TEST_ACCESS_KEY, TEST_SECRET_KEY, TEST_REGION);

    Files.write(
        credentialsFile, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
  }

  /**
   * Creates an AWS credentials file with assume role profile.
   *
   * @param credentialsFile Path to credentials file
   * @param profileName Profile name
   * @param roleArn ARN of role to assume
   * @param sourceProfile Source profile for credentials
   * @throws IOException if file creation fails
   */
  public static void createAssumeRoleCredentialsFile(
      Path credentialsFile, String profileName, String roleArn, String sourceProfile)
      throws IOException {
    String content =
        String.format(
            "[%s]%n"
                + "aws_access_key_id = %s%n"
                + "aws_secret_access_key = %s%n"
                + "region = %s%n%n"
                + "[profile %s]%n"
                + "role_arn = %s%n"
                + "source_profile = %s%n"
                + "region = %s%n",
            sourceProfile,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            profileName,
            roleArn,
            sourceProfile,
            TEST_REGION);

    Files.write(
        credentialsFile, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
  }

  /**
   * Creates invalid credentials for error testing.
   *
   * @return Map of invalid environment variables
   */
  public static Map<String, String> createInvalidCredentialEnvironment() {
    Map<String, String> env = new HashMap<>();
    env.put("AWS_ACCESS_KEY_ID", "INVALID_ACCESS_KEY");
    env.put("AWS_SECRET_ACCESS_KEY", "INVALID_SECRET_KEY");
    env.put("AWS_DEFAULT_REGION", TEST_REGION);
    return env;
  }

  /**
   * Creates JDBC URL with invalid static credentials.
   *
   * @param endpoint DynamoDB endpoint URL
   * @return JDBC URL with invalid static credentials
   */
  public static String createInvalidStaticCredentialJdbcUrl(String endpoint) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=STATIC;accessKey=INVALID;secretKey=INVALID;endpoint=%s",
        TEST_REGION, endpoint);
  }

  /**
   * Creates JDBC URL with missing required credential parameters.
   *
   * @param endpoint DynamoDB endpoint URL
   * @return JDBC URL missing required parameters
   */
  public static String createMissingParameterJdbcUrl(String endpoint) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=STATIC;endpoint=%s",
        TEST_REGION, endpoint);
  }

  /**
   * Test role ARN for assume role testing.
   *
   * @return Example role ARN
   */
  public static String getTestRoleArn() {
    return "arn:aws:iam::123456789012:role/TestRole";
  }

  /**
   * Test external ID for assume role testing.
   *
   * @return Example external ID
   */
  public static String getTestExternalId() {
    return "test-external-id";
  }
}
