package org.cjgratacos.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * Utility for managing AWS profile files during testing.
 *
 * <p>This class provides utilities for creating, modifying, and cleaning up AWS credential and
 * config files in temporary locations for testing purposes. It ensures that tests don't interfere
 * with the developer's actual AWS configuration.
 */
public final class AwsProfileManager {

  private final Path tempDirectory;
  private final Path credentialsFile;
  private final Path configFile;

  /**
   * Creates a new AWS profile manager with temporary directories.
   *
   * @throws IOException if temporary directory creation fails
   */
  public AwsProfileManager() throws IOException {
    this.tempDirectory = Files.createTempDirectory("aws-test-");
    this.credentialsFile = this.tempDirectory.resolve("credentials");
    this.configFile = this.tempDirectory.resolve("config");
  }

  /**
   * Gets the path to the temporary credentials file.
   *
   * @return Path to credentials file
   */
  public Path getCredentialsFile() {
    return this.credentialsFile;
  }

  /**
   * Gets the path to the temporary config file.
   *
   * @return Path to config file
   */
  public Path getConfigFile() {
    return this.configFile;
  }

  /**
   * Gets the temporary AWS directory path.
   *
   * @return Path to temporary AWS directory
   */
  public Path getAwsDirectory() {
    return this.tempDirectory;
  }

  /**
   * Creates a basic profile with access key and secret key.
   *
   * @param profileName Name of the profile
   * @param accessKey AWS access key
   * @param secretKey AWS secret key
   * @param region AWS region
   * @throws IOException if file writing fails
   */
  public void createBasicProfile(
      final String profileName, final String accessKey, final String secretKey, final String region)
      throws IOException {
    final String content =
        String.format(
            """
                [%s]%n\
                aws_access_key_id = %s%n\
                aws_secret_access_key = %s%n\
                region = %s%n""",
            profileName, accessKey, secretKey, region);

    this.appendToCredentialsFile(content);
  }

  /**
   * Creates an assume role profile.
   *
   * @param profileName Name of the profile
   * @param roleArn ARN of the role to assume
   * @param sourceProfile Source profile for credentials
   * @param region AWS region
   * @throws IOException if file writing fails
   */
  public void createAssumeRoleProfile(
      final String profileName,
      final String roleArn,
      final String sourceProfile,
      final String region)
      throws IOException {
    final String content =
        String.format(
            """
                [profile %s]%n\
                role_arn = %s%n\
                source_profile = %s%n\
                region = %s%n""",
            profileName, roleArn, sourceProfile, region);

    this.appendToConfigFile(content);
  }

  /**
   * Creates an assume role profile with external ID.
   *
   * @param profileName Name of the profile
   * @param roleArn ARN of the role to assume
   * @param sourceProfile Source profile for credentials
   * @param externalId External ID for role assumption
   * @param region AWS region
   * @throws IOException if file writing fails
   */
  public void createAssumeRoleProfileWithExternalId(
      final String profileName,
      final String roleArn,
      final String sourceProfile,
      final String externalId,
      final String region)
      throws IOException {
    final String content =
        String.format(
            """
                [profile %s]%n\
                role_arn = %s%n\
                source_profile = %s%n\
                external_id = %s%n\
                region = %s%n""",
            profileName, roleArn, sourceProfile, externalId, region);

    this.appendToConfigFile(content);
  }

  /**
   * Creates a profile with session token (temporary credentials).
   *
   * @param profileName Name of the profile
   * @param accessKey AWS access key
   * @param secretKey AWS secret key
   * @param sessionToken AWS session token
   * @param region AWS region
   * @throws IOException if file writing fails
   */
  public void createSessionTokenProfile(
      final String profileName,
      final String accessKey,
      final String secretKey,
      final String sessionToken,
      final String region)
      throws IOException {
    final String content =
        String.format(
            """
                [%s]%n\
                aws_access_key_id = %s%n\
                aws_secret_access_key = %s%n\
                aws_session_token = %s%n\
                region = %s%n""",
            profileName, accessKey, secretKey, sessionToken, region);

    this.appendToCredentialsFile(content);
  }

  /**
   * Sets up environment variables to point AWS SDK to the temporary profile directory.
   *
   * @return Properties containing environment variables
   */
  public Properties getAwsEnvironmentProperties() {
    final Properties props = new Properties();
    props.setProperty("AWS_CONFIG_FILE", this.configFile.toString());
    props.setProperty("AWS_SHARED_CREDENTIALS_FILE", this.credentialsFile.toString());
    return props;
  }

  /**
   * Creates a comprehensive test setup with multiple profiles for thorough testing.
   *
   * @throws IOException if file creation fails
   */
  public void createComprehensiveTestSetup() throws IOException {
    // Basic profile for testing
    this.createBasicProfile(
        "default",
        CredentialTestFixtures.TEST_ACCESS_KEY,
        CredentialTestFixtures.TEST_SECRET_KEY,
        CredentialTestFixtures.TEST_REGION);

    // Test profile for specific testing
    this.createBasicProfile(
        "test-profile",
        CredentialTestFixtures.TEST_ACCESS_KEY,
        CredentialTestFixtures.TEST_SECRET_KEY,
        CredentialTestFixtures.TEST_REGION);

    // Session token profile
    this.createSessionTokenProfile(
        "session-profile",
        CredentialTestFixtures.TEST_ACCESS_KEY,
        CredentialTestFixtures.TEST_SECRET_KEY,
        CredentialTestFixtures.TEST_SESSION_TOKEN,
        CredentialTestFixtures.TEST_REGION);

    // Assume role profile
    this.createAssumeRoleProfile(
        "assume-role-profile",
        CredentialTestFixtures.getTestRoleArn(),
        "default",
        CredentialTestFixtures.TEST_REGION);

    // Assume role with external ID
    this.createAssumeRoleProfileWithExternalId(
        "external-id-profile",
        CredentialTestFixtures.getTestRoleArn(),
        "default",
        CredentialTestFixtures.getTestExternalId(),
        CredentialTestFixtures.TEST_REGION);
  }

  /**
   * Cleans up temporary files and directories.
   *
   * @throws IOException if cleanup fails
   */
  public void cleanup() throws IOException {
    if (Files.exists(this.credentialsFile)) {
      Files.delete(this.credentialsFile);
    }
    if (Files.exists(this.configFile)) {
      Files.delete(this.configFile);
    }
    if (Files.exists(this.tempDirectory)) {
      Files.delete(this.tempDirectory);
    }
  }

  /**
   * Appends content to the credentials file.
   *
   * @param content Content to append
   * @throws IOException if writing fails
   */
  private void appendToCredentialsFile(final String content) throws IOException {
    Files.write(
        this.credentialsFile,
        (content + System.lineSeparator()).getBytes(),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  /**
   * Appends content to the config file.
   *
   * @param content Content to append
   * @throws IOException if writing fails
   */
  private void appendToConfigFile(final String content) throws IOException {
    Files.write(
        this.configFile,
        (content + System.lineSeparator()).getBytes(),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }
}
