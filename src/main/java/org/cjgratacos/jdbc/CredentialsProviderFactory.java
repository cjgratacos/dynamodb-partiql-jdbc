package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Factory class for creating AWS credentials providers for DynamoDB connections.
 *
 * <p>This factory creates appropriate AWS SDK credentials providers based on the credentials type
 * specified in the JDBC URL properties. It supports multiple authentication mechanisms to
 * accommodate different deployment scenarios.
 *
 * <h2>Supported Credential Types:</h2>
 *
 * <dl>
 *   <dt><strong>DEFAULT</strong>
 *   <dd>Uses AWS SDK's DefaultCredentialsProvider which tries multiple sources:
 *       <ul>
 *         <li>Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 *         <li>Java system properties (aws.accessKeyId, aws.secretAccessKey)
 *         <li>AWS credentials file (~/.aws/credentials)
 *         <li>AWS IAM roles for EC2 instances
 *         <li>Container credentials for ECS/Fargate
 *       </ul>
 *   <dt><strong>STATIC</strong>
 *   <dd>Uses explicitly provided access key and secret key from URL properties:
 *       <ul>
 *         <li><code>accessKey</code> - AWS access key ID (required)
 *         <li><code>secretKey</code> - AWS secret access key (required)
 *         <li><code>sessionToken</code> - AWS session token (optional, for temporary credentials)
 *       </ul>
 *   <dt><strong>PROFILE</strong>
 *   <dd>Uses a named profile from AWS credentials file:
 *       <ul>
 *         <li><code>profileName</code> - Name of the profile to use (required)
 *       </ul>
 * </dl>
 *
 * <h2>URL Examples:</h2>
 *
 * <pre>{@code
 * // DEFAULT credentials (recommended for most cases)
 * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;
 *
 * // STATIC credentials (for applications with embedded credentials)
 * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI...;secretKey=wJal...;
 *
 * // PROFILE credentials (for development with multiple AWS accounts)
 * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=PROFILE;profileName=dev-account;
 * }</pre>
 *
 * <h2>Security Considerations:</h2>
 *
 * <ul>
 *   <li><strong>DEFAULT</strong> is recommended for production as it uses secure credential sources
 *   <li><strong>STATIC</strong> should be avoided in production due to credential exposure risks
 *   <li><strong>PROFILE</strong> is useful for development environments with multiple AWS accounts
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see CredentialsType
 * @see DynamoDbConnection
 * @see <a
 *     href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html">AWS
 *     SDK Credentials</a>
 */
public class CredentialsProviderFactory {

  /** Private constructor to prevent instantiation of this factory class. */
  private CredentialsProviderFactory() {
    // Factory class - no instantiation
  }

  /**
   * Creates an appropriate AWS credentials provider based on the credentials type specified in
   * properties.
   *
   * <p>This method examines the {@code credentialsType} property and creates the corresponding AWS
   * SDK credentials provider. If no credentials type is specified, DEFAULT is used.
   *
   * <p>Supported credential types:
   *
   * <ul>
   *   <li>{@link CredentialsType#DEFAULT} - Uses AWS DefaultCredentialsProvider chain
   *   <li>{@link CredentialsType#STATIC} - Uses provided access key and secret key
   *   <li>{@link CredentialsType#PROFILE} - Uses named AWS profile
   * </ul>
   *
   * @param properties connection properties extracted from JDBC URL
   * @return an appropriate AwsCredentialsProvider instance
   * @throws SQLException if the credentials type is invalid or required properties are missing
   * @see CredentialsType
   * @see #createStaticCredentialsProvider(Properties)
   * @see #createProfileCredentialsProvider(Properties)
   */
  public static AwsCredentialsProvider createCredentialsProvider(final Properties properties)
      throws SQLException {
    final var credentialsType = properties.getProperty("credentialsType", "DEFAULT").toUpperCase();

    try {
      return switch (CredentialsType.valueOf(credentialsType)) {
        case STATIC -> CredentialsProviderFactory.createStaticCredentialsProvider(properties);
        case PROFILE -> CredentialsProviderFactory.createProfileCredentialsProvider(properties);
        case DEFAULT -> DefaultCredentialsProvider.builder().build();
        default -> DefaultCredentialsProvider.builder().build();
      };
    } catch (final IllegalArgumentException e) {
      throw new SQLException(
          "Invalid credentials type: "
              + credentialsType
              + ". Supported types: STATIC, PROFILE, DEFAULT",
          e);
    }
  }

  /**
   * Creates a static credentials provider using explicit access key and secret key.
   *
   * <p>This method creates AWS credentials from the provided access key and secret key. If a
   * session token is also provided, it creates session credentials for temporary access.
   *
   * <p>Required properties:
   *
   * <ul>
   *   <li>{@code accessKey} - AWS access key ID
   *   <li>{@code secretKey} - AWS secret access key
   * </ul>
   *
   * <p>Optional properties:
   *
   * <ul>
   *   <li>{@code sessionToken} - AWS session token for temporary credentials
   * </ul>
   *
   * @param properties connection properties containing credential information
   * @return a StaticCredentialsProvider with the specified credentials
   * @throws SQLException if required properties are missing or empty
   * @see AwsBasicCredentials
   * @see AwsSessionCredentials
   * @see StaticCredentialsProvider
   */
  private static AwsCredentialsProvider createStaticCredentialsProvider(final Properties properties)
      throws SQLException {
    final var accessKey = properties.getProperty("accessKey");
    final var secretKey = properties.getProperty("secretKey");
    final var sessionToken = properties.getProperty("sessionToken");

    if (accessKey == null || accessKey.trim().isEmpty()) {
      throw new SQLException("accessKey is required for STATIC credentials type");
    }

    if (secretKey == null || secretKey.trim().isEmpty()) {
      throw new SQLException("secretKey is required for STATIC credentials type");
    }

    // If sessionToken is provided, use it to create a session credentials provider
    if (sessionToken != null && !sessionToken.trim().isEmpty()) {
      return StaticCredentialsProvider.create(
          AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
    }

    // Otherwise, use the accessKey and secretKey to create a basic credentials provider
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
  }

  /**
   * Creates a profile-based credentials provider using a named AWS profile.
   *
   * <p>This method creates credentials provider that reads from AWS credentials file using the
   * specified profile name. The credentials file is expected to be in the standard location
   * (~/.aws/credentials).
   *
   * <p>Required properties:
   *
   * <ul>
   *   <li>{@code profileName} - Name of the AWS profile to use
   * </ul>
   *
   * @param properties connection properties containing profile information
   * @return a ProfileCredentialsProvider for the specified profile
   * @throws SQLException if the profileName property is missing or empty
   * @see ProfileCredentialsProvider
   */
  private static AwsCredentialsProvider createProfileCredentialsProvider(
      final Properties properties) throws SQLException {

    final var profileName = properties.getProperty("profileName");

    if (profileName == null || profileName.trim().isEmpty()) {
      throw new SQLException("profileName is required for PROFILE credentials type");
    }

    return ProfileCredentialsProvider.builder().profileName(profileName).build();
  }
}
