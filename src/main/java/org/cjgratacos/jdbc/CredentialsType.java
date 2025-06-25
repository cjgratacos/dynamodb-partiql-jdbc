package org.cjgratacos.jdbc;

/**
 * Enumeration of supported AWS credential provider types for DynamoDB JDBC connections.
 *
 * <p>This enum defines the different authentication mechanisms available when connecting to Amazon
 * DynamoDB through the JDBC driver. Each type corresponds to a different AWS SDK credentials
 * provider implementation.
 *
 * <h2>Usage in JDBC URLs:</h2>
 *
 * <p>The credential type is specified using the {@code credentialsType} parameter in the JDBC URL.
 * If not specified, {@link #DEFAULT} is used.
 *
 * <pre>{@code
 * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=...;secretKey=...;
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see CredentialsProviderFactory
 * @see DynamoDbDriver
 */
public enum CredentialsType {

  /**
   * Static credentials using explicitly provided access key and secret key.
   *
   * <p>Requires the following properties in the JDBC URL:
   *
   * <ul>
   *   <li><code>accessKey</code> - AWS access key ID (required)
   *   <li><code>secretKey</code> - AWS secret access key (required)
   *   <li><code>sessionToken</code> - AWS session token (optional, for temporary credentials)
   * </ul>
   *
   * <p><strong>Security Warning:</strong> This method exposes credentials in the URL and should be
   * avoided in production environments. Use {@link #DEFAULT} instead.
   *
   * <h4>Example:</h4>
   *
   * <pre>{@code
   * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI...;secretKey=wJal...;
   * }</pre>
   */
  STATIC,

  /**
   * Profile-based credentials using a named profile from AWS credentials file.
   *
   * <p>Requires the following property in the JDBC URL:
   *
   * <ul>
   *   <li><code>profileName</code> - Name of the AWS profile to use (required)
   * </ul>
   *
   * <p>The profile must be defined in the standard AWS credentials file location ({@code
   * ~/.aws/credentials} on Unix systems, {@code %USERPROFILE%\.aws\credentials} on Windows).
   *
   * <h4>Example:</h4>
   *
   * <pre>{@code
   * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=PROFILE;profileName=dev-account;
   * }</pre>
   */
  PROFILE,

  /**
   * Default credentials using AWS SDK's DefaultCredentialsProvider chain.
   *
   * <p>This is the recommended approach for production environments as it automatically tries
   * multiple secure credential sources in the following order:
   *
   * <ol>
   *   <li>Environment variables: {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}
   *   <li>Java system properties: {@code aws.accessKeyId} and {@code aws.secretAccessKey}
   *   <li>AWS credentials file: {@code ~/.aws/credentials}
   *   <li>AWS IAM roles for Amazon EC2 instances
   *   <li>Container credentials for Amazon ECS and AWS Fargate
   * </ol>
   *
   * <p>No additional properties are required in the JDBC URL when using this type.
   *
   * <h4>Example:</h4>
   *
   * <pre>{@code
   * jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;
   * // or simply (DEFAULT is the default value):
   * jdbc:dynamodb:partiql:region=us-east-1;
   * }</pre>
   */
  DEFAULT
}
