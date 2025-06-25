package org.cjgratacos.jdbc;

import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Test container for mocking AWS STS (Security Token Service) for assume role testing.
 *
 * <p>This container provides a mock STS endpoint that can simulate role assumption scenarios for
 * testing purposes. It's particularly useful for testing profiles that use assume role
 * functionality without requiring actual AWS credentials or roles.
 *
 * <p><strong>Note:</strong> This is a simplified mock for testing purposes. In a real-world
 * scenario, you would use actual AWS STS with proper IAM roles and policies.
 */
public class MockStsContainer {

  private static final String MOCK_STS_IMAGE = "localstack/localstack:3.0";
  private static final int STS_PORT = 4566;

  private final GenericContainer<?> container;

  /** Creates a new mock STS container. */
  public MockStsContainer() {
    this.container =
        new GenericContainer<>(DockerImageName.parse(MOCK_STS_IMAGE))
            .withExposedPorts(STS_PORT)
            .withEnv("SERVICES", "sts")
            .withEnv("DEBUG", "1")
            .withEnv("LAMBDA_EXECUTOR", "local")
            .withEnv("DOCKER_HOST", "unix:///var/run/docker.sock")
            .waitingFor(
                Wait.forHttp("/_localstack/health").withStartupTimeout(Duration.ofMinutes(2)))
            .withReuse(false);
  }

  /**
   * Starts the mock STS container.
   *
   * @return this container instance for method chaining
   */
  public MockStsContainer start() {
    container.start();
    return this;
  }

  /** Stops the mock STS container. */
  public void stop() {
    container.stop();
  }

  /**
   * Gets the STS endpoint URL.
   *
   * @return STS endpoint URL
   */
  public String getStsEndpoint() {
    return String.format("http://localhost:%d", container.getMappedPort(STS_PORT));
  }

  /**
   * Gets environment variables needed to configure AWS SDK to use this mock STS.
   *
   * @return Map of environment variables
   */
  public Map<String, String> getStsEnvironmentVariables() {
    return Map.of(
        "AWS_ENDPOINT_URL_STS", getStsEndpoint(),
        "AWS_ACCESS_KEY_ID", CredentialTestFixtures.TEST_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY", CredentialTestFixtures.TEST_SECRET_KEY,
        "AWS_DEFAULT_REGION", CredentialTestFixtures.TEST_REGION);
  }

  /**
   * Creates a JDBC URL configured to use this mock STS endpoint.
   *
   * @param dynamoEndpoint DynamoDB endpoint URL
   * @param profileName AWS profile name that uses assume role
   * @return JDBC URL configured for mock STS
   */
  public String createAssumeRoleJdbcUrl(String dynamoEndpoint, String profileName) {
    return String.format(
        "jdbc:dynamodb:partiql:region=%s;credentialsType=PROFILE;profileName=%s;endpoint=%s;stsEndpoint=%s",
        CredentialTestFixtures.TEST_REGION, profileName, dynamoEndpoint, getStsEndpoint());
  }

  /**
   * Checks if the mock STS container is running.
   *
   * @return true if container is running
   */
  public boolean isRunning() {
    return container.isRunning();
  }

  /**
   * Gets the container logs for debugging.
   *
   * @return Container logs
   */
  public String getLogs() {
    return container.getLogs();
  }

  /**
   * Creates a pre-configured mock STS container with standard test setup.
   *
   * @return Started mock STS container
   */
  public static MockStsContainer createStandardTestContainer() {
    MockStsContainer container = new MockStsContainer();
    container.start();
    return container;
  }
}
