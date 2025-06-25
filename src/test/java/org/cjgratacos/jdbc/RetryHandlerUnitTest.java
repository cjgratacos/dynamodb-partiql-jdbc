package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException;

@DisplayName("RetryHandler Unit Tests")
class RetryHandlerUnitTest {

  private RetryHandler retryHandler;
  private RetryPolicy policy;
  private QueryMetrics queryMetrics;

  @BeforeEach
  void setUp() {
    policy = new RetryPolicy(3, 100, 1000, true);
    queryMetrics = new QueryMetrics();
    retryHandler = new RetryHandler(policy, queryMetrics);
  }

  @Nested
  @DisplayName("Constructor Tests")
  class ConstructorTests {

    @Test
    @DisplayName("Constructor with policy only creates internal query metrics")
    void constructorWithPolicyOnlyCreatesInternalQueryMetrics() {
      final var handler = new RetryHandler(policy);

      assertThat(handler.getMetrics()).isNotNull();
      assertThat(handler.getQueryMetrics()).isNotNull();
    }

    @Test
    @DisplayName("Constructor with policy and query metrics uses provided metrics")
    void constructorWithPolicyAndQueryMetricsUsesProvidedMetrics() {
      final var customQueryMetrics = new QueryMetrics();
      final var handler = new RetryHandler(policy, customQueryMetrics);

      assertThat(handler.getMetrics()).isNotNull();
      assertThat(handler.getQueryMetrics()).isSameAs(customQueryMetrics);
    }
  }

  @Nested
  @DisplayName("Successful Operation Tests")
  class SuccessfulOperationTests {

    @Test
    @DisplayName("Operation succeeds on first attempt")
    void operationSucceedsOnFirstAttempt() throws SQLException {
      final var expectedResult = "success";
      final Supplier<String> operation = () -> expectedResult;

      final var result = retryHandler.executeWithRetry(operation, "test-operation");

      assertThat(result).isEqualTo(expectedResult);
      assertThat(retryHandler.getMetrics().getTotalRetries()).isEqualTo(0);
      assertThat(retryHandler.getMetrics().getSuccessfulRetries()).isEqualTo(0);
    }

    @Test
    @DisplayName("Operation succeeds after retries")
    void operationSucceedsAfterRetries() throws SQLException {
      final var expectedResult = "success";
      final var attemptCounter = new AtomicInteger(0);

      final Supplier<String> operation =
          () -> {
            if (attemptCounter.incrementAndGet() < 3) {
              throw ProvisionedThroughputExceededException.builder().message("Throttled").build();
            }
            return expectedResult;
          };

      final var result = retryHandler.executeWithRetry(operation, "test-operation");

      assertThat(result).isEqualTo(expectedResult);
      assertThat(retryHandler.getMetrics().getTotalRetries()).isEqualTo(1);
      assertThat(retryHandler.getMetrics().getSuccessfulRetries()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Retryable Exception Tests")
  class RetryableExceptionTests {

    @Test
    @DisplayName("ProvisionedThroughputExceededException is retryable")
    void provisionedThroughputExceededExceptionIsRetryable() {
      final Supplier<String> operation =
          () -> {
            throw ProvisionedThroughputExceededException.builder()
                .message("Capacity exceeded")
                .build();
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation failed after 3 attempts");

      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(1);
    }

    @Test
    @DisplayName("RequestLimitExceededException is retryable")
    void requestLimitExceededExceptionIsRetryable() {
      final Supplier<String> operation =
          () -> {
            throw RequestLimitExceededException.builder().message("Request limit exceeded").build();
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation failed after 3 attempts");

      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(1);
    }

    @Test
    @DisplayName("5xx DynamoDB exceptions are retryable")
    void fiveHundredDynamoDbExceptionsAreRetryable() {
      final var dynamoException =
          spy(DynamoDbException.builder().message("Internal error").build());
      when(dynamoException.statusCode()).thenReturn(500);

      final Supplier<String> operation =
          () -> {
            throw dynamoException;
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation failed after 3 attempts");

      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(1);
    }

    @Test
    @DisplayName("429 DynamoDB exceptions are retryable")
    void fourTwentyNineDynamoDbExceptionsAreRetryable() {
      final var dynamoException =
          spy(DynamoDbException.builder().message("Too many requests").build());
      when(dynamoException.statusCode()).thenReturn(429);

      final Supplier<String> operation =
          () -> {
            throw dynamoException;
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation failed after 3 attempts");

      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Non-Retryable Exception Tests")
  class NonRetryableExceptionTests {

    @Test
    @DisplayName("4xx DynamoDB exceptions are not retryable")
    void fourHundredDynamoDbExceptionsAreNotRetryable() {
      final var dynamoException = spy(DynamoDbException.builder().message("Bad request").build());
      when(dynamoException.statusCode()).thenReturn(400);

      final Supplier<String> operation =
          () -> {
            throw dynamoException;
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Non-retryable error");

      assertThat(retryHandler.getMetrics().getNonRetryableFailures()).isEqualTo(1);
      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(0);
    }

    @Test
    @DisplayName("Generic exceptions are not retryable")
    void genericExceptionsAreNotRetryable() {
      final Supplier<String> operation =
          () -> {
            throw new RuntimeException("Generic error");
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Non-retryable error");

      assertThat(retryHandler.getMetrics().getNonRetryableFailures()).isEqualTo(1);
      assertThat(retryHandler.getMetrics().getMaxRetriesExceeded()).isEqualTo(0);
    }

    @Test
    @DisplayName("IllegalArgumentException is not retryable")
    void illegalArgumentExceptionIsNotRetryable() {
      final Supplier<String> operation =
          () -> {
            throw new IllegalArgumentException("Invalid argument");
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Non-retryable error");

      assertThat(retryHandler.getMetrics().getNonRetryableFailures()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Throttling Detection Tests")
  class ThrottlingDetectionTests {

    @Test
    @DisplayName("ProvisionedThroughputExceededException is detected as throttling")
    void provisionedThroughputExceededExceptionIsDetectedAsThrottling() throws SQLException {
      final var attemptCounter = new AtomicInteger(0);

      final Supplier<String> operation =
          () -> {
            if (attemptCounter.incrementAndGet() == 1) {
              throw ProvisionedThroughputExceededException.builder().message("Throttled").build();
            }
            return "success";
          };

      retryHandler.executeWithRetry(operation, "test-operation");

      // Verify throttling was recorded in query metrics
      assertThat(queryMetrics.getThrottlingEvents()).isGreaterThan(0);
    }

    @Test
    @DisplayName("RequestLimitExceededException is detected as throttling")
    void requestLimitExceededExceptionIsDetectedAsThrottling() throws SQLException {
      final var attemptCounter = new AtomicInteger(0);

      final Supplier<String> operation =
          () -> {
            if (attemptCounter.incrementAndGet() == 1) {
              throw RequestLimitExceededException.builder()
                  .message("Request limit exceeded")
                  .build();
            }
            return "success";
          };

      retryHandler.executeWithRetry(operation, "test-operation");

      // Verify throttling was recorded in query metrics
      assertThat(queryMetrics.getThrottlingEvents()).isGreaterThan(0);
    }

    @Test
    @DisplayName("429 status code is detected as throttling")
    void fourTwentyNineStatusCodeIsDetectedAsThrottling() throws SQLException {
      final var attemptCounter = new AtomicInteger(0);
      final var dynamoException =
          spy(DynamoDbException.builder().message("Too many requests").build());
      when(dynamoException.statusCode()).thenReturn(429);

      final Supplier<String> operation =
          () -> {
            if (attemptCounter.incrementAndGet() == 1) {
              throw dynamoException;
            }
            return "success";
          };

      retryHandler.executeWithRetry(operation, "test-operation");

      // Verify throttling was recorded in query metrics
      assertThat(queryMetrics.getThrottlingEvents()).isGreaterThan(0);
    }
  }

  @Nested
  @DisplayName("Metrics Tests")
  class MetricsTests {

    @Test
    @DisplayName("Metrics track successful retries correctly")
    void metricsTrackSuccessfulRetriesCorrectly() throws SQLException {
      final var attemptCounter = new AtomicInteger(0);

      final Supplier<String> operation =
          () -> {
            if (attemptCounter.incrementAndGet() < 3) {
              throw ProvisionedThroughputExceededException.builder().message("Throttled").build();
            }
            return "success";
          };

      retryHandler.executeWithRetry(operation, "test-operation");

      final var metrics = retryHandler.getMetrics();
      assertThat(metrics.getTotalRetries()).isEqualTo(1);
      assertThat(metrics.getSuccessfulRetries()).isEqualTo(1);
      assertThat(metrics.getNonRetryableFailures()).isEqualTo(0);
      assertThat(metrics.getMaxRetriesExceeded()).isEqualTo(0);
    }

    @Test
    @DisplayName("Metrics track max retries exceeded correctly")
    void metricsTrackMaxRetriesExceededCorrectly() {
      final Supplier<String> operation =
          () -> {
            throw ProvisionedThroughputExceededException.builder().message("Always fail").build();
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class);

      final var metrics = retryHandler.getMetrics();
      assertThat(metrics.getTotalRetries()).isEqualTo(0);
      assertThat(metrics.getSuccessfulRetries()).isEqualTo(0);
      assertThat(metrics.getNonRetryableFailures()).isEqualTo(0);
      assertThat(metrics.getMaxRetriesExceeded()).isEqualTo(1);
    }

    @Test
    @DisplayName("Metrics track non-retryable failures correctly")
    void metricsTrackNonRetryableFailuresCorrectly() {
      final Supplier<String> operation =
          () -> {
            throw new IllegalArgumentException("Invalid input");
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class);

      final var metrics = retryHandler.getMetrics();
      assertThat(metrics.getTotalRetries()).isEqualTo(0);
      assertThat(metrics.getSuccessfulRetries()).isEqualTo(0);
      assertThat(metrics.getNonRetryableFailures()).isEqualTo(1);
      assertThat(metrics.getMaxRetriesExceeded()).isEqualTo(0);
    }

    @Test
    @DisplayName("Multiple operations accumulate metrics correctly")
    void multipleOperationsAccumulateMetricsCorrectly() throws SQLException {
      // Operation 1: Succeeds with retries
      final var attemptCounter1 = new AtomicInteger(0);
      final Supplier<String> operation1 =
          () -> {
            if (attemptCounter1.incrementAndGet() < 2) {
              throw ProvisionedThroughputExceededException.builder().message("Throttled").build();
            }
            return "success1";
          };

      // Operation 2: Non-retryable failure
      final Supplier<String> operation2 =
          () -> {
            throw new IllegalArgumentException("Invalid");
          };

      // Operation 3: Max retries exceeded
      final Supplier<String> operation3 =
          () -> {
            throw ProvisionedThroughputExceededException.builder().message("Always fail").build();
          };

      // Execute operations
      retryHandler.executeWithRetry(operation1, "operation1");

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation2, "operation2");
              })
          .isInstanceOf(SQLException.class);

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation3, "operation3");
              })
          .isInstanceOf(SQLException.class);

      // Verify accumulated metrics
      final var metrics = retryHandler.getMetrics();
      assertThat(metrics.getTotalRetries()).isEqualTo(1);
      assertThat(metrics.getSuccessfulRetries()).isEqualTo(1);
      assertThat(metrics.getNonRetryableFailures()).isEqualTo(1);
      assertThat(metrics.getMaxRetriesExceeded()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Interrupt Handling Tests")
  class InterruptHandlingTests {

    @Test
    @DisplayName("Interrupted thread throws SQLException")
    void interruptedThreadThrowsSQLException() {
      // Simulate thread interruption during retry delay
      Thread.currentThread().interrupt();

      final Supplier<String> operation =
          () -> {
            throw ProvisionedThroughputExceededException.builder().message("Throttled").build();
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation interrupted during retry");

      // Verify interrupt status is preserved
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      // Clear interrupt status for other tests
      Thread.interrupted();
    }
  }

  @Nested
  @DisplayName("SQL Exception Creation Tests")
  class SQLExceptionCreationTests {

    @Test
    @DisplayName("SQLException includes original exception message and cause")
    void sqlExceptionIncludesOriginalExceptionMessageAndCause() {
      final var originalException = new IllegalArgumentException("Original error message");
      final Supplier<String> operation =
          () -> {
            throw originalException;
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Non-retryable error in test-operation")
          .hasMessageContaining("Original error message")
          .hasCause(originalException);
    }

    @Test
    @DisplayName("SQLException for max retries includes attempt count")
    void sqlExceptionForMaxRetriesIncludesAttemptCount() {
      final Supplier<String> operation =
          () -> {
            throw ProvisionedThroughputExceededException.builder().message("Always fail").build();
          };

      assertThatThrownBy(
              () -> {
                retryHandler.executeWithRetry(operation, "test-operation");
              })
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Operation failed after 3 attempts: test-operation");
    }
  }
}
