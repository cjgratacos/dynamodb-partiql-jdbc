package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException;

/**
 * Handles retry logic with exponential backoff for DynamoDB operations.
 *
 * <p>This class implements retry mechanisms for DynamoDB operations that may fail due to
 * throttling, capacity limits, or temporary service issues. It uses configurable exponential
 * backoff with jitter to reduce load and avoid thundering herd problems.
 *
 * <h2>Retryable Exceptions:</h2>
 *
 * <ul>
 *   <li><strong>ProvisionedThroughputExceededException</strong>: Table capacity exceeded
 *   <li><strong>RequestLimitExceededException</strong>: Account-level limits exceeded
 * </ul>
 *
 * <h2>Retry Strategy:</h2>
 *
 * <ul>
 *   <li>Exponential backoff with optional jitter
 *   <li>Configurable maximum attempts and delay bounds
 *   <li>Immediate failure for non-retryable exceptions
 *   <li>Detailed logging of retry attempts and outcomes
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class RetryHandler {

  private static final Logger logger = LoggerFactory.getLogger(RetryHandler.class);

  private final RetryPolicy policy;
  private final RetryMetrics metrics;
  private final QueryMetrics queryMetrics;

  /**
   * Creates a retry handler with the specified policy.
   *
   * @param policy retry policy configuration
   */
  public RetryHandler(final RetryPolicy policy) {
    this.policy = policy;
    this.metrics = new RetryMetrics();
    this.queryMetrics = new QueryMetrics();
  }

  /**
   * Creates a retry handler with the specified policy and shared query metrics.
   *
   * @param policy retry policy configuration
   * @param queryMetrics shared query metrics instance
   */
  public RetryHandler(final RetryPolicy policy, final QueryMetrics queryMetrics) {
    this.policy = policy;
    this.metrics = new RetryMetrics();
    this.queryMetrics = queryMetrics;
  }

  /**
   * Executes an operation with retry logic for transient failures.
   *
   * @param <T> the return type of the operation
   * @param operation the operation to execute
   * @param operationName descriptive name for logging
   * @return the result of the successful operation
   * @throws SQLException if the operation fails after all retries
   */
  public <T> T executeWithRetry(final Supplier<T> operation, final String operationName)
      throws SQLException {
    final var startTime = Instant.now();
    var attempt = 0;
    Throwable lastException = null;
    final var isThrottling = false;

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Starting operation: {} with retry policy: max_attempts={}, base_delay={}ms",
          operationName,
          policy.getMaxAttempts(),
          policy.getBaseDelayMs());
    }

    while (attempt <= policy.getMaxAttempts()) {
      try {
        final var result = operation.get();
        final var executionTime = Duration.between(startTime, Instant.now());

        if (attempt > 0) {
          metrics.recordSuccessfulRetry(operationName, attempt);
          queryMetrics.recordRetryAttempt(attempt, isThrottling, true);

          if (logger.isInfoEnabled()) {
            logger.info(
                "Operation succeeded after {} retries: {} (total_time={}ms)",
                attempt,
                operationName,
                executionTime.toMillis());
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Operation succeeded on first attempt: {} (time={}ms)",
                operationName,
                executionTime.toMillis());
          }
        }

        return result;
      } catch (final Exception e) {
        lastException = e;
        final var isThrottlingException = isThrottlingException(e);
        final var isRetryable = isRetryableException(e);

        if (!isRetryable) {
          metrics.recordNonRetryableFailure(operationName, e);
          queryMetrics.recordError("NON_RETRYABLE", e);

          logger.error(
              "Non-retryable error in operation: {} (attempt={})", operationName, attempt + 1, e);
          throw createSQLException("Non-retryable error in " + operationName, e);
        }

        if (attempt >= policy.getMaxAttempts()) {
          metrics.recordMaxRetriesExceeded(operationName, attempt);
          queryMetrics.recordRetryAttempt(attempt, isThrottlingException, false);

          logger.warn(
              "Max retries exceeded for operation: {} (attempts={})",
              operationName,
              attempt + 1,
              e);
          break;
        }

        final var delay = policy.calculateDelay(attempt);
        metrics.recordRetryAttempt(operationName, attempt, delay, e);
        queryMetrics.recordRetryAttempt(attempt + 1, isThrottlingException, false);

        if (isThrottlingException) {
          logger.warn(
              "Throttling detected, retrying operation: {} (attempt={}, delay={}ms)",
              operationName,
              attempt + 1,
              delay,
              e);
        } else {
          logger.warn(
              "Retryable error, retrying operation: {} (attempt={}, delay={}ms)",
              operationName,
              attempt + 1,
              delay,
              e);
        }

        try {
          Thread.sleep(delay);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          logger.error("Operation interrupted during retry: {}", operationName, ie);
          throw createSQLException("Operation interrupted during retry", ie);
        }

        attempt++;
      }
    }

    final var totalTime = Duration.between(startTime, Instant.now());
    logger.error(
        "Operation failed after {} attempts and {}ms: {}",
        attempt,
        totalTime.toMillis(),
        operationName,
        lastException);

    throw createSQLException(
        "Operation failed after " + attempt + " attempts: " + operationName, lastException);
  }

  /**
   * Checks if an exception is retryable based on DynamoDB error types.
   *
   * @param exception the exception to check
   * @return true if the exception indicates a transient error that should be retried
   */
  private boolean isRetryableException(final Throwable exception) {
    if (exception instanceof ProvisionedThroughputExceededException) {
      return true;
    }
    if (exception instanceof RequestLimitExceededException) {
      return true;
    }

    // Check for AWS SDK exceptions that might indicate transient issues
    if (exception instanceof DynamoDbException) {
      final var dynamoException = (DynamoDbException) exception;
      final var statusCode = dynamoException.statusCode();

      // 5xx errors are typically retryable
      if (statusCode >= 500 && statusCode < 600) {
        return true;
      }

      // 429 Too Many Requests
      if (statusCode == 429) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks if an exception is specifically related to throttling.
   *
   * @param exception the exception to check
   * @return true if the exception indicates throttling
   */
  private boolean isThrottlingException(final Throwable exception) {
    if (exception instanceof ProvisionedThroughputExceededException) {
      return true;
    }
    if (exception instanceof RequestLimitExceededException) {
      return true;
    }

    if (exception instanceof DynamoDbException) {
      final var dynamoException = (DynamoDbException) exception;
      return dynamoException.statusCode() == 429;
    }

    return false;
  }

  /**
   * Creates a SQLException with appropriate message and cause chain.
   *
   * @param message descriptive error message
   * @param cause the underlying exception
   * @return SQLException wrapping the original error
   */
  private SQLException createSQLException(final String message, final Throwable cause) {
    return new SQLException(message + ": " + cause.getMessage(), cause);
  }

  /**
   * Gets the retry metrics for monitoring and debugging.
   *
   * @return current retry metrics
   */
  public RetryMetrics getMetrics() {
    return metrics;
  }

  /**
   * Gets the query metrics for comprehensive monitoring.
   *
   * @return current query metrics
   */
  public QueryMetrics getQueryMetrics() {
    return queryMetrics;
  }

  /** Inner class to track retry metrics and statistics. */
  public static final class RetryMetrics {
    private long totalRetries = 0;
    private long successfulRetries = 0;
    private long nonRetryableFailures = 0;
    private long maxRetriesExceeded = 0;

    /** Creates a new RetryMetrics instance. */
    public RetryMetrics() {
      // Default constructor
    }

    /**
     * Records a successful retry operation.
     *
     * @param operationName name of the operation
     * @param attemptCount number of attempts made
     */
    public void recordSuccessfulRetry(final String operationName, final int attemptCount) {
      totalRetries++;
      successfulRetries++;

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Successful retry recorded: operation={}, attempts={}, total_retries={}",
            operationName,
            attemptCount,
            totalRetries);
      }
    }

    /**
     * Records a retry attempt.
     *
     * @param operationName name of the operation
     * @param attemptNumber current attempt number (0-based)
     * @param delayMs delay applied before this attempt
     * @param exception the exception that triggered the retry
     */
    public void recordRetryAttempt(
        final String operationName,
        final int attemptNumber,
        final long delayMs,
        final Exception exception) {

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Retry attempt recorded: operation={}, attempt={}, delay={}ms, error={}",
            operationName,
            attemptNumber + 1,
            delayMs,
            exception.getClass().getSimpleName());
      }
    }

    /**
     * Records a non-retryable failure.
     *
     * @param operationName name of the operation
     * @param exception the non-retryable exception
     */
    public void recordNonRetryableFailure(final String operationName, final Exception exception) {
      nonRetryableFailures++;

      if (logger.isWarnEnabled()) {
        logger.warn(
            "Non-retryable failure recorded: operation={}, total_failures={}, error={}",
            operationName,
            nonRetryableFailures,
            exception.getClass().getSimpleName());
      }
    }

    /**
     * Records when maximum retries are exceeded.
     *
     * @param operationName name of the operation
     * @param finalAttemptCount total attempts made
     */
    public void recordMaxRetriesExceeded(final String operationName, final int finalAttemptCount) {
      maxRetriesExceeded++;

      if (logger.isWarnEnabled()) {
        logger.warn(
            "Max retries exceeded: operation={}, attempts={}, total_max_exceeded={}",
            operationName,
            finalAttemptCount,
            maxRetriesExceeded);
      }
    }

    /**
     * Gets the total number of retry attempts.
     *
     * @return the total retry count
     */
    public long getTotalRetries() {
      return totalRetries;
    }

    /**
     * Gets the number of successful retry operations.
     *
     * @return the successful retry count
     */
    public long getSuccessfulRetries() {
      return successfulRetries;
    }

    /**
     * Gets the number of non-retryable failures.
     *
     * @return the non-retryable failure count
     */
    public long getNonRetryableFailures() {
      return nonRetryableFailures;
    }

    /**
     * Gets the number of operations that exceeded max retries.
     *
     * @return the max retries exceeded count
     */
    public long getMaxRetriesExceeded() {
      return maxRetriesExceeded;
    }
  }
}
