package org.cjgratacos.jdbc;

import java.util.Properties;

/**
 * Configuration class for retry policies in DynamoDB operations.
 *
 * <p>This class encapsulates retry behavior configuration including maximum retry attempts, base
 * delay timing, maximum delay limits, and jitter application for exponential backoff strategies.
 *
 * <h2>Retry Strategy:</h2>
 *
 * <ul>
 *   <li><strong>Exponential Backoff</strong>: Each retry doubles the delay time
 *   <li><strong>Jitter</strong>: Random variance added to prevent thundering herd
 *   <li><strong>Configurable Limits</strong>: Maximum retries and delay bounds
 *   <li><strong>Targeted Exceptions</strong>: Specific handling for throttling errors
 * </ul>
 *
 * <h2>Configuration Properties:</h2>
 *
 * <ul>
 *   <li><code>retryMaxAttempts</code> - Maximum retry attempts (default: 3)
 *   <li><code>retryBaseDelayMs</code> - Base delay in milliseconds (default: 100)
 *   <li><code>retryMaxDelayMs</code> - Maximum delay in milliseconds (default: 20000)
 *   <li><code>retryJitterEnabled</code> - Enable jitter (default: true)
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class RetryPolicy {

  private final int maxAttempts;
  private final long baseDelayMs;
  private final long maxDelayMs;
  private final boolean jitterEnabled;

  /**
   * Creates a retry policy with the specified configuration.
   *
   * @param maxAttempts maximum number of retry attempts (must be >= 0)
   * @param baseDelayMs base delay in milliseconds (must be > 0)
   * @param maxDelayMs maximum delay in milliseconds (must be >= baseDelayMs)
   * @param jitterEnabled whether to apply jitter to delays
   * @throws IllegalArgumentException if parameters are invalid
   */
  public RetryPolicy(
      final int maxAttempts,
      final long baseDelayMs,
      final long maxDelayMs,
      final boolean jitterEnabled) {
    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts must be >= 0");
    }
    if (baseDelayMs <= 0) {
      throw new IllegalArgumentException("baseDelayMs must be > 0");
    }
    if (maxDelayMs < baseDelayMs) {
      throw new IllegalArgumentException("maxDelayMs must be >= baseDelayMs");
    }

    this.maxAttempts = maxAttempts;
    this.baseDelayMs = baseDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.jitterEnabled = jitterEnabled;
  }

  /**
   * Creates a retry policy from connection properties.
   *
   * @param properties connection properties containing retry configuration
   * @return configured retry policy with defaults for missing properties
   */
  public static RetryPolicy fromProperties(final Properties properties) {
    final var maxAttempts = Integer.parseInt(properties.getProperty("retryMaxAttempts", "3"));
    final var baseDelayMs = Long.parseLong(properties.getProperty("retryBaseDelayMs", "100"));
    final var maxDelayMs = Long.parseLong(properties.getProperty("retryMaxDelayMs", "20000"));
    final var jitterEnabled =
        Boolean.parseBoolean(properties.getProperty("retryJitterEnabled", "true"));

    return new RetryPolicy(maxAttempts, baseDelayMs, maxDelayMs, jitterEnabled);
  }

  /**
   * Creates a default retry policy with standard values.
   *
   * @return retry policy with 3 max attempts, 100ms base delay, 20s max delay, and jitter enabled
   */
  public static RetryPolicy defaultPolicy() {
    return new RetryPolicy(3, 100L, 20000L, true);
  }

  /**
   * Gets the maximum number of retry attempts.
   *
   * @return the maximum attempts
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * Gets the base delay in milliseconds for exponential backoff.
   *
   * @return the base delay in milliseconds
   */
  public long getBaseDelayMs() {
    return baseDelayMs;
  }

  /**
   * Gets the maximum delay in milliseconds between retries.
   *
   * @return the maximum delay in milliseconds
   */
  public long getMaxDelayMs() {
    return maxDelayMs;
  }

  /**
   * Checks if jitter is enabled for retry delays.
   *
   * @return true if jitter is enabled, false otherwise
   */
  public boolean isJitterEnabled() {
    return jitterEnabled;
  }

  /**
   * Calculates the delay for a given retry attempt.
   *
   * @param attempt the current attempt number (0-based)
   * @return delay in milliseconds, applying exponential backoff and optional jitter
   */
  public long calculateDelay(final int attempt) {
    if (attempt < 0) {
      return 0L;
    }

    // Exponential backoff: delay = baseDelay * 2^attempt
    final var exponentialDelay = Math.min(baseDelayMs * (1L << attempt), maxDelayMs);

    if (!jitterEnabled) {
      return exponentialDelay;
    }

    // Apply jitter: randomize between 50% and 100% of calculated delay
    final var jitterMin = exponentialDelay / 2;
    final var jitterRange = exponentialDelay - jitterMin;
    return jitterMin + (long) (Math.random() * jitterRange);
  }
}
