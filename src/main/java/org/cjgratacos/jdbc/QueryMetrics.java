package org.cjgratacos.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics collection class for tracking DynamoDB query performance and operational statistics.
 *
 * <p>This class provides comprehensive metrics collection for monitoring query execution times,
 * throughput, capacity consumption, and error rates. It supports both real-time monitoring and
 * historical analysis of DynamoDB operations.
 *
 * <h2>Collected Metrics:</h2>
 *
 * <ul>
 *   <li><strong>Execution Times</strong>: Min, max, average query execution times
 *   <li><strong>Throughput</strong>: Query count, queries per second
 *   <li><strong>Capacity</strong>: Read/write capacity unit consumption
 *   <li><strong>Errors</strong>: Error counts by type and severity
 *   <li><strong>Retry Statistics</strong>: Retry attempts and success rates
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 *
 * <p>This class is thread-safe and designed for concurrent access from multiple threads. All atomic
 * operations ensure consistency without requiring external synchronization.
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * QueryMetrics metrics = new QueryMetrics();
 *
 * // Record query execution
 * Instant start = Instant.now();
 * // ... execute query ...
 * metrics.recordQueryExecution(Duration.between(start, Instant.now()), "SELECT");
 *
 * // Record capacity consumption
 * metrics.recordCapacityConsumption(2.5, 0.0);
 *
 * // Get statistics
 * System.out.println("Average execution time: " + metrics.getAverageExecutionTimeMs() + "ms");
 * System.out.println("Total queries: " + metrics.getTotalQueries());
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class QueryMetrics {

  /** Creates a new QueryMetrics instance. */
  public QueryMetrics() {
    // Default constructor
  }

  private static final Logger logger = LoggerFactory.getLogger(QueryMetrics.class);

  // Query execution metrics
  private final AtomicLong totalQueries = new AtomicLong(0);
  private final AtomicLong totalErrors = new AtomicLong(0);
  private final AtomicLong minExecutionTimeNanos = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong maxExecutionTimeNanos = new AtomicLong(0);
  private final DoubleAdder totalExecutionTimeNanos = new DoubleAdder();

  // Capacity metrics
  private final DoubleAdder totalReadCapacityUnits = new DoubleAdder();
  private final DoubleAdder totalWriteCapacityUnits = new DoubleAdder();

  // Query type counters
  private final AtomicLong selectQueries = new AtomicLong(0);
  private final AtomicLong insertQueries = new AtomicLong(0);
  private final AtomicLong updateQueries = new AtomicLong(0);
  private final AtomicLong deleteQueries = new AtomicLong(0);
  private final AtomicLong otherQueries = new AtomicLong(0);

  // Retry metrics
  private final AtomicLong totalRetryAttempts = new AtomicLong(0);
  private final AtomicLong successfulRetries = new AtomicLong(0);
  private final AtomicLong throttlingEvents = new AtomicLong(0);

  // Timing
  private final Instant startTime = Instant.now();

  /**
   * Records the execution of a query with timing and type information.
   *
   * @param executionTime the time taken to execute the query
   * @param queryType the type of query (SELECT, INSERT, UPDATE, DELETE, etc.)
   */
  public void recordQueryExecution(final Duration executionTime, final String queryType) {
    final var nanos = executionTime.toNanos();

    totalQueries.incrementAndGet();
    totalExecutionTimeNanos.add(nanos);

    // Update min/max execution times
    minExecutionTimeNanos.updateAndGet(current -> Math.min(current, nanos));
    maxExecutionTimeNanos.updateAndGet(current -> Math.max(current, nanos));

    // Update query type counters
    incrementQueryTypeCounter(queryType);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Query executed: type={}, duration={}ms, total_queries={}",
          queryType,
          executionTime.toMillis(),
          totalQueries.get());
    }
  }

  /**
   * Records capacity unit consumption for a query.
   *
   * @param readCapacityUnits the read capacity units consumed
   * @param writeCapacityUnits the write capacity units consumed
   */
  public void recordCapacityConsumption(
      final double readCapacityUnits, final double writeCapacityUnits) {
    totalReadCapacityUnits.add(readCapacityUnits);
    totalWriteCapacityUnits.add(writeCapacityUnits);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Capacity consumed: read_cu={}, write_cu={}, total_read_cu={}, total_write_cu={}",
          readCapacityUnits,
          writeCapacityUnits,
          totalReadCapacityUnits.sum(),
          totalWriteCapacityUnits.sum());
    }
  }

  /**
   * Records a query execution error.
   *
   * @param errorType the type of error that occurred
   * @param exception the exception that was thrown
   */
  public void recordError(final String errorType, final Throwable exception) {
    totalErrors.incrementAndGet();

    if (logger.isWarnEnabled()) {
      logger.warn(
          "Query error: type={}, total_errors={}, message={}",
          errorType,
          totalErrors.get(),
          exception.getMessage(),
          exception);
    }
  }

  /**
   * Records a retry attempt.
   *
   * @param attemptNumber the attempt number (1-based)
   * @param isThrottling whether this retry was due to throttling
   * @param successful whether the retry was successful
   */
  public void recordRetryAttempt(
      final int attemptNumber, final boolean isThrottling, final boolean successful) {
    totalRetryAttempts.incrementAndGet();

    if (isThrottling) {
      throttlingEvents.incrementAndGet();
    }

    if (successful) {
      successfulRetries.incrementAndGet();
    }

    if (logger.isInfoEnabled()) {
      logger.info(
          "Retry attempt: attempt={}, throttling={}, successful={}, total_retries={}",
          attemptNumber,
          isThrottling,
          successful,
          totalRetryAttempts.get());
    }
  }

  private void incrementQueryTypeCounter(final String queryType) {
    if (queryType == null) {
      otherQueries.incrementAndGet();
      return;
    }

    switch (queryType.toUpperCase()) {
      case "SELECT":
        selectQueries.incrementAndGet();
        break;
      case "INSERT":
        insertQueries.incrementAndGet();
        break;
      case "UPDATE":
        updateQueries.incrementAndGet();
        break;
      case "DELETE":
        deleteQueries.incrementAndGet();
        break;
      default:
        otherQueries.incrementAndGet();
        break;
    }
  }

  // Getter methods for metrics

  /**
   * Gets the total number of queries executed.
   *
   * @return the total query count
   */
  public long getTotalQueries() {
    return totalQueries.get();
  }

  /**
   * Gets the total number of query execution errors.
   *
   * @return the total error count
   */
  public long getTotalErrors() {
    return totalErrors.get();
  }

  /**
   * Gets the error rate as a percentage of total queries.
   *
   * @return the error rate as a decimal between 0.0 and 1.0
   */
  public double getErrorRate() {
    final var total = totalQueries.get();
    return total > 0 ? (double) totalErrors.get() / total : 0.0;
  }

  /**
   * Gets the average query execution time in milliseconds.
   *
   * @return the average execution time in milliseconds, or 0.0 if no queries executed
   */
  public double getAverageExecutionTimeMs() {
    final var total = totalQueries.get();
    return total > 0 ? totalExecutionTimeNanos.sum() / (total * 1_000_000.0) : 0.0;
  }

  /**
   * Gets the minimum query execution time in milliseconds.
   *
   * @return the minimum execution time in milliseconds, or 0.0 if no queries executed
   */
  public double getMinExecutionTimeMs() {
    final var min = minExecutionTimeNanos.get();
    return min == Long.MAX_VALUE ? 0.0 : min / 1_000_000.0;
  }

  /**
   * Gets the maximum query execution time in milliseconds.
   *
   * @return the maximum execution time in milliseconds
   */
  public double getMaxExecutionTimeMs() {
    return maxExecutionTimeNanos.get() / 1_000_000.0;
  }

  /**
   * Gets the query throughput in queries per second.
   *
   * @return the average queries per second since metrics collection started
   */
  public double getQueriesPerSecond() {
    final var uptimeSeconds = Duration.between(startTime, Instant.now()).getSeconds();
    return uptimeSeconds > 0 ? (double) totalQueries.get() / uptimeSeconds : 0.0;
  }

  /**
   * Gets the total read capacity units consumed across all queries.
   *
   * @return the total read capacity units consumed
   */
  public double getTotalReadCapacityUnits() {
    return totalReadCapacityUnits.sum();
  }

  /**
   * Gets the total write capacity units consumed across all queries.
   *
   * @return the total write capacity units consumed
   */
  public double getTotalWriteCapacityUnits() {
    return totalWriteCapacityUnits.sum();
  }

  /**
   * Gets the number of SELECT queries executed.
   *
   * @return the count of SELECT queries
   */
  public long getSelectQueries() {
    return selectQueries.get();
  }

  /**
   * Gets the number of INSERT queries executed.
   *
   * @return the count of INSERT queries
   */
  public long getInsertQueries() {
    return insertQueries.get();
  }

  /**
   * Gets the number of UPDATE queries executed.
   *
   * @return the count of UPDATE queries
   */
  public long getUpdateQueries() {
    return updateQueries.get();
  }

  /**
   * Gets the number of DELETE queries executed.
   *
   * @return the count of DELETE queries
   */
  public long getDeleteQueries() {
    return deleteQueries.get();
  }

  /**
   * Gets the number of queries of types other than SELECT, INSERT, UPDATE, or DELETE.
   *
   * @return the count of other query types
   */
  public long getOtherQueries() {
    return otherQueries.get();
  }

  /**
   * Gets the total number of retry attempts across all queries.
   *
   * @return the total retry attempt count
   */
  public long getTotalRetryAttempts() {
    return totalRetryAttempts.get();
  }

  /**
   * Gets the number of successful retry attempts.
   *
   * @return the count of successful retries
   */
  public long getSuccessfulRetries() {
    return successfulRetries.get();
  }

  /**
   * Gets the number of throttling events encountered.
   *
   * @return the count of throttling events from DynamoDB
   */
  public long getThrottlingEvents() {
    return throttlingEvents.get();
  }

  /**
   * Gets the retry success rate as a percentage of total retry attempts.
   *
   * @return the retry success rate as a decimal between 0.0 and 1.0
   */
  public double getRetrySuccessRate() {
    final var attempts = totalRetryAttempts.get();
    return attempts > 0 ? (double) successfulRetries.get() / attempts : 0.0;
  }

  /**
   * Gets a summary of all collected metrics.
   *
   * @return formatted string containing key metrics
   */
  public String getSummary() {
    return String.format(
        "QueryMetrics{queries=%d, errors=%d, errorRate=%.2f%%, "
            + "avgTime=%.2fms, minTime=%.2fms, maxTime=%.2fms, "
            + "qps=%.2f, retries=%d, throttling=%d, "
            + "readCU=%.2f, writeCU=%.2f}",
        getTotalQueries(),
        getTotalErrors(),
        getErrorRate() * 100,
        getAverageExecutionTimeMs(),
        getMinExecutionTimeMs(),
        getMaxExecutionTimeMs(),
        getQueriesPerSecond(),
        getTotalRetryAttempts(),
        getThrottlingEvents(),
        getTotalReadCapacityUnits(),
        getTotalWriteCapacityUnits());
  }

  /**
   * Resets all metrics to zero.
   *
   * <p>This method is useful for periodic metrics reporting where you want to reset counters after
   * collecting data.
   */
  public void reset() {
    totalQueries.set(0);
    totalErrors.set(0);
    minExecutionTimeNanos.set(Long.MAX_VALUE);
    maxExecutionTimeNanos.set(0);
    totalExecutionTimeNanos.reset();
    totalReadCapacityUnits.reset();
    totalWriteCapacityUnits.reset();
    selectQueries.set(0);
    insertQueries.set(0);
    updateQueries.set(0);
    deleteQueries.set(0);
    otherQueries.set(0);
    totalRetryAttempts.set(0);
    successfulRetries.set(0);
    throttlingEvents.set(0);

    logger.info("Query metrics reset");
  }
}
