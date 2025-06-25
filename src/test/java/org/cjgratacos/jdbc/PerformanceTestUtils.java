package org.cjgratacos.jdbc;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Callable;

/**
 * Utility class for performance testing and measurement.
 *
 * <p>This class provides methods to measure execution time, memory usage, and perform warmup
 * operations for consistent performance testing.
 */
public class PerformanceTestUtils {

  /**
   * Measures the execution time of a runnable operation.
   *
   * @param operation the operation to measure
   * @return the execution time in milliseconds
   */
  public static long measureExecutionTime(Runnable operation) {
    final var startTime = System.currentTimeMillis();
    operation.run();
    return System.currentTimeMillis() - startTime;
  }

  /**
   * Measures the execution time of a callable operation.
   *
   * @param operation the operation to measure
   * @param <T> the return type of the operation
   * @return a result containing the execution time and operation result
   * @throws Exception if the operation throws an exception
   */
  public static <T> TimedResult<T> measureExecutionTime(Callable<T> operation) throws Exception {
    final var startTime = System.currentTimeMillis();
    final var result = operation.call();
    final var duration = System.currentTimeMillis() - startTime;
    return new TimedResult<>(result, duration);
  }

  /**
   * Measures memory usage before and after an operation.
   *
   * @param operation the operation to measure
   * @return memory usage information
   */
  public static MemoryUsageResult measureMemoryUsage(Runnable operation) {
    final var memoryBean = ManagementFactory.getMemoryMXBean();

    // Force garbage collection for more accurate measurement
    System.gc();

    final var beforeUsage = memoryBean.getHeapMemoryUsage();

    operation.run();

    final var afterUsage = memoryBean.getHeapMemoryUsage();

    return new MemoryUsageResult(beforeUsage, afterUsage);
  }

  /**
   * Performs warmup iterations to stabilize JVM performance.
   *
   * @param operation the operation to warm up
   * @param iterations the number of warmup iterations
   */
  public static void warmup(Runnable operation, int iterations) {
    for (int i = 0; i < iterations; i++) {
      operation.run();
    }

    // Force garbage collection after warmup
    System.gc();
  }

  /**
   * Performs warmup iterations for a callable operation.
   *
   * @param operation the operation to warm up
   * @param iterations the number of warmup iterations
   * @param <T> the return type of the operation
   */
  public static <T> void warmup(Callable<T> operation, int iterations) {
    for (int i = 0; i < iterations; i++) {
      try {
        operation.call();
      } catch (Exception e) {
        // Ignore exceptions during warmup
      }
    }

    // Force garbage collection after warmup
    System.gc();
  }

  /**
   * Runs a performance benchmark with multiple iterations.
   *
   * @param operation the operation to benchmark
   * @param iterations the number of iterations
   * @param warmupIterations the number of warmup iterations
   * @return benchmark results
   */
  public static BenchmarkResult benchmark(
      Runnable operation, int iterations, int warmupIterations) {
    // Warmup
    warmup(operation, warmupIterations);

    final var times = new long[iterations];
    long totalTime = 0;

    for (int i = 0; i < iterations; i++) {
      final var time = measureExecutionTime(operation);
      times[i] = time;
      totalTime += time;
    }

    return new BenchmarkResult(times, totalTime);
  }

  /**
   * Result of a timed operation.
   *
   * @param <T> the type of the result
   */
  public static class TimedResult<T> {
    private final T result;
    private final long durationMs;

    public TimedResult(T result, long durationMs) {
      this.result = result;
      this.durationMs = durationMs;
    }

    public T getResult() {
      return result;
    }

    public long getDurationMs() {
      return durationMs;
    }
  }

  /** Memory usage measurement result. */
  public static class MemoryUsageResult {
    private final MemoryUsage beforeUsage;
    private final MemoryUsage afterUsage;

    public MemoryUsageResult(MemoryUsage beforeUsage, MemoryUsage afterUsage) {
      this.beforeUsage = beforeUsage;
      this.afterUsage = afterUsage;
    }

    public MemoryUsage getBeforeUsage() {
      return beforeUsage;
    }

    public MemoryUsage getAfterUsage() {
      return afterUsage;
    }

    public long getUsedMemoryIncrease() {
      return afterUsage.getUsed() - beforeUsage.getUsed();
    }

    public double getUsedMemoryIncreasePercentage() {
      if (beforeUsage.getUsed() == 0) {
        return 0.0;
      }
      return (double) getUsedMemoryIncrease() / beforeUsage.getUsed() * 100.0;
    }
  }

  /** Benchmark result containing timing statistics. */
  public static class BenchmarkResult {
    private final long[] times;
    private final long totalTime;

    public BenchmarkResult(long[] times, long totalTime) {
      this.times = times.clone();
      this.totalTime = totalTime;
    }

    public long[] getTimes() {
      return times.clone();
    }

    public long getTotalTime() {
      return totalTime;
    }

    public double getAverageTime() {
      return (double) totalTime / times.length;
    }

    public long getMinTime() {
      long min = Long.MAX_VALUE;
      for (long time : times) {
        if (time < min) {
          min = time;
        }
      }
      return min;
    }

    public long getMaxTime() {
      long max = Long.MIN_VALUE;
      for (long time : times) {
        if (time > max) {
          max = time;
        }
      }
      return max;
    }

    public double getStandardDeviation() {
      final var average = getAverageTime();
      double sumSquaredDiffs = 0.0;

      for (long time : times) {
        final var diff = time - average;
        sumSquaredDiffs += diff * diff;
      }

      return Math.sqrt(sumSquaredDiffs / times.length);
    }

    public long getMedianTime() {
      final var sortedTimes = times.clone();
      java.util.Arrays.sort(sortedTimes);

      if (sortedTimes.length % 2 == 0) {
        return (sortedTimes[sortedTimes.length / 2 - 1] + sortedTimes[sortedTimes.length / 2]) / 2;
      } else {
        return sortedTimes[sortedTimes.length / 2];
      }
    }
  }
}
