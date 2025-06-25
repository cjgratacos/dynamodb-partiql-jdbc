package org.cjgratacos.jdbc;

import java.util.UUID;
import org.slf4j.MDC;

/**
 * Correlation context management for tracking operations across multiple components and pages.
 *
 * <p>This class provides thread-local correlation ID management to enable distributed tracing and
 * log correlation across query execution, pagination, retries, and other operations. It integrates
 * with SLF4J's Mapped Diagnostic Context (MDC) for automatic inclusion in log messages.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li><strong>Thread-local context</strong>: Each thread maintains its own correlation context
 *   <li><strong>Automatic UUID generation</strong>: Creates unique identifiers for operations
 *   <li><strong>SLF4J MDC integration</strong>: Correlation IDs appear in structured logs
 *   <li><strong>Nested operations</strong>: Support for operation hierarchies with parent/child
 *       relationships
 *   <li><strong>Resource cleanup</strong>: Automatic cleanup to prevent memory leaks
 * </ul>
 *
 * <h2>Usage Patterns:</h2>
 *
 * <pre>{@code
 * // Start a new operation
 * String correlationId = CorrelationContext.newOperation("query-execution");
 * try {
 *     // All log messages in this thread will include the correlation ID
 *     logger.info("Starting query execution"); // Will include correlationId in logs
 *
 *     // Nested operation for pagination
 *     CorrelationContext.withSubOperation("pagination", () -> {
 *         logger.info("Fetching next page"); // Will include parent and sub-operation IDs
 *         // ... pagination logic ...
 *     });
 * } finally {
 *     CorrelationContext.clear(); // Always clean up
 * }
 * }</pre>
 *
 * <h2>Log Integration:</h2>
 *
 * <p>When using logback or similar SLF4J implementations, you can include correlation IDs in your
 * log pattern:
 *
 * <pre>{@code
 * <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level [%X{correlationId}] %logger{36} - %msg%n</pattern>
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see org.slf4j.MDC
 */
public class CorrelationContext {

  private static final String CORRELATION_ID_KEY = "correlationId";
  private static final String OPERATION_KEY = "operation";
  private static final String SUB_OPERATION_KEY = "subOperation";

  private static final ThreadLocal<String> CORRELATION_ID = new ThreadLocal<>();
  private static final ThreadLocal<String> OPERATION_NAME = new ThreadLocal<>();

  /** Private constructor to prevent instantiation. */
  private CorrelationContext() {
    // Utility class
  }

  /**
   * Starts a new operation with a generated correlation ID.
   *
   * @param operationName descriptive name for the operation
   * @return the generated correlation ID
   */
  public static String newOperation(final String operationName) {
    final var correlationId = generateCorrelationId();
    setContext(correlationId, operationName);
    return correlationId;
  }

  /**
   * Starts a new operation with a specific correlation ID.
   *
   * @param correlationId the correlation ID to use
   * @param operationName descriptive name for the operation
   */
  public static void setOperation(final String correlationId, final String operationName) {
    setContext(correlationId, operationName);
  }

  /**
   * Executes a sub-operation within the current correlation context.
   *
   * @param subOperationName name of the sub-operation
   * @param runnable the code to execute
   */
  public static void withSubOperation(final String subOperationName, final Runnable runnable) {
    final var previousSubOperation = MDC.get(SUB_OPERATION_KEY);
    try {
      MDC.put(SUB_OPERATION_KEY, subOperationName);
      runnable.run();
    } finally {
      if (previousSubOperation != null) {
        MDC.put(SUB_OPERATION_KEY, previousSubOperation);
      } else {
        MDC.remove(SUB_OPERATION_KEY);
      }
    }
  }

  /**
   * Gets the current correlation ID for this thread.
   *
   * @return the correlation ID, or null if none is set
   */
  public static String getCurrentCorrelationId() {
    return CORRELATION_ID.get();
  }

  /**
   * Gets the current operation name for this thread.
   *
   * @return the operation name, or null if none is set
   */
  public static String getCurrentOperation() {
    return OPERATION_NAME.get();
  }

  /**
   * Checks if a correlation context is currently active.
   *
   * @return true if a correlation ID is set for this thread
   */
  public static boolean hasContext() {
    return CORRELATION_ID.get() != null;
  }

  /**
   * Clears the correlation context for the current thread.
   *
   * <p>This method should be called in a finally block to ensure proper cleanup and prevent memory
   * leaks in thread pools.
   */
  public static void clear() {
    CORRELATION_ID.remove();
    OPERATION_NAME.remove();
    MDC.remove(CORRELATION_ID_KEY);
    MDC.remove(OPERATION_KEY);
    MDC.remove(SUB_OPERATION_KEY);
  }

  /**
   * Executes code within a new correlation context.
   *
   * <p>This is a convenience method that automatically manages context lifecycle:
   *
   * <pre>{@code
   * CorrelationContext.withNewContext("query-execution", () -> {
   *     // All operations here will have the same correlation ID
   *     executeQuery();
   *     processPagination();
   * });
   * }</pre>
   *
   * @param operationName descriptive name for the operation
   * @param runnable the code to execute
   * @return the correlation ID that was used
   */
  public static String withNewContext(final String operationName, final Runnable runnable) {
    final var correlationId = newOperation(operationName);
    try {
      runnable.run();
      return correlationId;
    } finally {
      clear();
    }
  }

  /**
   * Executes code within an existing correlation context.
   *
   * @param correlationId the correlation ID to use
   * @param operationName descriptive name for the operation
   * @param runnable the code to execute
   */
  public static void withContext(
      final String correlationId, final String operationName, final Runnable runnable) {
    setOperation(correlationId, operationName);
    try {
      runnable.run();
    } finally {
      clear();
    }
  }

  private static void setContext(final String correlationId, final String operationName) {
    CORRELATION_ID.set(correlationId);
    OPERATION_NAME.set(operationName);
    MDC.put(CORRELATION_ID_KEY, correlationId);
    MDC.put(OPERATION_KEY, operationName);
  }

  private static String generateCorrelationId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
  }

  /**
   * Creates a child correlation ID for nested operations.
   *
   * @param parentCorrelationId the parent correlation ID
   * @param suffix a suffix to identify the child operation
   * @return a new correlation ID based on the parent
   */
  public static String createChildCorrelationId(
      final String parentCorrelationId, final String suffix) {
    return parentCorrelationId + "-" + suffix;
  }

  /**
   * Gets a formatted string representation of the current context.
   *
   * @return formatted context string for logging
   */
  public static String getContextString() {
    final var correlationId = getCurrentCorrelationId();
    final var operation = getCurrentOperation();
    final var subOperation = MDC.get(SUB_OPERATION_KEY);

    if (correlationId == null) {
      return "no-context";
    }

    final var sb = new StringBuilder();
    sb.append("cid=").append(correlationId);

    if (operation != null) {
      sb.append(",op=").append(operation);
    }

    if (subOperation != null) {
      sb.append(",sub=").append(subOperation);
    }

    return sb.toString();
  }
}
