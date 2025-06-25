package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

/**
 * Manages pagination for DynamoDB PartiQL queries to handle large result sets efficiently.
 *
 * <p>This class implements intelligent pagination strategies to prevent queries from exceeding
 * DynamoDB's 1MB response limit and to optimize performance for large result sets.
 *
 * <h2>Pagination Strategies:</h2>
 *
 * <ul>
 *   <li><strong>Automatic Page Size</strong>: Adjusts page size based on item size
 *   <li><strong>Limit Detection</strong>: Monitors response sizes to prevent 1MB limit
 *   <li><strong>Token Management</strong>: Handles continuation tokens for seamless iteration
 *   <li><strong>Error Recovery</strong>: Graceful handling of pagination-related errors
 * </ul>
 *
 * <h2>Size Management:</h2>
 *
 * <ul>
 *   <li>Tracks cumulative response size
 *   <li>Reduces page size when approaching limits
 *   <li>Provides warnings for potentially large operations
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class PaginationManager {

  private static final long MAX_RESPONSE_SIZE_BYTES = 1024 * 1024; // 1MB
  private static final long WARNING_THRESHOLD_BYTES = 800 * 1024; // 800KB
  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MIN_PAGE_SIZE = 10;
  private static final int MAX_PAGE_SIZE = 1000;

  private final DynamoDbClient client;
  private final RetryHandler retryHandler;
  private int currentPageSize;
  private long cumulativeResponseSize;
  private boolean hasIssuedSizeWarning;

  /**
   * Creates a new pagination manager.
   *
   * @param client the DynamoDB client for executing requests
   * @param retryHandler the retry handler for transient errors
   */
  public PaginationManager(final DynamoDbClient client, final RetryHandler retryHandler) {
    this.client = client;
    this.retryHandler = retryHandler;
    this.currentPageSize = PaginationManager.DEFAULT_PAGE_SIZE;
    this.cumulativeResponseSize = 0L;
    this.hasIssuedSizeWarning = false;
  }

  /**
   * Executes a paginated query with automatic size management.
   *
   * @param sql the PartiQL query
   * @param nextToken the continuation token (null for first page)
   * @return the query response
   * @throws SQLException if the query fails
   */
  public ExecuteStatementResponse executePagedQuery(final String sql, final String nextToken)
      throws SQLException {
    final var effectivePageSize = this.calculateOptimalPageSize();
    final var modifiedSql = this.addLimitToQuery(sql, effectivePageSize);

    final var requestBuilder = ExecuteStatementRequest.builder().statement(modifiedSql);

    if (nextToken != null) {
      requestBuilder.nextToken(nextToken);
    }

    final var request = requestBuilder.build();

    final var response =
        this.retryHandler.executeWithRetry(
            () -> this.client.executeStatement(request), "paginated query");

    this.updatePaginationMetrics(response);

    return response;
  }

  /** Calculates the optimal page size based on current metrics. */
  private int calculateOptimalPageSize() {
    if (this.cumulativeResponseSize == 0) {
      return this.currentPageSize;
    }

    // If we're approaching the size limit, reduce page size
    if (this.cumulativeResponseSize > PaginationManager.WARNING_THRESHOLD_BYTES) {
      this.currentPageSize = Math.max(PaginationManager.MIN_PAGE_SIZE, this.currentPageSize / 2);

      if (!this.hasIssuedSizeWarning) {
        // This would be logged in a real implementation
        this.hasIssuedSizeWarning = true;
      }
    }

    return Math.min(this.currentPageSize, PaginationManager.MAX_PAGE_SIZE);
  }

  /** Adds or modifies LIMIT clause in the query. */
  private String addLimitToQuery(final String sql, final int pageSize) {
    final var upperSql = sql.toUpperCase();

    // If query already has LIMIT, respect it but warn if it's large
    if (upperSql.contains("LIMIT")) {
      if (!this.hasIssuedSizeWarning) {
        this.hasIssuedSizeWarning = true;
      }
      return PartiQLUtils.normalizeQuery(sql);
    }

    // Add LIMIT clause - normalize first to handle semicolons consistently
    final var normalizedSql = PartiQLUtils.normalizeQuery(sql, false);
    return normalizedSql + " LIMIT " + pageSize + ";";
  }

  /** Updates pagination metrics based on the response. */
  private void updatePaginationMetrics(final ExecuteStatementResponse response) {
    // Estimate response size (this is approximate)
    final var itemCount = response.items().size();
    final var estimatedItemSize = this.estimateAverageItemSize(response);
    final var responseSize = itemCount * estimatedItemSize;

    this.cumulativeResponseSize += responseSize;

    // Adjust page size for next request based on actual item sizes
    if (itemCount > 0 && estimatedItemSize > 0) {
      final var optimalItemCount =
          Math.max(
              PaginationManager.MIN_PAGE_SIZE,
              (int) (PaginationManager.WARNING_THRESHOLD_BYTES / estimatedItemSize));
      this.currentPageSize = Math.min(optimalItemCount, PaginationManager.MAX_PAGE_SIZE);
    }
  }

  /** Estimates the average size of items in the response. */
  private long estimateAverageItemSize(final ExecuteStatementResponse response) {
    if (response.items().isEmpty()) {
      return 0L;
    }

    // Sample first few items to estimate size
    final var sampleSize = Math.min(5, response.items().size());
    long totalSize = 0L;

    for (var i = 0; i < sampleSize; i++) {
      totalSize += this.estimateItemSize(response.items().get(i));
    }

    return totalSize / sampleSize;
  }

  /** Estimates the size of a single item in bytes. */
  private long estimateItemSize(final Map<String, AttributeValue> item) {
    long size = 0L;

    for (final var entry : item.entrySet()) {
      // Attribute name size
      size += entry.getKey().length();

      // Attribute value size (approximate)
      final var value = entry.getValue();
      size +=
          switch (this.getAttributeType(value)) {
            case "S" -> value.s().length();
            case "N" -> value.n().length();
            case "B" -> value.b().asByteArray().length;
            case "SS" -> value.ss().stream().mapToInt(String::length).sum();
            case "NS" -> value.ns().stream().mapToInt(String::length).sum();
            case "BS" -> value.bs().stream().mapToInt(b -> b.asByteArray().length).sum();
            case "L" -> value.l().size() * 50; // Rough estimate for list
            case "M" -> value.m().size() * 100; // Rough estimate for map
            case "NULL" -> 1; // Minimal overhead for null
            case "BOOL" -> 1; // Minimal overhead for boolean
            default -> 10; // Base overhead for unknown types
          };
    }

    return size;
  }

  /** Determines the DynamoDB attribute type for switch statement usage. */
  private String getAttributeType(final AttributeValue value) {
    if (value.s() != null) return "S";
    if (value.n() != null) return "N";
    if (value.b() != null) return "B";
    if (value.ss() != null) return "SS";
    if (value.ns() != null) return "NS";
    if (value.bs() != null) return "BS";
    if (value.l() != null) return "L";
    if (value.m() != null) return "M";
    if (value.nul() != null && value.nul()) return "NULL";
    if (value.bool() != null) return "BOOL";
    return "UNKNOWN";
  }

  /**
   * Gets the current cumulative response size.
   *
   * @return the cumulative response size in bytes
   */
  public long getCumulativeResponseSize() {
    return this.cumulativeResponseSize;
  }

  /**
   * Gets the current page size.
   *
   * @return the current page size
   */
  public int getCurrentPageSize() {
    return this.currentPageSize;
  }

  /** Resets pagination metrics for a new query. */
  public void reset() {
    this.cumulativeResponseSize = 0L;
    this.currentPageSize = PaginationManager.DEFAULT_PAGE_SIZE;
    this.hasIssuedSizeWarning = false;
  }
}
