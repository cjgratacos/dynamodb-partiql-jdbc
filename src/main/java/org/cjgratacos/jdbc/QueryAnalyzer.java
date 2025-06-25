package org.cjgratacos.jdbc;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

/**
 * Analyzes PartiQL queries to detect potentially expensive operations and capacity concerns.
 *
 * <p>This class examines PartiQL statements to identify patterns that may result in high capacity
 * consumption, slow performance, or billing implications. It provides warnings and recommendations
 * for query optimization.
 *
 * <h2>Analysis Categories:</h2>
 *
 * <ul>
 *   <li><strong>Full Table Scans</strong>: SELECT without WHERE clause or inefficient filtering
 *   <li><strong>Large Result Sets</strong>: Queries likely to return many items
 *   <li><strong>Missing Indexes</strong>: Queries that cannot use primary key or GSI efficiently
 *   <li><strong>Complex Expressions</strong>: Operations requiring significant compute
 * </ul>
 *
 * <h2>Warning Types:</h2>
 *
 * <ul>
 *   <li><strong>PERFORMANCE</strong>: Operations likely to be slow
 *   <li><strong>COST</strong>: Operations that may consume significant capacity units
 *   <li><strong>SCALABILITY</strong>: Operations that may not scale well with data growth
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class QueryAnalyzer {

  /** Private constructor to prevent instantiation. */
  private QueryAnalyzer() {
    // Utility class
  }

  /**
   * Analyzes a PartiQL query and returns warnings for potentially expensive operations.
   *
   * @param sql the PartiQL statement to analyze
   * @return list of warnings, empty if no issues detected
   */
  public static List<QueryWarning> analyzeQuery(final String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return List.of();
    }

    final var warnings = new ArrayList<QueryWarning>();

    // First validate syntax using centralized parser
    if (PartiQLUtils.isValidSyntax(sql)) {
      // Since we can parse it successfully, analyze using string patterns
      // but with more confidence that the SQL structure is valid
      QueryAnalyzer.analyzeValidQuery(sql, warnings);
    } else {
      // If parsing fails, add a warning about syntax issues
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.PERFORMANCE,
              "Query syntax validation failed",
              "Fix syntax errors before analyzing performance characteristics"));
    }

    return warnings;
  }

  /** Analyzes a syntactically valid query for performance issues. */
  private static void analyzeValidQuery(final String sql, final List<QueryWarning> warnings) {
    final var upperSql = sql.toUpperCase();
    final var lowerSql = sql.toLowerCase();

    // Only analyze SELECT statements for now
    if (!upperSql.trim().startsWith("SELECT")) {
      return;
    }

    // Check for full table scan (no WHERE clause)
    if (!upperSql.contains("WHERE")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.PERFORMANCE,
              "Full table scan detected - no WHERE clause found",
              "Consider adding WHERE clause with primary key or GSI for better performance"));
    }

    // Check for SELECT *
    if (upperSql.contains("SELECT *") || upperSql.matches(".*SELECT\\s+\\*.*")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.COST,
              "SELECT * detected - retrieving all attributes",
              "Consider selecting only required attributes to reduce read capacity consumption"));
    }

    // Check for missing LIMIT clause
    if (!upperSql.contains("LIMIT")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.SCALABILITY,
              "No LIMIT clause found - query may return large result set",
              "Consider adding LIMIT clause to control result set size and improve performance"));
    }

    // Check for expensive functions
    if (lowerSql.contains("attribute_exists(")
        || lowerSql.contains("size(")
        || lowerSql.contains("contains(")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.COST,
              "Expensive functions detected in query",
              "Functions like attribute_exists(), size(), contains() require additional"
                  + " processing"));
    }

    // Check for range operations
    if (lowerSql.contains("begins_with(") || lowerSql.contains("between ")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.PERFORMANCE,
              "Range or pattern matching operations detected",
              "These operations may require scanning multiple items"));
    }

    // Check for operations that might indicate large scans
    if (lowerSql.contains("attribute_not_exists(") || lowerSql.contains("attribute_type(")) {
      warnings.add(
          new QueryWarning(
              QueryWarning.WarningType.COST,
              "Metadata functions detected that may require full item evaluation",
              "Functions like attribute_not_exists() and attribute_type() can be expensive"));
    }

    // Check for potentially large IN clauses
    if (lowerSql.contains(" in (")) {
      final var inClauseCount = QueryAnalyzer.countOccurrences(lowerSql, " in (");
      if (inClauseCount > 0) {
        warnings.add(
            new QueryWarning(
                QueryWarning.WarningType.PERFORMANCE,
                "IN clause detected - may require multiple lookups",
                "Consider breaking large IN clauses into smaller batches for better performance"));
      }
    }
  }

  /** Counts occurrences of a substring in a string. */
  private static int countOccurrences(final String text, final String substring) {
    var count = 0;
    var index = 0;

    while ((index = text.indexOf(substring, index)) != -1) {
      count++;
      index += substring.length();
    }

    return count;
  }

  /** Represents a warning about potentially expensive query operations. */
  public static final class QueryWarning {

    /** Types of query warnings that can be generated. */
    public enum WarningType {
      /** Performance-related warnings. */
      PERFORMANCE,
      /** Cost-related warnings. */
      COST,
      /** Scalability-related warnings. */
      SCALABILITY
    }

    private final WarningType type;
    private final String message;
    private final String recommendation;

    /**
     * Creates a new query warning.
     *
     * @param type the type of warning
     * @param message description of the issue
     * @param recommendation suggested solution
     */
    public QueryWarning(final WarningType type, final String message, final String recommendation) {
      this.type = type;
      this.message = message;
      this.recommendation = recommendation;
    }

    /**
     * Gets the type of this warning.
     *
     * @return the warning type
     */
    public WarningType getType() {
      return this.type;
    }

    /**
     * Gets the warning message.
     *
     * @return the warning message
     */
    public String getMessage() {
      return this.message;
    }

    /**
     * Gets the recommendation for addressing this warning.
     *
     * @return the recommendation
     */
    public String getRecommendation() {
      return this.recommendation;
    }

    /**
     * Converts this warning to a JDBC SQLWarning.
     *
     * @return SQLWarning instance
     */
    public SQLWarning toSQLWarning() {
      final var fullMessage =
          String.format("[%s] %s - %s", this.type, this.message, this.recommendation);
      return new SQLWarning(fullMessage);
    }

    @Override
    public String toString() {
      return String.format(
          "QueryWarning{type=%s, message='%s', recommendation='%s'}",
          this.type, this.message, this.recommendation);
    }
  }
}
