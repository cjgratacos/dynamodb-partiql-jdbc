package org.cjgratacos.jdbc;

import java.sql.SQLException;
import org.partiql.parser.PartiQLParser;

/**
 * Utility class for PartiQL parsing operations with cached parser instance.
 *
 * <p>This class provides centralized PartiQL parsing functionality with a static parser instance
 * for optimal performance. It includes validation methods and error handling for PartiQL queries.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li><strong>Cached Parser</strong>: Single static parser instance for performance
 *   <li><strong>Validation</strong>: Syntax validation with detailed error reporting
 *   <li><strong>Error Handling</strong>: Consistent error conversion to SQLException
 *   <li><strong>Thread Safety</strong>: PartiQL parser is thread-safe for concurrent use
 * </ul>
 *
 * <h2>Usage:</h2>
 *
 * <pre>{@code
 * // Validate query syntax
 * try {
 *     PartiQLUtils.validateSyntax("SELECT * FROM MyTable WHERE id = ?");
 *     System.out.println("Query is valid");
 * } catch (SQLException e) {
 *     System.err.println("Invalid query: " + e.getMessage());
 * }
 *
 * // Check if query is valid without throwing exception
 * if (PartiQLUtils.isValidSyntax("SELECT name FROM Users")) {
 *     // Process valid query
 * }
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class PartiQLUtils {

  // Static parser instance - PartiQLParser is thread-safe
  private static final PartiQLParser PARSER = PartiQLParser.standard();

  /** Private constructor to prevent instantiation. */
  private PartiQLUtils() {
    // Utility class
  }

  /**
   * Gets the shared PartiQL parser instance.
   *
   * <p>This method provides access to a thread-safe, cached parser instance that can be reused
   * across multiple parsing operations for optimal performance.
   *
   * @return the shared PartiQL parser instance
   */
  public static PartiQLParser getParser() {
    return PARSER;
  }

  /**
   * Validates PartiQL query syntax and throws SQLException if invalid.
   *
   * <p>This method attempts to parse the provided SQL string and throws a detailed SQLException if
   * the syntax is invalid. It should be used when syntax validation is required and exceptions are
   * the preferred error handling mechanism.
   *
   * @param sql the PartiQL query to validate
   * @throws SQLException if the query has invalid syntax
   * @throws IllegalArgumentException if sql is null or empty
   */
  public static void validateSyntax(final String sql) throws SQLException {
    if (sql == null || sql.trim().isEmpty()) {
      throw new IllegalArgumentException("SQL statement cannot be null or empty");
    }

    try {
      PARSER.parse(sql);
    } catch (final Exception e) {
      throw new SQLException("Invalid PartiQL syntax: " + e.getMessage(), e);
    }
  }

  /**
   * Checks if a PartiQL query has valid syntax without throwing exceptions.
   *
   * <p>This method provides a non-throwing way to check query syntax validity. It's useful when you
   * need to conditionally process queries based on their syntactic correctness without handling
   * exceptions.
   *
   * @param sql the PartiQL query to check
   * @return true if the query has valid syntax, false otherwise
   */
  public static boolean isValidSyntax(final String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    try {
      PARSER.parse(sql);
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Validates PartiQL syntax with custom error message prefix.
   *
   * <p>This method is similar to {@link #validateSyntax(String)} but allows customization of the
   * error message for better context in different use cases.
   *
   * @param sql the PartiQL query to validate
   * @param errorPrefix custom prefix for error messages
   * @throws SQLException if the query has invalid syntax
   * @throws IllegalArgumentException if sql is null or empty
   */
  public static void validateSyntax(final String sql, final String errorPrefix)
      throws SQLException {
    if (sql == null || sql.trim().isEmpty()) {
      throw new IllegalArgumentException("SQL statement cannot be null or empty");
    }

    try {
      PARSER.parse(sql);
    } catch (final Exception e) {
      final var message =
          errorPrefix != null && !errorPrefix.trim().isEmpty()
              ? errorPrefix + ": " + e.getMessage()
              : "Invalid PartiQL syntax: " + e.getMessage();
      throw new SQLException(message, e);
    }
  }

  /**
   * Determines if a query is a data modification statement (DML).
   *
   * <p>This method checks if the provided query is a DML statement that modifies data (INSERT,
   * UPDATE, DELETE, UPSERT, REPLACE) as opposed to a query statement (SELECT).
   *
   * @param sql the PartiQL statement to check
   * @return true if the statement is DML, false if it's a query or invalid
   */
  public static boolean isDMLStatement(final String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    final var trimmedSql = sql.trim().toUpperCase();
    return trimmedSql.startsWith("INSERT")
        || trimmedSql.startsWith("UPDATE")
        || trimmedSql.startsWith("DELETE")
        || trimmedSql.startsWith("UPSERT")
        || trimmedSql.startsWith("REPLACE");
  }

  /**
   * Determines if a query is a SELECT statement.
   *
   * <p>This method checks if the provided query is a SELECT statement that retrieves data as
   * opposed to a DML statement that modifies data.
   *
   * @param sql the PartiQL statement to check
   * @return true if the statement is a SELECT query, false otherwise
   */
  public static boolean isSelectStatement(final String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    return sql.trim().toUpperCase().startsWith("SELECT");
  }

  /**
   * Normalizes a PartiQL query by trimming whitespace and ensuring consistent formatting.
   *
   * <p>This method performs basic query normalization including:
   *
   * <ul>
   *   <li>Trimming leading and trailing whitespace
   *   <li>Ensuring the query ends with a semicolon if it doesn't already
   *   <li>Removing extra internal whitespace (optional)
   * </ul>
   *
   * @param sql the PartiQL query to normalize
   * @param ensureSemicolon whether to ensure the query ends with a semicolon
   * @return the normalized query string
   */
  public static String normalizeQuery(final String sql, final boolean ensureSemicolon) {
    if (sql == null || sql.trim().isEmpty()) {
      return sql;
    }

    var normalized = sql.trim();

    if (ensureSemicolon && !normalized.endsWith(";")) {
      normalized += ";";
    }

    return normalized;
  }

  /**
   * Normalizes a PartiQL query with default settings (adds semicolon if missing).
   *
   * @param sql the PartiQL query to normalize
   * @return the normalized query string
   * @see #normalizeQuery(String, boolean)
   */
  public static String normalizeQuery(final String sql) {
    return normalizeQuery(sql, true);
  }
}
