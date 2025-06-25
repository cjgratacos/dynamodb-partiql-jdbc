package org.cjgratacos.jdbc;

import java.sql.SQLWarning;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing SQL queries to extract and manipulate LIMIT and OFFSET clauses. Also
 * provides functionality for handling DynamoDB PartiQL index syntax normalization.
 */
public class SqlQueryParser {

  /** Private constructor to prevent instantiation of utility class. */
  private SqlQueryParser() {
    throw new UnsupportedOperationException("Utility class should not be instantiated");
  }

  // Pattern to match LIMIT and OFFSET in various formats
  // Supports: LIMIT n, LIMIT n OFFSET m, OFFSET m LIMIT n
  // Now also captures negative numbers
  private static final Pattern LIMIT_OFFSET_PATTERN =
      Pattern.compile(
          "\\s+(LIMIT\\s+(-?\\d+)(?:\\s+OFFSET\\s+(-?\\d+))?|OFFSET\\s+(-?\\d+)\\s+LIMIT\\s+(-?\\d+))\\s*$",
          Pattern.CASE_INSENSITIVE);

  // Pattern to match just OFFSET without LIMIT
  private static final Pattern OFFSET_ONLY_PATTERN =
      Pattern.compile("\\s+OFFSET\\s+(-?\\d+)\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Extracts LIMIT and OFFSET values from a SQL query.
   *
   * @param sql the SQL query to parse
   * @return a LimitOffsetInfo object containing the extracted values
   */
  public static LimitOffsetInfo extractLimitOffset(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return new LimitOffsetInfo(null, null);
    }

    Integer limit = null;
    Integer offset = null;

    // First, try to match LIMIT and OFFSET patterns
    Matcher limitOffsetMatcher = LIMIT_OFFSET_PATTERN.matcher(sql);
    if (limitOffsetMatcher.find()) {
      String limitValue1 = limitOffsetMatcher.group(2); // LIMIT n OFFSET m format
      String offsetValue1 = limitOffsetMatcher.group(3); // LIMIT n OFFSET m format
      String offsetValue2 = limitOffsetMatcher.group(4); // OFFSET m LIMIT n format
      String limitValue2 = limitOffsetMatcher.group(5); // OFFSET m LIMIT n format

      if (limitValue1 != null) {
        limit = Integer.parseInt(limitValue1);
        if (offsetValue1 != null) {
          offset = Integer.parseInt(offsetValue1);
        }
      } else if (offsetValue2 != null && limitValue2 != null) {
        offset = Integer.parseInt(offsetValue2);
        limit = Integer.parseInt(limitValue2);
      }
    } else {
      // Check for OFFSET without LIMIT
      Matcher offsetOnlyMatcher = OFFSET_ONLY_PATTERN.matcher(sql);
      if (offsetOnlyMatcher.find()) {
        offset = Integer.parseInt(offsetOnlyMatcher.group(1));
      }
    }

    // Validate values
    if (limit != null && limit < 0) {
      throw new IllegalArgumentException("LIMIT value cannot be negative: " + limit);
    }
    if (offset != null && offset < 0) {
      throw new IllegalArgumentException("OFFSET value cannot be negative: " + offset);
    }

    return new LimitOffsetInfo(limit, offset);
  }

  /**
   * Removes LIMIT and OFFSET clauses from a SQL query.
   *
   * @param sql the SQL query to process
   * @return the SQL query without LIMIT and OFFSET clauses
   */
  public static String removeLimitOffset(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return sql;
    }

    // First remove LIMIT/OFFSET combinations
    String result = LIMIT_OFFSET_PATTERN.matcher(sql).replaceAll("");

    // Then remove any standalone OFFSET
    result = OFFSET_ONLY_PATTERN.matcher(result).replaceAll("");

    return result;
  }

  /**
   * Validates that the LIMIT and OFFSET values are reasonable. This helps prevent potential
   * performance issues or memory problems.
   *
   * @param limitOffsetInfo the LIMIT and OFFSET values to validate
   * @throws IllegalArgumentException if values are unreasonable
   */
  public static void validateLimitOffset(LimitOffsetInfo limitOffsetInfo) {
    if (limitOffsetInfo == null) {
      return;
    }

    // Validate LIMIT
    if (limitOffsetInfo.hasLimit()) {
      int limit = limitOffsetInfo.getLimit();
      if (limit < 0) {
        throw new IllegalArgumentException("LIMIT value cannot be negative: " + limit);
      }
      if (limit > 1000000) {
        throw new IllegalArgumentException("LIMIT value is too large (max 1000000): " + limit);
      }
    }

    // Validate OFFSET
    if (limitOffsetInfo.hasOffset()) {
      int offset = limitOffsetInfo.getOffset();
      if (offset < 0) {
        throw new IllegalArgumentException("OFFSET value cannot be negative: " + offset);
      }
      if (offset > 10000000) {
        throw new IllegalArgumentException("OFFSET value is too large (max 10000000): " + offset);
      }
    }
  }

  /**
   * Extracts the table name from a SELECT query. Supports queries with FROM clause, including those
   * with joins.
   *
   * @param sql the SQL query to parse
   * @return the primary table name, or null if not found
   */
  public static String extractTableName(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return null;
    }

    // Pattern to match FROM tableName, handling various SQL formats
    // This captures the first table name after FROM, before any JOIN, WHERE, etc.
    Pattern pattern = Pattern.compile("\\bFROM\\s+([\\w]+)(?:\\s|,|$)", Pattern.CASE_INSENSITIVE);

    Matcher matcher = pattern.matcher(sql);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  /**
   * Checks if a query is a SELECT * query (selecting all columns).
   *
   * @param sql the SQL query to check
   * @return true if it's a SELECT * query, false otherwise
   */
  public static boolean isSelectAllQuery(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    // Pattern to match SELECT * FROM (with optional whitespace)
    Pattern pattern = Pattern.compile("^\\s*SELECT\\s+\\*\\s+FROM\\s+", Pattern.CASE_INSENSITIVE);

    return pattern.matcher(sql).find();
  }

  /**
   * Extracts table and index information from a query. Handles DynamoDB PartiQL syntax: FROM
   * "table"."index" or FROM "table"
   *
   * @param sql the SQL query to parse
   * @return TableIndexInfo containing table and optional index names, or null if not found
   */
  public static TableIndexInfo extractTableIndexInfo(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return null;
    }

    // Pattern to match:
    // 1. "table"."index" (with quotes)
    // 2. "table" (with quotes)
    // 3. table.index (without quotes)
    // 4. table (without quotes)
    Pattern pattern =
        Pattern.compile(
            "\\bFROM\\s+(?:\"([^\"]+)\"(?:\\.\"([^\"]+)\")?|([\\w-]+)(?:\\.([\\w-]+))?)",
            Pattern.CASE_INSENSITIVE);

    Matcher matcher = pattern.matcher(sql);
    if (matcher.find()) {
      String quotedTable = matcher.group(1);
      String quotedIndex = matcher.group(2);
      String unquotedTable = matcher.group(3);
      String unquotedIndex = matcher.group(4);

      String tableName = quotedTable != null ? quotedTable : unquotedTable;
      String indexName = quotedIndex != null ? quotedIndex : unquotedIndex;

      if (tableName != null) {
        return new TableIndexInfo(tableName, indexName);
      }
    }

    return null;
  }

  /**
   * Converts queries with old-style index syntax to proper DynamoDB PartiQL syntax. Transforms:
   * FROM "table.index" to FROM "table"."index" Special case: FROM "table.PRIMARY" to FROM "table"
   * (PRIMARY is not a real index in DynamoDB)
   *
   * @param sql the SQL query to transform
   * @return the SQL query with proper index syntax
   */
  public static String normalizeIndexSyntax(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return sql;
    }

    // Pattern to match "table.index" as a single quoted string
    // Capture the FROM keyword to preserve its case
    Pattern pattern =
        Pattern.compile("(\\bFROM\\s+)\"([^\"]+)\\.([^\"]+)\"", Pattern.CASE_INSENSITIVE);

    Matcher matcher = pattern.matcher(sql);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String fromKeyword = matcher.group(1);
      String tableName = matcher.group(2);
      String indexName = matcher.group(3);

      // Special handling for PRIMARY - just use the table name
      if ("PRIMARY".equalsIgnoreCase(indexName)) {
        String replacement = fromKeyword + "\"" + tableName + "\"";
        matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
      } else {
        // Convert to proper syntax: "table"."index", preserving FROM case
        String replacement = fromKeyword + "\"" + tableName + "\".\"" + indexName + "\"";
        matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
      }
    }
    matcher.appendTail(result);

    // Also handle JOIN clauses
    String intermediate = result.toString();
    Pattern joinPattern =
        Pattern.compile("(\\bJOIN\\s+)\"([^\"]+)\\.([^\"]+)\"", Pattern.CASE_INSENSITIVE);

    Matcher joinMatcher = joinPattern.matcher(intermediate);
    result = new StringBuffer();

    while (joinMatcher.find()) {
      String joinKeyword = joinMatcher.group(1);
      String tableName = joinMatcher.group(2);
      String indexName = joinMatcher.group(3);

      // Special handling for PRIMARY - just use the table name
      if ("PRIMARY".equalsIgnoreCase(indexName)) {
        String replacement = joinKeyword + "\"" + tableName + "\"";
        joinMatcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
      } else {
        // Convert to proper syntax: "table"."index", preserving JOIN case
        String replacement = joinKeyword + "\"" + tableName + "\".\"" + indexName + "\"";
        joinMatcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
      }
    }
    joinMatcher.appendTail(result);

    return result.toString();
  }

  /**
   * Creates a warning for large OFFSET values that may impact performance.
   *
   * @param offset the OFFSET value
   * @param threshold the threshold above which to generate a warning
   * @return a SQLWarning if offset exceeds threshold, null otherwise
   */
  public static SQLWarning createOffsetWarning(int offset, int threshold) {
    if (offset > threshold) {
      String message =
          String.format(
              "Large OFFSET value (%d) may impact performance. DynamoDB uses token-based pagination, "
                  + "so rows must be fetched and discarded to reach the desired offset. "
                  + "Consider using cursor-based pagination with WHERE clauses instead.",
              offset);
      return new SQLWarning(message, "01000"); // General warning
    }
    return null;
  }

  /**
   * Checks if the given offset is considered large and may impact performance.
   *
   * @param offset the OFFSET value
   * @param threshold the threshold to consider as large
   * @return true if the offset exceeds the threshold
   */
  public static boolean isLargeOffset(int offset, int threshold) {
    return offset > threshold;
  }
}
