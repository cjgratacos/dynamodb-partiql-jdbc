package org.cjgratacos.jdbc;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Type resolution utility for handling conflicting data types in DynamoDB schema detection.
 *
 * <p>When sampling DynamoDB items for schema inference, the same attribute may have different types
 * across different items. This class provides logic to resolve these conflicts by choosing the most
 * appropriate SQL type based on frequency and type compatibility.
 *
 * <h2>Resolution Strategy:</h2>
 *
 * <p>The resolver uses a priority-based approach combined with frequency analysis:
 *
 * <ol>
 *   <li><strong>Frequency</strong>: More common types are preferred
 *   <li><strong>Compatibility</strong>: Types that can accommodate other types are preferred
 *   <li><strong>Flexibility</strong>: More flexible types (like STRING) are preferred over rigid
 *       ones
 * </ol>
 *
 * <h2>Type Priority (from most to least flexible):</h2>
 *
 * <ol>
 *   <li>STRING - can represent any value as text
 *   <li>NUMBER - can handle numeric strings with conversion
 *   <li>BINARY - specific binary data
 *   <li>BOOLEAN - most restrictive
 *   <li>ARRAY - collection types
 *   <li>STRUCT - complex objects
 *   <li>NULL - represents missing values
 * </ol>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class TypeResolver {

  /** Creates a new TypeResolver instance. */
  public TypeResolver() {
    // Default constructor
  }

  private static final Logger logger = LoggerFactory.getLogger(TypeResolver.class);

  /**
   * Resolves conflicting types by analyzing frequency and compatibility.
   *
   * @param typeCounts a map of type names to their occurrence counts
   * @return the SQL type code that best represents the conflicting types
   */
  public int resolveConflictingTypes(Map<String, Integer> typeCounts) {
    if (typeCounts.isEmpty()) {
      return java.sql.Types.OTHER;
    }

    if (typeCounts.size() == 1) {
      // No conflict, return the single type
      var typeName = typeCounts.keySet().iterator().next();
      return sqlTypeFromName(typeName);
    }

    // Multiple types detected - resolve conflict
    if (logger.isDebugEnabled()) {
      logger.debug("Resolving type conflict: {}", typeCounts);
    }

    // Remove NULL types from consideration unless it's the only type
    var nonNullTypes =
        typeCounts.entrySet().stream()
            .filter(entry -> !"NULL".equals(entry.getKey()))
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var typesToConsider = nonNullTypes.isEmpty() ? typeCounts : nonNullTypes;

    // Find the most flexible type that can accommodate the data
    var resolvedType = findMostFlexibleType(typesToConsider);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Type conflict resolved: {} -> {}", typeCounts, getTypeNameFromSql(resolvedType));
    }

    return resolvedType;
  }

  private int findMostFlexibleType(Map<String, Integer> typeCounts) {
    // Check for STRING first - most flexible
    if (typeCounts.containsKey("STRING")) {
      return java.sql.Types.VARCHAR;
    }

    // Check for NUMBER - can handle numeric data
    if (typeCounts.containsKey("NUMBER")) {
      // If we have both NUMBER and other simple types, STRING is safer
      var hasNonNumericTypes =
          typeCounts.keySet().stream()
              .anyMatch(
                  type -> !"NUMBER".equals(type) && !"NULL".equals(type) && isSimpleType(type));

      if (hasNonNumericTypes) {
        return java.sql.Types.VARCHAR; // Fallback to string for mixed simple types
      }

      return java.sql.Types.NUMERIC;
    }

    // Check for BINARY
    if (typeCounts.containsKey("BINARY")) {
      // If mixed with other types, use string representation
      if (typeCounts.size() > 1) {
        return java.sql.Types.VARCHAR;
      }
      return java.sql.Types.BINARY;
    }

    // Check for BOOLEAN
    if (typeCounts.containsKey("BOOLEAN")) {
      // If mixed with other types, use string representation
      if (typeCounts.size() > 1) {
        return java.sql.Types.VARCHAR;
      }
      return java.sql.Types.BOOLEAN;
    }

    // Check for complex types
    if (typeCounts.containsKey("ARRAY")) {
      // If mixed with simple types, prefer string
      var hasSimpleTypes = typeCounts.keySet().stream().anyMatch(this::isSimpleType);

      if (hasSimpleTypes) {
        return java.sql.Types.VARCHAR;
      }
      return java.sql.Types.ARRAY;
    }

    if (typeCounts.containsKey("STRUCT")) {
      // If mixed with simple types, prefer string
      var hasSimpleTypes = typeCounts.keySet().stream().anyMatch(this::isSimpleType);

      if (hasSimpleTypes) {
        return java.sql.Types.VARCHAR;
      }
      return java.sql.Types.STRUCT;
    }

    // Default fallback
    return java.sql.Types.VARCHAR;
  }

  private boolean isSimpleType(String typeName) {
    return "STRING".equals(typeName)
        || "NUMBER".equals(typeName)
        || "BINARY".equals(typeName)
        || "BOOLEAN".equals(typeName);
  }

  private int sqlTypeFromName(String typeName) {
    switch (typeName) {
      case "STRING":
        return java.sql.Types.VARCHAR;
      case "NUMBER":
        return java.sql.Types.NUMERIC;
      case "BINARY":
        return java.sql.Types.BINARY;
      case "BOOLEAN":
        return java.sql.Types.BOOLEAN;
      case "ARRAY":
        return java.sql.Types.ARRAY;
      case "STRUCT":
        return java.sql.Types.STRUCT;
      case "NULL":
        return java.sql.Types.NULL;
      default:
        return java.sql.Types.OTHER;
    }
  }

  private String getTypeNameFromSql(int sqlType) {
    switch (sqlType) {
      case java.sql.Types.VARCHAR:
        return "STRING";
      case java.sql.Types.NUMERIC:
        return "NUMBER";
      case java.sql.Types.BINARY:
        return "BINARY";
      case java.sql.Types.BOOLEAN:
        return "BOOLEAN";
      case java.sql.Types.ARRAY:
        return "ARRAY";
      case java.sql.Types.STRUCT:
        return "STRUCT";
      case java.sql.Types.NULL:
        return "NULL";
      default:
        return "OTHER";
    }
  }

  /**
   * Determines if two SQL types are compatible for merging.
   *
   * @param type1 the first SQL type
   * @param type2 the second SQL type
   * @return true if the types can be safely merged
   */
  public boolean areTypesCompatible(int type1, int type2) {
    if (type1 == type2) {
      return true;
    }

    // VARCHAR is compatible with everything (as string representation)
    if (type1 == java.sql.Types.VARCHAR || type2 == java.sql.Types.VARCHAR) {
      return true;
    }

    // NULL is compatible with everything
    if (type1 == java.sql.Types.NULL || type2 == java.sql.Types.NULL) {
      return true;
    }

    // NUMBER and BOOLEAN can be represented as strings
    var numericTypes = java.util.Set.of(java.sql.Types.NUMERIC, java.sql.Types.BOOLEAN);

    if (numericTypes.contains(type1) && numericTypes.contains(type2)) {
      return true;
    }

    return false;
  }
}
