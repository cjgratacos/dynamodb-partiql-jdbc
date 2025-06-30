package org.cjgratacos.jdbc.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates foreign key metadata to ensure referential integrity. Performs various checks including
 * table existence, column existence, circular references, and duplicate constraints.
 */
public class ForeignKeyValidator {
  private static final Logger logger = LoggerFactory.getLogger(ForeignKeyValidator.class);

  private final TableValidator tableValidator;

  public ForeignKeyValidator(TableValidator tableValidator) {
    this.tableValidator = tableValidator;
  }

  /**
   * Validates a single foreign key metadata object.
   *
   * @param foreignKey the foreign key to validate
   * @return validation result with any errors found
   */
  public ValidationResult validate(ForeignKeyMetadata foreignKey) {
    List<String> errors = new ArrayList<>();

    // Validate constraint name
    if (foreignKey.getConstraintName() == null || foreignKey.getConstraintName().trim().isEmpty()) {
      errors.add("Constraint name is missing or empty");
    }

    // Validate primary table and column
    String primaryTable = foreignKey.getPrimaryTable();
    String primaryColumn = foreignKey.getPrimaryColumn();

    if (!tableValidator.tableExists(primaryTable)) {
      errors.add(String.format("Primary table '%s' does not exist", primaryTable));
    } else if (!tableValidator.columnExists(primaryTable, primaryColumn)) {
      errors.add(
          String.format(
              "Primary column '%s' does not exist in table '%s'", primaryColumn, primaryTable));
    }

    // Validate foreign table and column
    String foreignTable = foreignKey.getForeignTable();
    String foreignColumn = foreignKey.getForeignColumn();

    if (!tableValidator.tableExists(foreignTable)) {
      errors.add(String.format("Foreign table '%s' does not exist", foreignTable));
    } else if (!tableValidator.columnExists(foreignTable, foreignColumn)) {
      errors.add(
          String.format(
              "Foreign column '%s' does not exist in table '%s'", foreignColumn, foreignTable));
    }

    // Check for self-referential foreign key (allowed but log info)
    if (primaryTable.equals(foreignTable) && primaryColumn.equals(foreignColumn)) {
      logger.info("Self-referential foreign key detected: {}.{}", foreignTable, foreignColumn);
    }

    // Build validated foreign key metadata
    ForeignKeyMetadata validatedFK =
        ForeignKeyMetadata.builder()
            .constraintName(foreignKey.getConstraintName())
            .primaryCatalog(foreignKey.getPrimaryCatalog())
            .primarySchema(foreignKey.getPrimarySchema())
            .primaryTable(foreignKey.getPrimaryTable())
            .primaryColumn(foreignKey.getPrimaryColumn())
            .foreignCatalog(foreignKey.getForeignCatalog())
            .foreignSchema(foreignKey.getForeignSchema())
            .foreignTable(foreignKey.getForeignTable())
            .foreignColumn(foreignKey.getForeignColumn())
            .keySeq(foreignKey.getKeySeq())
            .updateRule(foreignKey.getUpdateRule())
            .deleteRule(foreignKey.getDeleteRule())
            .deferrability(foreignKey.getDeferrability())
            .validated(true)
            .validationErrors(errors)
            .build();

    return new ValidationResult(validatedFK, errors);
  }

  /**
   * Validates multiple foreign keys and checks for additional issues like circular references and
   * duplicate constraints.
   *
   * @param foreignKeys the list of foreign keys to validate
   * @return list of validation results
   */
  public List<ValidationResult> validateAll(List<ForeignKeyMetadata> foreignKeys) {
    List<ValidationResult> results = new ArrayList<>();
    Map<String, ForeignKeyMetadata> constraintMap = new HashMap<>();
    Map<String, Set<String>> dependencyGraph = new HashMap<>();

    // First pass: validate individual foreign keys and check for duplicates
    for (ForeignKeyMetadata fk : foreignKeys) {
      ValidationResult result = validate(fk);

      // Check for duplicate constraint names
      if (constraintMap.containsKey(fk.getConstraintName())) {
        List<String> errors = new ArrayList<>(result.getErrors());
        errors.add(String.format("Duplicate constraint name: '%s'", fk.getConstraintName()));
        result = new ValidationResult(result.getForeignKey(), errors);
      } else {
        constraintMap.put(fk.getConstraintName(), fk);
      }

      results.add(result);

      // Build dependency graph for circular reference detection
      String foreignTable = fk.getForeignTable();
      String primaryTable = fk.getPrimaryTable();
      dependencyGraph.computeIfAbsent(foreignTable, k -> new HashSet<>()).add(primaryTable);
    }

    // Second pass: check for circular references
    List<String> circularReferences = detectCircularReferences(dependencyGraph);
    if (!circularReferences.isEmpty()) {
      logger.warn("Circular foreign key references detected: {}", circularReferences);
      // Add circular reference warnings to affected foreign keys
      for (int i = 0; i < results.size(); i++) {
        ValidationResult result = results.get(i);
        ForeignKeyMetadata fk = result.getForeignKey();
        if (isInvolvedInCircularReference(fk, circularReferences)) {
          List<String> errors = new ArrayList<>(result.getErrors());
          errors.add("Foreign key is part of a circular reference chain");
          results.set(i, new ValidationResult(fk, errors));
        }
      }
    }

    return results;
  }

  /**
   * Validates a foreign key and throws an exception if validation fails.
   *
   * @param foreignKey the foreign key to validate
   * @throws ForeignKeyValidationException if validation fails
   */
  public void validateOrThrow(ForeignKeyMetadata foreignKey) throws ForeignKeyValidationException {
    ValidationResult result = validate(foreignKey);
    if (result.hasErrors()) {
      throw new ForeignKeyValidationException(
          ForeignKeyValidationException.formatErrors(result.getErrors()), result.getErrors());
    }
  }

  /**
   * Detects circular references in the foreign key dependency graph.
   *
   * @param dependencyGraph map of table to its dependencies
   * @return list of tables involved in circular references
   */
  private List<String> detectCircularReferences(Map<String, Set<String>> dependencyGraph) {
    Set<String> visited = new HashSet<>();
    Set<String> recursionStack = new HashSet<>();
    List<String> circularTables = new ArrayList<>();

    for (String table : dependencyGraph.keySet()) {
      if (hasCircularDependency(table, dependencyGraph, visited, recursionStack)) {
        circularTables.add(table);
      }
    }

    return circularTables;
  }

  private boolean hasCircularDependency(
      String table,
      Map<String, Set<String>> graph,
      Set<String> visited,
      Set<String> recursionStack) {
    visited.add(table);
    recursionStack.add(table);

    Set<String> dependencies = graph.get(table);
    if (dependencies != null) {
      for (String dependency : dependencies) {
        if (!visited.contains(dependency)) {
          if (hasCircularDependency(dependency, graph, visited, recursionStack)) {
            return true;
          }
        } else if (recursionStack.contains(dependency)) {
          return true;
        }
      }
    }

    recursionStack.remove(table);
    return false;
  }

  private boolean isInvolvedInCircularReference(
      ForeignKeyMetadata fk, List<String> circularTables) {
    return circularTables.contains(fk.getForeignTable())
        || circularTables.contains(fk.getPrimaryTable());
  }

  /** Represents the result of validating a foreign key. */
  public static class ValidationResult {
    private final ForeignKeyMetadata foreignKey;
    private final List<String> errors;

    public ValidationResult(ForeignKeyMetadata foreignKey, List<String> errors) {
      this.foreignKey = foreignKey;
      this.errors = errors != null ? new ArrayList<>(errors) : new ArrayList<>();
    }

    public ForeignKeyMetadata getForeignKey() {
      return foreignKey;
    }

    public List<String> getErrors() {
      return new ArrayList<>(errors);
    }

    public boolean hasErrors() {
      return !errors.isEmpty();
    }

    public boolean isValid() {
      return errors.isEmpty();
    }

    @Override
    public String toString() {
      if (isValid()) {
        return String.format("Valid: %s", foreignKey);
      } else {
        return String.format("Invalid: %s - Errors: %s", foreignKey, errors);
      }
    }
  }

  /** Represents a validation report for multiple foreign keys. */
  public static class ValidationReport {
    private final List<ValidationResult> results;

    public ValidationReport(List<ValidationResult> results) {
      this.results = new ArrayList<>(results);
    }

    public List<ValidationResult> getResults() {
      return new ArrayList<>(results);
    }

    public List<ForeignKeyMetadata> getValidForeignKeys() {
      return results.stream()
          .filter(ValidationResult::isValid)
          .map(ValidationResult::getForeignKey)
          .collect(Collectors.toList());
    }

    public List<ForeignKeyMetadata> getInvalidForeignKeys() {
      return results.stream()
          .filter(r -> !r.isValid())
          .map(ValidationResult::getForeignKey)
          .collect(Collectors.toList());
    }

    public boolean hasErrors() {
      return results.stream().anyMatch(r -> !r.isValid());
    }

    public int getTotalCount() {
      return results.size();
    }

    public int getValidCount() {
      return (int) results.stream().filter(ValidationResult::isValid).count();
    }

    public int getInvalidCount() {
      return (int) results.stream().filter(r -> !r.isValid()).count();
    }

    public List<String> getAllErrors() {
      return results.stream()
          .filter(r -> !r.isValid())
          .flatMap(r -> r.getErrors().stream())
          .collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return String.format(
          "ValidationReport[total=%d, valid=%d, invalid=%d]",
          getTotalCount(), getValidCount(), getInvalidCount());
    }
  }
}
