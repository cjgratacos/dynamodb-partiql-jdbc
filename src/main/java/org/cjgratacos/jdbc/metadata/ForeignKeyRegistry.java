package org.cjgratacos.jdbc.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registry for storing and querying logical foreign key relationships. This class manages the
 * in-memory storage of user-defined foreign keys for DynamoDB tables.
 */
public class ForeignKeyRegistry {
  private final Map<String, List<ForeignKeyMetadata>> importedKeys = new ConcurrentHashMap<>();
  private final Map<String, List<ForeignKeyMetadata>> exportedKeys = new ConcurrentHashMap<>();
  private final List<ForeignKeyMetadata> allKeys = Collections.synchronizedList(new ArrayList<>());
  private boolean validateOnRegister = false;
  private ForeignKeyValidator validator;

  /** Creates a new ForeignKeyRegistry without validation. */
  public ForeignKeyRegistry() {
    this(false, null);
  }

  /**
   * Creates a new ForeignKeyRegistry with optional validation.
   *
   * @param validateOnRegister whether to validate foreign keys during registration
   * @param validator the validator to use (required if validateOnRegister is true)
   */
  public ForeignKeyRegistry(boolean validateOnRegister, ForeignKeyValidator validator) {
    this.validateOnRegister = validateOnRegister;
    this.validator = validator;
    if (validateOnRegister && validator == null) {
      throw new IllegalArgumentException("Validator is required when validateOnRegister is true");
    }
  }

  /**
   * Sets whether to validate foreign keys during registration.
   *
   * @param validateOnRegister true to enable validation
   */
  public void setValidateOnRegister(boolean validateOnRegister) {
    this.validateOnRegister = validateOnRegister;
  }

  /**
   * Sets the validator to use for foreign key validation.
   *
   * @param validator the validator
   */
  public void setValidator(ForeignKeyValidator validator) {
    this.validator = validator;
  }

  /**
   * Registers a foreign key relationship.
   *
   * @param foreignKey the foreign key metadata to register
   * @throws ForeignKeyValidationException if validation is enabled and the foreign key is invalid
   */
  public void registerForeignKey(ForeignKeyMetadata foreignKey)
      throws ForeignKeyValidationException {
    if (foreignKey == null) {
      throw new IllegalArgumentException("Foreign key cannot be null");
    }

    // Validate if enabled
    ForeignKeyMetadata toRegister = foreignKey;
    if (validateOnRegister && validator != null) {
      ForeignKeyValidator.ValidationResult result = validator.validate(foreignKey);
      if (result.hasErrors() && !allowInvalidKeys()) {
        throw new ForeignKeyValidationException(
            ForeignKeyValidationException.formatErrors(result.getErrors()), result.getErrors());
      }
      toRegister = result.getForeignKey(); // Use validated version
    }

    // Add to all keys list
    allKeys.add(toRegister);

    // Add to imported keys (foreign table imports from primary table)
    importedKeys
        .computeIfAbsent(toRegister.getForeignTable(), k -> new ArrayList<>())
        .add(toRegister);

    // Add to exported keys (primary table exports to foreign table)
    exportedKeys
        .computeIfAbsent(toRegister.getPrimaryTable(), k -> new ArrayList<>())
        .add(toRegister);
  }

  private boolean allowInvalidKeys() {
    // Could be made configurable in the future
    return false;
  }

  /**
   * Registers multiple foreign key relationships.
   *
   * @param foreignKeys the foreign key metadata to register
   * @throws ForeignKeyValidationException if validation is enabled and any foreign key is invalid
   */
  public void registerForeignKeys(List<ForeignKeyMetadata> foreignKeys)
      throws ForeignKeyValidationException {
    if (foreignKeys != null) {
      for (ForeignKeyMetadata fk : foreignKeys) {
        registerForeignKey(fk);
      }
    }
  }

  /**
   * Gets all foreign keys imported by the specified table. These are keys where the specified table
   * references another table.
   *
   * @param tableName the table name
   * @return list of imported foreign keys
   */
  public List<ForeignKeyMetadata> getImportedKeys(String tableName) {
    if (tableName == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(importedKeys.getOrDefault(tableName, Collections.emptyList()));
  }

  /**
   * Gets all foreign keys exported by the specified table. These are keys where other tables
   * reference this table.
   *
   * @param tableName the table name
   * @return list of exported foreign keys
   */
  public List<ForeignKeyMetadata> getExportedKeys(String tableName) {
    if (tableName == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(exportedKeys.getOrDefault(tableName, Collections.emptyList()));
  }

  /**
   * Gets cross-reference between two tables.
   *
   * @param primaryTable the primary (referenced) table
   * @param foreignTable the foreign (referencing) table
   * @return list of foreign keys between the two tables
   */
  public List<ForeignKeyMetadata> getCrossReference(String primaryTable, String foreignTable) {
    if (primaryTable == null || foreignTable == null) {
      return Collections.emptyList();
    }

    return allKeys.stream()
        .filter(
            fk ->
                primaryTable.equals(fk.getPrimaryTable())
                    && foreignTable.equals(fk.getForeignTable()))
        .collect(Collectors.toList());
  }

  /**
   * Gets all registered foreign keys.
   *
   * @return list of all foreign keys
   */
  public List<ForeignKeyMetadata> getAllForeignKeys() {
    return new ArrayList<>(allKeys);
  }

  /** Clears all registered foreign keys. */
  public void clear() {
    importedKeys.clear();
    exportedKeys.clear();
    allKeys.clear();
  }

  /**
   * Gets the number of registered foreign keys.
   *
   * @return the count of foreign keys
   */
  public int size() {
    return allKeys.size();
  }

  /**
   * Validates all registered foreign keys and returns a validation report.
   *
   * @return validation report containing results for all foreign keys
   */
  public ForeignKeyValidator.ValidationReport validateAll() {
    if (validator == null) {
      throw new IllegalStateException("No validator configured");
    }
    List<ForeignKeyValidator.ValidationResult> results = validator.validateAll(allKeys);
    return new ForeignKeyValidator.ValidationReport(results);
  }

  /**
   * Gets all valid foreign keys (those that have been validated and have no errors).
   *
   * @return list of valid foreign keys
   */
  public List<ForeignKeyMetadata> getValidForeignKeys() {
    return allKeys.stream()
        .filter(ForeignKeyMetadata::isValidated)
        .filter(fk -> !fk.hasValidationErrors())
        .collect(Collectors.toList());
  }

  /**
   * Gets all invalid foreign keys (those that have been validated and have errors).
   *
   * @return list of invalid foreign keys
   */
  public List<ForeignKeyMetadata> getInvalidForeignKeys() {
    return allKeys.stream()
        .filter(ForeignKeyMetadata::isValidated)
        .filter(ForeignKeyMetadata::hasValidationErrors)
        .collect(Collectors.toList());
  }

  /**
   * Gets all unvalidated foreign keys.
   *
   * @return list of unvalidated foreign keys
   */
  public List<ForeignKeyMetadata> getUnvalidatedForeignKeys() {
    return allKeys.stream().filter(fk -> !fk.isValidated()).collect(Collectors.toList());
  }
}
