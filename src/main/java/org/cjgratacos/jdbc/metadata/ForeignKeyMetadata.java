package org.cjgratacos.jdbc.metadata;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents metadata for a logical foreign key relationship in DynamoDB. Since DynamoDB doesn't
 * support actual foreign keys, this class stores user-defined logical relationships for tool
 * compatibility.
 */
public class ForeignKeyMetadata {
  private final String constraintName;
  private final String primaryCatalog;
  private final String primarySchema;
  private final String primaryTable;
  private final String primaryColumn;
  private final String foreignCatalog;
  private final String foreignSchema;
  private final String foreignTable;
  private final String foreignColumn;
  private final int keySeq;
  private final int updateRule;
  private final int deleteRule;
  private final int deferrability;
  private final boolean validated;
  private final List<String> validationErrors;

  private ForeignKeyMetadata(Builder builder) {
    this.constraintName = builder.constraintName;
    this.primaryCatalog = builder.primaryCatalog;
    this.primarySchema = builder.primarySchema;
    this.primaryTable =
        Objects.requireNonNull(builder.primaryTable, "Primary table cannot be null");
    this.primaryColumn =
        Objects.requireNonNull(builder.primaryColumn, "Primary column cannot be null");
    this.foreignCatalog = builder.foreignCatalog;
    this.foreignSchema = builder.foreignSchema;
    this.foreignTable =
        Objects.requireNonNull(builder.foreignTable, "Foreign table cannot be null");
    this.foreignColumn =
        Objects.requireNonNull(builder.foreignColumn, "Foreign column cannot be null");
    this.keySeq = builder.keySeq;
    this.updateRule = builder.updateRule;
    this.deleteRule = builder.deleteRule;
    this.deferrability = builder.deferrability;
    this.validated = builder.validated;
    this.validationErrors =
        builder.validationErrors != null
            ? Collections.unmodifiableList(new ArrayList<>(builder.validationErrors))
            : Collections.emptyList();
  }

  /**
   * Gets the constraint name.
   *
   * @return the constraint name
   */
  public String getConstraintName() {
    return constraintName;
  }

  /**
   * Gets the primary key catalog.
   *
   * @return the primary catalog
   */
  public String getPrimaryCatalog() {
    return primaryCatalog;
  }

  /**
   * Gets the primary key schema.
   *
   * @return the primary schema
   */
  public String getPrimarySchema() {
    return primarySchema;
  }

  /**
   * Gets the primary key table name.
   *
   * @return the primary table name
   */
  public String getPrimaryTable() {
    return primaryTable;
  }

  /**
   * Gets the primary key column name.
   *
   * @return the primary column name
   */
  public String getPrimaryColumn() {
    return primaryColumn;
  }

  /**
   * Gets the foreign key catalog.
   *
   * @return the foreign catalog
   */
  public String getForeignCatalog() {
    return foreignCatalog;
  }

  /**
   * Gets the foreign key schema.
   *
   * @return the foreign schema
   */
  public String getForeignSchema() {
    return foreignSchema;
  }

  /**
   * Gets the foreign key table name.
   *
   * @return the foreign table name
   */
  public String getForeignTable() {
    return foreignTable;
  }

  /**
   * Gets the foreign key column name.
   *
   * @return the foreign column name
   */
  public String getForeignColumn() {
    return foreignColumn;
  }

  /**
   * Gets the key sequence number.
   *
   * @return the key sequence number
   */
  public int getKeySeq() {
    return keySeq;
  }

  /**
   * Gets the update rule.
   *
   * @return the update rule as defined in DatabaseMetaData
   */
  public int getUpdateRule() {
    return updateRule;
  }

  /**
   * Gets the delete rule.
   *
   * @return the delete rule as defined in DatabaseMetaData
   */
  public int getDeleteRule() {
    return deleteRule;
  }

  /**
   * Gets the deferrability.
   *
   * @return the deferrability as defined in DatabaseMetaData
   */
  public int getDeferrability() {
    return deferrability;
  }

  /**
   * Checks if this foreign key has been validated.
   *
   * @return true if validated, false otherwise
   */
  public boolean isValidated() {
    return validated;
  }

  /**
   * Gets the validation errors.
   *
   * @return an unmodifiable list of validation errors
   */
  public List<String> getValidationErrors() {
    return validationErrors;
  }

  /**
   * Checks if this foreign key has validation errors.
   *
   * @return true if there are validation errors, false otherwise
   */
  public boolean hasValidationErrors() {
    return !validationErrors.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ForeignKeyMetadata that = (ForeignKeyMetadata) o;
    return keySeq == that.keySeq
        && Objects.equals(constraintName, that.constraintName)
        && Objects.equals(primaryTable, that.primaryTable)
        && Objects.equals(primaryColumn, that.primaryColumn)
        && Objects.equals(foreignTable, that.foreignTable)
        && Objects.equals(foreignColumn, that.foreignColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        constraintName, primaryTable, primaryColumn, foreignTable, foreignColumn, keySeq);
  }

  @Override
  public String toString() {
    return String.format(
        "ForeignKey[%s: %s.%s -> %s.%s]",
        constraintName != null ? constraintName : "unnamed",
        foreignTable,
        foreignColumn,
        primaryTable,
        primaryColumn);
  }

  /**
   * Creates a new builder for ForeignKeyMetadata.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for ForeignKeyMetadata objects.
   */
  public static class Builder {
    private String constraintName;
    private String primaryCatalog;
    private String primarySchema;
    private String primaryTable;
    private String primaryColumn;
    private String foreignCatalog;
    private String foreignSchema;
    private String foreignTable;
    private String foreignColumn;
    private int keySeq = 1;
    private int updateRule = DatabaseMetaData.importedKeyNoAction;
    private int deleteRule = DatabaseMetaData.importedKeyNoAction;
    private int deferrability = DatabaseMetaData.importedKeyNotDeferrable;
    private boolean validated = false;
    private List<String> validationErrors = null;

    /**
     * Sets the constraint name.
     *
     * @param constraintName the constraint name
     * @return this builder
     */
    public Builder constraintName(String constraintName) {
      this.constraintName = constraintName;
      return this;
    }

    /**
     * Sets the primary catalog.
     *
     * @param primaryCatalog the primary catalog
     * @return this builder
     */
    public Builder primaryCatalog(String primaryCatalog) {
      this.primaryCatalog = primaryCatalog;
      return this;
    }

    /**
     * Sets the primary schema.
     *
     * @param primarySchema the primary schema
     * @return this builder
     */
    public Builder primarySchema(String primarySchema) {
      this.primarySchema = primarySchema;
      return this;
    }

    /**
     * Sets the primary table.
     *
     * @param primaryTable the primary table
     * @return this builder
     */
    public Builder primaryTable(String primaryTable) {
      this.primaryTable = primaryTable;
      return this;
    }

    /**
     * Sets the primary column.
     *
     * @param primaryColumn the primary column
     * @return this builder
     */
    public Builder primaryColumn(String primaryColumn) {
      this.primaryColumn = primaryColumn;
      return this;
    }

    /**
     * Sets the foreign catalog.
     *
     * @param foreignCatalog the foreign catalog
     * @return this builder
     */
    public Builder foreignCatalog(String foreignCatalog) {
      this.foreignCatalog = foreignCatalog;
      return this;
    }

    /**
     * Sets the foreign schema.
     *
     * @param foreignSchema the foreign schema
     * @return this builder
     */
    public Builder foreignSchema(String foreignSchema) {
      this.foreignSchema = foreignSchema;
      return this;
    }

    /**
     * Sets the foreign table.
     *
     * @param foreignTable the foreign table
     * @return this builder
     */
    public Builder foreignTable(String foreignTable) {
      this.foreignTable = foreignTable;
      return this;
    }

    /**
     * Sets the foreign column.
     *
     * @param foreignColumn the foreign column
     * @return this builder
     */
    public Builder foreignColumn(String foreignColumn) {
      this.foreignColumn = foreignColumn;
      return this;
    }

    /**
     * Sets the key sequence.
     *
     * @param keySeq the key sequence
     * @return this builder
     */
    public Builder keySeq(int keySeq) {
      this.keySeq = keySeq;
      return this;
    }

    /**
     * Sets the update rule.
     *
     * @param updateRule the update rule
     * @return this builder
     */
    public Builder updateRule(int updateRule) {
      this.updateRule = updateRule;
      return this;
    }

    /**
     * Sets the delete rule.
     *
     * @param deleteRule the delete rule
     * @return this builder
     */
    public Builder deleteRule(int deleteRule) {
      this.deleteRule = deleteRule;
      return this;
    }

    /**
     * Sets the deferrability.
     *
     * @param deferrability the deferrability
     * @return this builder
     */
    public Builder deferrability(int deferrability) {
      this.deferrability = deferrability;
      return this;
    }

    /**
     * Sets whether the foreign key is validated.
     *
     * @param validated true if validated
     * @return this builder
     */
    public Builder validated(boolean validated) {
      this.validated = validated;
      return this;
    }

    /**
     * Sets the validation errors.
     *
     * @param validationErrors the validation errors
     * @return this builder
     */
    public Builder validationErrors(List<String> validationErrors) {
      this.validationErrors = validationErrors;
      return this;
    }

    /**
     * Builds the ForeignKeyMetadata object.
     *
     * @return a new ForeignKeyMetadata instance
     */
    public ForeignKeyMetadata build() {
      return new ForeignKeyMetadata(this);
    }
  }
}
