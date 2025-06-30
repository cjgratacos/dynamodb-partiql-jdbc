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

  public String getConstraintName() {
    return constraintName;
  }

  public String getPrimaryCatalog() {
    return primaryCatalog;
  }

  public String getPrimarySchema() {
    return primarySchema;
  }

  public String getPrimaryTable() {
    return primaryTable;
  }

  public String getPrimaryColumn() {
    return primaryColumn;
  }

  public String getForeignCatalog() {
    return foreignCatalog;
  }

  public String getForeignSchema() {
    return foreignSchema;
  }

  public String getForeignTable() {
    return foreignTable;
  }

  public String getForeignColumn() {
    return foreignColumn;
  }

  public int getKeySeq() {
    return keySeq;
  }

  public int getUpdateRule() {
    return updateRule;
  }

  public int getDeleteRule() {
    return deleteRule;
  }

  public int getDeferrability() {
    return deferrability;
  }

  public boolean isValidated() {
    return validated;
  }

  public List<String> getValidationErrors() {
    return validationErrors;
  }

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

  public static Builder builder() {
    return new Builder();
  }

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

    public Builder constraintName(String constraintName) {
      this.constraintName = constraintName;
      return this;
    }

    public Builder primaryCatalog(String primaryCatalog) {
      this.primaryCatalog = primaryCatalog;
      return this;
    }

    public Builder primarySchema(String primarySchema) {
      this.primarySchema = primarySchema;
      return this;
    }

    public Builder primaryTable(String primaryTable) {
      this.primaryTable = primaryTable;
      return this;
    }

    public Builder primaryColumn(String primaryColumn) {
      this.primaryColumn = primaryColumn;
      return this;
    }

    public Builder foreignCatalog(String foreignCatalog) {
      this.foreignCatalog = foreignCatalog;
      return this;
    }

    public Builder foreignSchema(String foreignSchema) {
      this.foreignSchema = foreignSchema;
      return this;
    }

    public Builder foreignTable(String foreignTable) {
      this.foreignTable = foreignTable;
      return this;
    }

    public Builder foreignColumn(String foreignColumn) {
      this.foreignColumn = foreignColumn;
      return this;
    }

    public Builder keySeq(int keySeq) {
      this.keySeq = keySeq;
      return this;
    }

    public Builder updateRule(int updateRule) {
      this.updateRule = updateRule;
      return this;
    }

    public Builder deleteRule(int deleteRule) {
      this.deleteRule = deleteRule;
      return this;
    }

    public Builder deferrability(int deferrability) {
      this.deferrability = deferrability;
      return this;
    }

    public Builder validated(boolean validated) {
      this.validated = validated;
      return this;
    }

    public Builder validationErrors(List<String> validationErrors) {
      this.validationErrors = validationErrors;
      return this;
    }

    public ForeignKeyMetadata build() {
      return new ForeignKeyMetadata(this);
    }
  }
}
