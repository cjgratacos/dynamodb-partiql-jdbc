package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ForeignKeyValidatorTest {

  private TableValidator mockTableValidator;
  private ForeignKeyValidator validator;

  @BeforeEach
  void setUp() {
    mockTableValidator = mock(TableValidator.class);
    validator = new ForeignKeyValidator(mockTableValidator);
  }

  @Test
  void testValidateSingleForeignKeyAllValid() {
    // Mock all tables and columns exist
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    ForeignKeyValidator.ValidationResult result = validator.validate(fk);

    assertThat(result.isValid()).isTrue();
    assertThat(result.hasErrors()).isFalse();
    assertThat(result.getErrors()).isEmpty();
    assertThat(result.getForeignKey().isValidated()).isTrue();
    assertThat(result.getForeignKey().hasValidationErrors()).isFalse();
  }

  @Test
  void testValidateSingleForeignKeyPrimaryTableNotExists() {
    // Mock primary table doesn't exist
    when(mockTableValidator.tableExists("Users")).thenReturn(false);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    ForeignKeyValidator.ValidationResult result = validator.validate(fk);

    assertThat(result.isValid()).isFalse();
    assertThat(result.hasErrors()).isTrue();
    assertThat(result.getErrors()).contains("Primary table 'Users' does not exist");
    assertThat(result.getForeignKey().isValidated()).isTrue();
    assertThat(result.getForeignKey().hasValidationErrors()).isTrue();
  }

  @Test
  void testValidateSingleForeignKeyColumnNotExists() {
    // Mock tables exist but column doesn't
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(false);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    ForeignKeyValidator.ValidationResult result = validator.validate(fk);

    assertThat(result.isValid()).isFalse();
    assertThat(result.hasErrors()).isTrue();
    assertThat(result.getErrors())
        .contains("Foreign column 'customerId' does not exist in table 'Orders'");
  }

  @Test
  void testValidateMissingConstraintName() {
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    ForeignKeyValidator.ValidationResult result = validator.validate(fk);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getErrors()).contains("Constraint name is missing or empty");
  }

  @Test
  void testValidateMultipleForeignKeys() {
    // Mock tables and columns
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.tableExists("OrderItems")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "orderId")).thenReturn(true);
    when(mockTableValidator.columnExists("OrderItems", "orderId")).thenReturn(true);

    List<ForeignKeyMetadata> foreignKeys =
        Arrays.asList(
            ForeignKeyMetadata.builder()
                .constraintName("FK_Orders_Users")
                .foreignTable("Orders")
                .foreignColumn("customerId")
                .primaryTable("Users")
                .primaryColumn("userId")
                .build(),
            ForeignKeyMetadata.builder()
                .constraintName("FK_OrderItems_Orders")
                .foreignTable("OrderItems")
                .foreignColumn("orderId")
                .primaryTable("Orders")
                .primaryColumn("orderId")
                .build());

    List<ForeignKeyValidator.ValidationResult> results = validator.validateAll(foreignKeys);

    assertThat(results).hasSize(2);
    assertThat(results).allMatch(ForeignKeyValidator.ValidationResult::isValid);
  }

  @Test
  void testValidateDuplicateConstraintNames() {
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "sellerId")).thenReturn(true);

    List<ForeignKeyMetadata> foreignKeys =
        Arrays.asList(
            ForeignKeyMetadata.builder()
                .constraintName("FK_Orders_Users") // Same name
                .foreignTable("Orders")
                .foreignColumn("customerId")
                .primaryTable("Users")
                .primaryColumn("userId")
                .build(),
            ForeignKeyMetadata.builder()
                .constraintName("FK_Orders_Users") // Same name
                .foreignTable("Orders")
                .foreignColumn("sellerId")
                .primaryTable("Users")
                .primaryColumn("userId")
                .build());

    List<ForeignKeyValidator.ValidationResult> results = validator.validateAll(foreignKeys);

    assertThat(results).hasSize(2);
    assertThat(results.get(0).isValid()).isTrue(); // First one is valid
    assertThat(results.get(1).isValid()).isFalse(); // Second one has duplicate name
    assertThat(results.get(1).getErrors()).contains("Duplicate constraint name: 'FK_Orders_Users'");
  }

  @Test
  void testCircularReferenceDetection() {
    when(mockTableValidator.tableExists("Table1")).thenReturn(true);
    when(mockTableValidator.tableExists("Table2")).thenReturn(true);
    when(mockTableValidator.tableExists("Table3")).thenReturn(true);
    when(mockTableValidator.columnExists("Table1", "col1")).thenReturn(true);
    when(mockTableValidator.columnExists("Table2", "col2")).thenReturn(true);
    when(mockTableValidator.columnExists("Table3", "col3")).thenReturn(true);

    // Create circular reference: Table1 -> Table2 -> Table3 -> Table1
    List<ForeignKeyMetadata> foreignKeys =
        Arrays.asList(
            ForeignKeyMetadata.builder()
                .constraintName("FK1")
                .foreignTable("Table1")
                .foreignColumn("col1")
                .primaryTable("Table2")
                .primaryColumn("col2")
                .build(),
            ForeignKeyMetadata.builder()
                .constraintName("FK2")
                .foreignTable("Table2")
                .foreignColumn("col2")
                .primaryTable("Table3")
                .primaryColumn("col3")
                .build(),
            ForeignKeyMetadata.builder()
                .constraintName("FK3")
                .foreignTable("Table3")
                .foreignColumn("col3")
                .primaryTable("Table1")
                .primaryColumn("col1")
                .build());

    List<ForeignKeyValidator.ValidationResult> results = validator.validateAll(foreignKeys);

    // All foreign keys should have circular reference warning
    assertThat(results)
        .allMatch(
            result ->
                result.getErrors().contains("Foreign key is part of a circular reference chain"));
  }

  @Test
  void testValidateOrThrowValid() throws ForeignKeyValidationException {
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    // Should not throw
    validator.validateOrThrow(fk);
  }

  @Test
  void testValidateOrThrowInvalid() {
    when(mockTableValidator.tableExists("Users")).thenReturn(false);

    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .primaryTable("Users")
            .primaryColumn("userId")
            .build();

    ForeignKeyValidationException exception =
        assertThrows(ForeignKeyValidationException.class, () -> validator.validateOrThrow(fk));

    assertThat(exception.getMessage()).contains("Primary table 'Users' does not exist");
    assertThat(exception.getValidationErrors()).isNotEmpty();
  }

  @Test
  void testValidationReport() {
    when(mockTableValidator.tableExists("Users")).thenReturn(true);
    when(mockTableValidator.tableExists("Orders")).thenReturn(true);
    when(mockTableValidator.tableExists("InvalidTable")).thenReturn(false);
    when(mockTableValidator.columnExists("Users", "userId")).thenReturn(true);
    when(mockTableValidator.columnExists("Orders", "customerId")).thenReturn(true);

    List<ForeignKeyMetadata> foreignKeys =
        Arrays.asList(
            ForeignKeyMetadata.builder()
                .constraintName("FK_Valid")
                .foreignTable("Orders")
                .foreignColumn("customerId")
                .primaryTable("Users")
                .primaryColumn("userId")
                .build(),
            ForeignKeyMetadata.builder()
                .constraintName("FK_Invalid")
                .foreignTable("Orders")
                .foreignColumn("customerId")
                .primaryTable("InvalidTable")
                .primaryColumn("id")
                .build());

    List<ForeignKeyValidator.ValidationResult> results = validator.validateAll(foreignKeys);
    ForeignKeyValidator.ValidationReport report = new ForeignKeyValidator.ValidationReport(results);

    assertThat(report.getTotalCount()).isEqualTo(2);
    assertThat(report.getValidCount()).isEqualTo(1);
    assertThat(report.getInvalidCount()).isEqualTo(1);
    assertThat(report.hasErrors()).isTrue();
    assertThat(report.getValidForeignKeys()).hasSize(1);
    assertThat(report.getInvalidForeignKeys()).hasSize(1);
    assertThat(report.getAllErrors()).isNotEmpty();
  }
}
