package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.DatabaseMetaData;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ForeignKeyMetadataTest {

  private ForeignKeyParser parser;
  private ForeignKeyRegistry registry;

  @BeforeEach
  void setUp() {
    parser = new ForeignKeyParser();
    registry = new ForeignKeyRegistry();
  }

  @Test
  void testForeignKeyMetadataBuilder() {
    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .keySeq(1)
            .updateRule(DatabaseMetaData.importedKeyCascade)
            .deleteRule(DatabaseMetaData.importedKeyRestrict)
            .build();

    assertThat(fk.getConstraintName()).isEqualTo("FK_Orders_Users");
    assertThat(fk.getPrimaryTable()).isEqualTo("Users");
    assertThat(fk.getPrimaryColumn()).isEqualTo("userId");
    assertThat(fk.getForeignTable()).isEqualTo("Orders");
    assertThat(fk.getForeignColumn()).isEqualTo("customerId");
    assertThat(fk.getKeySeq()).isEqualTo(1);
    assertThat(fk.getUpdateRule()).isEqualTo(DatabaseMetaData.importedKeyCascade);
    assertThat(fk.getDeleteRule()).isEqualTo(DatabaseMetaData.importedKeyRestrict);
  }

  @Test
  void testForeignKeyMetadataRequiredFields() {
    // Missing primary table
    assertThrows(
        NullPointerException.class,
        () -> {
          ForeignKeyMetadata.builder()
              .primaryColumn("userId")
              .foreignTable("Orders")
              .foreignColumn("customerId")
              .build();
        });

    // Missing foreign table
    assertThrows(
        NullPointerException.class,
        () -> {
          ForeignKeyMetadata.builder()
              .primaryTable("Users")
              .primaryColumn("userId")
              .foreignColumn("customerId")
              .build();
        });
  }

  @Test
  void testForeignKeyParser() {
    Properties props = new Properties();
    props.setProperty("foreignKey.FK1", "Orders.customerId->Users.userId");
    props.setProperty("foreignKey.FK2", "OrderItems.orderId->Orders.orderId");

    var foreignKeys = parser.parseFromProperties(props);

    assertThat(foreignKeys).hasSize(2);

    var fk1 =
        foreignKeys.stream()
            .filter(fk -> "FK1".equals(fk.getConstraintName()))
            .findFirst()
            .orElse(null);

    assertThat(fk1).isNotNull();
    assertThat(fk1.getForeignTable()).isEqualTo("Orders");
    assertThat(fk1.getForeignColumn()).isEqualTo("customerId");
    assertThat(fk1.getPrimaryTable()).isEqualTo("Users");
    assertThat(fk1.getPrimaryColumn()).isEqualTo("userId");
  }

  @Test
  void testForeignKeyParserInvalidSyntax() {
    Properties props = new Properties();
    props.setProperty("foreignKey.FK1", "invalid-syntax");
    props.setProperty("foreignKey.FK2", "Orders->Users");
    props.setProperty("foreignKey.FK3", "");

    var foreignKeys = parser.parseFromProperties(props);

    // Invalid entries should be skipped
    assertThat(foreignKeys).isEmpty();
  }

  @Test
  void testForeignKeyRegistry() {
    ForeignKeyMetadata fk1 =
        ForeignKeyMetadata.builder()
            .constraintName("FK1")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .build();

    ForeignKeyMetadata fk2 =
        ForeignKeyMetadata.builder()
            .constraintName("FK2")
            .primaryTable("Orders")
            .primaryColumn("orderId")
            .foreignTable("OrderItems")
            .foreignColumn("orderId")
            .build();

    try {
      registry.registerForeignKey(fk1);
      registry.registerForeignKey(fk2);
    } catch (ForeignKeyValidationException e) {
      // Not expected in this test
      throw new RuntimeException(e);
    }

    // Test imported keys
    var ordersImported = registry.getImportedKeys("Orders");
    assertThat(ordersImported).hasSize(1);
    assertThat(ordersImported.get(0).getPrimaryTable()).isEqualTo("Users");

    // Test exported keys
    var usersExported = registry.getExportedKeys("Users");
    assertThat(usersExported).hasSize(1);
    assertThat(usersExported.get(0).getForeignTable()).isEqualTo("Orders");

    // Test cross reference
    var crossRef = registry.getCrossReference("Users", "Orders");
    assertThat(crossRef).hasSize(1);
    assertThat(crossRef.get(0).getConstraintName()).isEqualTo("FK1");
  }

  @Test
  void testForeignKeyRegistryNullHandling() {
    assertThat(registry.getImportedKeys(null)).isEmpty();
    assertThat(registry.getExportedKeys(null)).isEmpty();
    assertThat(registry.getCrossReference(null, "Orders")).isEmpty();
    assertThat(registry.getCrossReference("Users", null)).isEmpty();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          registry.registerForeignKey(null);
        });
  }

  @Test
  void testForeignKeyEqualsAndHashCode() {
    ForeignKeyMetadata fk1 =
        ForeignKeyMetadata.builder()
            .constraintName("FK1")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .keySeq(1)
            .build();

    ForeignKeyMetadata fk2 =
        ForeignKeyMetadata.builder()
            .constraintName("FK1")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .keySeq(1)
            .build();

    ForeignKeyMetadata fk3 =
        ForeignKeyMetadata.builder()
            .constraintName("FK2")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .keySeq(1)
            .build();

    assertThat(fk1).isEqualTo(fk2);
    assertThat(fk1.hashCode()).isEqualTo(fk2.hashCode());
    assertThat(fk1).isNotEqualTo(fk3);
  }

  @Test
  void testForeignKeyToString() {
    ForeignKeyMetadata fk =
        ForeignKeyMetadata.builder()
            .constraintName("FK_Orders_Users")
            .primaryTable("Users")
            .primaryColumn("userId")
            .foreignTable("Orders")
            .foreignColumn("customerId")
            .build();

    String str = fk.toString();
    assertThat(str).contains("FK_Orders_Users");
    assertThat(str).contains("Orders.customerId");
    assertThat(str).contains("Users.userId");
  }
}
