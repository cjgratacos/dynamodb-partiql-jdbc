package org.cjgratacos.jdbc.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.DatabaseMetaData;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PropertiesFileForeignKeyLoaderTest {

  @TempDir File tempDir;

  private PropertiesFileForeignKeyLoader loader;

  @BeforeEach
  void setUp() {
    loader = new PropertiesFileForeignKeyLoader();
  }

  @Test
  void testLoadSimpleFormat() throws Exception {
    File propsFile = new File(tempDir, "foreign-keys.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("foreignKey.FK1=Orders.customerId->Users.userId\n");
      writer.write("foreignKey.FK2=OrderItems.orderId->Orders.orderId\n");
    }

    List<ForeignKeyMetadata> foreignKeys = loader.load(propsFile.getAbsolutePath());

    assertThat(foreignKeys).hasSize(2);

    ForeignKeyMetadata fk1 =
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
  void testLoadDetailedFormat() throws Exception {
    File propsFile = new File(tempDir, "foreign-keys.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("fk.1.name=FK_Orders_Users\n");
      writer.write("fk.1.foreign.table=Orders\n");
      writer.write("fk.1.foreign.column=customerId\n");
      writer.write("fk.1.primary.table=Users\n");
      writer.write("fk.1.primary.column=userId\n");
      writer.write("fk.1.updateRule=CASCADE\n");
      writer.write("fk.1.deleteRule=RESTRICT\n");
      writer.write("fk.1.keySeq=1\n");
    }

    List<ForeignKeyMetadata> foreignKeys = loader.load(propsFile.getAbsolutePath());

    assertThat(foreignKeys).hasSize(1);

    ForeignKeyMetadata fk = foreignKeys.get(0);
    assertThat(fk.getConstraintName()).isEqualTo("FK_Orders_Users");
    assertThat(fk.getForeignTable()).isEqualTo("Orders");
    assertThat(fk.getForeignColumn()).isEqualTo("customerId");
    assertThat(fk.getPrimaryTable()).isEqualTo("Users");
    assertThat(fk.getPrimaryColumn()).isEqualTo("userId");
    assertThat(fk.getUpdateRule()).isEqualTo(DatabaseMetaData.importedKeyCascade);
    assertThat(fk.getDeleteRule()).isEqualTo(DatabaseMetaData.importedKeyRestrict);
    assertThat(fk.getKeySeq()).isEqualTo(1);
  }

  @Test
  void testLoadMixedFormats() throws Exception {
    File propsFile = new File(tempDir, "foreign-keys.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("# Simple format\n");
      writer.write("foreignKey.FK1=Orders.customerId->Users.userId\n");
      writer.write("\n");
      writer.write("# Detailed format\n");
      writer.write("fk.1.name=FK_OrderItems_Orders\n");
      writer.write("fk.1.foreign.table=OrderItems\n");
      writer.write("fk.1.foreign.column=orderId\n");
      writer.write("fk.1.primary.table=Orders\n");
      writer.write("fk.1.primary.column=orderId\n");
    }

    List<ForeignKeyMetadata> foreignKeys = loader.load(propsFile.getAbsolutePath());

    assertThat(foreignKeys).hasSize(2);
    assertThat(foreignKeys.stream().map(ForeignKeyMetadata::getConstraintName))
        .containsExactlyInAnyOrder("FK1", "FK_OrderItems_Orders");
  }

  @Test
  void testInvalidSource() {
    assertThat(loader.isValidSource(null)).isFalse();
    assertThat(loader.isValidSource("")).isFalse();
    assertThat(loader.isValidSource("/non/existent/file.properties")).isFalse();
  }

  @Test
  void testValidSource() throws IOException {
    File propsFile = new File(tempDir, "test.properties");
    Files.createFile(propsFile.toPath());

    assertThat(loader.isValidSource(propsFile.getAbsolutePath())).isTrue();
  }

  @Test
  void testLoadNonExistentFile() {
    String nonExistentFile = "/non/existent/file.properties";

    assertThrows(
        ForeignKeyLoadException.class,
        () -> {
          loader.load(nonExistentFile);
        });
  }

  @Test
  void testIncompleteForeignKeyDefinition() throws Exception {
    File propsFile = new File(tempDir, "incomplete.properties");
    try (FileWriter writer = new FileWriter(propsFile)) {
      writer.write("fk.1.name=FK_Incomplete\n");
      writer.write("fk.1.foreign.table=Orders\n");
      // Missing required fields
    }

    List<ForeignKeyMetadata> foreignKeys = loader.load(propsFile.getAbsolutePath());

    // Incomplete definitions should be skipped
    assertThat(foreignKeys).isEmpty();
  }
}
