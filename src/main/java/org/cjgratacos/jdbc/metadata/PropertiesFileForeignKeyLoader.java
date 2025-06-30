package org.cjgratacos.jdbc.metadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads foreign key metadata from a properties file.
 *
 * <p>File format: foreignKey.FK1=Orders.customerId->Users.userId
 * foreignKey.FK2=OrderItems.orderId->Orders.orderId
 *
 * <p>OR with additional properties: fk.1.name=FK_Orders_Users fk.1.foreign.table=Orders
 * fk.1.foreign.column=customerId fk.1.primary.table=Users fk.1.primary.column=userId
 * fk.1.updateRule=CASCADE fk.1.deleteRule=RESTRICT
 */
public class PropertiesFileForeignKeyLoader implements ForeignKeyLoader {
  private static final Logger logger =
      LoggerFactory.getLogger(PropertiesFileForeignKeyLoader.class);

  private final ForeignKeyParser parser = new ForeignKeyParser();

  @Override
  public List<ForeignKeyMetadata> load(String source) throws ForeignKeyLoadException {
    if (!isValidSource(source)) {
      throw new ForeignKeyLoadException("Invalid source file: " + source);
    }

    Properties props = new Properties();
    try (InputStream input = new FileInputStream(source)) {
      props.load(input);
      return loadFromProperties(props);
    } catch (IOException e) {
      throw new ForeignKeyLoadException("Failed to load foreign keys from file: " + source, e);
    }
  }

  @Override
  public boolean isValidSource(String source) {
    if (source == null || source.isEmpty()) {
      return false;
    }

    java.io.File file = new java.io.File(source);
    return file.exists() && file.isFile() && file.canRead();
  }

  private List<ForeignKeyMetadata> loadFromProperties(Properties props) {
    List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

    // First, try the simple format used by ForeignKeyParser
    List<ForeignKeyMetadata> simpleFks = parser.parseFromProperties(props);
    foreignKeys.addAll(simpleFks);

    // Then look for the detailed format (fk.N.*)
    foreignKeys.addAll(parseDetailedFormat(props));

    return foreignKeys;
  }

  private List<ForeignKeyMetadata> parseDetailedFormat(Properties props) {
    List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

    // Find all foreign key indices
    int maxIndex = 0;
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith("fk.") && key.contains(".")) {
        String[] parts = key.split("\\.");
        if (parts.length >= 2) {
          try {
            int index = Integer.parseInt(parts[1]);
            maxIndex = Math.max(maxIndex, index);
          } catch (NumberFormatException e) {
            // Skip invalid indices
          }
        }
      }
    }

    // Parse each foreign key
    for (int i = 1; i <= maxIndex; i++) {
      ForeignKeyMetadata fk = parseForeignKeyAtIndex(props, i);
      if (fk != null) {
        foreignKeys.add(fk);
      }
    }

    return foreignKeys;
  }

  private ForeignKeyMetadata parseForeignKeyAtIndex(Properties props, int index) {
    String prefix = "fk." + index + ".";

    String name = props.getProperty(prefix + "name");
    String foreignTable = props.getProperty(prefix + "foreign.table");
    String foreignColumn = props.getProperty(prefix + "foreign.column");
    String primaryTable = props.getProperty(prefix + "primary.table");
    String primaryColumn = props.getProperty(prefix + "primary.column");

    // All required fields must be present
    if (foreignTable == null
        || foreignColumn == null
        || primaryTable == null
        || primaryColumn == null) {
      logger.warn("Incomplete foreign key definition at index {}", index);
      return null;
    }

    ForeignKeyMetadata.Builder builder =
        ForeignKeyMetadata.builder()
            .constraintName(name != null ? name : "FK_" + index)
            .foreignTable(foreignTable)
            .foreignColumn(foreignColumn)
            .primaryTable(primaryTable)
            .primaryColumn(primaryColumn);

    // Optional fields
    String keySeq = props.getProperty(prefix + "keySeq");
    if (keySeq != null) {
      try {
        builder.keySeq(Integer.parseInt(keySeq));
      } catch (NumberFormatException e) {
        logger.warn("Invalid keySeq value: {}", keySeq);
      }
    }

    String updateRule = props.getProperty(prefix + "updateRule");
    if (updateRule != null) {
      builder.updateRule(parseRule(updateRule));
    }

    String deleteRule = props.getProperty(prefix + "deleteRule");
    if (deleteRule != null) {
      builder.deleteRule(parseRule(deleteRule));
    }

    return builder.build();
  }

  private int parseRule(String rule) {
    if (rule == null) {
      return java.sql.DatabaseMetaData.importedKeyNoAction;
    }

    switch (rule.toUpperCase()) {
      case "CASCADE":
        return java.sql.DatabaseMetaData.importedKeyCascade;
      case "RESTRICT":
        return java.sql.DatabaseMetaData.importedKeyRestrict;
      case "SET_NULL":
      case "SET NULL":
        return java.sql.DatabaseMetaData.importedKeySetNull;
      case "SET_DEFAULT":
      case "SET DEFAULT":
        return java.sql.DatabaseMetaData.importedKeySetDefault;
      case "NO_ACTION":
      case "NO ACTION":
      default:
        return java.sql.DatabaseMetaData.importedKeyNoAction;
    }
  }
}
