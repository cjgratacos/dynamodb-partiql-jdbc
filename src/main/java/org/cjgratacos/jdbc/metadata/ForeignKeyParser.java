package org.cjgratacos.jdbc.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Parser for foreign key configurations from connection properties or JSON files. */
public class ForeignKeyParser {
  private static final Logger logger = LoggerFactory.getLogger(ForeignKeyParser.class);

  private static final String FOREIGN_KEY_PREFIX = "foreignKey.";
  private static final String FOREIGN_KEYS_FILE = "foreignKeysFile";
  private static final String FOREIGN_KEYS_TABLE = "foreignKeysTable";

  // Pattern for parsing simple foreign key syntax: table1.column1->table2.column2
  private static final Pattern SIMPLE_FK_PATTERN =
      Pattern.compile("^([^.]+)\\.([^-]+)->([^.]+)\\.([^$]+)$");

  private final DynamoDbClient dynamoDbClient;
  private final TableValidator tableValidator;
  private final ForeignKeyValidator foreignKeyValidator;
  private boolean validateOnParse = false;

  /**
   * Creates a ForeignKeyParser without validation support.
   */
  public ForeignKeyParser() {
    this.dynamoDbClient = null;
    this.tableValidator = null;
    this.foreignKeyValidator = null;
  }

  /**
   * Creates a ForeignKeyParser with validation support.
   *
   * @param dynamoDbClient the DynamoDB client for validation
   */
  public ForeignKeyParser(DynamoDbClient dynamoDbClient) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableValidator = dynamoDbClient != null ? new TableValidator(dynamoDbClient) : null;
    this.foreignKeyValidator =
        tableValidator != null ? new ForeignKeyValidator(tableValidator) : null;
  }

  /**
   * Creates a ForeignKeyParser with validation support and auto-validation option.
   *
   * @param dynamoDbClient the DynamoDB client for validation
   * @param validateOnParse whether to validate foreign keys during parsing
   */
  public ForeignKeyParser(DynamoDbClient dynamoDbClient, boolean validateOnParse) {
    this(dynamoDbClient);
    this.validateOnParse = validateOnParse;
  }

  /**
   * Sets whether to validate foreign keys during parsing.
   *
   * @param validateOnParse true to validate during parsing
   */
  public void setValidateOnParse(boolean validateOnParse) {
    this.validateOnParse = validateOnParse;
  }

  /**
   * Parses foreign keys from connection properties. Supports inline definitions, file-based, and
   * DynamoDB table-based configuration.
   *
   * @param properties the connection properties
   * @return list of parsed foreign key metadata
   */
  public List<ForeignKeyMetadata> parseFromProperties(Properties properties) {
    List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

    // 1. Check for file-based foreign keys
    String foreignKeysFile = properties.getProperty(FOREIGN_KEYS_FILE);
    if (foreignKeysFile != null && !foreignKeysFile.isEmpty()) {
      try {
        PropertiesFileForeignKeyLoader fileLoader = new PropertiesFileForeignKeyLoader();
        List<ForeignKeyMetadata> fileForeignKeys = fileLoader.load(foreignKeysFile);
        foreignKeys.addAll(fileForeignKeys);
        logger.info(
            "Loaded {} foreign keys from file: {}", fileForeignKeys.size(), foreignKeysFile);
      } catch (ForeignKeyLoadException e) {
        logger.error("Failed to load foreign keys from file: {}", foreignKeysFile, e);
      }
    }

    // 2. Check for DynamoDB table-based foreign keys
    String foreignKeysTable = properties.getProperty(FOREIGN_KEYS_TABLE);
    if (foreignKeysTable != null && !foreignKeysTable.isEmpty() && dynamoDbClient != null) {
      try {
        DynamoDbForeignKeyLoader tableLoader = new DynamoDbForeignKeyLoader(dynamoDbClient);
        List<ForeignKeyMetadata> tableForeignKeys = tableLoader.load(foreignKeysTable);
        foreignKeys.addAll(tableForeignKeys);
        logger.info(
            "Loaded {} foreign keys from DynamoDB table: {}",
            tableForeignKeys.size(),
            foreignKeysTable);
      } catch (ForeignKeyLoadException e) {
        logger.error("Failed to load foreign keys from DynamoDB table: {}", foreignKeysTable, e);
      }
    }

    // 3. Parse inline foreign key definitions
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith(FOREIGN_KEY_PREFIX)) {
        String constraintName = key.substring(FOREIGN_KEY_PREFIX.length());
        String value = properties.getProperty(key);

        ForeignKeyMetadata fk = parseSimpleForeignKey(constraintName, value);
        if (fk != null) {
          foreignKeys.add(fk);
        }
      }
    }

    // 4. Validate foreign keys if enabled
    if (validateOnParse && foreignKeyValidator != null && !foreignKeys.isEmpty()) {
      List<ForeignKeyValidator.ValidationResult> results =
          foreignKeyValidator.validateAll(foreignKeys);

      // Replace foreign keys with validated versions
      List<ForeignKeyMetadata> validatedForeignKeys = new ArrayList<>();
      for (ForeignKeyValidator.ValidationResult result : results) {
        if (result.hasErrors()) {
          logger.warn(
              "Foreign key validation failed for '{}': {}",
              result.getForeignKey().getConstraintName(),
              result.getErrors());
        }
        validatedForeignKeys.add(result.getForeignKey());
      }
      return validatedForeignKeys;
    }

    return foreignKeys;
  }

  /**
   * Parses a simple foreign key definition. Format:
   * foreignTable.foreignColumn->primaryTable.primaryColumn
   *
   * @param constraintName the constraint name
   * @param definition the foreign key definition
   * @return parsed foreign key metadata or null if invalid
   */
  private ForeignKeyMetadata parseSimpleForeignKey(String constraintName, String definition) {
    if (definition == null || definition.isEmpty()) {
      logger.warn("Empty foreign key definition for constraint: {}", constraintName);
      return null;
    }

    Matcher matcher = SIMPLE_FK_PATTERN.matcher(definition.trim());
    if (!matcher.matches()) {
      logger.warn(
          "Invalid foreign key syntax for {}: {}. Expected format: table1.column1->table2.column2",
          constraintName,
          definition);
      return null;
    }

    String foreignTable = matcher.group(1);
    String foreignColumn = matcher.group(2);
    String primaryTable = matcher.group(3);
    String primaryColumn = matcher.group(4);

    return ForeignKeyMetadata.builder()
        .constraintName(constraintName)
        .foreignTable(foreignTable)
        .foreignColumn(foreignColumn)
        .primaryTable(primaryTable)
        .primaryColumn(primaryColumn)
        .build();
  }
}
