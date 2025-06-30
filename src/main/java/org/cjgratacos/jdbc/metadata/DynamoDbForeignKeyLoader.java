package org.cjgratacos.jdbc.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Loads foreign key metadata from a DynamoDB table.
 *
 * <p>Expected table schema: - constraintName (String) - Primary key - foreignTable (String) -
 * foreignColumn (String) - primaryTable (String) - primaryColumn (String) - keySeq (Number) -
 * optional, defaults to 1 - updateRule (String) - optional,
 * CASCADE/RESTRICT/SET_NULL/SET_DEFAULT/NO_ACTION - deleteRule (String) - optional,
 * CASCADE/RESTRICT/SET_NULL/SET_DEFAULT/NO_ACTION
 *
 * <p>Example table item: { "constraintName": "FK_Orders_Users", "foreignTable": "Orders",
 * "foreignColumn": "customerId", "primaryTable": "Users", "primaryColumn": "userId", "updateRule":
 * "CASCADE", "deleteRule": "RESTRICT" }
 */
public class DynamoDbForeignKeyLoader implements ForeignKeyLoader {
  private static final Logger logger = LoggerFactory.getLogger(DynamoDbForeignKeyLoader.class);

  private final DynamoDbClient dynamoDbClient;

  public DynamoDbForeignKeyLoader(DynamoDbClient dynamoDbClient) {
    this.dynamoDbClient = dynamoDbClient;
  }

  @Override
  public List<ForeignKeyMetadata> load(String source) throws ForeignKeyLoadException {
    if (!isValidSource(source)) {
      throw new ForeignKeyLoadException("Invalid table name: " + source);
    }

    List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

    try {
      ScanRequest scanRequest = ScanRequest.builder().tableName(source).build();

      ScanResponse response;
      do {
        response = dynamoDbClient.scan(scanRequest);

        for (Map<String, AttributeValue> item : response.items()) {
          ForeignKeyMetadata fk = parseForeignKey(item);
          if (fk != null) {
            foreignKeys.add(fk);
          }
        }

        // Continue scanning if there are more results
        scanRequest =
            scanRequest.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build();

      } while (response.hasLastEvaluatedKey());

    } catch (Exception e) {
      throw new ForeignKeyLoadException(
          "Failed to load foreign keys from DynamoDB table: " + source, e);
    }

    logger.info("Loaded {} foreign keys from DynamoDB table: {}", foreignKeys.size(), source);
    return foreignKeys;
  }

  @Override
  public boolean isValidSource(String source) {
    if (source == null || source.isEmpty()) {
      return false;
    }

    try {
      // Try to describe the table to see if it exists
      dynamoDbClient.describeTable(builder -> builder.tableName(source));
      return true;
    } catch (Exception e) {
      logger.debug("Table {} does not exist or is not accessible", source);
      return false;
    }
  }

  private ForeignKeyMetadata parseForeignKey(Map<String, AttributeValue> item) {
    // Extract required fields
    String constraintName = getStringValue(item, "constraintName");
    String foreignTable = getStringValue(item, "foreignTable");
    String foreignColumn = getStringValue(item, "foreignColumn");
    String primaryTable = getStringValue(item, "primaryTable");
    String primaryColumn = getStringValue(item, "primaryColumn");

    // Validate required fields
    if (foreignTable == null
        || foreignColumn == null
        || primaryTable == null
        || primaryColumn == null) {
      logger.warn("Incomplete foreign key definition: {}", constraintName);
      return null;
    }

    ForeignKeyMetadata.Builder builder =
        ForeignKeyMetadata.builder()
            .constraintName(constraintName)
            .foreignTable(foreignTable)
            .foreignColumn(foreignColumn)
            .primaryTable(primaryTable)
            .primaryColumn(primaryColumn);

    // Extract optional fields
    Integer keySeq = getNumberValue(item, "keySeq");
    if (keySeq != null) {
      builder.keySeq(keySeq);
    }

    String updateRule = getStringValue(item, "updateRule");
    if (updateRule != null) {
      builder.updateRule(parseRule(updateRule));
    }

    String deleteRule = getStringValue(item, "deleteRule");
    if (deleteRule != null) {
      builder.deleteRule(parseRule(deleteRule));
    }

    // Extract optional catalog/schema fields
    String primaryCatalog = getStringValue(item, "primaryCatalog");
    if (primaryCatalog != null) {
      builder.primaryCatalog(primaryCatalog);
    }

    String primarySchema = getStringValue(item, "primarySchema");
    if (primarySchema != null) {
      builder.primarySchema(primarySchema);
    }

    String foreignCatalog = getStringValue(item, "foreignCatalog");
    if (foreignCatalog != null) {
      builder.foreignCatalog(foreignCatalog);
    }

    String foreignSchema = getStringValue(item, "foreignSchema");
    if (foreignSchema != null) {
      builder.foreignSchema(foreignSchema);
    }

    return builder.build();
  }

  private String getStringValue(Map<String, AttributeValue> item, String key) {
    AttributeValue value = item.get(key);
    return (value != null && value.s() != null) ? value.s() : null;
  }

  private Integer getNumberValue(Map<String, AttributeValue> item, String key) {
    AttributeValue value = item.get(key);
    if (value != null && value.n() != null) {
      try {
        return Integer.parseInt(value.n());
      } catch (NumberFormatException e) {
        logger.warn("Invalid number value for {}: {}", key, value.n());
      }
    }
    return null;
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
