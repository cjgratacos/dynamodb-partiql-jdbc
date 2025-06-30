package org.cjgratacos.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.cjgratacos.jdbc.metadata.ForeignKeyMetadata;
import org.cjgratacos.jdbc.metadata.ForeignKeyParser;
import org.cjgratacos.jdbc.metadata.ForeignKeyRegistry;
import org.cjgratacos.jdbc.metadata.ForeignKeyValidationException;
import org.cjgratacos.jdbc.metadata.ForeignKeyValidator;
import org.cjgratacos.jdbc.metadata.TableValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * DatabaseMetaData implementation for Amazon DynamoDB with PartiQL support.
 *
 * <p>This class provides metadata about the DynamoDB database, including information about
 * supported features, data types, and database structure. Since DynamoDB is a NoSQL database, many
 * traditional SQL metadata concepts are not applicable or have limited support.
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public class DynamoDbDatabaseMetaData implements DatabaseMetaData {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbDatabaseMetaData.class);

  private final DynamoDbConnection connection;
  private final Properties connectionProperties;
  private final SchemaCache schemaCache;
  private final EnhancedSchemaDetector enhancedSchemaDetector;
  private final ForeignKeyRegistry foreignKeyRegistry;

  /**
   * Creates a new DatabaseMetaData instance for the given connection.
   *
   * @param connection the DynamoDB connection
   * @param connectionProperties the connection properties from the JDBC URL
   * @param schemaCache the schema cache for enhanced column type detection
   * @throws SQLException if foreign key validation fails in strict mode
   */
  public DynamoDbDatabaseMetaData(
      final DynamoDbConnection connection,
      final Properties connectionProperties,
      final SchemaCache schemaCache)
      throws SQLException {
    this.connection = connection;
    this.connectionProperties = connectionProperties;
    this.schemaCache = schemaCache;
    this.enhancedSchemaDetector =
        new EnhancedSchemaDetector(connection.getDynamoDbClient(), connectionProperties);

    // Initialize foreign key support with validation configuration
    boolean validateForeignKeys =
        Boolean.parseBoolean(connectionProperties.getProperty("validateForeignKeys", "false"));
    String validationMode = connectionProperties.getProperty("foreignKeyValidationMode", "lenient");
    boolean cacheTableMetadata =
        Boolean.parseBoolean(connectionProperties.getProperty("cacheTableMetadata", "true"));

    // Create validator if validation is enabled
    ForeignKeyValidator validator = null;
    if (validateForeignKeys) {
      TableValidator tableValidator =
          new TableValidator(connection.getDynamoDbClient(), cacheTableMetadata, 15);
      validator = new ForeignKeyValidator(tableValidator);
    }

    // Initialize registry with validation settings
    boolean strictMode = "strict".equalsIgnoreCase(validationMode);
    this.foreignKeyRegistry = new ForeignKeyRegistry(validateForeignKeys && strictMode, validator);

    // Parse foreign keys with validation if enabled
    ForeignKeyParser parser =
        new ForeignKeyParser(connection.getDynamoDbClient(), validateForeignKeys && !strictMode);
    List<ForeignKeyMetadata> foreignKeys = parser.parseFromProperties(connectionProperties);

    // Register foreign keys
    for (ForeignKeyMetadata fk : foreignKeys) {
      try {
        this.foreignKeyRegistry.registerForeignKey(fk);
      } catch (ForeignKeyValidationException e) {
        if (strictMode) {
          throw new SQLException("Failed to register foreign key: " + fk.getConstraintName(), e);
        } else {
          // In lenient mode, log and continue
          logger.warn(
              "Foreign key validation failed for '{}': {}", fk.getConstraintName(), e.getMessage());
        }
      }
    }
  }

  // Basic database information methods

  @Override
  public String getURL() throws SQLException {
    final StringBuilder url = new StringBuilder("jdbc:dynamodb:partiql:");

    final String region = this.connectionProperties.getProperty("region");
    if (region != null) {
      url.append("region=").append(region).append(";");
    }

    final String endpoint = this.connectionProperties.getProperty("endpoint");
    if (endpoint != null) {
      url.append("endpoint=").append(endpoint).append(";");
    }

    return url.toString();
  }

  @Override
  public String getUserName() throws SQLException {
    return ""; // DynamoDB doesn't have traditional usernames
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Amazon DynamoDB";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "1.0"; // DynamoDB API version
  }

  @Override
  public String getDriverName() throws SQLException {
    return "DynamoDB PartiQL JDBC Driver";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return "1.0.0";
  }

  @Override
  public int getDriverMajorVersion() {
    return 1;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return 1;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  // Identifier and naming methods

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\""; // DynamoDB uses double quotes for identifiers
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return "SELECT,FROM,WHERE,AND,OR,NOT,IN,BETWEEN,LIKE,IS,NULL,ORDER,BY,LIMIT,ASC,DESC";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "procedure";
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  // Case sensitivity methods

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  // Database capability methods

  @Override
  public boolean isReadOnly() throws SQLException {
    return false; // DynamoDB supports writes
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false; // DynamoDB doesn't support stored procedures
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true; // All DynamoDB tables can be queried
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false; // DynamoDB is a cloud service
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false; // DynamoDB is a cloud service
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false; // DynamoDB doesn't support transactions in traditional sense
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return true; // DynamoDB doesn't support transactions
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE; // DynamoDB doesn't support transactions
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false; // DynamoDB doesn't support traditional transactions
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
    return level == Connection.TRANSACTION_NONE;
  }

  // SQL feature support methods

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true; // PartiQL supports basic SQL
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false; // Limited SQL support
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return false; // DynamoDB doesn't support joins
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return false; // DynamoDB doesn't support GROUP BY
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true; // DynamoDB can have required attributes
  }

  // Additional SQL feature support methods

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true; // PartiQL supports column aliases
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false; // DynamoDB is schemaless
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false; // DynamoDB is schemaless
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false; // DynamoDB doesn't support stored procedures
  }

  // Maximum limits methods

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 400000; // DynamoDB item size limit is 400KB
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 400000; // DynamoDB item size limit is 400KB
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 255; // DynamoDB attribute name limit
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0; // GROUP BY not supported
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 2; // DynamoDB GSI can have at most hash + range key
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 1; // DynamoDB can only sort by sort key
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0; // No specific limit
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0; // DynamoDB is schemaless, no column limit
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0; // No specific connection limit
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0; // Cursors not supported
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 1024; // DynamoDB key size limit
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 0; // Schemas not supported
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0; // Procedures not supported
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0; // Catalogs not supported
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 400000; // DynamoDB item size limit is 400KB
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return true;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0; // No specific limit
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0; // No specific limit
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 255; // DynamoDB table name limit
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 1; // DynamoDB queries single table
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0; // No traditional users
  }

  // Null value handling methods

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return true; // DynamoDB sorts null values low
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  // Function support methods

  @Override
  public String getNumericFunctions() throws SQLException {
    return "ABS,CEIL,FLOOR,MOD,ROUND,TRUNC";
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return "CONCAT,LENGTH,LOWER,LTRIM,RTRIM,SUBSTRING,UPPER";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  // Catalog and schema support methods

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true; // Now we report tables as catalogs
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false; // Still false - DynamoDB doesn't have procedures
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true; // Now we report tables as catalogs
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true; // Now we report tables as catalogs
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false; // Still false - DynamoDB doesn't have privilege definitions
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true; // Now we report indexes as schemas
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false; // Still false - DynamoDB doesn't have procedures
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true; // Now we report indexes as schemas
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true; // Now we report indexes as schemas
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false; // Still false - DynamoDB doesn't have privilege definitions
  }

  // Additional support methods

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  // ResultSet support methods

  @Override
  public boolean supportsResultSetType(final int type) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(final int type, final int concurrency)
      throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  // Row visibility and detection methods

  @Override
  public boolean ownUpdatesAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean updatesAreDetected(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(final int type) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(final int type) throws SQLException {
    return false;
  }

  // Row ID methods

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  // Metadata retrieval methods that return ResultSet

  @Override
  public ResultSet getTables(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String[] types)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> tableRows = new ArrayList<>();

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // Use listTablesPaginator to get all tables
      final var paginator = client.listTablesPaginator();

      // Get table filter from connection properties
      final String tableFilter = this.connectionProperties.getProperty("tableFilter");

      for (final var page : paginator) {
        for (final String tableName : page.tableNames()) {
          // Apply table filter property if specified
          if (tableFilter != null && !tableFilter.isEmpty()) {
            if (!tableName.matches(tableFilter.replace("%", ".*").replace("_", "."))) {
              continue;
            }
          }

          // Apply table name pattern filter if specified
          if (tableNamePattern != null
              && !tableNamePattern.isEmpty()
              && !"%".equals(tableNamePattern)) {
            if (!tableName.matches(tableNamePattern.replace("%", ".*").replace("_", "."))) {
              continue;
            }
          }

          // Apply catalog filter if specified (catalog = table name in our mapping)
          if (catalog != null && !tableName.equals(catalog)) {
            continue;
          }

          // If schema pattern is specified, we need to check if this table has matching indexes
          boolean includeTable = true;
          if (schemaPattern != null && !schemaPattern.isEmpty() && !"%".equals(schemaPattern)) {
            includeTable = false;
            try {
              // Check if table has any matching indexes (schemas)
              final var describeResponse =
                  client.describeTable(builder -> builder.tableName(tableName));
              final var tableDesc = describeResponse.table();

              // Check PRIMARY index
              if ("PRIMARY".matches(schemaPattern.replace("%", ".*"))) {
                includeTable = true;
              }

              // Check GSIs
              if (!includeTable && tableDesc.globalSecondaryIndexes() != null) {
                for (final var gsi : tableDesc.globalSecondaryIndexes()) {
                  if (gsi.indexName().matches(schemaPattern.replace("%", ".*"))) {
                    includeTable = true;
                    break;
                  }
                }
              }

              // Check LSIs
              if (!includeTable && tableDesc.localSecondaryIndexes() != null) {
                for (final var lsi : tableDesc.localSecondaryIndexes()) {
                  if (lsi.indexName().matches(schemaPattern.replace("%", ".*"))) {
                    includeTable = true;
                    break;
                  }
                }
              }
            } catch (final Exception e) {
              // If we can't describe the table, include it by default
              includeTable = true;
            }
          }

          if (!includeTable) {
            continue;
          }

          // Create a row for each table following JDBC getTables() specification
          // Use LinkedHashMap to preserve JDBC-compliant column order
          final Map<String, AttributeValue> row = new LinkedHashMap<>();
          row.put(
              "TABLE_CAT", AttributeValue.builder().s(tableName).build()); // Table name as catalog
          row.put(
              "TABLE_SCHEM",
              AttributeValue.builder().nul(true).build()); // Could be index name if filtered
          row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
          row.put("TABLE_TYPE", AttributeValue.builder().s("TABLE").build());
          row.put("REMARKS", AttributeValue.builder().s("DynamoDB Table").build());
          row.put("TYPE_CAT", AttributeValue.builder().nul(true).build());
          row.put("TYPE_SCHEM", AttributeValue.builder().nul(true).build());
          row.put("TYPE_NAME", AttributeValue.builder().nul(true).build());
          row.put("SELF_REFERENCING_COL_NAME", AttributeValue.builder().nul(true).build());
          row.put("REF_GENERATION", AttributeValue.builder().nul(true).build());

          tableRows.add(row);
        }
      }

      return new DynamoDbResultSet(tableRows);

    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve tables: " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getColumns(
      final String catalog,
      final String schema,
      final String tableNamePattern,
      final String columnNamePattern)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> columnRows = new ArrayList<>();

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // If no table pattern specified, return empty result
      if (tableNamePattern == null || tableNamePattern.isEmpty()) {
        return new DynamoDbResultSet(columnRows);
      }

      if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
        DynamoDbDatabaseMetaData.logger.debug(
            "Getting columns for tables matching '{}', columns matching '{}'",
            tableNamePattern,
            columnNamePattern);
      }

      // Get all table names and filter by pattern
      final var paginator = client.listTablesPaginator();

      for (final var page : paginator) {
        for (final String tableName : page.tableNames()) {
          // Apply table name pattern filter
          if (!"%".equals(tableNamePattern)
              && !tableName.matches(tableNamePattern.replace("%", ".*").replace("_", "."))) {
            continue;
          }

          try {
            // Use intelligent column metadata detection with fallback logic
            final Map<String, ColumnMetadata> columnMetadata =
                this.getIntelligentColumnMetadata(tableName);

            int ordinalPosition = 1;

            for (final var entry : columnMetadata.entrySet()) {
              final String attrName = entry.getKey();
              final ColumnMetadata metadata = entry.getValue();

              // Apply column name pattern filter if specified
              if (columnNamePattern != null
                  && !columnNamePattern.isEmpty()
                  && !"%".equals(columnNamePattern)) {
                if (!attrName.matches(columnNamePattern.replace("%", ".*").replace("_", "."))) {
                  continue;
                }
              }

              final Map<String, AttributeValue> row =
                  this.createEnhancedColumnRow(metadata, ordinalPosition++);
              columnRows.add(row);
            }

          } catch (final Exception e) {
            DynamoDbDatabaseMetaData.logger.warn(
                "Enhanced column detection failed for table {}, using fallback", tableName, e);

            // Fallback to basic attribute definitions
            final var fallbackRows = this.getFallbackColumnRows(tableName, columnNamePattern);
            columnRows.addAll(fallbackRows);
          }
        }
      }

      if (DynamoDbDatabaseMetaData.logger.isInfoEnabled()) {
        DynamoDbDatabaseMetaData.logger.info(
            "Retrieved {} column definitions for pattern '{}'",
            columnRows.size(),
            tableNamePattern);
      }

      return new DynamoDbResultSet(columnRows);

    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve columns: " + e.getMessage(), e);
    }
  }

  /** Gets intelligent column metadata using enhanced detection with caching and fallback logic. */
  private Map<String, ColumnMetadata> getIntelligentColumnMetadata(final String tableName)
      throws SQLException {
    // Try enhanced schema cache first
    try {
      if (this.schemaCache != null) {
        final var cachedColumnMetadata =
            this.schemaCache.getTableColumnMetadata(tableName, this.enhancedSchemaDetector);
        if (!cachedColumnMetadata.isEmpty()) {
          if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
            DynamoDbDatabaseMetaData.logger.debug(
                "Enhanced cache found {} columns for table {}",
                cachedColumnMetadata.size(),
                tableName);
          }
          return cachedColumnMetadata;
        }
      }
    } catch (final Exception e) {
      DynamoDbDatabaseMetaData.logger.warn(
          "Enhanced schema cache failed for table {}: {}", tableName, e.getMessage());
    }

    // Fallback to direct enhanced detection
    try {
      final var columnMetadata = this.enhancedSchemaDetector.detectTableColumnMetadata(tableName);
      if (!columnMetadata.isEmpty()) {
        // Cache the results
        if (this.schemaCache != null) {
          this.schemaCache.cacheTableColumnMetadata(tableName, columnMetadata);
        }
        if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
          DynamoDbDatabaseMetaData.logger.debug(
              "Enhanced detection found {} columns for table {}", columnMetadata.size(), tableName);
        }
        return columnMetadata;
      }
    } catch (final Exception e) {
      DynamoDbDatabaseMetaData.logger.warn(
          "Enhanced schema detection failed for table {}: {}", tableName, e.getMessage());
    }

    // Fallback to basic schema cache
    try {
      if (this.schemaCache != null) {
        final var detectedSchema = this.schemaCache.getTableSchema(tableName);
        if (!detectedSchema.isEmpty()) {
          final var columnMetadata = new HashMap<String, ColumnMetadata>();
          for (final var entry : detectedSchema.entrySet()) {
            final var metadata = new ColumnMetadata(tableName, entry.getKey());
            metadata.recordTypeObservation(entry.getValue(), false);
            metadata.setDiscoverySource("Basic Schema Cache");
            columnMetadata.put(entry.getKey(), metadata);
          }
          if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
            DynamoDbDatabaseMetaData.logger.debug(
                "Basic schema cache found {} columns for table {}",
                columnMetadata.size(),
                tableName);
          }
          return columnMetadata;
        }
      }
    } catch (final Exception e) {
      DynamoDbDatabaseMetaData.logger.warn(
          "Basic schema cache lookup failed for table {}: {}", tableName, e.getMessage());
    }

    // Final fallback to key attributes
    return this.getKeyAttributeMetadata(tableName);
  }

  /** Gets column metadata from table key attributes only. */
  private Map<String, ColumnMetadata> getKeyAttributeMetadata(final String tableName)
      throws SQLException {
    final var columnMetadata = new HashMap<String, ColumnMetadata>();

    try {
      final var describeResponse =
          this.connection
              .getDynamoDbClient()
              .describeTable(builder -> builder.tableName(tableName));
      final var tableDesc = describeResponse.table();

      for (final var attr : tableDesc.attributeDefinitions()) {
        final var metadata = new ColumnMetadata(tableName, attr.attributeName());
        final var sqlType = this.mapDynamoDbTypeToSql(attr.attributeType());
        metadata.recordTypeObservation(sqlType, false);
        metadata.setDiscoverySource("Key Attributes");
        columnMetadata.put(attr.attributeName(), metadata);
      }

      if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
        DynamoDbDatabaseMetaData.logger.debug(
            "Key attributes provided {} columns for table {}", columnMetadata.size(), tableName);
      }

    } catch (final Exception e) {
      throw new SQLException("Failed to get key attributes for table " + tableName, e);
    }

    return columnMetadata;
  }

  /** Creates an enhanced column row with intelligent metadata. */
  private Map<String, AttributeValue> createEnhancedColumnRow(
      final ColumnMetadata metadata, final int ordinalPosition) {
    final Map<String, AttributeValue> row = new LinkedHashMap<>();
    row.put(
        "TABLE_CAT",
        AttributeValue.builder().s(metadata.getTableName()).build()); // Table name as catalog
    row.put(
        "TABLE_SCHEM",
        AttributeValue.builder().nul(true).build()); // Could be index context in future
    row.put("TABLE_NAME", AttributeValue.builder().s(metadata.getTableName()).build());
    row.put("COLUMN_NAME", AttributeValue.builder().s(metadata.getColumnName()).build());

    row.put(
        "DATA_TYPE",
        AttributeValue.builder().n(String.valueOf(metadata.getResolvedSqlType())).build());
    row.put("TYPE_NAME", AttributeValue.builder().s(metadata.getTypeName()).build());

    row.put(
        "COLUMN_SIZE",
        AttributeValue.builder().n(String.valueOf(metadata.getColumnSize())).build());
    row.put("BUFFER_LENGTH", AttributeValue.builder().nul(true).build());
    row.put(
        "DECIMAL_DIGITS",
        AttributeValue.builder().n(String.valueOf(metadata.getDecimalDigits())).build());
    row.put("NUM_PREC_RADIX", AttributeValue.builder().n("10").build());

    // Use intelligent nullable detection
    final var nullableCode = metadata.isNullable() ? "1" : "0";
    final var nullableText = metadata.isNullable() ? "YES" : "NO";
    row.put("NULLABLE", AttributeValue.builder().n(nullableCode).build());
    row.put("IS_NULLABLE", AttributeValue.builder().s(nullableText).build());

    // DynamoDB-aware remarks with detection information
    var remarks =
        String.format(
            "DynamoDB %s (confidence: %.2f%%, observations: %d)",
            metadata.getDiscoverySource(),
            metadata.getTypeConfidence() * 100,
            metadata.getTotalObservations());
    if (metadata.hasTypeConflict()) {
      remarks += " [NoSQL type flexibility - conflict resolved]";
    }
    remarks += " - Schema inferred from sampled items";
    row.put("REMARKS", AttributeValue.builder().s(remarks).build());

    row.put("COLUMN_DEF", AttributeValue.builder().nul(true).build());
    row.put(
        "SQL_DATA_TYPE",
        AttributeValue.builder().n(String.valueOf(metadata.getResolvedSqlType())).build());
    row.put("SQL_DATETIME_SUB", AttributeValue.builder().nul(true).build());
    row.put(
        "CHAR_OCTET_LENGTH",
        AttributeValue.builder().n(String.valueOf(metadata.getColumnSize())).build());
    row.put(
        "ORDINAL_POSITION", AttributeValue.builder().n(String.valueOf(ordinalPosition)).build());
    row.put("SCOPE_CATALOG", AttributeValue.builder().nul(true).build());
    row.put("SCOPE_SCHEMA", AttributeValue.builder().nul(true).build());
    row.put("SCOPE_TABLE", AttributeValue.builder().nul(true).build());
    row.put("SOURCE_DATA_TYPE", AttributeValue.builder().nul(true).build());
    row.put("IS_AUTOINCREMENT", AttributeValue.builder().s("NO").build());
    row.put("IS_GENERATEDCOLUMN", AttributeValue.builder().s("NO").build());

    return row;
  }

  /** Gets fallback column rows using basic table description. */
  private List<Map<String, AttributeValue>> getFallbackColumnRows(
      final String tableName, final String columnNamePattern) {
    final var columnRows = new ArrayList<Map<String, AttributeValue>>();

    try {
      final var describeResponse =
          this.connection
              .getDynamoDbClient()
              .describeTable(builder -> builder.tableName(tableName));
      final var tableDesc = describeResponse.table();

      int ordinalPosition = 1;

      for (final var attr : tableDesc.attributeDefinitions()) {
        final String attrName = attr.attributeName();

        // Apply column name pattern filter if specified
        if (columnNamePattern != null
            && !columnNamePattern.isEmpty()
            && !"%".equals(columnNamePattern)) {
          if (!attrName.matches(columnNamePattern.replace("%", ".*").replace("_", "."))) {
            continue;
          }
        }

        final int sqlType = this.mapDynamoDbTypeToSql(attr.attributeType());
        final Map<String, AttributeValue> row =
            this.createColumnRow(
                tableName, attrName, sqlType, ordinalPosition++, "Fallback - Key Attribute");
        columnRows.add(row);
      }

    } catch (final Exception e) {
      DynamoDbDatabaseMetaData.logger.error(
          "Fallback column detection failed for table {}", tableName, e);
    }

    return columnRows;
  }

  private Map<String, AttributeValue> createColumnRow(
      final String tableName,
      final String columnName,
      final int sqlType,
      final int ordinalPosition,
      final String remarks) {
    final Map<String, AttributeValue> row = new LinkedHashMap<>();
    row.put("TABLE_CAT", AttributeValue.builder().s(tableName).build()); // Table name as catalog
    row.put(
        "TABLE_SCHEM",
        AttributeValue.builder().nul(true).build()); // Could be index context in future
    row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
    row.put("COLUMN_NAME", AttributeValue.builder().s(columnName).build());

    row.put("DATA_TYPE", AttributeValue.builder().n(String.valueOf(sqlType)).build());
    row.put("TYPE_NAME", AttributeValue.builder().s(this.getSqlTypeName(sqlType)).build());

    row.put(
        "COLUMN_SIZE",
        AttributeValue.builder().n("0").build()); // DynamoDB doesn't have fixed sizes
    row.put("BUFFER_LENGTH", AttributeValue.builder().nul(true).build());
    row.put("DECIMAL_DIGITS", AttributeValue.builder().n("0").build());
    row.put("NUM_PREC_RADIX", AttributeValue.builder().n("10").build());
    row.put("NULLABLE", AttributeValue.builder().n("1").build()); // Nullable
    row.put("REMARKS", AttributeValue.builder().s(remarks).build());
    row.put("COLUMN_DEF", AttributeValue.builder().nul(true).build());
    row.put("SQL_DATA_TYPE", AttributeValue.builder().n(String.valueOf(sqlType)).build());
    row.put("SQL_DATETIME_SUB", AttributeValue.builder().nul(true).build());
    row.put("CHAR_OCTET_LENGTH", AttributeValue.builder().n("0").build());
    row.put(
        "ORDINAL_POSITION", AttributeValue.builder().n(String.valueOf(ordinalPosition)).build());
    row.put("IS_NULLABLE", AttributeValue.builder().s("YES").build());
    row.put("SCOPE_CATALOG", AttributeValue.builder().nul(true).build());
    row.put("SCOPE_SCHEMA", AttributeValue.builder().nul(true).build());
    row.put("SCOPE_TABLE", AttributeValue.builder().nul(true).build());
    row.put("SOURCE_DATA_TYPE", AttributeValue.builder().nul(true).build());
    row.put("IS_AUTOINCREMENT", AttributeValue.builder().s("NO").build());
    row.put("IS_GENERATEDCOLUMN", AttributeValue.builder().s("NO").build());

    return row;
  }

  private String getSqlTypeName(final int sqlType) {
    return switch (sqlType) {
      case java.sql.Types.VARCHAR -> "VARCHAR";
      case java.sql.Types.NUMERIC -> "NUMERIC";
      case java.sql.Types.BINARY -> "BINARY";
      case java.sql.Types.BOOLEAN -> "BOOLEAN";
      case java.sql.Types.ARRAY -> "ARRAY";
      case java.sql.Types.STRUCT -> "STRUCT";
      case java.sql.Types.NULL -> "NULL";
      default -> "OTHER";
    };
  }

  private int mapDynamoDbTypeToSql(
      final software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType type) {
    return switch (type) {
      case S -> java.sql.Types.VARCHAR;
      case N -> java.sql.Types.NUMERIC;
      case B -> java.sql.Types.BINARY;
      default -> java.sql.Types.OTHER;
    };
  }

  /**
   * Converts DynamoDB scalar attribute types to human-readable type names.
   *
   * <p>This helper method maps DynamoDB's internal attribute type representations to standardized
   * type names suitable for display in database tools and metadata exports.
   *
   * @param type the DynamoDB scalar attribute type (S, N, B)
   * @return readable type name: "String" for S, "Number" for N, "Binary" for B, "Unknown" for
   *     others
   */
  private String getDynamoDbTypeName(
      final software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType type) {
    return switch (type) {
      case S -> "String";
      case N -> "Number";
      case B -> "Binary";
      default -> "Unknown";
    };
  }

  /**
   * Retrieves a description of the given table's primary key columns.
   *
   * <p>For DynamoDB tables, this method returns information about the table's partition key and
   * sort key (if present). Each key is returned as a separate row in the ResultSet.
   *
   * <p><strong>Enhanced with TYPE_NAME field:</strong> This implementation includes an additional
   * TYPE_NAME field showing the DynamoDB attribute type for better tool integration.
   *
   * <p><strong>Column Schema:</strong>
   *
   * <ul>
   *   <li><strong>TABLE_CAT</strong> - Always null (DynamoDB has no catalogs)
   *   <li><strong>TABLE_SCHEM</strong> - Always null (DynamoDB has no schemas)
   *   <li><strong>TABLE_NAME</strong> - The DynamoDB table name
   *   <li><strong>COLUMN_NAME</strong> - The attribute name of the primary key
   *   <li><strong>KEY_SEQ</strong> - Sequence number within the primary key (1 for partition key, 2
   *       for sort key)
   *   <li><strong>PK_NAME</strong> - Primary key name: "PK_PARTITION" for partition key, "PK_SORT"
   *       for sort key
   *   <li><strong>TYPE_NAME</strong> - <em>(Enhanced field)</em> DynamoDB attribute type: "String",
   *       "Number", "Binary", or "Unknown"
   * </ul>
   *
   * <p><strong>Example Usage:</strong>
   *
   * <pre>{@code
   * DatabaseMetaData meta = connection.getMetaData();
   * ResultSet keys = meta.getPrimaryKeys(null, null, "MyTable");
   * while (keys.next()) {
   *     System.out.println("Key: " + keys.getString("COLUMN_NAME"));
   *     System.out.println("Type: " + keys.getString("TYPE_NAME")); // "String", "Number", etc.
   *     System.out.println("Sequence: " + keys.getInt("KEY_SEQ"));
   *     System.out.println("Role: " + keys.getString("PK_NAME")); // "PK_PARTITION" or "PK_SORT"
   * }
   * }</pre>
   *
   * @param catalog ignored (DynamoDB has no catalog concept)
   * @param schema ignored (DynamoDB has no schema concept)
   * @param table the table name; if null or empty, returns empty ResultSet
   * @return ResultSet containing primary key information with enhanced type metadata
   * @throws SQLException if there's an error retrieving primary key information
   */
  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> keyRows = new ArrayList<>();

      if (table == null || table.isEmpty()) {
        return new DynamoDbResultSet(keyRows);
      }

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // Describe the table to get key schema
      final var describeResponse = client.describeTable(builder -> builder.tableName(table));
      final var tableDesc = describeResponse.table();

      short keySeq = 1;

      // Process key schema to get primary keys
      for (final var keyElement : tableDesc.keySchema()) {
        final Map<String, AttributeValue> row = new LinkedHashMap<>();
        row.put("TABLE_CAT", AttributeValue.builder().nul(true).build());
        row.put("TABLE_SCHEM", AttributeValue.builder().nul(true).build());
        row.put("TABLE_NAME", AttributeValue.builder().s(table).build());
        row.put("COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());
        row.put("KEY_SEQ", AttributeValue.builder().n(String.valueOf(keySeq++)).build());

        // Generate a primary key name based on key type
        final String pkName =
            "HASH".equals(keyElement.keyType().toString()) ? "PK_PARTITION" : "PK_SORT";
        row.put("PK_NAME", AttributeValue.builder().s(pkName).build());

        // Add TYPE_NAME field - get the attribute type from table description
        String typeName = "String"; // Default type
        for (final var attr : tableDesc.attributeDefinitions()) {
          if (attr.attributeName().equals(keyElement.attributeName())) {
            typeName = this.getDynamoDbTypeName(attr.attributeType());
            break;
          }
        }
        row.put("TYPE_NAME", AttributeValue.builder().s(typeName).build());

        keyRows.add(row);
      }

      return new DynamoDbResultSet(keyRows);

    } catch (final Exception e) {
      throw new SQLException(
          "Failed to retrieve primary keys for table " + table + ": " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    // Get logical foreign keys for the specified table
    List<ForeignKeyMetadata> importedKeys = foreignKeyRegistry.getImportedKeys(table);
    List<Map<String, AttributeValue>> rows = new ArrayList<>();

    for (ForeignKeyMetadata fk : importedKeys) {
      Map<String, AttributeValue> row = new LinkedHashMap<>();

      // Primary (referenced) table information
      row.put(
          "PKTABLE_CAT",
          fk.getPrimaryCatalog() != null
              ? AttributeValue.builder().s(fk.getPrimaryCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PKTABLE_SCHEM",
          fk.getPrimarySchema() != null
              ? AttributeValue.builder().s(fk.getPrimarySchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("PKTABLE_NAME", AttributeValue.builder().s(fk.getPrimaryTable()).build());
      row.put("PKCOLUMN_NAME", AttributeValue.builder().s(fk.getPrimaryColumn()).build());

      // Foreign (referencing) table information
      row.put(
          "FKTABLE_CAT",
          fk.getForeignCatalog() != null
              ? AttributeValue.builder().s(fk.getForeignCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "FKTABLE_SCHEM",
          fk.getForeignSchema() != null
              ? AttributeValue.builder().s(fk.getForeignSchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("FKTABLE_NAME", AttributeValue.builder().s(fk.getForeignTable()).build());
      row.put("FKCOLUMN_NAME", AttributeValue.builder().s(fk.getForeignColumn()).build());

      // Key sequence and rules
      row.put("KEY_SEQ", AttributeValue.builder().n(String.valueOf(fk.getKeySeq())).build());
      row.put(
          "UPDATE_RULE", AttributeValue.builder().n(String.valueOf(fk.getUpdateRule())).build());
      row.put(
          "DELETE_RULE", AttributeValue.builder().n(String.valueOf(fk.getDeleteRule())).build());

      // Constraint names
      row.put(
          "FK_NAME",
          fk.getConstraintName() != null
              ? AttributeValue.builder().s(fk.getConstraintName()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PK_NAME", AttributeValue.builder().nul(true).build()); // DynamoDB doesn't have named PKs

      // Deferrability
      row.put(
          "DEFERRABILITY",
          AttributeValue.builder().n(String.valueOf(fk.getDeferrability())).build());

      rows.add(row);
    }

    return new DynamoDbResultSet(rows);
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    // Get logical foreign keys exported by the specified table
    List<ForeignKeyMetadata> exportedKeys = foreignKeyRegistry.getExportedKeys(table);
    List<Map<String, AttributeValue>> rows = new ArrayList<>();

    for (ForeignKeyMetadata fk : exportedKeys) {
      Map<String, AttributeValue> row = new LinkedHashMap<>();

      // Primary (referenced) table information - this is the table parameter
      row.put(
          "PKTABLE_CAT",
          fk.getPrimaryCatalog() != null
              ? AttributeValue.builder().s(fk.getPrimaryCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PKTABLE_SCHEM",
          fk.getPrimarySchema() != null
              ? AttributeValue.builder().s(fk.getPrimarySchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("PKTABLE_NAME", AttributeValue.builder().s(fk.getPrimaryTable()).build());
      row.put("PKCOLUMN_NAME", AttributeValue.builder().s(fk.getPrimaryColumn()).build());

      // Foreign (referencing) table information
      row.put(
          "FKTABLE_CAT",
          fk.getForeignCatalog() != null
              ? AttributeValue.builder().s(fk.getForeignCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "FKTABLE_SCHEM",
          fk.getForeignSchema() != null
              ? AttributeValue.builder().s(fk.getForeignSchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("FKTABLE_NAME", AttributeValue.builder().s(fk.getForeignTable()).build());
      row.put("FKCOLUMN_NAME", AttributeValue.builder().s(fk.getForeignColumn()).build());

      // Key sequence and rules
      row.put("KEY_SEQ", AttributeValue.builder().n(String.valueOf(fk.getKeySeq())).build());
      row.put(
          "UPDATE_RULE", AttributeValue.builder().n(String.valueOf(fk.getUpdateRule())).build());
      row.put(
          "DELETE_RULE", AttributeValue.builder().n(String.valueOf(fk.getDeleteRule())).build());

      // Constraint names
      row.put(
          "FK_NAME",
          fk.getConstraintName() != null
              ? AttributeValue.builder().s(fk.getConstraintName()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PK_NAME", AttributeValue.builder().nul(true).build()); // DynamoDB doesn't have named PKs

      // Deferrability
      row.put(
          "DEFERRABILITY",
          AttributeValue.builder().n(String.valueOf(fk.getDeferrability())).build());

      rows.add(row);
    }

    return new DynamoDbResultSet(rows);
  }

  @Override
  public ResultSet getCrossReference(
      final String parentCatalog,
      final String parentSchema,
      final String parentTable,
      final String foreignCatalog,
      final String foreignSchema,
      final String foreignTable)
      throws SQLException {
    // Get logical foreign keys between the two specified tables
    List<ForeignKeyMetadata> crossRefs =
        foreignKeyRegistry.getCrossReference(parentTable, foreignTable);
    List<Map<String, AttributeValue>> rows = new ArrayList<>();

    for (ForeignKeyMetadata fk : crossRefs) {
      Map<String, AttributeValue> row = new LinkedHashMap<>();

      // Primary (parent) table information
      row.put(
          "PKTABLE_CAT",
          fk.getPrimaryCatalog() != null
              ? AttributeValue.builder().s(fk.getPrimaryCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PKTABLE_SCHEM",
          fk.getPrimarySchema() != null
              ? AttributeValue.builder().s(fk.getPrimarySchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("PKTABLE_NAME", AttributeValue.builder().s(fk.getPrimaryTable()).build());
      row.put("PKCOLUMN_NAME", AttributeValue.builder().s(fk.getPrimaryColumn()).build());

      // Foreign table information
      row.put(
          "FKTABLE_CAT",
          fk.getForeignCatalog() != null
              ? AttributeValue.builder().s(fk.getForeignCatalog()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "FKTABLE_SCHEM",
          fk.getForeignSchema() != null
              ? AttributeValue.builder().s(fk.getForeignSchema()).build()
              : AttributeValue.builder().nul(true).build());
      row.put("FKTABLE_NAME", AttributeValue.builder().s(fk.getForeignTable()).build());
      row.put("FKCOLUMN_NAME", AttributeValue.builder().s(fk.getForeignColumn()).build());

      // Key sequence and rules
      row.put("KEY_SEQ", AttributeValue.builder().n(String.valueOf(fk.getKeySeq())).build());
      row.put(
          "UPDATE_RULE", AttributeValue.builder().n(String.valueOf(fk.getUpdateRule())).build());
      row.put(
          "DELETE_RULE", AttributeValue.builder().n(String.valueOf(fk.getDeleteRule())).build());

      // Constraint names
      row.put(
          "FK_NAME",
          fk.getConstraintName() != null
              ? AttributeValue.builder().s(fk.getConstraintName()).build()
              : AttributeValue.builder().nul(true).build());
      row.put(
          "PK_NAME", AttributeValue.builder().nul(true).build()); // DynamoDB doesn't have named PKs

      // Deferrability
      row.put(
          "DEFERRABILITY",
          AttributeValue.builder().n(String.valueOf(fk.getDeferrability())).build());

      rows.add(row);
    }

    return new DynamoDbResultSet(rows);
  }

  /**
   * Retrieves a description of all the data types supported by DynamoDB.
   *
   * <p>DynamoDB supports three fundamental data types that map to SQL types:
   *
   * <ul>
   *   <li><strong>String</strong> - Maps to VARCHAR, used for text data
   *   <li><strong>Number</strong> - Maps to NUMERIC, used for all numeric values
   *   <li><strong>Binary</strong> - Maps to BINARY, used for binary data (e.g., images, compressed
   *       data)
   * </ul>
   *
   * <p><strong>Column Schema:</strong>
   *
   * <ul>
   *   <li><strong>TYPE_NAME</strong> - DynamoDB type name: "String", "Number", or "Binary"
   *   <li><strong>DATA_TYPE</strong> - SQL type from java.sql.Types
   *   <li><strong>PRECISION</strong> - Always 0 (DynamoDB has no fixed precision)
   *   <li><strong>LITERAL_PREFIX/SUFFIX</strong> - Quote characters for String type
   *   <li><strong>NULLABLE</strong> - Always typeNullable (all DynamoDB attributes can be null)
   *   <li><strong>CASE_SENSITIVE</strong> - true for String/Binary, false for Number
   *   <li><strong>SEARCHABLE</strong> - typeSearchable for String/Number, typePredNone for Binary
   *   <li><strong>UNSIGNED_ATTRIBUTE</strong> - Always false
   *   <li><strong>FIXED_PREC_SCALE</strong> - Always false (no fixed precision)
   *   <li><strong>AUTO_INCREMENT</strong> - Always false (DynamoDB doesn't support auto-increment)
   * </ul>
   *
   * <p><strong>Example Usage:</strong>
   *
   * <pre>{@code
   * DatabaseMetaData meta = connection.getMetaData();
   * ResultSet types = meta.getTypeInfo();
   * while (types.next()) {
   *     System.out.println("Type: " + types.getString("TYPE_NAME"));
   *     System.out.println("SQL Type: " + types.getInt("DATA_TYPE"));
   *     System.out.println("Searchable: " + types.getInt("SEARCHABLE"));
   * }
   * }</pre>
   *
   * @return ResultSet containing supported type information
   * @throws SQLException if there's an error retrieving type information
   */
  @Override
  public ResultSet getTypeInfo() throws SQLException {
    try {
      final List<Map<String, AttributeValue>> typeRows = new ArrayList<>();

      // DynamoDB supports three data types: String (S), Number (N), and Binary (B)
      // Add String type
      Map<String, AttributeValue> stringRow = new LinkedHashMap<>();
      stringRow.put("TYPE_NAME", AttributeValue.builder().s("String").build());
      stringRow.put(
          "DATA_TYPE", AttributeValue.builder().n(String.valueOf(java.sql.Types.VARCHAR)).build());
      stringRow.put(
          "PRECISION", AttributeValue.builder().n("0").build()); // No fixed precision in DynamoDB
      stringRow.put("LITERAL_PREFIX", AttributeValue.builder().s("'").build());
      stringRow.put("LITERAL_SUFFIX", AttributeValue.builder().s("'").build());
      stringRow.put("CREATE_PARAMS", AttributeValue.builder().nul(true).build());
      stringRow.put(
          "NULLABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typeNullable)).build());
      stringRow.put("CASE_SENSITIVE", AttributeValue.builder().bool(true).build());
      stringRow.put(
          "SEARCHABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typeSearchable)).build());
      stringRow.put("UNSIGNED_ATTRIBUTE", AttributeValue.builder().bool(false).build());
      stringRow.put("FIXED_PREC_SCALE", AttributeValue.builder().bool(false).build());
      stringRow.put("AUTO_INCREMENT", AttributeValue.builder().bool(false).build());
      stringRow.put("LOCAL_TYPE_NAME", AttributeValue.builder().s("String").build());
      stringRow.put("MINIMUM_SCALE", AttributeValue.builder().n("0").build());
      stringRow.put("MAXIMUM_SCALE", AttributeValue.builder().n("0").build());
      stringRow.put(
          "SQL_DATA_TYPE",
          AttributeValue.builder().n(String.valueOf(java.sql.Types.VARCHAR)).build());
      stringRow.put("SQL_DATETIME_SUB", AttributeValue.builder().nul(true).build());
      stringRow.put("NUM_PREC_RADIX", AttributeValue.builder().n("10").build());
      typeRows.add(stringRow);

      // Add Number type
      Map<String, AttributeValue> numberRow = new LinkedHashMap<>();
      numberRow.put("TYPE_NAME", AttributeValue.builder().s("Number").build());
      numberRow.put(
          "DATA_TYPE", AttributeValue.builder().n(String.valueOf(java.sql.Types.NUMERIC)).build());
      numberRow.put(
          "PRECISION", AttributeValue.builder().n("0").build()); // No fixed precision in DynamoDB
      numberRow.put("LITERAL_PREFIX", AttributeValue.builder().nul(true).build());
      numberRow.put("LITERAL_SUFFIX", AttributeValue.builder().nul(true).build());
      numberRow.put("CREATE_PARAMS", AttributeValue.builder().nul(true).build());
      numberRow.put(
          "NULLABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typeNullable)).build());
      numberRow.put("CASE_SENSITIVE", AttributeValue.builder().bool(false).build());
      numberRow.put(
          "SEARCHABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typeSearchable)).build());
      numberRow.put("UNSIGNED_ATTRIBUTE", AttributeValue.builder().bool(false).build());
      numberRow.put("FIXED_PREC_SCALE", AttributeValue.builder().bool(false).build());
      numberRow.put("AUTO_INCREMENT", AttributeValue.builder().bool(false).build());
      numberRow.put("LOCAL_TYPE_NAME", AttributeValue.builder().s("Number").build());
      numberRow.put("MINIMUM_SCALE", AttributeValue.builder().n("0").build());
      numberRow.put("MAXIMUM_SCALE", AttributeValue.builder().n("0").build());
      numberRow.put(
          "SQL_DATA_TYPE",
          AttributeValue.builder().n(String.valueOf(java.sql.Types.NUMERIC)).build());
      numberRow.put("SQL_DATETIME_SUB", AttributeValue.builder().nul(true).build());
      numberRow.put("NUM_PREC_RADIX", AttributeValue.builder().n("10").build());
      typeRows.add(numberRow);

      // Add Binary type
      Map<String, AttributeValue> binaryRow = new LinkedHashMap<>();
      binaryRow.put("TYPE_NAME", AttributeValue.builder().s("Binary").build());
      binaryRow.put(
          "DATA_TYPE", AttributeValue.builder().n(String.valueOf(java.sql.Types.BINARY)).build());
      binaryRow.put(
          "PRECISION", AttributeValue.builder().n("0").build()); // No fixed precision in DynamoDB
      binaryRow.put("LITERAL_PREFIX", AttributeValue.builder().nul(true).build());
      binaryRow.put("LITERAL_SUFFIX", AttributeValue.builder().nul(true).build());
      binaryRow.put("CREATE_PARAMS", AttributeValue.builder().nul(true).build());
      binaryRow.put(
          "NULLABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typeNullable)).build());
      binaryRow.put("CASE_SENSITIVE", AttributeValue.builder().bool(true).build());
      binaryRow.put(
          "SEARCHABLE",
          AttributeValue.builder().n(String.valueOf(DatabaseMetaData.typePredNone)).build());
      binaryRow.put("UNSIGNED_ATTRIBUTE", AttributeValue.builder().bool(false).build());
      binaryRow.put("FIXED_PREC_SCALE", AttributeValue.builder().bool(false).build());
      binaryRow.put("AUTO_INCREMENT", AttributeValue.builder().bool(false).build());
      binaryRow.put("LOCAL_TYPE_NAME", AttributeValue.builder().s("Binary").build());
      binaryRow.put("MINIMUM_SCALE", AttributeValue.builder().n("0").build());
      binaryRow.put("MAXIMUM_SCALE", AttributeValue.builder().n("0").build());
      binaryRow.put(
          "SQL_DATA_TYPE",
          AttributeValue.builder().n(String.valueOf(java.sql.Types.BINARY)).build());
      binaryRow.put("SQL_DATETIME_SUB", AttributeValue.builder().nul(true).build());
      binaryRow.put("NUM_PREC_RADIX", AttributeValue.builder().n("10").build());
      typeRows.add(binaryRow);

      return new DynamoDbResultSet(typeRows);
    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve type info: " + e.getMessage(), e);
    }
  }

  /**
   * Retrieves a description of the given table's indices and statistics.
   *
   * <p>This method returns information about DynamoDB table indexes including:
   *
   * <ul>
   *   <li><strong>Primary Index</strong> - Main table partition and sort keys
   *   <li><strong>Global Secondary Indexes (GSI)</strong> - Independent indexes with their own
   *       partition/sort keys
   *   <li><strong>Local Secondary Indexes (LSI)</strong> - Alternate sort keys sharing the table's
   *       partition key
   * </ul>
   *
   * <p>Each index is represented as a single row showing both partition key and sort key (when
   * present) in the COLUMN_NAME field using the format: "keyName (keyType), sortKey (RANGE)". The
   * key types are displayed as HASH for partition keys and RANGE for sort keys.
   *
   * <p><strong>Column Schema:</strong>
   *
   * <ul>
   *   <li><strong>TABLE_CAT</strong> - Always null (DynamoDB has no catalogs)
   *   <li><strong>TABLE_SCHEM</strong> - Always null (DynamoDB has no schemas)
   *   <li><strong>TABLE_NAME</strong> - The DynamoDB table name
   *   <li><strong>NON_UNIQUE</strong> - false for PRIMARY, true for GSI/LSI
   *   <li><strong>INDEX_QUALIFIER</strong> - Always null
   *   <li><strong>INDEX_NAME</strong> - "PRIMARY", GSI name, or LSI name
   *   <li><strong>TYPE</strong> - Always tableIndexOther (3)
   *   <li><strong>ORDINAL_POSITION</strong> - Sequential position starting from 1
   *   <li><strong>COLUMN_NAME</strong> - Formatted key schema showing all keys
   *   <li><strong>ASC_OR_DESC</strong> - Always "A" (ascending)
   *   <li><strong>CARDINALITY</strong> - Always 0 (unknown)
   *   <li><strong>PAGES</strong> - Always 0 (unknown)
   *   <li><strong>FILTER_CONDITION</strong> - Always null
   * </ul>
   *
   * <p><strong>Error Handling:</strong> If the specified table does not exist or is not accessible,
   * an empty ResultSet is returned instead of throwing an exception, providing graceful
   * degradation.
   *
   * @param catalog ignored (DynamoDB has no catalog concept)
   * @param schema ignored (DynamoDB has no schema concept)
   * @param table the table name; if null or empty, returns empty ResultSet
   * @param unique ignored (all indexes are returned regardless)
   * @param approximate ignored (statistics are not available)
   * @return ResultSet containing index information, empty if table not found
   * @throws SQLException if there's an error retrieving index information
   */
  @Override
  public ResultSet getIndexInfo(
      final String catalog,
      final String schema,
      final String table,
      final boolean unique,
      final boolean approximate)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> indexRows = new ArrayList<>();

      if (table == null || table.isEmpty()) {
        return new DynamoDbResultSet(indexRows);
      }

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // Describe the table to get index information
      try {
        final var describeResponse = client.describeTable(builder -> builder.tableName(table));
        final var tableDesc = describeResponse.table();

        // Add primary table index as a single row
        if (!tableDesc.keySchema().isEmpty()) {
          final Map<String, AttributeValue> row = new LinkedHashMap<>();
          row.put("TABLE_CAT", AttributeValue.builder().s(table).build()); // Table name as catalog
          row.put(
              "TABLE_SCHEM", AttributeValue.builder().s("PRIMARY").build()); // PRIMARY as schema
          row.put("TABLE_NAME", AttributeValue.builder().s(table).build());
          row.put(
              "NON_UNIQUE", AttributeValue.builder().bool(false).build()); // Primary key is unique
          row.put("INDEX_QUALIFIER", AttributeValue.builder().nul(true).build());
          row.put("INDEX_NAME", AttributeValue.builder().s("PRIMARY").build());
          row.put(
              "TYPE",
              AttributeValue.builder().n(String.valueOf(DatabaseMetaData.tableIndexOther)).build());
          row.put("ORDINAL_POSITION", AttributeValue.builder().n("1").build()); // Primary index
          // For getIndexInfo, show combined key schema in COLUMN_NAME
          final String keySchema =
              tableDesc.keySchema().stream()
                  .map(key -> key.attributeName() + " (" + key.keyType().toString() + ")")
                  .collect(java.util.stream.Collectors.joining(", "));
          row.put("COLUMN_NAME", AttributeValue.builder().s(keySchema).build());
          // Add KEY_NAME with just the first key name for DbVisualizer
          String firstKeyName =
              tableDesc.keySchema().isEmpty() ? "" : tableDesc.keySchema().get(0).attributeName();
          row.put("KEY_NAME", AttributeValue.builder().s(firstKeyName).build());
          row.put("ASC_OR_DESC", AttributeValue.builder().s("A").build());
          row.put("CARDINALITY", AttributeValue.builder().n("0").build()); // Unknown cardinality
          row.put("PAGES", AttributeValue.builder().n("0").build()); // Unknown pages
          row.put("FILTER_CONDITION", AttributeValue.builder().nul(true).build());

          indexRows.add(row);
        }

        // Add Global Secondary Indexes (GSI) - one row per index
        if (tableDesc.globalSecondaryIndexes() != null) {
          for (final var gsi : tableDesc.globalSecondaryIndexes()) {
            if (!gsi.keySchema().isEmpty()) {
              final Map<String, AttributeValue> row = new LinkedHashMap<>();
              row.put(
                  "TABLE_CAT", AttributeValue.builder().s(table).build()); // Table name as catalog
              row.put(
                  "TABLE_SCHEM",
                  AttributeValue.builder().s(gsi.indexName()).build()); // GSI name as schema
              row.put("TABLE_NAME", AttributeValue.builder().s(table).build());
              row.put(
                  "NON_UNIQUE",
                  AttributeValue.builder().bool(true).build()); // GSI allows duplicates
              row.put("INDEX_QUALIFIER", AttributeValue.builder().nul(true).build());
              row.put("INDEX_NAME", AttributeValue.builder().s(gsi.indexName()).build());
              row.put(
                  "TYPE",
                  AttributeValue.builder()
                      .n(String.valueOf(DatabaseMetaData.tableIndexOther))
                      .build());
              row.put("ORDINAL_POSITION", AttributeValue.builder().n("1").build()); // Index ordinal
              // Show combined key schema
              final String keySchema =
                  gsi.keySchema().stream()
                      .map(key -> key.attributeName() + " (" + key.keyType().toString() + ")")
                      .collect(java.util.stream.Collectors.joining(", "));
              row.put("COLUMN_NAME", AttributeValue.builder().s(keySchema).build());
              // Add KEY_NAME with just the first key name for DbVisualizer
              String firstKeyName =
                  gsi.keySchema().isEmpty() ? "" : gsi.keySchema().get(0).attributeName();
              row.put("KEY_NAME", AttributeValue.builder().s(firstKeyName).build());
              row.put("ASC_OR_DESC", AttributeValue.builder().s("A").build());
              row.put(
                  "CARDINALITY", AttributeValue.builder().n("0").build()); // Unknown cardinality
              row.put("PAGES", AttributeValue.builder().n("0").build()); // Unknown pages
              row.put("FILTER_CONDITION", AttributeValue.builder().nul(true).build());

              indexRows.add(row);
            }
          }
        }

        // Add Local Secondary Indexes (LSI) - one row per index
        if (tableDesc.localSecondaryIndexes() != null) {
          for (final var lsi : tableDesc.localSecondaryIndexes()) {
            if (!lsi.keySchema().isEmpty()) {
              final Map<String, AttributeValue> row = new LinkedHashMap<>();
              row.put(
                  "TABLE_CAT", AttributeValue.builder().s(table).build()); // Table name as catalog
              row.put(
                  "TABLE_SCHEM",
                  AttributeValue.builder().s(lsi.indexName()).build()); // LSI name as schema
              row.put("TABLE_NAME", AttributeValue.builder().s(table).build());
              row.put(
                  "NON_UNIQUE",
                  AttributeValue.builder().bool(true).build()); // LSI allows duplicates
              row.put("INDEX_QUALIFIER", AttributeValue.builder().nul(true).build());
              row.put("INDEX_NAME", AttributeValue.builder().s(lsi.indexName()).build());
              row.put(
                  "TYPE",
                  AttributeValue.builder()
                      .n(String.valueOf(DatabaseMetaData.tableIndexOther))
                      .build());
              row.put("ORDINAL_POSITION", AttributeValue.builder().n("1").build()); // Index ordinal
              // Show combined key schema
              final String keySchema =
                  lsi.keySchema().stream()
                      .map(key -> key.attributeName() + " (" + key.keyType().toString() + ")")
                      .collect(java.util.stream.Collectors.joining(", "));
              row.put("COLUMN_NAME", AttributeValue.builder().s(keySchema).build());
              // Add KEY_NAME with just the first key name for DbVisualizer
              String firstKeyName =
                  lsi.keySchema().isEmpty() ? "" : lsi.keySchema().get(0).attributeName();
              row.put("KEY_NAME", AttributeValue.builder().s(firstKeyName).build());
              row.put("ASC_OR_DESC", AttributeValue.builder().s("A").build());
              row.put(
                  "CARDINALITY", AttributeValue.builder().n("0").build()); // Unknown cardinality
              row.put("PAGES", AttributeValue.builder().n("0").build()); // Unknown pages
              row.put("FILTER_CONDITION", AttributeValue.builder().nul(true).build());

              indexRows.add(row);
            }
          }
        }

        return new DynamoDbResultSet(indexRows);
      } catch (final software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
        // Table doesn't exist - return empty result set instead of throwing error
        return new DynamoDbResultSet(indexRows);
      }

    } catch (final Exception e) {
      throw new SQLException(
          "Failed to retrieve index info for table " + table + ": " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getProcedures(
      final String catalog, final String schemaPattern, final String procedureNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support stored procedures
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getProcedureColumns(
      final String catalog,
      final String schemaPattern,
      final String procedureNamePattern,
      final String columnNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support stored procedures
    return new DynamoDbResultSet(new ArrayList<>());
  }

  /**
   * Retrieves the table types available in DynamoDB.
   *
   * <p>DynamoDB only supports one type of table: "TABLE". Unlike traditional SQL databases,
   * DynamoDB doesn't have views, system tables, or other table types.
   *
   * <p>The result set contains one column:
   *
   * <ol>
   *   <li><strong>TABLE_TYPE</strong> (String) - The table type name
   * </ol>
   *
   * <p>For DynamoDB, this always returns a single row with TABLE_TYPE = "TABLE".
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Get all supported table types
   * ResultSet rs = metaData.getTableTypes();
   * while (rs.next()) {
   *     System.out.println("Supported table type: " + rs.getString("TABLE_TYPE"));
   * }
   * // Output: "Supported table type: TABLE"
   * }</pre>
   *
   * @return ResultSet where each row describes a table type available in DynamoDB
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getTableTypes() throws SQLException {
    try {
      final List<Map<String, AttributeValue>> typeRows = new ArrayList<>();

      // DynamoDB only supports one type of table
      final Map<String, AttributeValue> row = new LinkedHashMap<>();
      row.put("TABLE_TYPE", AttributeValue.builder().s("TABLE").build());
      typeRows.add(row);

      return new DynamoDbResultSet(typeRows);
    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve table types: " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    try {
      final List<Map<String, AttributeValue>> catalogRows = new ArrayList<>();

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // Use listTablesPaginator to get all tables as catalogs
      final var paginator = client.listTablesPaginator();

      for (final var page : paginator) {
        for (final String tableName : page.tableNames()) {
          // Create a row for each table as a catalog
          final Map<String, AttributeValue> row = new LinkedHashMap<>();
          row.put("TABLE_CAT", AttributeValue.builder().s(tableName).build());
          catalogRows.add(row);
        }
      }

      return new DynamoDbResultSet(catalogRows);
    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve catalogs: " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return this.getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> schemaRows = new ArrayList<>();

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      // Use listTablesPaginator to get all tables
      final var paginator = client.listTablesPaginator();

      for (final var page : paginator) {
        for (final String tableName : page.tableNames()) {
          // If catalog filter is specified, skip tables that don't match
          if (catalog != null && !tableName.equals(catalog)) {
            continue;
          }

          try {
            // Describe table to get indexes
            final var describeResponse =
                client.describeTable(builder -> builder.tableName(tableName));
            final var tableDesc = describeResponse.table();

            // Add PRIMARY index as a schema
            if (!tableDesc.keySchema().isEmpty()) {
              if (schemaPattern == null || "PRIMARY".matches(schemaPattern.replace("%", ".*"))) {
                final Map<String, AttributeValue> row = new LinkedHashMap<>();
                row.put("TABLE_SCHEM", AttributeValue.builder().s("PRIMARY").build());
                row.put("TABLE_CATALOG", AttributeValue.builder().s(tableName).build());
                schemaRows.add(row);
              }
            }

            // Add Global Secondary Indexes as schemas
            if (tableDesc.globalSecondaryIndexes() != null) {
              for (final var gsi : tableDesc.globalSecondaryIndexes()) {
                final String indexName = gsi.indexName();
                if (schemaPattern == null || indexName.matches(schemaPattern.replace("%", ".*"))) {
                  final Map<String, AttributeValue> row = new LinkedHashMap<>();
                  row.put("TABLE_SCHEM", AttributeValue.builder().s(indexName).build());
                  row.put("TABLE_CATALOG", AttributeValue.builder().s(tableName).build());
                  schemaRows.add(row);
                }
              }
            }

            // Add Local Secondary Indexes as schemas
            if (tableDesc.localSecondaryIndexes() != null) {
              for (final var lsi : tableDesc.localSecondaryIndexes()) {
                final String indexName = lsi.indexName();
                if (schemaPattern == null || indexName.matches(schemaPattern.replace("%", ".*"))) {
                  final Map<String, AttributeValue> row = new LinkedHashMap<>();
                  row.put("TABLE_SCHEM", AttributeValue.builder().s(indexName).build());
                  row.put("TABLE_CATALOG", AttributeValue.builder().s(tableName).build());
                  schemaRows.add(row);
                }
              }
            }
          } catch (final Exception e) {
            // Log but continue with other tables
            if (DynamoDbDatabaseMetaData.logger.isDebugEnabled()) {
              DynamoDbDatabaseMetaData.logger.debug(
                  "Failed to describe table {}: {}", tableName, e.getMessage());
            }
          }
        }
      }

      return new DynamoDbResultSet(schemaRows);
    } catch (final Exception e) {
      throw new SQLException("Failed to retrieve schemas: " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getTablePrivileges(
      final String catalog, final String schemaPattern, final String tableNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB uses IAM for permissions
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getColumnPrivileges(
      final String catalog, final String schema, final String table, final String columnNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB uses IAM for permissions
    return new DynamoDbResultSet(new ArrayList<>());
  }

  /**
   * Retrieves a description of a table's optimal set of columns that uniquely identifies a row.
   *
   * <p>In DynamoDB, the best row identifier is always the primary key, which consists of:
   *
   * <ul>
   *   <li>A partition key (HASH) - required for all tables
   *   <li>An optional sort key (RANGE) - for composite primary keys
   * </ul>
   *
   * <p>The result set contains the following columns:
   *
   * <ol>
   *   <li><strong>SCOPE</strong> (short) - Actual scope of result (always bestRowSession for
   *       DynamoDB)
   *   <li><strong>COLUMN_NAME</strong> (String) - Column name
   *   <li><strong>DATA_TYPE</strong> (int) - SQL data type from java.sql.Types
   *   <li><strong>TYPE_NAME</strong> (String) - DynamoDB type name: "String", "Number", or "Binary"
   *   <li><strong>COLUMN_SIZE</strong> (int) - Column size (0 for DynamoDB)
   *   <li><strong>BUFFER_LENGTH</strong> (int) - Not used (0)
   *   <li><strong>DECIMAL_DIGITS</strong> (short) - Decimal digits (0)
   *   <li><strong>PSEUDO_COLUMN</strong> (short) - Whether this is a pseudo column
   *       (bestRowNotPseudo)
   * </ol>
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Get the primary key columns for a table
   * ResultSet rs = metaData.getBestRowIdentifier(null, null, "Users",
   *                                              DatabaseMetaData.bestRowSession, false);
   * while (rs.next()) {
   *     System.out.println("Key column: " + rs.getString("COLUMN_NAME") +
   *                        " Type: " + rs.getString("TYPE_NAME"));
   * }
   * }</pre>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; null means catalog name should not be used
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; null means schema name should not be used
   * @param table a table name; must match the table name as it is stored in the database
   * @param scope the scope of interest; use same values as SCOPE column values: bestRowTemporary -
   *     very temporary, while using row bestRowTransaction - valid for remainder of current
   *     transaction bestRowSession - valid for remainder of current session
   * @param nullable include columns that are nullable?
   * @return ResultSet where each row describes a primary key column
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getBestRowIdentifier(
      final String catalog,
      final String schema,
      final String table,
      final int scope,
      final boolean nullable)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> identifierRows = new ArrayList<>();

      if (table == null || table.isEmpty()) {
        return new DynamoDbResultSet(identifierRows);
      }

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      try {
        // Describe the table to get key schema
        final var describeResponse = client.describeTable(builder -> builder.tableName(table));
        final var tableDesc = describeResponse.table();

        // The best row identifier for DynamoDB is the primary key (partition + sort key)
        for (final var keyElement : tableDesc.keySchema()) {
          final Map<String, AttributeValue> row = new LinkedHashMap<>();

          // SCOPE - for DynamoDB, keys are always permanent (bestRowSession)
          row.put(
              "SCOPE",
              AttributeValue.builder().n(String.valueOf(DatabaseMetaData.bestRowSession)).build());

          // COLUMN_NAME
          row.put("COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());

          // DATA_TYPE and TYPE_NAME - need to look up the attribute type
          String typeName = "String"; // Default
          int dataType = java.sql.Types.VARCHAR; // Default

          for (final var attr : tableDesc.attributeDefinitions()) {
            if (attr.attributeName().equals(keyElement.attributeName())) {
              typeName = this.getDynamoDbTypeName(attr.attributeType());
              dataType = this.mapDynamoDbTypeToSql(attr.attributeType());
              break;
            }
          }

          row.put("DATA_TYPE", AttributeValue.builder().n(String.valueOf(dataType)).build());
          row.put("TYPE_NAME", AttributeValue.builder().s(typeName).build());

          // COLUMN_SIZE - DynamoDB doesn't have fixed sizes
          row.put("COLUMN_SIZE", AttributeValue.builder().n("0").build());

          // BUFFER_LENGTH - not used
          row.put("BUFFER_LENGTH", AttributeValue.builder().nul(true).build());

          // DECIMAL_DIGITS - only relevant for numeric types
          row.put("DECIMAL_DIGITS", AttributeValue.builder().n("0").build());

          // PSEUDO_COLUMN - DynamoDB primary keys are real columns
          row.put(
              "PSEUDO_COLUMN",
              AttributeValue.builder()
                  .n(String.valueOf(DatabaseMetaData.bestRowNotPseudo))
                  .build());

          identifierRows.add(row);
        }
      } catch (final software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
        // Table doesn't exist - return empty result set
        return new DynamoDbResultSet(identifierRows);
      }

      return new DynamoDbResultSet(identifierRows);
    } catch (final Exception e) {
      throw new SQLException(
          "Failed to retrieve best row identifier for table " + table + ": " + e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't have version columns
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getSuperTypes(
      final String catalog, final String schemaPattern, final String typeNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support user-defined types
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getSuperTables(
      final String catalog, final String schemaPattern, final String tableNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support table inheritance
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getAttributes(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final String attributeNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support user-defined types
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    // Return empty result set for now
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getFunctions(
      final String catalog, final String schemaPattern, final String functionNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support user-defined functions
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getFunctionColumns(
      final String catalog,
      final String schemaPattern,
      final String functionNamePattern,
      final String columnNamePattern)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support user-defined functions
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getUDTs(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final int[] types)
      throws SQLException {
    // Return empty result set - DynamoDB doesn't support user-defined types
    return new DynamoDbResultSet(new ArrayList<>());
  }

  @Override
  public ResultSet getPseudoColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern)
      throws SQLException {
    // Return empty result set for now
    return new DynamoDbResultSet(new ArrayList<>());
  }

  /**
   * Retrieves detailed information about index columns for DynamoDB tables.
   *
   * <p>This method provides comprehensive metadata about individual keys within each index,
   * including both primary indexes (table keys) and secondary indexes (GSI/LSI). Each key column is
   * returned as a separate row with detailed type and role information.
   *
   * <p><strong>Enhanced Metadata:</strong> This method includes TYPE_NAME and KEY_NAME fields that
   * are not available in the standard JDBC specification, providing better integration with
   * database tools and enhanced schema introspection capabilities.
   *
   * <p><strong>Index Types Supported:</strong>
   *
   * <ul>
   *   <li><strong>PRIMARY</strong> - Main table partition and sort keys
   *   <li><strong>Global Secondary Indexes (GSI)</strong> - Independent indexes with their own
   *       partition/sort keys
   *   <li><strong>Local Secondary Indexes (LSI)</strong> - Alternate sort keys sharing the table's
   *       partition key
   * </ul>
   *
   * <p><strong>Column Schema:</strong>
   *
   * <ul>
   *   <li><strong>TABLE_CAT</strong> - The DynamoDB table name (used as catalog)
   *   <li><strong>TABLE_SCHEM</strong> - The index name (used as schema): "PRIMARY", GSI name, or
   *       LSI name
   *   <li><strong>TABLE_NAME</strong> - The DynamoDB table name
   *   <li><strong>COLUMN_NAME</strong> - The attribute name of the index key
   *   <li><strong>KEY_NAME</strong> - <em>(Enhanced field)</em> Same as COLUMN_NAME for consistency
   *   <li><strong>TYPE_NAME</strong> - <em>(Enhanced field)</em> DynamoDB attribute type: "String",
   *       "Number", "Binary", or "Unknown"
   *   <li><strong>ORDINAL_POSITION</strong> - Position within the index (1-based): 1 for partition
   *       key, 2 for sort key
   *   <li><strong>KEY_TYPE</strong> - <em>(Enhanced field)</em> Key role: "HASH" for partition key,
   *       "RANGE" for sort key
   *   <li><strong>INDEX_NAME</strong> - <em>(Enhanced field)</em> The index name for reference
   * </ul>
   *
   * <p><strong>Filtering:</strong>
   *
   * <ul>
   *   <li>If {@code schema} is null or empty, returns columns for all indexes on the table
   *   <li>If {@code schema} is specified, returns columns only for that specific index
   *   <li>Use "PRIMARY" as the schema name to get only the table's primary key columns
   * </ul>
   *
   * <p><strong>Example Usage:</strong>
   *
   * <pre>{@code
   * // Get all index columns for a table
   * DynamoDbDatabaseMetaData dynaMeta = (DynamoDbDatabaseMetaData) connection.getMetaData();
   * ResultSet indexColumns = dynaMeta.getIndexColumns("MyTable", null, "MyTable");
   * while (indexColumns.next()) {
   *     System.out.println("Column: " + indexColumns.getString("COLUMN_NAME"));
   *     System.out.println("Key Name: " + indexColumns.getString("KEY_NAME"));
   *     System.out.println("Type: " + indexColumns.getString("TYPE_NAME"));        // "String", "Number", etc.
   *     System.out.println("Key Type: " + indexColumns.getString("KEY_TYPE"));     // "HASH" or "RANGE"
   *     System.out.println("Index: " + indexColumns.getString("INDEX_NAME"));      // Index name
   *     System.out.println("Position: " + indexColumns.getInt("ORDINAL_POSITION")); // 1, 2, etc.
   * }
   *
   * // Get columns for a specific index
   * ResultSet gsiColumns = dynaMeta.getIndexColumns("MyTable", "MyGSI", "MyTable");
   * }</pre>
   *
   * <p><strong>Benefits for Database Tools:</strong>
   *
   * <ul>
   *   <li><strong>Type-aware display</strong> - Tools can show appropriate icons/formatting based
   *       on data type
   *   <li><strong>Enhanced schema browsing</strong> - Individual index columns are properly
   *       represented
   *   <li><strong>Better query building</strong> - Tools can generate appropriate type-specific
   *       queries
   *   <li><strong>Schema export</strong> - Export tools get complete type and structure information
   * </ul>
   *
   * @param catalog the table name (used as catalog in DynamoDB mapping); can be null
   * @param schema the index name to filter by (used as schema in DynamoDB mapping); null/empty
   *     returns all indexes
   * @param table the table name; if null or empty, returns empty ResultSet
   * @return ResultSet containing detailed index column information with enhanced metadata
   * @throws SQLException if there's an error retrieving index column information or if the table
   *     does not exist
   */
  public ResultSet getIndexColumns(final String catalog, final String schema, final String table)
      throws SQLException {
    try {
      final List<Map<String, AttributeValue>> indexColumnRows = new ArrayList<>();

      if (table == null || table.isEmpty()) {
        return new DynamoDbResultSet(indexColumnRows);
      }

      // Get DynamoDB client from connection
      final DynamoDbClient client = this.connection.getDynamoDbClient();

      try {
        final var describeResponse = client.describeTable(builder -> builder.tableName(table));
        final var tableDesc = describeResponse.table();

        // Process PRIMARY index columns if schema is null or matches "PRIMARY"
        if (schema == null || schema.isEmpty() || "PRIMARY".equals(schema)) {
          this.addIndexColumnRows(
              indexColumnRows,
              table,
              "PRIMARY",
              tableDesc.keySchema(),
              tableDesc.attributeDefinitions());
        }

        // Process Global Secondary Index columns
        if (tableDesc.globalSecondaryIndexes() != null) {
          for (final var gsi : tableDesc.globalSecondaryIndexes()) {
            if (schema == null || schema.isEmpty() || schema.equals(gsi.indexName())) {
              this.addIndexColumnRows(
                  indexColumnRows,
                  table,
                  gsi.indexName(),
                  gsi.keySchema(),
                  tableDesc.attributeDefinitions());
            }
          }
        }

        // Process Local Secondary Index columns
        if (tableDesc.localSecondaryIndexes() != null) {
          for (final var lsi : tableDesc.localSecondaryIndexes()) {
            if (schema == null || schema.isEmpty() || schema.equals(lsi.indexName())) {
              this.addIndexColumnRows(
                  indexColumnRows,
                  table,
                  lsi.indexName(),
                  lsi.keySchema(),
                  tableDesc.attributeDefinitions());
            }
          }
        }

        return new DynamoDbResultSet(indexColumnRows);
      } catch (final software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
        // Table doesn't exist - return empty result set
        return new DynamoDbResultSet(indexColumnRows);
      }

    } catch (final Exception e) {
      throw new SQLException(
          "Failed to retrieve index columns for table " + table + ": " + e.getMessage(), e);
    }
  }

  /**
   * Helper method to add index column rows for a specific index.
   *
   * <p>This method processes the key schema of a single index (PRIMARY, GSI, or LSI) and creates
   * detailed metadata rows for each key column. It handles type resolution by matching attribute
   * names from the key schema with their corresponding type definitions.
   *
   * @param indexColumnRows the list to add generated rows to
   * @param tableName the DynamoDB table name
   * @param indexName the index name ("PRIMARY" for table keys, or GSI/LSI name)
   * @param keySchema the key schema elements (partition key, sort key) for this index
   * @param attributeDefinitions the table's attribute definitions for type resolution
   */
  private void addIndexColumnRows(
      final List<Map<String, AttributeValue>> indexColumnRows,
      final String tableName,
      final String indexName,
      final List<software.amazon.awssdk.services.dynamodb.model.KeySchemaElement> keySchema,
      final List<software.amazon.awssdk.services.dynamodb.model.AttributeDefinition>
          attributeDefinitions) {

    for (int i = 0; i < keySchema.size(); i++) {
      final var keyElement = keySchema.get(i);
      final Map<String, AttributeValue> row = new LinkedHashMap<>();

      row.put("TABLE_CAT", AttributeValue.builder().s(tableName).build()); // Table name as catalog
      row.put("TABLE_SCHEM", AttributeValue.builder().s(indexName).build()); // Index name as schema
      row.put("TABLE_NAME", AttributeValue.builder().s(tableName).build());
      row.put("COLUMN_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());

      // Add KEY_NAME field - same as COLUMN_NAME
      row.put("KEY_NAME", AttributeValue.builder().s(keyElement.attributeName()).build());

      // Add TYPE_NAME field - get the attribute type from table description
      String typeName = "String"; // Default type
      for (final var attr : attributeDefinitions) {
        if (attr.attributeName().equals(keyElement.attributeName())) {
          typeName = this.getDynamoDbTypeName(attr.attributeType());
          break;
        }
      }
      row.put("TYPE_NAME", AttributeValue.builder().s(typeName).build());

      // Additional index column metadata
      row.put("ORDINAL_POSITION", AttributeValue.builder().n(String.valueOf(i + 1)).build());
      row.put(
          "KEY_TYPE",
          AttributeValue.builder().s(keyElement.keyType().toString()).build()); // HASH or RANGE
      row.put("INDEX_NAME", AttributeValue.builder().s(indexName).build());

      indexColumnRows.add(row);
    }
  }

  // Wrapper methods

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(this.getClass());
  }

  // Additional missing methods from compiler errors

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }
}
