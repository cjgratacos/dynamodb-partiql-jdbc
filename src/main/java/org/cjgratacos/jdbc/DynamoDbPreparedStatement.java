package org.cjgratacos.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * PreparedStatement implementation for DynamoDB PartiQL queries.
 *
 * <p>This class provides parameter binding and execution for prepared PartiQL statements. It
 * supports standard JDBC parameter binding methods and converts them to DynamoDB AttributeValues.
 *
 * <p>Parameter placeholders (?) in the SQL are replaced with actual values when the statement is
 * executed. The implementation handles type conversion from Java types to DynamoDB types.
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class DynamoDbPreparedStatement extends DynamoDbStatement implements PreparedStatement {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbPreparedStatement.class);

  private final String originalSql;
  private final List<String> sqlParts;
  private final int parameterCount;
  private final Map<Integer, Object> parameters = new HashMap<>();
  private final Map<Integer, AttributeValue> parameterValues = new HashMap<>();

  // Pattern to find parameter placeholders
  private static final Pattern PARAMETER_PATTERN = Pattern.compile("\\?");
  
  // Batch support
  private final List<Map<Integer, Object>> batchParametersList = new ArrayList<>();

  /**
   * Creates a new prepared statement.
   *
   * @param connection the connection that created this statement
   * @param sql the SQL statement with parameter placeholders
   * @throws SQLException if the SQL is invalid
   */
  public DynamoDbPreparedStatement(DynamoDbConnection connection, String sql) throws SQLException {
    super(connection, connection.getDynamoDbClient(), connection.getRetryHandler());
    this.originalSql = sql;

    // Parse the SQL to find parameter placeholders
    this.sqlParts = parseSQL(sql);
    this.parameterCount = sqlParts.size() - 1;

    if (logger.isDebugEnabled()) {
      logger.debug("Created prepared statement with {} parameters: {}", parameterCount, sql);
    }
  }

  /**
   * Parses the SQL to extract parts between parameter placeholders.
   *
   * @param sql the SQL to parse
   * @return list of SQL parts
   */
  private List<String> parseSQL(String sql) {
    List<String> parts = new ArrayList<>();
    Matcher matcher = PARAMETER_PATTERN.matcher(sql);
    int lastEnd = 0;

    while (matcher.find()) {
      parts.add(sql.substring(lastEnd, matcher.start()));
      lastEnd = matcher.end();
    }
    parts.add(sql.substring(lastEnd));

    return parts;
  }

  /**
   * Builds the final SQL by replacing placeholders with parameter values.
   *
   * @return the SQL with parameters substituted
   * @throws SQLException if not all parameters are set
   */
  private String buildSQL() throws SQLException {
    if (parameters.size() != parameterCount) {
      throw new SQLException(
          String.format(
              "Not all parameters set. Expected %d, but only %d are set",
              parameterCount, parameters.size()));
    }

    StringBuilder sql = new StringBuilder();
    for (int i = 0; i < sqlParts.size() - 1; i++) {
      sql.append(sqlParts.get(i));
      Object param = parameters.get(i + 1);
      if (param == null) {
        sql.append("NULL");
      } else if (param instanceof String) {
        // Escape single quotes and wrap in quotes
        String escaped = ((String) param).replace("'", "''");
        sql.append("'").append(escaped).append("'");
      } else if (param instanceof Number || param instanceof Boolean) {
        sql.append(param.toString());
      } else {
        // For other types, convert to string and quote
        String escaped = param.toString().replace("'", "''");
        sql.append("'").append(escaped).append("'");
      }
    }
    sql.append(sqlParts.get(sqlParts.size() - 1));

    return sql.toString();
  }

  /**
   * Validates that the parameter index is valid.
   *
   * @param parameterIndex the parameter index (1-based)
   * @throws SQLException if the index is invalid
   */
  private void validateParameterIndex(int parameterIndex) throws SQLException {
    if (parameterIndex < 1 || parameterIndex > parameterCount) {
      throw new SQLException(
          String.format(
              "Parameter index %d is out of range. Valid range is 1 to %d",
              parameterIndex, parameterCount));
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    String sql = buildSQL();
    if (logger.isDebugEnabled()) {
      logger.debug("Executing prepared query: {}", sql);
    }
    return super.executeQuery(sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    String sql = buildSQL();
    if (logger.isDebugEnabled()) {
      logger.debug("Executing prepared update: {}", sql);
    }
    return super.executeUpdate(sql);
  }

  @Override
  public boolean execute() throws SQLException {
    String sql = buildSQL();
    if (logger.isDebugEnabled()) {
      logger.debug("Executing prepared statement: {}", sql);
    }
    return super.execute(sql);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, null);
    parameterValues.put(parameterIndex, AttributeValue.builder().nul(true).build());
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().bool(x).build());
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    validateParameterIndex(parameterIndex);
    parameters.put(parameterIndex, x);
    parameterValues.put(parameterIndex, AttributeValue.builder().n(String.valueOf(x)).build());
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.DECIMAL);
    } else {
      parameters.put(parameterIndex, x);
      parameterValues.put(parameterIndex, AttributeValue.builder().n(x.toString()).build());
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.VARCHAR);
    } else {
      parameters.put(parameterIndex, x);
      parameterValues.put(parameterIndex, AttributeValue.builder().s(x).build());
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.BINARY);
    } else {
      parameters.put(parameterIndex, x);
      parameterValues.put(
          parameterIndex, AttributeValue.builder().b(SdkBytes.fromByteArray(x)).build());
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.DATE);
    } else {
      String iso8601 = x.toString();
      parameters.put(parameterIndex, iso8601);
      parameterValues.put(parameterIndex, AttributeValue.builder().s(iso8601).build());
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.TIME);
    } else {
      String iso8601 = x.toString();
      parameters.put(parameterIndex, iso8601);
      parameterValues.put(parameterIndex, AttributeValue.builder().s(iso8601).build());
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.TIMESTAMP);
    } else {
      String iso8601 = x.toInstant().toString();
      parameters.put(parameterIndex, iso8601);
      parameterValues.put(parameterIndex, AttributeValue.builder().s(iso8601).build());
    }
  }

  @Override
  public void clearParameters() throws SQLException {
    parameters.clear();
    parameterValues.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    validateParameterIndex(parameterIndex);
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else if (x instanceof Date) {
      setDate(parameterIndex, (Date) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof BigDecimal) {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    } else {
      // Convert to string as fallback
      setString(parameterIndex, x.toString());
    }
  }

  @Override
  public void addBatch() throws SQLException {
    // Check if the statement is a DML statement
    if (!PartiQLUtils.isDMLStatement(originalSql)) {
      throw new SQLException("Only DML statements (INSERT, UPDATE, DELETE, UPSERT, REPLACE) are allowed in batch operations");
    }
    
    // Validate all parameters are set
    if (parameters.size() != parameterCount) {
      throw new SQLException(
          String.format(
              "Not all parameters set. Expected %d, but only %d are set",
              parameterCount, parameters.size()));
    }
    
    // Create a copy of current parameters
    Map<Integer, Object> parametersCopy = new HashMap<>(parameters);
    batchParametersList.add(parametersCopy);
    
    if (logger.isDebugEnabled()) {
      logger.debug("Added parameters to batch. Total batch size: {}", batchParametersList.size());
    }
  }
  
  @Override
  public int[] executeBatch() throws SQLException {
    if (batchParametersList.isEmpty()) {
      return new int[0];
    }
    
    final int batchSize = batchParametersList.size();
    final int[] results = new int[batchSize];
    int successCount = 0;
    int failureCount = 0;
    
    if (logger.isInfoEnabled()) {
      logger.info("Executing prepared statement batch with {} commands", batchSize);
    }
    
    // Save current parameters to restore later
    Map<Integer, Object> savedParameters = new HashMap<>(parameters);
    
    try {
      // Execute each set of parameters in the batch
      for (int i = 0; i < batchSize; i++) {
        Map<Integer, Object> batchParams = batchParametersList.get(i);
        try {
          // Set parameters from batch
          parameters.clear();
          parameters.putAll(batchParams);
          
          // Execute the statement with these parameters
          final int updateCount = this.executeUpdate();
          results[i] = updateCount;
          successCount++;
        } catch (Exception e) {
          // According to JDBC spec, set to EXECUTE_FAILED on error
          results[i] = Statement.EXECUTE_FAILED;
          failureCount++;
          
          logger.warn("Batch command {} failed: {}", i, e.getMessage());
          
          // Continue processing remaining commands
        }
      }
    } finally {
      // Restore original parameters
      parameters.clear();
      parameters.putAll(savedParameters);
      
      // Clear the batch
      batchParametersList.clear();
    }
    
    if (logger.isInfoEnabled()) {
      logger.info("Batch execution completed. Success: {}, Failed: {}", successCount, failureCount);
    }
    
    // If any commands failed, throw BatchUpdateException
    if (failureCount > 0) {
      throw new java.sql.BatchUpdateException(
          String.format("Batch execution completed with %d failures out of %d commands", failureCount, batchSize),
          results
      );
    }
    
    return results;
  }
  
  @Override
  public void clearBatch() throws SQLException {
    batchParametersList.clear();
    
    if (logger.isDebugEnabled()) {
      logger.debug("Batch cleared");
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    // Try to get metadata without executing the query
    // This is challenging for DynamoDB, so we'll return null for now
    return null;
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return new DynamoDbParameterMetaData(parameterCount, parameterValues);
  }

  // Methods not commonly used or not applicable to DynamoDB

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("setAsciiStream not supported");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("setUnicodeStream not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("setBinaryStream not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException("setCharacterStream not supported");
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLException("setRef not supported");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLException("setBlob not supported");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLException("setClob not supported");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLException("setArray not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    setDate(parameterIndex, x); // Ignore calendar for now
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    setTime(parameterIndex, x); // Ignore calendar for now
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    setTimestamp(parameterIndex, x); // Ignore calendar for now
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    setNull(parameterIndex, sqlType);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, java.sql.Types.VARCHAR);
    } else {
      setString(parameterIndex, x.toString());
    }
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException("setRowId not supported");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    setString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException("setNCharacterStream not supported");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException("setNClob not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("setClob not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException("setBlob not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("setNClob not supported");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException("setSQLXML not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException("setAsciiStream not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException("setBinaryStream not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException("setCharacterStream not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException("setAsciiStream not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException("setBinaryStream not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException("setCharacterStream not supported");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLException("setNCharacterStream not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException("setClob not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLException("setBlob not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException("setNClob not supported");
  }
}