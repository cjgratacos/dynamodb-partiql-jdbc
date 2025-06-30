package org.cjgratacos.jdbc.pool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pooled connection wrapper that delegates to a real DynamoDB connection.
 *
 * <p>This class wraps a {@link DynamoDbConnection} and manages its lifecycle within a connection
 * pool. When {@link #close()} is called, the connection is returned to the pool instead of being
 * physically closed.
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class PooledConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(PooledConnection.class);

  private final DynamoDbConnection physicalConnection;
  private final DynamoDbConnectionPool pool;
  private final Instant creationTime;
  private volatile Instant lastAccessTime;
  private volatile Instant lastValidationTime;
  final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Creates a new pooled connection wrapper.
   *
   * @param physicalConnection the underlying physical connection
   * @param pool the pool that manages this connection
   */
  public PooledConnection(DynamoDbConnection physicalConnection, DynamoDbConnectionPool pool) {
    this.physicalConnection = physicalConnection;
    this.pool = pool;
    this.creationTime = Instant.now();
    this.lastAccessTime = this.creationTime;
    this.lastValidationTime = this.creationTime;
  }

  /**
   * Updates the last access time for this connection.
   */
  private void touchLastAccessTime() {
    this.lastAccessTime = Instant.now();
  }

  /**
   * Validates that this connection is still open.
   *
   * @throws SQLException if the connection is closed
   */
  private void checkClosed() throws SQLException {
    if (closed.get()) {
      throw new SQLException("Connection is closed");
    }
  }

  /**
   * Gets the creation time of this pooled connection.
   *
   * @return the creation time
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Gets the last access time of this pooled connection.
   *
   * @return the last access time
   */
  public Instant getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * Gets the last validation time of this pooled connection.
   *
   * @return the last validation time
   */
  public Instant getLastValidationTime() {
    return lastValidationTime;
  }

  /**
   * Updates the last validation time.
   */
  public void setLastValidationTime(Instant time) {
    this.lastValidationTime = time;
  }

  /**
   * Gets the underlying physical connection.
   *
   * @return the physical connection
   */
  public DynamoDbConnection getPhysicalConnection() {
    return physicalConnection;
  }

  /**
   * Closes the physical connection. This is called by the pool when the connection is being
   * evicted.
   *
   * @throws SQLException if an error occurs closing the connection
   */
  public void closePhysicalConnection() throws SQLException {
    closed.set(true);
    physicalConnection.close();
  }

  // Connection interface implementation

  @Override
  public Statement createStatement() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareCall(sql);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.rollback();
  }

  @Override
  public void close() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      // Return connection to pool instead of closing
      pool.returnConnection(this);
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed.get();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.clearWarnings();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.setSavepoint(name);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.releaseSavepoint(savepoint);
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createStatement(
        resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(
        sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareCall(
        sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.prepareStatement(sql, columnNames);
  }

  @Override
  public Clob createClob() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (closed.get()) {
      return false;
    }
    touchLastAccessTime();
    return physicalConnection.isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      checkClosed();
      touchLastAccessTime();
      physicalConnection.setClientInfo(name, value);
    } catch (SQLException e) {
      throw new SQLClientInfoException("Connection is closed", e.getSQLState(), null);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      checkClosed();
      touchLastAccessTime();
      physicalConnection.setClientInfo(properties);
    } catch (SQLException e) {
      throw new SQLClientInfoException("Connection is closed", e.getSQLState(), null);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    closed.set(true);
    physicalConnection.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    checkClosed();
    touchLastAccessTime();
    physicalConnection.setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    checkClosed();
    touchLastAccessTime();
    return physicalConnection.getNetworkTimeout();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    checkClosed();
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    return physicalConnection.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkClosed();
    return iface.isAssignableFrom(getClass()) || physicalConnection.isWrapperFor(iface);
  }
}