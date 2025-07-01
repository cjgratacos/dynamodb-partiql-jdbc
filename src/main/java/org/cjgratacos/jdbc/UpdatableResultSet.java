package org.cjgratacos.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * An updatable ResultSet implementation for DynamoDB that supports row modifications.
 * 
 * <p>This class extends DynamoDbResultSet to provide update, insert, and delete functionality.
 * It tracks changes made to the current row and applies them to DynamoDB when updateRow(),
 * insertRow(), or deleteRow() is called.
 * 
 * <p>Limitations:
 * <ul>
 *   <li>Updates require the table to have a defined primary key</li>
 *   <li>Only simple single-table queries are updatable</li>
 *   <li>Joins, aggregations, and complex queries are read-only</li>
 *   <li>Binary data updates use base64 encoding</li>
 * </ul>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class UpdatableResultSet extends DynamoDbResultSet {
    
    private static final Logger logger = LoggerFactory.getLogger(UpdatableResultSet.class);
    
    private final DynamoDbStatement statement;
    private final String tableName;
    private final Map<String, String> primaryKeyColumns;
    private final Map<String, AttributeValue> pendingUpdates;
    private final Map<String, AttributeValue> insertRowData;
    private boolean onInsertRow;
    private final boolean isUpdatable;
    
    /**
     * Creates an updatable ResultSet.
     *
     * @param client the DynamoDB client
     * @param sql the original SQL query
     * @param response the query response
     * @param fetchSize the fetch size
     * @param limitOffsetInfo limit/offset information
     * @param tableKeyInfo table key information
     * @param maxRows maximum rows
     * @param offsetTokenCache offset token cache
     * @param statement the statement that created this ResultSet
     * @param tableName the table name (null for non-updatable queries)
     * @param primaryKeyColumns map of primary key column names to their types
     */
    public UpdatableResultSet(
            DynamoDbClient client,
            String sql,
            ExecuteStatementResponse response,
            int fetchSize,
            LimitOffsetInfo limitOffsetInfo,
            TableKeyInfo tableKeyInfo,
            int maxRows,
            OffsetTokenCache offsetTokenCache,
            DynamoDbStatement statement,
            String tableName,
            Map<String, String> primaryKeyColumns) {
        super(client, sql, response, fetchSize, limitOffsetInfo, tableKeyInfo, maxRows, offsetTokenCache);
        this.statement = statement;
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns != null ? new HashMap<>(primaryKeyColumns) : new HashMap<>();
        this.pendingUpdates = new HashMap<>();
        this.insertRowData = new HashMap<>();
        this.onInsertRow = false;
        this.isUpdatable = tableName != null && !primaryKeyColumns.isEmpty();
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        return isUpdatable ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
    }
    
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().nul(true).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().nul(true).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().bool(x).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().bool(x).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        updateInt(columnIndex, x);
    }
    
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateInt(columnLabel, x);
    }
    
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        updateInt(columnIndex, x);
    }
    
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateInt(columnLabel, x);
    }
    
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnName, value);
    }
    
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        checkUpdatable();
        AttributeValue value = AttributeValue.builder().n(String.valueOf(x)).build();
        updateValue(columnLabel, value);
    }
    
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        if (x == null) {
            updateNull(columnIndex);
        } else {
            AttributeValue value = AttributeValue.builder().n(x.toString()).build();
            updateValue(columnName, value);
        }
    }
    
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        checkUpdatable();
        if (x == null) {
            updateNull(columnLabel);
        } else {
            AttributeValue value = AttributeValue.builder().n(x.toString()).build();
            updateValue(columnLabel, value);
        }
    }
    
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        if (x == null) {
            updateNull(columnIndex);
        } else {
            AttributeValue value = AttributeValue.builder().s(x).build();
            updateValue(columnName, value);
        }
    }
    
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        checkUpdatable();
        if (x == null) {
            updateNull(columnLabel);
        } else {
            AttributeValue value = AttributeValue.builder().s(x).build();
            updateValue(columnLabel, value);
        }
    }
    
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        checkUpdatable();
        String columnName = getColumnNameFromIndex(columnIndex);
        if (x == null) {
            updateNull(columnIndex);
        } else {
            AttributeValue value = AttributeValue.builder()
                .b(software.amazon.awssdk.core.SdkBytes.fromByteArray(x))
                .build();
            updateValue(columnName, value);
        }
    }
    
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        checkUpdatable();
        if (x == null) {
            updateNull(columnLabel);
        } else {
            AttributeValue value = AttributeValue.builder()
                .b(software.amazon.awssdk.core.SdkBytes.fromByteArray(x))
                .build();
            updateValue(columnLabel, value);
        }
    }
    
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        updateString(columnIndex, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateString(columnLabel, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        updateString(columnIndex, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateString(columnLabel, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateString(columnIndex, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateString(columnLabel, x != null ? x.toString() : null);
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stream updates not supported");
    }
    
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        updateObject(columnIndex, x);
    }
    
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(columnLabel, x);
    }
    
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        checkUpdatable();
        if (x == null) {
            updateNull(columnIndex);
        } else if (x instanceof String) {
            updateString(columnIndex, (String) x);
        } else if (x instanceof Integer) {
            updateInt(columnIndex, (Integer) x);
        } else if (x instanceof Long) {
            updateLong(columnIndex, (Long) x);
        } else if (x instanceof Double) {
            updateDouble(columnIndex, (Double) x);
        } else if (x instanceof Float) {
            updateFloat(columnIndex, (Float) x);
        } else if (x instanceof BigDecimal) {
            updateBigDecimal(columnIndex, (BigDecimal) x);
        } else if (x instanceof Boolean) {
            updateBoolean(columnIndex, (Boolean) x);
        } else if (x instanceof byte[]) {
            updateBytes(columnIndex, (byte[]) x);
        } else if (x instanceof Date) {
            updateDate(columnIndex, (Date) x);
        } else if (x instanceof Time) {
            updateTime(columnIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            updateTimestamp(columnIndex, (Timestamp) x);
        } else {
            updateString(columnIndex, x.toString());
        }
    }
    
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        checkUpdatable();
        if (x == null) {
            updateNull(columnLabel);
        } else if (x instanceof String) {
            updateString(columnLabel, (String) x);
        } else if (x instanceof Integer) {
            updateInt(columnLabel, (Integer) x);
        } else if (x instanceof Long) {
            updateLong(columnLabel, (Long) x);
        } else if (x instanceof Double) {
            updateDouble(columnLabel, (Double) x);
        } else if (x instanceof Float) {
            updateFloat(columnLabel, (Float) x);
        } else if (x instanceof BigDecimal) {
            updateBigDecimal(columnLabel, (BigDecimal) x);
        } else if (x instanceof Boolean) {
            updateBoolean(columnLabel, (Boolean) x);
        } else if (x instanceof byte[]) {
            updateBytes(columnLabel, (byte[]) x);
        } else if (x instanceof Date) {
            updateDate(columnLabel, (Date) x);
        } else if (x instanceof Time) {
            updateTime(columnLabel, (Time) x);
        } else if (x instanceof Timestamp) {
            updateTimestamp(columnLabel, (Timestamp) x);
        } else {
            updateString(columnLabel, x.toString());
        }
    }
    
    @Override
    public void insertRow() throws SQLException {
        checkUpdatable();
        if (!onInsertRow) {
            throw new SQLException("Not on insert row");
        }
        
        // Validate that all primary key columns have values
        for (String keyColumn : primaryKeyColumns.keySet()) {
            if (!insertRowData.containsKey(keyColumn)) {
                throw new SQLException("Primary key column '" + keyColumn + "' must have a value");
            }
        }
        
        // Build the INSERT statement
        StringBuilder sql = new StringBuilder("INSERT INTO \"").append(tableName).append("\" VALUE {");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : insertRowData.entrySet()) {
            if (!first) {
                sql.append(", ");
            }
            first = false;
            sql.append("'").append(entry.getKey()).append("': ");
            appendAttributeValue(sql, entry.getValue());
        }
        sql.append("}");
        
        logger.debug("Executing insert: {}", sql);
        
        // Execute the INSERT
        statement.execute(sql.toString());
        
        // Clear insert row data and return to normal navigation
        insertRowData.clear();
        onInsertRow = false;
    }
    
    @Override
    public void updateRow() throws SQLException {
        checkUpdatable();
        if (onInsertRow) {
            throw new SQLException("Cannot update row while on insert row");
        }
        
        if (pendingUpdates.isEmpty()) {
            return; // Nothing to update
        }
        
        // Get the current row's primary key values
        Map<String, AttributeValue> keyValues = extractPrimaryKeyValues();
        
        // Build the UPDATE statement
        StringBuilder sql = new StringBuilder("UPDATE \"").append(tableName).append("\" SET ");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : pendingUpdates.entrySet()) {
            if (!first) {
                sql.append(", ");
            }
            first = false;
            sql.append("\"").append(entry.getKey()).append("\" = ");
            appendAttributeValue(sql, entry.getValue());
        }
        
        sql.append(" WHERE ");
        first = true;
        for (Map.Entry<String, AttributeValue> entry : keyValues.entrySet()) {
            if (!first) {
                sql.append(" AND ");
            }
            first = false;
            sql.append("\"").append(entry.getKey()).append("\" = ");
            appendAttributeValue(sql, entry.getValue());
        }
        
        logger.debug("Executing update: {}", sql);
        
        // Execute the UPDATE
        statement.execute(sql.toString());
        
        // Clear pending updates
        pendingUpdates.clear();
    }
    
    @Override
    public void deleteRow() throws SQLException {
        checkUpdatable();
        if (onInsertRow) {
            throw new SQLException("Cannot delete row while on insert row");
        }
        
        // Get the current row's primary key values
        Map<String, AttributeValue> keyValues = extractPrimaryKeyValues();
        
        // Build the DELETE statement
        StringBuilder sql = new StringBuilder("DELETE FROM \"").append(tableName).append("\" WHERE ");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : keyValues.entrySet()) {
            if (!first) {
                sql.append(" AND ");
            }
            first = false;
            sql.append("\"").append(entry.getKey()).append("\" = ");
            appendAttributeValue(sql, entry.getValue());
        }
        
        logger.debug("Executing delete: {}", sql);
        
        // Execute the DELETE
        statement.execute(sql.toString());
    }
    
    @Override
    public void refreshRow() throws SQLException {
        checkUpdatable();
        if (onInsertRow) {
            throw new SQLException("Cannot refresh row while on insert row");
        }
        
        // Cancel any pending updates
        cancelRowUpdates();
        
        // Re-read the current row from the database
        Map<String, AttributeValue> keyValues = extractPrimaryKeyValues();
        
        // Build a SELECT query to refresh the row
        StringBuilder sql = new StringBuilder("SELECT * FROM \"").append(tableName).append("\" WHERE ");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : keyValues.entrySet()) {
            if (!first) {
                sql.append(" AND ");
            }
            first = false;
            sql.append("\"").append(entry.getKey()).append("\" = ");
            appendAttributeValue(sql, entry.getValue());
        }
        
        logger.debug("Refreshing row: {}", sql);
        
        // Execute the query and update the current row
        try (ResultSet rs = statement.executeQuery(sql.toString())) {
            if (rs.next()) {
                // Update current item with refreshed data
                Map<String, AttributeValue> refreshedItem = new HashMap<>();
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String columnName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    if (value != null) {
                        refreshedItem.put(columnName, convertToAttributeValue(value));
                    }
                }
                updateCurrentItem(refreshedItem);
            }
        }
    }
    
    @Override
    public void cancelRowUpdates() throws SQLException {
        checkUpdatable();
        if (onInsertRow) {
            insertRowData.clear();
        } else {
            pendingUpdates.clear();
        }
    }
    
    @Override
    public void moveToInsertRow() throws SQLException {
        checkUpdatable();
        onInsertRow = true;
        insertRowData.clear();
        pendingUpdates.clear();
    }
    
    @Override
    public void moveToCurrentRow() throws SQLException {
        checkUpdatable();
        onInsertRow = false;
        insertRowData.clear();
    }
    
    @Override
    public boolean rowUpdated() throws SQLException {
        return false; // Cannot detect if row was updated by another transaction
    }
    
    @Override
    public boolean rowInserted() throws SQLException {
        return false; // Cannot detect if row was inserted by another transaction
    }
    
    @Override
    public boolean rowDeleted() throws SQLException {
        return false; // Cannot detect if row was deleted by another transaction
    }
    
    // Helper methods
    
    /**
     * Updates a value in either the pending updates map or insert row data map.
     */
    private void updateValue(String columnName, AttributeValue value) {
        if (onInsertRow) {
            insertRowData.put(columnName, value);
        } else {
            pendingUpdates.put(columnName, value);
        }
    }
    
    private void checkUpdatable() throws SQLException {
        try {
            if (isClosed()) {
                throw new SQLException("ResultSet is closed");
            }
        } catch (SQLException e) {
            throw new SQLException("ResultSet is closed");
        }
        if (!isUpdatable) {
            throw new SQLException("ResultSet is not updatable");
        }
    }
    
    private String getColumnNameFromIndex(int columnIndex) throws SQLException {
        ResultSetMetaData meta = getMetaData();
        if (columnIndex < 1 || columnIndex > meta.getColumnCount()) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        return meta.getColumnName(columnIndex);
    }
    
    private Map<String, AttributeValue> extractPrimaryKeyValues() throws SQLException {
        Map<String, AttributeValue> keyValues = new HashMap<>();
        Map<String, AttributeValue> currentItem = getCurrentItem();
        
        if (currentItem == null) {
            throw new SQLException("No current row");
        }
        
        for (String keyColumn : primaryKeyColumns.keySet()) {
            AttributeValue value = currentItem.get(keyColumn);
            if (value == null) {
                throw new SQLException("Primary key column '" + keyColumn + "' has no value");
            }
            keyValues.put(keyColumn, value);
        }
        
        return keyValues;
    }
    
    private void appendAttributeValue(StringBuilder sql, AttributeValue value) {
        if (value.s() != null) {
            sql.append("'").append(value.s().replace("'", "''")).append("'");
        } else if (value.n() != null) {
            sql.append(value.n());
        } else if (value.bool() != null) {
            sql.append(value.bool());
        } else if (value.nul() != null && value.nul()) {
            sql.append("NULL");
        } else if (value.b() != null) {
            sql.append("'").append(value.b().asUtf8String()).append("'");
        } else {
            sql.append("NULL");
        }
    }
    
    private AttributeValue convertToAttributeValue(Object value) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        } else if (value instanceof String) {
            return AttributeValue.builder().s((String) value).build();
        } else if (value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        } else if (value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean) value).build();
        } else if (value instanceof byte[]) {
            return AttributeValue.builder()
                .b(software.amazon.awssdk.core.SdkBytes.fromByteArray((byte[]) value))
                .build();
        } else {
            return AttributeValue.builder().s(value.toString()).build();
        }
    }
    
    // Package-private methods to access parent state
    
    Map<String, AttributeValue> getCurrentItem() {
        return currentItem;
    }
    
    void updateCurrentItem(Map<String, AttributeValue> newItem) {
        currentItem = newItem;
    }
}