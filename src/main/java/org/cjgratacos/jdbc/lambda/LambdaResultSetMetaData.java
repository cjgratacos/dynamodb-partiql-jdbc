package org.cjgratacos.jdbc.lambda;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * ResultSetMetaData implementation for Lambda function results.
 * 
 * <p>This class provides metadata about the columns in a Lambda result set,
 * including column names, types, and other properties.
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class LambdaResultSetMetaData implements ResultSetMetaData {
    
    private final List<String> columnNames;
    private final Map<String, Integer> columnIndices;
    private final Map<String, Class<?>> columnTypes;
    
    /**
     * Creates metadata from Lambda result data.
     * 
     * @param data the result data
     */
    public LambdaResultSetMetaData(List<Map<String, Object>> data) {
        this.columnNames = new ArrayList<>();
        this.columnIndices = new HashMap<>();
        this.columnTypes = new HashMap<>();
        
        if (!data.isEmpty()) {
            // Extract column names from the first row
            Map<String, Object> firstRow = data.get(0);
            int index = 1;
            for (String columnName : firstRow.keySet()) {
                columnNames.add(columnName);
                columnIndices.put(columnName, index++);
            }
            
            // Infer column types from all rows
            for (Map<String, Object> row : data) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String columnName = entry.getKey();
                    Object value = entry.getValue();
                    if (value != null && !columnTypes.containsKey(columnName)) {
                        columnTypes.put(columnName, value.getClass());
                    }
                }
            }
        }
    }
    
    /**
     * Gets the column index for a column name.
     * 
     * @param columnName the column name
     * @return the column index (1-based)
     * @throws SQLException if column not found
     */
    public int getColumnIndex(String columnName) throws SQLException {
        Integer index = columnIndices.get(columnName);
        if (index == null) {
            throw new SQLException("Column not found: " + columnName);
        }
        return index;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }
    
    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }
    
    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }
    
    @Override
    public boolean isSigned(int column) throws SQLException {
        Class<?> type = getColumnClass(column);
        return Number.class.isAssignableFrom(type);
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 100; // Default display size
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        validateColumnIndex(column);
        return columnNames.get(column - 1);
    }
    
    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }
    
    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }
    
    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        Class<?> type = getColumnClass(column);
        
        if (String.class.equals(type)) {
            return Types.VARCHAR;
        } else if (Integer.class.equals(type) || int.class.equals(type)) {
            return Types.INTEGER;
        } else if (Long.class.equals(type) || long.class.equals(type)) {
            return Types.BIGINT;
        } else if (Double.class.equals(type) || double.class.equals(type)) {
            return Types.DOUBLE;
        } else if (Float.class.equals(type) || float.class.equals(type)) {
            return Types.FLOAT;
        } else if (Boolean.class.equals(type) || boolean.class.equals(type)) {
            return Types.BOOLEAN;
        } else if (java.sql.Date.class.equals(type)) {
            return Types.DATE;
        } else if (java.sql.Time.class.equals(type)) {
            return Types.TIME;
        } else if (java.sql.Timestamp.class.equals(type)) {
            return Types.TIMESTAMP;
        } else if (byte[].class.equals(type)) {
            return Types.BINARY;
        } else if (java.math.BigDecimal.class.equals(type)) {
            return Types.DECIMAL;
        } else {
            return Types.OTHER;
        }
    }
    
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.FLOAT:
                return "FLOAT";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "BINARY";
            case Types.DECIMAL:
                return "DECIMAL";
            default:
                return "OTHER";
        }
    }
    
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }
    
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }
    
    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnClass(column).getName();
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    private void validateColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Invalid column index: " + column);
        }
    }
    
    private Class<?> getColumnClass(int column) throws SQLException {
        String columnName = getColumnName(column);
        Class<?> type = columnTypes.get(columnName);
        return type != null ? type : Object.class;
    }
}