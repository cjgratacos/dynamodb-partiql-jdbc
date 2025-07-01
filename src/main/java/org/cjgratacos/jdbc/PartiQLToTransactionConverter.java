package org.cjgratacos.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts PartiQL DML statements to DynamoDB transaction operations.
 * 
 * <p>This class parses INSERT, UPDATE, and DELETE statements and extracts
 * the necessary information to create transaction items.
 * 
 * <p>Limitations:
 * <ul>
 *   <li>Only supports simple DML operations</li>
 *   <li>Does not support complex expressions or functions</li>
 *   <li>UPDATE requires explicit key conditions</li>
 * </ul>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class PartiQLToTransactionConverter {
    
    /**
     * Creates a new PartiQLToTransactionConverter.
     */
    public PartiQLToTransactionConverter() {
        // Default constructor
    }
    
    private static final Logger logger = LoggerFactory.getLogger(PartiQLToTransactionConverter.class);
    
    // Pattern for INSERT statements: INSERT INTO table VALUE { 'key': 'value', ... }
    private static final Pattern INSERT_PATTERN = Pattern.compile(
        "INSERT\\s+INTO\\s+\"?([^\"\\s]+)\"?\\s+VALUE\\s*\\{(.+?)\\}",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    
    // Pattern for DELETE statements: DELETE FROM table WHERE key = value
    private static final Pattern DELETE_PATTERN = Pattern.compile(
        "DELETE\\s+FROM\\s+\"?([^\"\\s]+)\"?\\s+WHERE\\s+(.+)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    
    // Pattern for UPDATE statements: UPDATE table SET ... WHERE key = value
    private static final Pattern UPDATE_PATTERN = Pattern.compile(
        "UPDATE\\s+\"?([^\"\\s]+)\"?\\s+SET\\s+(.+)\\s+WHERE\\s+(.+)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    
    // Pattern for parsing key-value pairs in JSON-like format
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile(
        "'([^']+)'\\s*:\\s*(?:'([^']*)'|(\\d+(?:\\.\\d+)?)|true|false|null)"
    );
    
    // Pattern for parsing WHERE conditions
    private static final Pattern WHERE_CONDITION_PATTERN = Pattern.compile(
        "\"?([^\"\\s=]+)\"?\\s*=\\s*(?:'([^']*)'|(\\d+(?:\\.\\d+)?))"
    );
    
    // Pattern for parsing SET expressions
    private static final Pattern SET_EXPRESSION_PATTERN = Pattern.compile(
        "\"?([^\"\\s=]+)\"?\\s*=\\s*(?:'([^']*)'|(\\d+(?:\\.\\d+)?))"
    );
    
    /**
     * Represents a parsed DML operation.
     */
    public static class DMLOperation {
        private final DMLType type;
        private final String tableName;
        private final Map<String, AttributeValue> item;
        private final Map<String, AttributeValue> key;
        private final String updateExpression;
        private final Map<String, String> expressionAttributeNames;
        private final Map<String, AttributeValue> expressionAttributeValues;
        
        private DMLOperation(DMLType type, String tableName) {
            this.type = type;
            this.tableName = tableName;
            this.item = new HashMap<>();
            this.key = new HashMap<>();
            this.updateExpression = null;
            this.expressionAttributeNames = null;
            this.expressionAttributeValues = null;
        }
        
        private DMLOperation(DMLType type, String tableName, Map<String, AttributeValue> item) {
            this.type = type;
            this.tableName = tableName;
            this.item = item;
            this.key = null;
            this.updateExpression = null;
            this.expressionAttributeNames = null;
            this.expressionAttributeValues = null;
        }
        
        private DMLOperation(DMLType type, String tableName, Map<String, AttributeValue> key,
                           String updateExpression, Map<String, String> names, 
                           Map<String, AttributeValue> values) {
            this.type = type;
            this.tableName = tableName;
            this.item = null;
            this.key = key;
            this.updateExpression = updateExpression;
            this.expressionAttributeNames = names;
            this.expressionAttributeValues = values;
        }
        
        /**
         * Gets the DML operation type.
         *
         * @return the operation type
         */
        public DMLType getType() { return type; }
        
        /**
         * Gets the table name.
         *
         * @return the table name
         */
        public String getTableName() { return tableName; }
        
        /**
         * Gets the item for INSERT operations.
         *
         * @return the item attributes
         */
        public Map<String, AttributeValue> getItem() { return item; }
        
        /**
         * Gets the key for UPDATE/DELETE operations.
         *
         * @return the key attributes
         */
        public Map<String, AttributeValue> getKey() { return key; }
        
        /**
         * Gets the update expression for UPDATE operations.
         *
         * @return the update expression
         */
        public String getUpdateExpression() { return updateExpression; }
        
        /**
         * Gets the expression attribute names for UPDATE operations.
         *
         * @return the expression attribute names
         */
        public Map<String, String> getExpressionAttributeNames() { return expressionAttributeNames; }
        
        /**
         * Gets the expression attribute values for UPDATE operations.
         *
         * @return the expression attribute values
         */
        public Map<String, AttributeValue> getExpressionAttributeValues() { return expressionAttributeValues; }
    }
    
    /**
     * Enumeration of supported DML operation types.
     */
    public enum DMLType {
        /** Insert operation */
        INSERT,
        /** Update operation */
        UPDATE,
        /** Delete operation */
        DELETE
    }
    
    /**
     * Parses a PartiQL DML statement and returns the operation details.
     * 
     * @param sql the SQL statement to parse
     * @return the parsed DML operation, or null if not a supported DML statement
     * @throws SQLException if the statement cannot be parsed
     */
    public DMLOperation parseDMLStatement(String sql) throws SQLException {
        if (sql == null || sql.trim().isEmpty()) {
            return null;
        }
        
        sql = sql.trim();
        
        // Try INSERT
        Matcher insertMatcher = INSERT_PATTERN.matcher(sql);
        if (insertMatcher.matches()) {
            return parseInsert(insertMatcher);
        }
        
        // Try DELETE
        Matcher deleteMatcher = DELETE_PATTERN.matcher(sql);
        if (deleteMatcher.matches()) {
            return parseDelete(deleteMatcher);
        }
        
        // Try UPDATE
        Matcher updateMatcher = UPDATE_PATTERN.matcher(sql);
        if (updateMatcher.matches()) {
            return parseUpdate(updateMatcher);
        }
        
        // Not a DML statement we can handle in transactions
        return null;
    }
    
    private DMLOperation parseInsert(Matcher matcher) throws SQLException {
        String tableName = matcher.group(1);
        String itemJson = matcher.group(2);
        
        Map<String, AttributeValue> item = parseJsonLikeItem(itemJson);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Parsed INSERT: table={}, item={}", tableName, item);
        }
        
        return new DMLOperation(DMLType.INSERT, tableName, item);
    }
    
    private DMLOperation parseDelete(Matcher matcher) throws SQLException {
        String tableName = matcher.group(1);
        String whereClause = matcher.group(2);
        
        Map<String, AttributeValue> key = parseWhereClause(whereClause);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Parsed DELETE: table={}, key={}", tableName, key);
        }
        
        DMLOperation op = new DMLOperation(DMLType.DELETE, tableName);
        op.key.putAll(key);
        return op;
    }
    
    private DMLOperation parseUpdate(Matcher matcher) throws SQLException {
        String tableName = matcher.group(1);
        String setClause = matcher.group(2);
        String whereClause = matcher.group(3);
        
        Map<String, AttributeValue> key = parseWhereClause(whereClause);
        
        // Build update expression from SET clause
        StringBuilder updateExpression = new StringBuilder("SET ");
        Map<String, String> names = new HashMap<>();
        Map<String, AttributeValue> values = new HashMap<>();
        
        Matcher setMatcher = SET_EXPRESSION_PATTERN.matcher(setClause);
        int valueIndex = 0;
        boolean first = true;
        
        while (setMatcher.find()) {
            if (!first) {
                updateExpression.append(", ");
            }
            first = false;
            
            String attributeName = setMatcher.group(1);
            String stringValue = setMatcher.group(2);
            String numericValue = setMatcher.group(3);
            
            String nameKey = "#n" + valueIndex;
            String valueKey = ":v" + valueIndex;
            
            names.put(nameKey, attributeName);
            updateExpression.append(nameKey).append(" = ").append(valueKey);
            
            if (stringValue != null) {
                values.put(valueKey, AttributeValue.builder().s(stringValue).build());
            } else if (numericValue != null) {
                values.put(valueKey, AttributeValue.builder().n(numericValue).build());
            } else {
                values.put(valueKey, AttributeValue.builder().nul(true).build());
            }
            
            valueIndex++;
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Parsed UPDATE: table={}, key={}, expression={}", 
                tableName, key, updateExpression);
        }
        
        return new DMLOperation(DMLType.UPDATE, tableName, key, 
            updateExpression.toString(), names, values);
    }
    
    private Map<String, AttributeValue> parseJsonLikeItem(String json) throws SQLException {
        Map<String, AttributeValue> item = new HashMap<>();
        
        Matcher matcher = KEY_VALUE_PATTERN.matcher(json);
        while (matcher.find()) {
            String key = matcher.group(1);
            String stringValue = matcher.group(2);
            String numericValue = matcher.group(3);
            
            if (stringValue != null) {
                item.put(key, AttributeValue.builder().s(stringValue).build());
            } else if (numericValue != null) {
                item.put(key, AttributeValue.builder().n(numericValue).build());
            } else {
                // Handle true/false/null
                String fullMatch = matcher.group(0);
                if (fullMatch.contains("true") || fullMatch.contains("false")) {
                    item.put(key, AttributeValue.builder()
                        .bool(fullMatch.contains("true"))
                        .build());
                } else {
                    item.put(key, AttributeValue.builder().nul(true).build());
                }
            }
        }
        
        if (item.isEmpty()) {
            throw new SQLException("Failed to parse item from INSERT statement");
        }
        
        return item;
    }
    
    private Map<String, AttributeValue> parseWhereClause(String whereClause) throws SQLException {
        Map<String, AttributeValue> conditions = new HashMap<>();
        
        // Split by AND
        String[] andConditions = whereClause.split("\\s+AND\\s+", Pattern.CASE_INSENSITIVE);
        
        for (String condition : andConditions) {
            Matcher matcher = WHERE_CONDITION_PATTERN.matcher(condition.trim());
            if (matcher.matches()) {
                String attributeName = matcher.group(1);
                String stringValue = matcher.group(2);
                String numericValue = matcher.group(3);
                
                if (stringValue != null) {
                    conditions.put(attributeName, AttributeValue.builder().s(stringValue).build());
                } else if (numericValue != null) {
                    conditions.put(attributeName, AttributeValue.builder().n(numericValue).build());
                }
            }
        }
        
        if (conditions.isEmpty()) {
            throw new SQLException("Failed to parse WHERE clause: " + whereClause);
        }
        
        return conditions;
    }
    
    /**
     * Checks if a SQL statement is a DML statement that can be handled in a transaction.
     * 
     * @param sql the SQL statement
     * @return true if the statement is INSERT, UPDATE, or DELETE
     */
    public boolean isDMLStatement(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        
        String upperSql = sql.trim().toUpperCase();
        return upperSql.startsWith("INSERT") || 
               upperSql.startsWith("UPDATE") || 
               upperSql.startsWith("DELETE");
    }
}