package org.cjgratacos.jdbc.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.cjgratacos.jdbc.DynamoDbPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

/**
 * CallableStatement implementation for invoking AWS Lambda functions as stored procedures.
 * 
 * <p>This class supports the JDBC CallableStatement interface for Lambda functions using
 * the syntax: {@code {call lambda:functionName(?, ?)}}
 * 
 * <p>Features:
 * <ul>
 *   <li>IN, OUT, and INOUT parameter support</li>
 *   <li>Environment variable passing</li>
 *   <li>Execution context metadata</li>
 *   <li>Result set and scalar return values</li>
 *   <li>Security filtering for functions and variables</li>
 * </ul>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class LambdaCallableStatement extends DynamoDbPreparedStatement implements CallableStatement {
    
    private static final Logger logger = LoggerFactory.getLogger(LambdaCallableStatement.class);
    private static final Pattern LAMBDA_CALL_PATTERN = Pattern.compile(
        "\\{\\s*call\\s+lambda:([\\w-]+)\\s*\\(([^)]*)\\)\\s*\\}",
        Pattern.CASE_INSENSITIVE
    );
    
    private final LambdaClient lambdaClient;
    private final LambdaConnectionConfig lambdaConfig;
    private final String functionName;
    private final int parameterCount;
    private final ObjectMapper objectMapper;
    
    // Parameter management
    private final Map<Integer, ParameterInfo> parameters = new HashMap<>();
    private final Map<Integer, Object> parameterValues = new HashMap<>(); // Input parameter values
    private final Map<Integer, Object> outputValues = new HashMap<>();
    private final Map<String, Integer> namedParameterIndices = new HashMap<>();
    
    // Variable management
    private final Map<String, String> statementEnvironmentVariables = new HashMap<>();
    private final Map<String, Object> executionContext = new HashMap<>();
    private String lambdaQualifier;
    private InvocationType invocationType;
    private LogType logType;
    
    // Result handling
    private ResultSet currentResultSet;
    private boolean hasResultSet;
    private int updateCount = -1;
    
    /**
     * Creates a new Lambda callable statement.
     * 
     * @param connection the database connection
     * @param sql the callable statement SQL
     * @param lambdaClient the Lambda client
     * @param lambdaConfig the Lambda configuration
     * @throws SQLException if the SQL cannot be parsed or is invalid
     */
    public LambdaCallableStatement(
            DynamoDbConnection connection,
            String sql,
            LambdaClient lambdaClient,
            LambdaConnectionConfig lambdaConfig) throws SQLException {
        
        super(connection, sql);
        this.lambdaClient = lambdaClient;
        this.lambdaConfig = lambdaConfig;
        this.objectMapper = new ObjectMapper();
        
        // Parse the callable statement
        Matcher matcher = LAMBDA_CALL_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new SQLException("Invalid Lambda callable statement syntax. Expected: {call lambda:functionName(?, ?)}");
        }
        
        this.functionName = matcher.group(1);
        String paramString = matcher.group(2).trim();
        
        // Validate function name against allowed/denied lists
        validateFunctionName(functionName);
        
        // Count parameters
        if (paramString.isEmpty()) {
            this.parameterCount = 0;
        } else {
            this.parameterCount = paramString.split(",").length;
        }
        
        // Initialize parameter info
        for (int i = 1; i <= parameterCount; i++) {
            parameters.put(i, new ParameterInfo(i));
        }
        
        // Set default invocation settings
        this.invocationType = InvocationType.fromValue(lambdaConfig.getInvocationType());
        this.logType = LogType.fromValue(lambdaConfig.getLogType());
        this.lambdaQualifier = lambdaConfig.getQualifier();
    }
    
    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        
        try {
            // Build the Lambda payload
            ObjectNode payload = buildPayload();
            
            // Build invocation request
            InvokeRequest.Builder requestBuilder = InvokeRequest.builder()
                .functionName(functionName)
                .invocationType(invocationType)
                .logType(logType)
                .payload(SdkBytes.fromUtf8String(objectMapper.writeValueAsString(payload)));
            
            if (lambdaQualifier != null) {
                requestBuilder.qualifier(lambdaQualifier);
            }
            
            // Invoke the Lambda function
            if (logger.isDebugEnabled()) {
                logger.debug("Invoking Lambda function: {} with payload: {}", functionName, payload);
            }
            
            InvokeResponse response = lambdaClient.invoke(requestBuilder.build());
            
            // Check for errors
            if (response.functionError() != null) {
                String errorMsg = "Lambda function error: " + response.functionError();
                if (response.payload() != null) {
                    try {
                        String payloadStr = response.payload().asUtf8String();
                        if (payloadStr != null && !payloadStr.isEmpty()) {
                            Map<String, Object> errorData = objectMapper.readValue(payloadStr, Map.class);
                            String errorMessage = (String) errorData.get("errorMessage");
                            if (errorMessage != null) {
                                errorMsg = "Lambda function error: " + errorMessage;
                            }
                        }
                    } catch (Exception e) {
                        // Ignore parsing errors
                    }
                }
                throw new SQLException(errorMsg);
            }
            
            // Process the response
            processResponse(response);
            
            return hasResultSet;
            
        } catch (SQLException e) {
            throw e; // Re-throw SQLException as-is
        } catch (Exception e) {
            throw new SQLException("Failed to execute Lambda function: " + functionName, e);
        }
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        if (!hasResultSet) {
            throw new SQLException("Lambda function did not return a result set");
        }
        return currentResultSet;
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        execute();
        if (hasResultSet) {
            throw new SQLException("Lambda function returned a result set, use executeQuery() instead");
        }
        return updateCount;
    }
    
    // OUT parameter registration methods
    
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        
        ParameterInfo param = parameters.get(parameterIndex);
        param.setMode(ParameterMode.OUT);
        param.setSqlType(sqlType);
    }
    
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
        parameters.get(parameterIndex).setScale(scale);
    }
    
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
        parameters.get(parameterIndex).setTypeName(typeName);
    }
    
    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        int index = getParameterIndex(parameterName);
        registerOutParameter(index, sqlType);
    }
    
    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        int index = getParameterIndex(parameterName);
        registerOutParameter(index, sqlType, scale);
    }
    
    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        int index = getParameterIndex(parameterName);
        registerOutParameter(index, sqlType, typeName);
    }
    
    // Parameter setting methods (override to track parameter mode and values)
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        super.setObject(parameterIndex, x);
        parameterValues.put(parameterIndex, x); // Track the value
        if (parameters.containsKey(parameterIndex)) {
            ParameterInfo param = parameters.get(parameterIndex);
            if (param.getMode() == ParameterMode.OUT) {
                param.setMode(ParameterMode.INOUT);
            }
        }
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        super.setString(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        super.setDouble(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        super.setInt(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        super.setLong(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        super.setBoolean(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        super.setBigDecimal(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        super.setTimestamp(parameterIndex, x);
        parameterValues.put(parameterIndex, x);
        checkParameterMode(parameterIndex);
    }
    
    private void checkParameterMode(int parameterIndex) {
        if (parameters.containsKey(parameterIndex)) {
            ParameterInfo param = parameters.get(parameterIndex);
            if (param.getMode() == ParameterMode.OUT) {
                param.setMode(ParameterMode.INOUT);
            }
        }
    }
    
    private Object getParameterValue(int parameterIndex) {
        return parameterValues.get(parameterIndex);
    }
    
    // OUT parameter retrieval methods
    
    @Override
    public boolean wasNull() throws SQLException {
        return lastReadValue == null;
    }
    
    private Object lastReadValue;
    
    @Override
    public String getString(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        return value != null ? value.toString() : null;
    }
    
    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        return Boolean.parseBoolean(value.toString());
    }
    
    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return (byte) getInt(parameterIndex);
    }
    
    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return (short) getInt(parameterIndex);
    }
    
    @Override
    public int getInt(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).intValue();
        return Integer.parseInt(value.toString());
    }
    
    @Override
    public long getLong(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }
    
    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return 0.0f;
        if (value instanceof Number) return ((Number) value).floatValue();
        return Float.parseFloat(value.toString());
    }
    
    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return 0.0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }
    
    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal value = getBigDecimal(parameterIndex);
        if (value != null) {
            value = value.setScale(scale, BigDecimal.ROUND_HALF_UP);
        }
        return value;
    }
    
    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        return new BigDecimal(value.toString());
    }
    
    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(getParameterIndex(parameterName));
    }
    
    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return null;
        if (value instanceof byte[]) return (byte[]) value;
        return value.toString().getBytes();
    }
    
    @Override
    public java.sql.Date getDate(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return null;
        if (value instanceof java.sql.Date) return (java.sql.Date) value;
        return java.sql.Date.valueOf(value.toString());
    }
    
    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return null;
        if (value instanceof Time) return (Time) value;
        return Time.valueOf(value.toString());
    }
    
    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        return Timestamp.valueOf(value.toString());
    }
    
    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);
        lastReadValue = value;
        return value;
    }
    
    // Named parameter methods
    
    @Override
    public String getString(String parameterName) throws SQLException {
        return getString(getParameterIndex(parameterName));
    }
    
    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(getParameterIndex(parameterName));
    }
    
    @Override
    public byte getByte(String parameterName) throws SQLException {
        return getByte(getParameterIndex(parameterName));
    }
    
    @Override
    public short getShort(String parameterName) throws SQLException {
        return getShort(getParameterIndex(parameterName));
    }
    
    @Override
    public int getInt(String parameterName) throws SQLException {
        return getInt(getParameterIndex(parameterName));
    }
    
    @Override
    public long getLong(String parameterName) throws SQLException {
        return getLong(getParameterIndex(parameterName));
    }
    
    @Override
    public float getFloat(String parameterName) throws SQLException {
        return getFloat(getParameterIndex(parameterName));
    }
    
    @Override
    public double getDouble(String parameterName) throws SQLException {
        return getDouble(getParameterIndex(parameterName));
    }
    
    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(getParameterIndex(parameterName));
    }
    
    @Override
    public java.sql.Date getDate(String parameterName) throws SQLException {
        return getDate(getParameterIndex(parameterName));
    }
    
    @Override
    public Time getTime(String parameterName) throws SQLException {
        return getTime(getParameterIndex(parameterName));
    }
    
    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(getParameterIndex(parameterName));
    }
    
    @Override
    public Object getObject(String parameterName) throws SQLException {
        return getObject(getParameterIndex(parameterName));
    }
    
    // Variable and context methods
    
    /**
     * Sets an environment variable for this Lambda invocation.
     * 
     * @param name the variable name
     * @param value the variable value
     * @throws SQLException if the variable is not allowed
     */
    public void setEnvironmentVariable(String name, String value) throws SQLException {
        validateVariableName(name);
        statementEnvironmentVariables.put(name, value);
    }
    
    /**
     * Sets multiple environment variables for this Lambda invocation.
     * 
     * @param env the environment variables
     * @throws SQLException if any variable is not allowed
     */
    public void setEnvironmentVariables(Map<String, String> env) throws SQLException {
        for (String name : env.keySet()) {
            validateVariableName(name);
        }
        statementEnvironmentVariables.putAll(env);
    }
    
    /**
     * Sets the execution context for this Lambda invocation.
     * 
     * @param context the execution context
     */
    public void setExecutionContext(Map<String, Object> context) {
        executionContext.putAll(context);
    }
    
    /**
     * Sets the Lambda qualifier (version or alias) for this invocation.
     * 
     * @param qualifier the qualifier
     */
    public void setQualifier(String qualifier) {
        this.lambdaQualifier = qualifier;
    }
    
    /**
     * Sets the invocation type.
     * 
     * @param invocationType the invocation type
     */
    public void setInvocationType(InvocationType invocationType) {
        this.invocationType = invocationType;
    }
    
    /**
     * Sets the log type.
     * 
     * @param logType the log type
     */
    public void setLogType(LogType logType) {
        this.logType = logType;
    }
    
    // Helper methods
    
    private void validateFunctionName(String functionName) throws SQLException {
        // Check allowed functions
        if (lambdaConfig.getAllowedFunctions() != null) {
            Set<String> allowed = new HashSet<>(Arrays.asList(lambdaConfig.getAllowedFunctions().split(",")));
            if (!allowed.contains(functionName)) {
                throw new SQLException("Lambda function '" + functionName + "' is not in the allowed list");
            }
        }
        
        // Check denied functions
        if (lambdaConfig.getDeniedFunctions() != null) {
            Set<String> denied = new HashSet<>(Arrays.asList(lambdaConfig.getDeniedFunctions().split(",")));
            if (denied.contains(functionName)) {
                throw new SQLException("Lambda function '" + functionName + "' is in the denied list");
            }
        }
    }
    
    private void validateVariableName(String variableName) throws SQLException {
        // Check allowed variables
        if (lambdaConfig.getAllowedVariables() != null) {
            Set<String> allowed = new HashSet<>(Arrays.asList(lambdaConfig.getAllowedVariables().split(",")));
            if (!allowed.contains(variableName)) {
                throw new SQLException("Variable '" + variableName + "' is not in the allowed list");
            }
        }
        
        // Check denied variables
        if (lambdaConfig.getDeniedVariables() != null) {
            Set<String> denied = new HashSet<>(Arrays.asList(lambdaConfig.getDeniedVariables().split(",")));
            if (denied.contains(variableName)) {
                throw new SQLException("Variable '" + variableName + "' is in the denied list");
            }
        }
    }
    
    private void validateParameterIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterCount) {
            throw new SQLException("Parameter index out of range: " + parameterIndex);
        }
    }
    
    private int getParameterIndex(String parameterName) throws SQLException {
        Integer index = namedParameterIndices.get(parameterName);
        if (index == null) {
            throw new SQLException("Unknown parameter name: " + parameterName);
        }
        return index;
    }
    
    private Object getOutputValue(int parameterIndex) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        
        ParameterInfo param = parameters.get(parameterIndex);
        if (param.getMode() != ParameterMode.OUT && param.getMode() != ParameterMode.INOUT) {
            throw new SQLException("Parameter " + parameterIndex + " is not an OUT parameter");
        }
        
        return outputValues.get(parameterIndex);
    }
    
    private ObjectNode buildPayload() throws Exception {
        ObjectNode payload = objectMapper.createObjectNode();
        
        // Add action (function name)
        payload.put("action", functionName);
        
        // Add parameters
        ObjectNode params = objectMapper.createObjectNode();
        for (int i = 1; i <= parameterCount; i++) {
            ParameterInfo paramInfo = parameters.get(i);
            if (paramInfo.getMode() != ParameterMode.OUT) {
                // We need to get the parameter value that was set via setXXX methods
                // For now, we'll store them in our own map in setObject override
                Object value = getParameterValue(i);
                if (value != null) {
                    params.putPOJO("param" + i, value);
                }
            }
        }
        payload.set("parameters", params);
        
        // Add environment variables
        ObjectNode env = objectMapper.createObjectNode();
        // Connection-level variables
        for (Map.Entry<String, String> entry : lambdaConfig.getEnvironmentVariables().entrySet()) {
            env.put(entry.getKey(), entry.getValue());
        }
        // Statement-level variables (override connection-level)
        for (Map.Entry<String, String> entry : statementEnvironmentVariables.entrySet()) {
            env.put(entry.getKey(), entry.getValue());
        }
        payload.set("environment", env);
        
        // Add execution context
        ObjectNode context = objectMapper.createObjectNode();
        for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
            context.putPOJO(entry.getKey(), entry.getValue());
        }
        // Add standard context
        context.put("jdbcVersion", "1.0.0");
        context.put("requestTime", new Timestamp(System.currentTimeMillis()).toString());
        payload.set("context", context);
        
        // Add configuration
        ObjectNode config = objectMapper.createObjectNode();
        for (Map.Entry<String, String> entry : lambdaConfig.getConfigurationParameters().entrySet()) {
            config.put(entry.getKey(), entry.getValue());
        }
        if (lambdaQualifier != null) {
            config.put("qualifier", lambdaQualifier);
        }
        payload.set("configuration", config);
        
        return payload;
    }
    
    private void processResponse(InvokeResponse response) throws Exception {
        String responseJson = response.payload().asUtf8String();
        
        if (responseJson == null || responseJson.trim().isEmpty()) {
            hasResultSet = false;
            updateCount = 0;
            return;
        }
        
        Map<String, Object> responseData = objectMapper.readValue(responseJson, Map.class);
        
        // Check for success
        Boolean success = (Boolean) responseData.get("success");
        if (success == null || !success) {
            String error = (String) responseData.get("error");
            throw new SQLException("Lambda function failed: " + (error != null ? error : "Unknown error"));
        }
        
        // Process output parameters
        Map<String, Object> outputParams = (Map<String, Object>) responseData.get("outputParameters");
        if (outputParams != null) {
            for (Map.Entry<String, Object> entry : outputParams.entrySet()) {
                String paramName = entry.getKey();
                if (paramName.startsWith("param")) {
                    try {
                        int index = Integer.parseInt(paramName.substring(5));
                        outputValues.put(index, entry.getValue());
                    } catch (NumberFormatException e) {
                        // Handle named parameters if implemented
                    }
                }
            }
        }
        
        // Process result set
        List<Map<String, Object>> resultSetData = (List<Map<String, Object>>) responseData.get("resultSet");
        if (resultSetData != null && !resultSetData.isEmpty()) {
            currentResultSet = new LambdaResultSet(resultSetData);
            hasResultSet = true;
            updateCount = -1;
        } else {
            // Process scalar result or update count
            Object result = responseData.get("result");
            if (result instanceof Number) {
                updateCount = ((Number) result).intValue();
                hasResultSet = false;
            } else if (result != null) {
                // Treat as single value result set
                Map<String, Object> singleRow = new HashMap<>();
                singleRow.put("result", result);
                currentResultSet = new LambdaResultSet(Collections.singletonList(singleRow));
                hasResultSet = true;
                updateCount = -1;
            } else {
                hasResultSet = false;
                updateCount = 0;
            }
        }
    }
    
    // Parameter info class
    
    private static class ParameterInfo {
        private final int index;
        private ParameterMode mode = ParameterMode.IN;
        private int sqlType;
        private int scale;
        private String typeName;
        
        public ParameterInfo(int index) {
            this.index = index;
        }
        
        public ParameterMode getMode() {
            return mode;
        }
        
        public void setMode(ParameterMode mode) {
            this.mode = mode;
        }
        
        public int getSqlType() {
            return sqlType;
        }
        
        public void setSqlType(int sqlType) {
            this.sqlType = sqlType;
        }
        
        public int getScale() {
            return scale;
        }
        
        public void setScale(int scale) {
            this.scale = scale;
        }
        
        public String getTypeName() {
            return typeName;
        }
        
        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }
    }
    
    private enum ParameterMode {
        IN, OUT, INOUT
    }
    
    // Unsupported methods (for now)
    
    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob parameters not supported");
    }
    
    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob parameters not supported");
    }
    
    // Additional CallableStatement methods with named parameters
    
    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setString(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getParameterIndex(parameterName), sqlType);
    }
    
    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(getParameterIndex(parameterName), sqlType, typeName);
    }
    
    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setDate(String parameterName, java.sql.Date x) throws SQLException {
        setDate(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setDate(String parameterName, java.sql.Date x, Calendar cal) throws SQLException {
        setDate(getParameterIndex(parameterName), x, cal);
    }
    
    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(getParameterIndex(parameterName), x, cal);
    }
    
    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(getParameterIndex(parameterName), x, cal);
    }
    
    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(getParameterIndex(parameterName), x, targetSqlType, scale);
    }
    
    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(getParameterIndex(parameterName), x, targetSqlType);
    }
    
    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(getParameterIndex(parameterName), x, length);
    }
    
    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(getParameterIndex(parameterName), x, length);
    }
    
    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(getParameterIndex(parameterName), x, length);
    }
    
    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(getParameterIndex(parameterName), x, length);
    }
    
    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(getParameterIndex(parameterName), x);
    }
    
    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(getParameterIndex(parameterName), reader, length);
    }
    
    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(getParameterIndex(parameterName), reader, length);
    }
    
    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(getParameterIndex(parameterName), reader);
    }
    
    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(getParameterIndex(parameterName), val);
    }
    
    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setString(parameterName, value);
    }
    
    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    // Additional getter methods with Calendar
    
    @Override
    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return getDate(parameterIndex);
    }
    
    @Override
    public java.sql.Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(parameterName);
    }
    
    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return getTime(parameterIndex);
    }
    
    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(parameterName);
    }
    
    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return getTimestamp(parameterIndex);
    }
    
    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(parameterName);
    }
    
    // Additional getters
    
    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }
    
    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }
    
    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return getString(parameterIndex);
    }
    
    @Override
    public String getNString(String parameterName) throws SQLException {
        return getString(parameterName);
    }
    
    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type conversion not supported");
    }
    
    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type conversion not supported");
    }
    
    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type mapping not supported");
    }
    
    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type mapping not supported");
    }
    
    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    /**
     * Checks if the statement is closed and throws an exception if it is.
     * 
     * @throws SQLException if the statement is closed
     */
    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }
}