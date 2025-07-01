package org.cjgratacos.jdbc.lambda;

import java.sql.*;
import java.util.Properties;

/**
 * Example demonstrating Lambda integration with DynamoDB JDBC driver.
 * 
 * This is a documentation example showing how to use Lambda functions
 * as stored procedures with the DynamoDB JDBC driver.
 */
public class LambdaIntegrationExample {
    
    public static void main(String[] args) throws SQLException {
        // Example 1: Basic Lambda configuration
        basicLambdaExample();
        
        // Example 2: Lambda with separate credentials
        separateCredentialsExample();
        
        // Example 3: Lambda with environment variables
        environmentVariablesExample();
    }
    
    /**
     * Basic Lambda integration example.
     */
    private static void basicLambdaExample() throws SQLException {
        Properties props = new Properties();
        props.setProperty("region", "us-east-1");
        props.setProperty("credentialsType", "DEFAULT");
        
        // Enable Lambda support
        props.setProperty("lambda.region", "us-east-1");
        props.setProperty("lambda.allowedFunctions", "calculateMetrics,processOrder,getUsers");
        
        String url = "jdbc:dynamodb:partiql:";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Call a Lambda function with parameters
            CallableStatement cs = conn.prepareCall("{call lambda:processOrder(?, ?, ?)}");
            cs.setString(1, "order123");
            cs.setDouble(2, 99.99);
            cs.registerOutParameter(3, Types.VARCHAR);
            
            cs.execute();
            
            String result = cs.getString(3);
            System.out.println("Order processed: " + result);
        }
    }
    
    /**
     * Lambda with separate AWS credentials example.
     */
    private static void separateCredentialsExample() throws SQLException {
        Properties props = new Properties();
        // DynamoDB credentials
        props.setProperty("region", "us-east-1");
        props.setProperty("credentialsType", "PROFILE");
        props.setProperty("profileName", "dynamodb-user");
        
        // Lambda with different credentials
        props.setProperty("lambda.credentialsType", "ASSUME_ROLE");
        props.setProperty("lambda.assumeRoleArn", "arn:aws:iam::123456789012:role/LambdaExecutor");
        props.setProperty("lambda.externalId", "unique-external-id");
        props.setProperty("lambda.sessionDurationSeconds", "3600");
        props.setProperty("lambda.allowedFunctions", "processPayment,validateOrder");
        
        String url = "jdbc:dynamodb:partiql:";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            CallableStatement cs = conn.prepareCall("{call lambda:processPayment(?, ?)}");
            cs.setString(1, "payment123");
            cs.setBigDecimal(2, new java.math.BigDecimal("199.99"));
            
            boolean hasResultSet = cs.execute();
            
            if (hasResultSet) {
                ResultSet rs = cs.getResultSet();
                while (rs.next()) {
                    System.out.println("Payment ID: " + rs.getString("paymentId"));
                    System.out.println("Status: " + rs.getString("status"));
                }
            }
        }
    }
    
    /**
     * Lambda with environment variables example.
     */
    private static void environmentVariablesExample() throws SQLException {
        Properties props = new Properties();
        props.setProperty("region", "us-east-1");
        
        // Connection-level environment variables
        props.setProperty("lambda.env.ENVIRONMENT", "production");
        props.setProperty("lambda.env.API_VERSION", "v2");
        props.setProperty("lambda.env.TENANT_ID", "tenant-123");
        
        // Configuration parameters
        props.setProperty("lambda.config.timeout", "30000");
        props.setProperty("lambda.config.memorySize", "512");
        
        // Security settings
        props.setProperty("lambda.allowedFunctions", "generateReport,analyzeData");
        props.setProperty("lambda.allowedVariables", "PRIORITY,REGION_OVERRIDE,DEBUG");
        
        String url = "jdbc:dynamodb:partiql:";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            CallableStatement cs = conn.prepareCall("{call lambda:generateReport(?, ?)}");
            cs.setString(1, "monthly");
            cs.setDate(2, new Date(System.currentTimeMillis()));
            
            // Set statement-level environment variables
            if (cs instanceof LambdaCallableStatement) {
                LambdaCallableStatement lcs = (LambdaCallableStatement) cs;
                lcs.setEnvironmentVariable("PRIORITY", "high");
                lcs.setEnvironmentVariable("REGION_OVERRIDE", "eu-west-1");
            }
            
            ResultSet rs = cs.executeQuery();
            
            // Process report data
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            System.out.println("Report columns:");
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(metaData.getColumnName(i) + " (" + metaData.getColumnTypeName(i) + ")");
            }
            
            while (rs.next()) {
                // Process each row
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getObject(i) + "\t");
                }
                System.out.println();
            }
        }
    }
    
    /**
     * Example Lambda function JSON contract.
     * 
     * Request:
     * {
     *   "action": "processOrder",
     *   "parameters": {
     *     "param1": "order123",
     *     "param2": 99.99
     *   },
     *   "environment": {
     *     "ENVIRONMENT": "production",
     *     "TENANT_ID": "tenant-123"
     *   },
     *   "context": {
     *     "correlationId": "550e8400-e29b-41d4-a716-446655440000",
     *     "userId": "user@example.com",
     *     "requestTime": "2024-01-01T12:00:00Z",
     *     "jdbcVersion": "1.0.0"
     *   },
     *   "configuration": {
     *     "timeout": 30000,
     *     "memorySize": 512
     *   }
     * }
     * 
     * Response for scalar/update:
     * {
     *   "success": true,
     *   "result": 1,
     *   "outputParameters": {
     *     "param3": "processed"
     *   }
     * }
     * 
     * Response for ResultSet:
     * {
     *   "success": true,
     *   "resultSet": [
     *     {"id": "1", "name": "Item 1", "value": 100},
     *     {"id": "2", "name": "Item 2", "value": 200}
     *   ],
     *   "outputParameters": {}
     * }
     */
}