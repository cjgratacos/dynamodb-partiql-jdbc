# Lambda Integration

The DynamoDB PartiQL JDBC driver supports AWS Lambda integration, allowing you to invoke Lambda functions as stored procedures through standard JDBC CallableStatement syntax.

## Overview

Lambda integration enables you to:

- Execute serverless functions for complex business logic
- Use separate AWS credentials for Lambda vs DynamoDB
- Pass parameters and environment variables to functions
- Retrieve results as JDBC ResultSets or scalar values
- Implement stored procedure-like functionality

## Basic Configuration

### Connection Properties

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");

// Enable Lambda support
props.setProperty("lambda.region", "us-east-1");
props.setProperty("lambda.allowedFunctions", "processOrder,calculateMetrics,getUsers");

// Optional: Separate credentials for Lambda
props.setProperty("lambda.credentialsType", "ASSUME_ROLE");
props.setProperty("lambda.assumeRoleArn", "arn:aws:iam::123456789012:role/LambdaExecutor");
props.setProperty("lambda.externalId", "unique-external-id");

Connection conn = DriverManager.getConnection("jdbc:dynamodb:partiql:", props);
```

### Configuration Options

| Property | Description | Default | Required |
|----------|-------------|---------|----------|
| `lambda.region` | AWS region for Lambda | Same as DynamoDB | No |
| `lambda.allowedFunctions` | Comma-separated list of allowed functions | None | Yes |
| `lambda.deniedFunctions` | Functions to explicitly deny | None | No |
| `lambda.credentialsType` | Credential type for Lambda | Same as DynamoDB | No |
| `lambda.endpoint` | Custom Lambda endpoint (for testing) | AWS default | No |
| `lambda.timeout` | Function invocation timeout (ms) | 30000 | No |
| `lambda.qualifier` | Lambda function version/alias | $LATEST | No |

## Usage Examples

### Basic Lambda Function Call

```java
// Call a Lambda function with IN and OUT parameters
CallableStatement cs = conn.prepareCall("{call lambda:processOrder(?, ?, ?)}");

// Set input parameters
cs.setString(1, "order123");        // Order ID
cs.setDouble(2, 99.99);            // Order amount

// Register output parameter
cs.registerOutParameter(3, Types.VARCHAR);

// Execute the function
cs.execute();

// Get the output
String confirmationNumber = cs.getString(3);
System.out.println("Order confirmed: " + confirmationNumber);
```

### Lambda Function Returning ResultSet

```java
// Call a function that returns multiple rows
CallableStatement cs = conn.prepareCall("{call lambda:getUsers(?)}");
cs.setString(1, "active");  // User status filter

// Execute and get ResultSet
ResultSet rs = cs.executeQuery();

// Process results
while (rs.next()) {
    System.out.println("User: " + rs.getString("name") + 
                      ", Email: " + rs.getString("email"));
}
```

### Using Environment Variables

```java
CallableStatement cs = conn.prepareCall("{call lambda:generateReport(?, ?)}");
cs.setString(1, "monthly");
cs.setDate(2, new Date(System.currentTimeMillis()));

// Cast to LambdaCallableStatement for additional features
if (cs instanceof LambdaCallableStatement) {
    LambdaCallableStatement lcs = (LambdaCallableStatement) cs;
    
    // Set environment variables for this invocation
    lcs.setEnvironmentVariable("PRIORITY", "high");
    lcs.setEnvironmentVariable("FORMAT", "PDF");
    lcs.setEnvironmentVariable("REGION_OVERRIDE", "eu-west-1");
}

cs.execute();
```

## Lambda Function Contract

### Request Format

Lambda functions receive a JSON request with this structure:

```json
{
  "action": "functionName",
  "parameters": {
    "param1": "value1",
    "param2": 123,
    "param3": null
  },
  "environment": {
    "ENV_VAR1": "value1",
    "ENV_VAR2": "value2"
  },
  "context": {
    "correlationId": "550e8400-e29b-41d4-a716-446655440000",
    "requestTime": "2024-01-01T12:00:00Z",
    "jdbcVersion": "1.0.0",
    "userId": "authenticated-user"
  },
  "configuration": {
    "timeout": 30000,
    "qualifier": "PROD"
  }
}
```

### Response Formats

#### Scalar Result Response

```json
{
  "success": true,
  "result": 1,
  "outputParameters": {
    "param3": "processed-value",
    "param4": 12345
  }
}
```

#### ResultSet Response

```json
{
  "success": true,
  "resultSet": [
    {"id": "1", "name": "John Doe", "email": "john@example.com", "status": "active"},
    {"id": "2", "name": "Jane Smith", "email": "jane@example.com", "status": "active"}
  ],
  "outputParameters": {}
}
```

#### Error Response

```json
{
  "success": false,
  "error": "Validation failed: Invalid user ID"
}
```

## Lambda Function Examples

### Node.js Lambda Function

```javascript
exports.handler = async (event) => {
    const { action, parameters, environment, context } = event;
    
    console.log(`Processing action: ${action}`);
    console.log(`Correlation ID: ${context.correlationId}`);
    
    try {
        switch (action) {
            case 'processOrder':
                return await processOrder(parameters);
            
            case 'getUsers':
                return await getUsers(parameters);
            
            case 'generateReport':
                return await generateReport(parameters, environment);
            
            default:
                return {
                    success: false,
                    error: `Unknown action: ${action}`
                };
        }
    } catch (error) {
        console.error('Lambda execution error:', error);
        return {
            success: false,
            error: error.message
        };
    }
};

async function processOrder(params) {
    const { param1: orderId, param2: amount } = params;
    
    // Business logic here
    const confirmationNumber = `CONF-${Date.now()}`;
    
    // Log for monitoring
    console.log(`Processed order ${orderId} for $${amount}`);
    
    return {
        success: true,
        result: 1,
        outputParameters: {
            param3: confirmationNumber
        }
    };
}

async function getUsers(params) {
    const { param1: status } = params;
    
    // Query users (example data)
    const users = await queryUsersByStatus(status);
    
    return {
        success: true,
        resultSet: users.map(user => ({
            id: user.id,
            name: user.name,
            email: user.email,
            status: user.status,
            lastLogin: user.lastLogin
        }))
    };
}

async function generateReport(params, env) {
    const { param1: reportType, param2: date } = params;
    const { PRIORITY, FORMAT } = env;
    
    console.log(`Generating ${PRIORITY} priority ${reportType} report in ${FORMAT} format`);
    
    // Generate report logic
    const reportData = await createReport(reportType, date, FORMAT);
    
    return {
        success: true,
        resultSet: reportData
    };
}
```

### Python Lambda Function

```python
import json
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Main Lambda handler for JDBC stored procedures"""
    
    action = event.get('action')
    parameters = event.get('parameters', {})
    environment = event.get('environment', {})
    jdbc_context = event.get('context', {})
    
    logger.info(f"Processing action: {action}")
    logger.info(f"Correlation ID: {jdbc_context.get('correlationId')}")
    
    try:
        if action == 'calculateMetrics':
            return calculate_metrics(parameters)
        
        elif action == 'processTransaction':
            return process_transaction(parameters, environment)
        
        elif action == 'getAnalytics':
            return get_analytics(parameters)
        
        else:
            return {
                'success': False,
                'error': f'Unknown action: {action}'
            }
            
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def calculate_metrics(params):
    """Calculate business metrics"""
    
    start_date = params.get('param1')
    end_date = params.get('param2')
    metric_type = params.get('param3')
    
    # Calculate metrics (example)
    total_sales = Decimal('45678.90')
    transaction_count = 1234
    
    return {
        'success': True,
        'result': 1,
        'outputParameters': {
            'param4': str(total_sales),
            'param5': transaction_count
        }
    }

def get_analytics(params):
    """Return analytics data as ResultSet"""
    
    category = params.get('param1')
    limit = int(params.get('param2', 100))
    
    # Generate analytics data
    analytics_data = []
    for i in range(min(limit, 10)):
        analytics_data.append({
            'date': datetime.now().isoformat(),
            'category': category,
            'metric': f'metric_{i}',
            'value': float(i * 100.5),
            'trend': 'up' if i % 2 == 0 else 'down'
        })
    
    return {
        'success': True,
        'resultSet': analytics_data
    }

# Helper for JSON serialization of Decimal
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
```

## Advanced Features

### Cross-Account Lambda Execution

```java
Properties props = new Properties();
// DynamoDB in account A
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "PROFILE");
props.setProperty("profileName", "account-a");

// Lambda in account B
props.setProperty("lambda.credentialsType", "ASSUME_ROLE");
props.setProperty("lambda.assumeRoleArn", "arn:aws:iam::ACCOUNT-B:role/CrossAccountLambda");
props.setProperty("lambda.externalId", "shared-secret");
props.setProperty("lambda.sessionDurationSeconds", "3600");
```

### Different Regions

```java
// DynamoDB in us-east-1, Lambda in eu-west-1
props.setProperty("region", "us-east-1");
props.setProperty("lambda.region", "eu-west-1");
```

### Asynchronous Invocation

```java
// Configure async invocation
props.setProperty("lambda.invocationType", "Event");  // Async
props.setProperty("lambda.logType", "Tail");         // Get logs

CallableStatement cs = conn.prepareCall("{call lambda:asyncProcess(?)}");
cs.setString(1, "large-dataset");

// Returns immediately
cs.execute();

// Check update count for async invocations
int statusCode = cs.getUpdateCount();  // 202 for accepted
```

### Lambda Qualifiers

```java
// Call specific version
props.setProperty("lambda.qualifier", "2");

// Call alias
props.setProperty("lambda.qualifier", "PROD");

// Use in SQL
CallableStatement cs = conn.prepareCall("{call lambda:myFunction:PROD(?)}");
```

## Security Configuration

### Function Allowlist

```java
// Only allow specific functions
props.setProperty("lambda.allowedFunctions", "func1,func2,func3");

// Wildcard patterns
props.setProperty("lambda.allowedFunctions", "prod-*,analytics-*");

// Deny specific functions
props.setProperty("lambda.deniedFunctions", "dangerous-func,admin-*");
```

### Environment Variable Filtering

```java
// Only allow specific environment variables
props.setProperty("lambda.allowedVariables", "ENV,DEBUG,REGION");

// Deny sensitive variables
props.setProperty("lambda.deniedVariables", "AWS_SECRET_ACCESS_KEY,PASSWORD,API_KEY");
```

### IAM Permissions Required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction",
        "lambda:GetFunction"
      ],
      "Resource": [
        "arn:aws:lambda:*:*:function:processOrder",
        "arn:aws:lambda:*:*:function:getUsers",
        "arn:aws:lambda:*:*:function:generateReport"
      ]
    }
  ]
}
```

## Error Handling

### Lambda Function Errors

```java
try {
    CallableStatement cs = conn.prepareCall("{call lambda:riskyFunction(?)}");
    cs.setString(1, "input");
    cs.execute();
    
} catch (SQLException e) {
    if (e.getMessage().contains("Lambda function error")) {
        // Function returned error response
        System.err.println("Business logic error: " + e.getMessage());
        
    } else if (e.getMessage().contains("ResourceNotFoundException")) {
        // Function doesn't exist
        System.err.println("Function not found");
        
    } else if (e.getMessage().contains("TooManyRequestsException")) {
        // Rate limited
        System.err.println("Rate limit exceeded, retry later");
        
    } else {
        throw e;  // Re-throw unexpected errors
    }
}
```

### Timeout Handling

```java
// Set custom timeout
props.setProperty("lambda.timeout", "60000");  // 60 seconds

try {
    cs.execute();
} catch (SQLException e) {
    if (e.getCause() instanceof java.util.concurrent.TimeoutException) {
        System.err.println("Lambda function timed out");
        // Consider async invocation for long-running functions
    }
}
```

## Performance Optimization

### Connection Pooling for Lambda Client

```java
// Lambda client is reused across connections
props.setProperty("lambda.client.maxConnections", "50");
props.setProperty("lambda.client.connectionTimeout", "10000");
props.setProperty("lambda.client.socketTimeout", "60000");
```

### Caching Function Metadata

```java
// Cache function configurations
props.setProperty("lambda.cache.enabled", "true");
props.setProperty("lambda.cache.ttl", "300000");  // 5 minutes
props.setProperty("lambda.cache.size", "100");
```

### Batch Processing

```java
// Process multiple items in single Lambda invocation
public void batchProcess(List<String> items) throws SQLException {
    CallableStatement cs = conn.prepareCall("{call lambda:batchProcessor(?)}");
    
    // Convert to JSON array
    String jsonArray = new ObjectMapper().writeValueAsString(items);
    cs.setString(1, jsonArray);
    
    ResultSet rs = cs.executeQuery();
    
    // Process batch results
    while (rs.next()) {
        String itemId = rs.getString("itemId");
        String status = rs.getString("status");
        System.out.println("Processed " + itemId + ": " + status);
    }
}
```

## Best Practices

### 1. Design for Idempotency

```java
// Include idempotency token
CallableStatement cs = conn.prepareCall("{call lambda:processPayment(?, ?, ?)}");
cs.setString(1, paymentId);
cs.setBigDecimal(2, amount);
cs.setString(3, UUID.randomUUID().toString());  // Idempotency token
```

### 2. Handle Cold Starts

```java
// Warm up Lambda functions
public void warmUpFunctions(List<String> functions) {
    for (String function : functions) {
        try {
            CallableStatement cs = conn.prepareCall(
                "{call lambda:" + function + "(?)}"
            );
            cs.setString(1, "warmup");
            cs.execute();
        } catch (SQLException e) {
            // Log but don't fail
            logger.warn("Failed to warm up " + function, e);
        }
    }
}
```

### 3. Monitor Performance

```java
// Track Lambda invocation metrics
long start = System.currentTimeMillis();

cs.execute();

long duration = System.currentTimeMillis() - start;

// Log slow invocations
if (duration > 5000) {
    logger.warn("Slow Lambda invocation: {} ms for {}", 
                duration, functionName);
}

// Update metrics
metricsCollector.recordLambdaInvocation(functionName, duration);
```

### 4. Use Appropriate Payload Sizes

```java
// Check payload size before invocation
public void validatePayloadSize(String payload) throws SQLException {
    int payloadSize = payload.getBytes(StandardCharsets.UTF_8).length;
    
    // Lambda synchronous payload limit
    if (payloadSize > 6 * 1024 * 1024) {
        throw new SQLException("Payload too large: " + payloadSize + " bytes");
    }
    
    // Warn for large payloads
    if (payloadSize > 1024 * 1024) {
        logger.warn("Large Lambda payload: {} MB", payloadSize / (1024.0 * 1024.0));
    }
}
```

## Testing

### Local Lambda Testing

```java
// Use local Lambda endpoint for testing
props.setProperty("lambda.endpoint", "http://localhost:3001");

// Or use LocalStack
props.setProperty("lambda.endpoint", "http://localhost:4566");
props.setProperty("lambda.region", "us-east-1");
```

### Mock Lambda Responses

```java
@Test
public void testLambdaIntegration() {
    // Set up mock Lambda endpoint
    props.setProperty("lambda.endpoint", mockServer.getUrl());
    
    // Configure mock response
    mockServer.when(request()
        .withPath("/2015-03-31/functions/processOrder/invocations"))
        .respond(response()
            .withBody("{\"success\":true,\"result\":1}"));
    
    // Test Lambda call
    CallableStatement cs = conn.prepareCall("{call lambda:processOrder(?)}");
    cs.setString(1, "test-order");
    
    boolean hasResultSet = cs.execute();
    assertFalse(hasResultSet);
    assertEquals(1, cs.getUpdateCount());
}
```

## Troubleshooting

### Function Not Found

```text
Error: Lambda function not found
Solutions:
1. Verify function name in allowedFunctions list
2. Check IAM permissions for lambda:InvokeFunction
3. Ensure function exists in specified region
4. Verify function ARN if using cross-account
```

### Permission Errors

```text
Error: Access denied invoking Lambda function
Solutions:
1. Check IAM role has lambda:InvokeFunction permission
2. Verify resource-based policy on Lambda function
3. For cross-account, check trust relationship
4. Ensure STS permissions for assume role
```

### Payload Issues

```text
Error: Payload too large
Solutions:
1. Reduce data size in parameters
2. Use S3 for large data and pass references
3. Consider asynchronous invocation
4. Implement pagination for large result sets
```
