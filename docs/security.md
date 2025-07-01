# Security Guide

This guide covers security best practices and configurations for the DynamoDB PartiQL JDBC driver, including authentication, authorization, encryption, and secure coding practices.

## Authentication Methods

### 1. IAM Roles (Recommended)

Use IAM roles for EC2 instances or ECS tasks:

```java
Properties props = new Properties();
props.setProperty("region", "us-east-1");
props.setProperty("credentialsType", "DEFAULT");
// SDK automatically uses instance profile
```

### 2. AssumeRole for Cross-Account Access

Access resources in different AWS accounts:

```java
props.setProperty("credentialsType", "ASSUME_ROLE");
props.setProperty("assumeRoleArn", "arn:aws:iam::123456789012:role/DynamoDBAccess");
props.setProperty("externalId", "unique-external-id"); // Optional but recommended
props.setProperty("sessionDurationSeconds", "3600");
props.setProperty("sessionName", "dynamodb-jdbc-session");
```

### 3. AWS Profiles

Use named profiles for development:

```java
props.setProperty("credentialsType", "PROFILE");
props.setProperty("profileName", "dev-dynamodb");
```

### 4. Static Credentials (Not Recommended)

Only for testing - never use in production:

```java
// WARNING: Do not hardcode credentials
props.setProperty("credentialsType", "STATIC");
props.setProperty("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
props.setProperty("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
```

## IAM Permissions

### Minimum Required Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PartiQLSelect",
        "dynamodb:DescribeTable",
        "dynamodb:ListTables"
      ],
      "Resource": "*"
    }
  ]
}
```

### Full Access Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PartiQLSelect",
        "dynamodb:PartiQLInsert",
        "dynamodb:PartiQLUpdate",
        "dynamodb:PartiQLDelete",
        "dynamodb:DescribeTable",
        "dynamodb:ListTables",
        "dynamodb:TransactWriteItems",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/*"
    }
  ]
}
```

### Table-Specific Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PartiQLSelect",
        "dynamodb:PartiQLUpdate"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:123456789012:table/Users",
        "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
      ]
    }
  ]
}
```

## Encryption

### Encryption in Transit

All connections use HTTPS/TLS by default:

```java
// TLS is enforced automatically
// To use custom SSL settings:
props.setProperty("ssl.protocol", "TLSv1.2");
props.setProperty("ssl.provider", "SunJSSE");
```

### Encryption at Rest

Configure DynamoDB table encryption:

```sql
-- Tables should be created with encryption enabled
-- This is configured in AWS, not through JDBC
```

### Client-Side Encryption

For sensitive data, encrypt before storing:

```java
// Example: Encrypt sensitive fields
public class EncryptedPreparedStatement {
    private final PreparedStatement ps;
    private final Encryptor encryptor;
    
    public void setEncryptedString(int index, String value) throws SQLException {
        String encrypted = encryptor.encrypt(value);
        ps.setString(index, encrypted);
    }
}
```

## Credential Management

### 1. AWS Secrets Manager Integration

Store database credentials securely:

```java
public class SecureConnectionFactory {
    
    public Connection getConnection() throws SQLException {
        // Retrieve credentials from Secrets Manager
        String secret = getSecret("dynamodb-jdbc-credentials");
        JsonObject creds = parseJson(secret);
        
        Properties props = new Properties();
        props.setProperty("region", creds.getString("region"));
        props.setProperty("credentialsType", "STATIC");
        props.setProperty("accessKeyId", creds.getString("accessKey"));
        props.setProperty("secretAccessKey", creds.getString("secretKey"));
        
        return DriverManager.getConnection("jdbc:dynamodb:partiql:", props);
    }
}
```

### 2. Environment Variables

Use environment variables for configuration:

```java
Properties props = new Properties();
props.setProperty("region", System.getenv("AWS_REGION"));
props.setProperty("assumeRoleArn", System.getenv("DYNAMODB_ROLE_ARN"));
```

### 3. Credential Rotation

Implement automatic credential rotation:

```java
public class RotatingCredentialsProvider implements AwsCredentialsProvider {
    private volatile AwsCredentials current;
    private final ScheduledExecutorService executor;
    
    public RotatingCredentialsProvider() {
        this.executor = Executors.newSingleThreadScheduledExecutor();
        // Rotate every hour
        executor.scheduleAtFixedRate(this::rotateCredentials, 0, 1, TimeUnit.HOURS);
    }
    
    private void rotateCredentials() {
        // Fetch new credentials
        this.current = fetchNewCredentials();
    }
}
```

## Secure Coding Practices

### 1. SQL Injection Prevention

Always use prepared statements:

```java
// GOOD: Using prepared statements
PreparedStatement ps = conn.prepareStatement(
    "SELECT * FROM Users WHERE userId = ?"
);
ps.setString(1, userInput);

// BAD: String concatenation
Statement stmt = conn.createStatement();
stmt.executeQuery("SELECT * FROM Users WHERE userId = '" + userInput + "'");
```

### 2. Input Validation

Validate all user inputs:

```java
public class InputValidator {
    
    public static String validateUserId(String userId) throws ValidationException {
        if (userId == null || userId.isEmpty()) {
            throw new ValidationException("User ID cannot be empty");
        }
        
        // Allow only alphanumeric and dash
        if (!userId.matches("^[a-zA-Z0-9-]+$")) {
            throw new ValidationException("Invalid user ID format");
        }
        
        if (userId.length() > 128) {
            throw new ValidationException("User ID too long");
        }
        
        return userId;
    }
}
```

### 3. Error Handling

Don't expose sensitive information in errors:

```java
try {
    // Database operation
} catch (SQLException e) {
    // Log full error internally
    logger.error("Database error", e);
    
    // Return generic error to user
    throw new ApplicationException("Unable to process request");
}
```

## Lambda Security

### Function Access Control

Restrict Lambda function access:

```java
// Only allow specific functions
props.setProperty("lambda.allowedFunctions", "processOrder,validateUser");
props.setProperty("lambda.deniedFunctions", "deleteAll,dropTable");

// Validate function names
props.setProperty("lambda.functionNamePattern", "^[a-zA-Z][a-zA-Z0-9-]*$");
```

### Environment Variable Filtering

Control which variables can be passed:

```java
// Whitelist allowed variables
props.setProperty("lambda.allowedVariables", "ENVIRONMENT,DEBUG_LEVEL");

// Blacklist sensitive variables
props.setProperty("lambda.deniedVariables", "AWS_SECRET_ACCESS_KEY,DATABASE_PASSWORD");
```

### Cross-Account Lambda Execution

Secure cross-account access:

```java
// Use external ID for additional security
props.setProperty("lambda.assumeRoleArn", "arn:aws:iam::987654321098:role/LambdaExecutor");
props.setProperty("lambda.externalId", "unique-shared-secret");
props.setProperty("lambda.requireExternalId", "true");
```

## Network Security

### VPC Configuration

Run in a VPC for network isolation:

```java
// Configure VPC endpoint for DynamoDB
props.setProperty("vpc.endpoint", "vpce-1234567890abcdef0");
props.setProperty("vpc.privateConnection", "true");
```

### IP Whitelisting

Restrict access by IP (configured in AWS):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:*",
      "Resource": "*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": [
            "192.168.1.0/24",
            "10.0.0.0/8"
          ]
        }
      }
    }
  ]
}
```

## Audit and Compliance

### Enable CloudTrail Logging

Track all DynamoDB API calls:

```java
// Add tracking information to queries
props.setProperty("audit.userId", getCurrentUserId());
props.setProperty("audit.sessionId", getSessionId());
props.setProperty("audit.applicationName", "MyApp");
```

### Query Logging

Log queries for audit (be careful with sensitive data):

```java
public class AuditingConnection implements Connection {
    private final Connection delegate;
    private final AuditLogger auditLogger;
    
    @Override
    public Statement createStatement() throws SQLException {
        Statement stmt = delegate.createStatement();
        return new AuditingStatement(stmt, auditLogger);
    }
}
```

### Compliance Features

```java
// Enable FIPS endpoints
props.setProperty("fips.enabled", "true");

// Use AWS GovCloud regions
props.setProperty("region", "us-gov-west-1");

// Enable detailed metrics for compliance
props.setProperty("compliance.detailedMetrics", "true");
props.setProperty("compliance.metricRetention", "90"); // days
```

## Security Checklist

### Development

- [ ] Never hardcode credentials
- [ ] Use IAM roles when possible
- [ ] Validate all user inputs
- [ ] Use prepared statements
- [ ] Handle errors securely
- [ ] Enable query logging

### Production

- [ ] Use IAM roles or AssumeRole
- [ ] Implement credential rotation
- [ ] Enable encryption at rest
- [ ] Use VPC endpoints
- [ ] Configure minimum IAM permissions
- [ ] Enable CloudTrail logging
- [ ] Regular security audits
- [ ] Monitor for anomalies

### Lambda Specific

- [ ] Whitelist allowed functions
- [ ] Use separate IAM roles
- [ ] Validate function inputs
- [ ] Filter environment variables
- [ ] Use external IDs for cross-account
- [ ] Monitor invocation patterns

## Incident Response

### Suspected Compromise

1. **Immediate Actions**:

   ```java
   // Revoke credentials
   // Rotate all secrets
   // Review CloudTrail logs
   ```

2. **Investigation**:
   - Check CloudTrail for unauthorized API calls
   - Review application logs
   - Analyze query patterns

3. **Remediation**:
   - Update IAM policies
   - Rotate all credentials
   - Patch vulnerabilities
   - Update security groups

### Security Monitoring

```java
// Monitor for suspicious patterns
public class SecurityMonitor {
    
    public void checkQueryPatterns(QueryMetrics metrics) {
        // Alert on unusual query volume
        if (metrics.getQueriesPerSecond() > threshold) {
            alert("High query rate detected");
        }
        
        // Alert on failed authentication
        if (metrics.getAuthFailures() > 0) {
            alert("Authentication failures detected");
        }
        
        // Alert on permission errors
        if (metrics.getPermissionErrors() > 0) {
            alert("Permission errors detected");
        }
    }
}
```
