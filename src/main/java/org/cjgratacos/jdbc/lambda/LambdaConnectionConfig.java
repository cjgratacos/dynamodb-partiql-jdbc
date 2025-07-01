package org.cjgratacos.jdbc.lambda;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Configuration for Lambda client connections.
 * 
 * <p>This class holds all Lambda-specific configuration including:
 * <ul>
 *   <li>AWS region and endpoint settings</li>
 *   <li>Credential provider configuration</li>
 *   <li>AssumeRole settings for cross-account access</li>
 *   <li>Environment variables and configuration parameters</li>
 *   <li>Security settings (allowed functions, variable filtering)</li>
 * </ul>
 * 
 * <h2>Configuration Properties:</h2>
 * <pre>
 * lambda.region=us-east-1
 * lambda.endpoint=https://lambda.amazonaws.com (optional)
 * lambda.credentialsType=ASSUME_ROLE
 * lambda.assumeRoleArn=arn:aws:iam::123456789012:role/LambdaExecutor
 * lambda.externalId=unique-external-id
 * lambda.sessionDurationSeconds=3600
 * lambda.allowedFunctions=func1,func2,func3
 * lambda.deniedFunctions=dangerousFunc
 * lambda.invocationType=RequestResponse
 * lambda.logType=None
 * lambda.timeout=30000
 * </pre>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class LambdaConnectionConfig {
    
    /**
     * Supported credential types for Lambda connections.
     */
    public enum CredentialsType {
        /** Use the same credentials as the DynamoDB connection */
        DEFAULT,
        /** Use static access key and secret key */
        STATIC,
        /** Use a named profile from AWS credentials file */
        PROFILE,
        /** Assume an IAM role for Lambda invocations */
        ASSUME_ROLE,
        /** Use instance profile credentials */
        INSTANCE_PROFILE
    }
    
    private final String region;
    private final String endpoint;
    private final CredentialsType credentialsType;
    private final AwsCredentialsProvider credentialsProvider;
    
    // AssumeRole configuration
    private final String assumeRoleArn;
    private final String externalId;
    private final String sessionName;
    private final Duration sessionDuration;
    
    // Static credentials configuration
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    
    // Profile configuration
    private final String profileName;
    
    // Environment variables for Lambda functions
    private final Map<String, String> environmentVariables;
    
    // Configuration parameters
    private final Map<String, String> configurationParameters;
    
    // Security settings
    private final String allowedFunctions;
    private final String deniedFunctions;
    private final String allowedVariables;
    private final String deniedVariables;
    
    // Invocation settings
    private final String invocationType;
    private final String logType;
    private final String qualifier;
    private final Integer timeout;
    
    /**
     * Creates a Lambda connection configuration from properties.
     * 
     * @param props the connection properties
     */
    public LambdaConnectionConfig(Properties props) {
        // Region configuration
        this.region = props.getProperty("lambda.region", props.getProperty("region"));
        this.endpoint = props.getProperty("lambda.endpoint");
        
        // Credentials configuration
        String credTypeStr = props.getProperty("lambda.credentialsType", "DEFAULT");
        this.credentialsType = CredentialsType.valueOf(credTypeStr.toUpperCase());
        
        // AssumeRole configuration
        this.assumeRoleArn = props.getProperty("lambda.assumeRoleArn");
        this.externalId = props.getProperty("lambda.externalId");
        this.sessionName = props.getProperty("lambda.sessionName", "dynamodb-jdbc-lambda");
        String sessionDurationStr = props.getProperty("lambda.sessionDurationSeconds", "3600");
        this.sessionDuration = Duration.ofSeconds(Long.parseLong(sessionDurationStr));
        
        // Static credentials
        this.accessKeyId = props.getProperty("lambda.accessKeyId");
        this.secretAccessKey = props.getProperty("lambda.secretAccessKey");
        this.sessionToken = props.getProperty("lambda.sessionToken");
        
        // Profile
        this.profileName = props.getProperty("lambda.profileName");
        
        // Security settings
        this.allowedFunctions = props.getProperty("lambda.allowedFunctions");
        this.deniedFunctions = props.getProperty("lambda.deniedFunctions");
        this.allowedVariables = props.getProperty("lambda.allowedVariables");
        this.deniedVariables = props.getProperty("lambda.deniedVariables");
        
        // Invocation settings
        this.invocationType = props.getProperty("lambda.invocationType", "RequestResponse");
        this.logType = props.getProperty("lambda.logType", "None");
        this.qualifier = props.getProperty("lambda.qualifier");
        String timeoutStr = props.getProperty("lambda.timeout");
        this.timeout = timeoutStr != null ? Integer.parseInt(timeoutStr) : null;
        
        // Parse environment variables
        this.environmentVariables = parseEnvironmentVariables(props);
        
        // Parse configuration parameters
        this.configurationParameters = parseConfigurationParameters(props);
        
        // Credentials provider will be set by the factory
        this.credentialsProvider = null;
    }
    
    private Map<String, String> parseEnvironmentVariables(Properties props) {
        Map<String, String> env = new HashMap<>();
        String prefix = "lambda.env.";
        
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String varName = key.substring(prefix.length());
                env.put(varName, props.getProperty(key));
            }
        }
        
        return env;
    }
    
    private Map<String, String> parseConfigurationParameters(Properties props) {
        Map<String, String> config = new HashMap<>();
        String prefix = "lambda.config.";
        
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String paramName = key.substring(prefix.length());
                config.put(paramName, props.getProperty(key));
            }
        }
        
        return config;
    }
    
    /**
     * Checks if Lambda support is enabled.
     * 
     * @param props the connection properties
     * @return true if Lambda support is enabled
     */
    public static boolean isLambdaEnabled(Properties props) {
        // Lambda is enabled if any Lambda-specific property is set
        return props.stringPropertyNames().stream()
            .anyMatch(key -> key.startsWith("lambda."));
    }
    
    // Getters
    
    /**
     * Gets the AWS region for Lambda invocations.
     * 
     * @return the AWS region
     */
    public String getRegion() {
        return region;
    }
    
    /**
     * Gets the Lambda endpoint URL if configured.
     * 
     * @return the endpoint URL or null if not configured
     */
    public String getEndpoint() {
        return endpoint;
    }
    
    /**
     * Gets the credentials type for Lambda authentication.
     * 
     * @return the credentials type
     */
    public CredentialsType getCredentialsType() {
        return credentialsType;
    }
    
    /**
     * Gets the AWS credentials provider for Lambda authentication.
     * 
     * @return the credentials provider or null if not configured
     */
    public AwsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }
    
    /**
     * Gets the ARN of the role to assume for Lambda invocations.
     * 
     * @return the role ARN or null if not configured
     */
    public String getAssumeRoleArn() {
        return assumeRoleArn;
    }
    
    /**
     * Gets the external ID for role assumption.
     * 
     * @return the external ID or null if not configured
     */
    public String getExternalId() {
        return externalId;
    }
    
    /**
     * Gets the session name for role assumption.
     * 
     * @return the session name or null if not configured
     */
    public String getSessionName() {
        return sessionName;
    }
    
    /**
     * Gets the session duration for role assumption.
     * 
     * @return the session duration
     */
    public Duration getSessionDuration() {
        return sessionDuration;
    }
    
    /**
     * Gets the AWS access key ID for static credentials.
     * 
     * @return the access key ID or null if not configured
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }
    
    /**
     * Gets the AWS secret access key for static credentials.
     * 
     * @return the secret access key or null if not configured
     */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }
    
    /**
     * Gets the AWS session token for temporary credentials.
     * 
     * @return the session token or null if not configured
     */
    public String getSessionToken() {
        return sessionToken;
    }
    
    /**
     * Gets the AWS profile name for profile credentials.
     * 
     * @return the profile name or null if not configured
     */
    public String getProfileName() {
        return profileName;
    }
    
    /**
     * Gets the environment variables to pass to Lambda functions.
     * 
     * @return the environment variables map
     */
    public Map<String, String> getEnvironmentVariables() {
        return new HashMap<>(environmentVariables);
    }
    
    /**
     * Gets additional configuration parameters for Lambda functions.
     * 
     * @return the configuration parameters map
     */
    public Map<String, String> getConfigurationParameters() {
        return new HashMap<>(configurationParameters);
    }
    
    /**
     * Gets the comma-separated list of allowed Lambda functions.
     * 
     * @return the allowed functions list or null if not configured
     */
    public String getAllowedFunctions() {
        return allowedFunctions;
    }
    
    /**
     * Gets the comma-separated list of denied Lambda functions.
     * 
     * @return the denied functions list or null if not configured
     */
    public String getDeniedFunctions() {
        return deniedFunctions;
    }
    
    /**
     * Gets the comma-separated list of allowed environment variables.
     * 
     * @return the allowed variables list or null if not configured
     */
    public String getAllowedVariables() {
        return allowedVariables;
    }
    
    /**
     * Gets the comma-separated list of denied environment variables.
     * 
     * @return the denied variables list or null if not configured
     */
    public String getDeniedVariables() {
        return deniedVariables;
    }
    
    /**
     * Gets the invocation type for Lambda functions.
     * 
     * @return the invocation type (default: RequestResponse)
     */
    public String getInvocationType() {
        return invocationType;
    }
    
    /**
     * Gets the log type for Lambda function invocations.
     * 
     * @return the log type (default: None)
     */
    public String getLogType() {
        return logType;
    }
    
    /**
     * Gets the Lambda function qualifier (version or alias).
     * 
     * @return the qualifier or null if not configured
     */
    public String getQualifier() {
        return qualifier;
    }
    
    /**
     * Gets the timeout for Lambda function invocations in milliseconds.
     * 
     * @return the timeout or null if not configured
     */
    public Integer getTimeout() {
        return timeout;
    }
}