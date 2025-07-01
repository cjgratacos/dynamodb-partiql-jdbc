package org.cjgratacos.jdbc.lambda;

import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Factory for creating Lambda clients with various credential configurations.
 * 
 * <p>This factory supports multiple authentication methods:
 * <ul>
 *   <li>DEFAULT - Use the same credentials as DynamoDB</li>
 *   <li>STATIC - Use provided access key and secret key</li>
 *   <li>PROFILE - Use a named AWS profile</li>
 *   <li>ASSUME_ROLE - Assume an IAM role for Lambda invocations</li>
 *   <li>INSTANCE_PROFILE - Use EC2 instance profile credentials</li>
 * </ul>
 * 
 * @author CJ Gratacos
 * @since 1.0
 */
public class LambdaClientFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(LambdaClientFactory.class);
    
    /**
     * Creates a Lambda client based on the provided configuration.
     * 
     * @param config the Lambda connection configuration
     * @param defaultCredentialsProvider the default credentials provider from DynamoDB connection
     * @return a configured Lambda client
     * @throws SQLException if the client cannot be created
     */
    public static LambdaClient createClient(
            LambdaConnectionConfig config,
            AwsCredentialsProvider defaultCredentialsProvider) throws SQLException {
        
        if (logger.isInfoEnabled()) {
            logger.info("Creating Lambda client with credentials type: {}", config.getCredentialsType());
        }
        
        try {
            AwsCredentialsProvider credentialsProvider;
            
            switch (config.getCredentialsType()) {
                case ASSUME_ROLE:
                    credentialsProvider = createAssumeRoleProvider(config);
                    break;
                    
                case STATIC:
                    credentialsProvider = createStaticCredentialsProvider(config);
                    break;
                    
                case PROFILE:
                    credentialsProvider = createProfileProvider(config);
                    break;
                    
                case INSTANCE_PROFILE:
                    credentialsProvider = InstanceProfileCredentialsProvider.create();
                    break;
                    
                case DEFAULT:
                default:
                    credentialsProvider = defaultCredentialsProvider != null 
                        ? defaultCredentialsProvider 
                        : DefaultCredentialsProvider.builder().build();
                    break;
            }
            
            // Build Lambda client
            var builder = LambdaClient.builder()
                .credentialsProvider(credentialsProvider);
            
            // Configure region
            if (config.getRegion() != null) {
                builder.region(Region.of(config.getRegion()));
            }
            
            // Configure endpoint if provided
            if (config.getEndpoint() != null && !config.getEndpoint().trim().isEmpty()) {
                builder.endpointOverride(URI.create(config.getEndpoint()));
            }
            
            return builder.build();
            
        } catch (Exception e) {
            throw new SQLException("Failed to create Lambda client", e);
        }
    }
    
    /**
     * Creates a Lambda client from connection properties.
     * 
     * @param props the connection properties
     * @param defaultCredentialsProvider the default credentials provider
     * @return a configured Lambda client
     * @throws SQLException if the client cannot be created
     */
    public static LambdaClient createClient(
            Properties props,
            AwsCredentialsProvider defaultCredentialsProvider) throws SQLException {
        
        LambdaConnectionConfig config = new LambdaConnectionConfig(props);
        return createClient(config, defaultCredentialsProvider);
    }
    
    private static AwsCredentialsProvider createAssumeRoleProvider(LambdaConnectionConfig config) 
            throws SQLException {
        
        if (config.getAssumeRoleArn() == null || config.getAssumeRoleArn().trim().isEmpty()) {
            throw new SQLException("AssumeRole ARN is required for ASSUME_ROLE credentials type");
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Creating AssumeRole credentials provider for role: {}", config.getAssumeRoleArn());
        }
        
        // Create STS client
        StsClient stsClient = StsClient.create();
        
        try {
            // Build assume role request
            AssumeRoleRequest.Builder requestBuilder = AssumeRoleRequest.builder()
                .roleArn(config.getAssumeRoleArn())
                .roleSessionName(config.getSessionName())
                .durationSeconds((int) config.getSessionDuration().getSeconds());
            
            if (config.getExternalId() != null && !config.getExternalId().trim().isEmpty()) {
                requestBuilder.externalId(config.getExternalId());
            }
            
            // Assume the role
            AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());
            Credentials credentials = response.credentials();
            
            // Create session credentials
            AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken()
            );
            
            return StaticCredentialsProvider.create(sessionCredentials);
            
        } finally {
            stsClient.close();
        }
    }
    
    private static AwsCredentialsProvider createStaticCredentialsProvider(LambdaConnectionConfig config) 
            throws SQLException {
        
        if (config.getAccessKeyId() == null || config.getSecretAccessKey() == null) {
            throw new SQLException("Access key ID and secret access key are required for STATIC credentials type");
        }
        
        if (config.getSessionToken() != null && !config.getSessionToken().trim().isEmpty()) {
            // Session credentials
            return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    config.getAccessKeyId(),
                    config.getSecretAccessKey(),
                    config.getSessionToken()
                )
            );
        } else {
            // Basic credentials
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    config.getAccessKeyId(),
                    config.getSecretAccessKey()
                )
            );
        }
    }
    
    private static AwsCredentialsProvider createProfileProvider(LambdaConnectionConfig config) 
            throws SQLException {
        
        if (config.getProfileName() == null || config.getProfileName().trim().isEmpty()) {
            throw new SQLException("Profile name is required for PROFILE credentials type");
        }
        
        return ProfileCredentialsProvider.create(config.getProfileName());
    }
}