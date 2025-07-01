package org.cjgratacos.jdbc.lambda;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.util.*;
import org.cjgratacos.jdbc.DynamoDbConnection;
import org.cjgratacos.jdbc.QueryMetrics;
import org.cjgratacos.jdbc.RetryHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

@ExtendWith(MockitoExtension.class)
class LambdaCallableStatementTest {
    
    @Mock
    private DynamoDbConnection mockConnection;
    
    @Mock
    private DynamoDbClient mockDynamoDbClient;
    
    @Mock
    private RetryHandler mockRetryHandler;
    
    @Mock
    private QueryMetrics mockQueryMetrics;
    
    @Mock
    private LambdaClient mockLambdaClient;
    
    @Mock
    private LambdaConnectionConfig mockLambdaConfig;
    
    private LambdaCallableStatement statement;
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @BeforeEach
    void setUp() throws SQLException {
        lenient().when(mockConnection.getDynamoDbClient()).thenReturn(mockDynamoDbClient);
        lenient().when(mockConnection.getRetryHandler()).thenReturn(mockRetryHandler);
        lenient().when(mockRetryHandler.getQueryMetrics()).thenReturn(mockQueryMetrics);
        
        lenient().when(mockLambdaConfig.getInvocationType()).thenReturn("RequestResponse");
        lenient().when(mockLambdaConfig.getLogType()).thenReturn("None");
        lenient().when(mockLambdaConfig.getEnvironmentVariables()).thenReturn(new HashMap<>());
        lenient().when(mockLambdaConfig.getConfigurationParameters()).thenReturn(new HashMap<>());
    }
    
    @Test
    void testParseLambdaCallSyntax() throws SQLException {
        // Valid syntax
        assertDoesNotThrow(() -> {
            new LambdaCallableStatement(mockConnection, "{call lambda:myFunction()}", mockLambdaClient, mockLambdaConfig);
        });
        
        assertDoesNotThrow(() -> {
            new LambdaCallableStatement(mockConnection, "{call lambda:myFunction(?)}", mockLambdaClient, mockLambdaConfig);
        });
        
        assertDoesNotThrow(() -> {
            new LambdaCallableStatement(mockConnection, "{call lambda:myFunction(?, ?, ?)}", mockLambdaClient, mockLambdaConfig);
        });
        
        // Invalid syntax
        assertThrows(SQLException.class, () -> {
            new LambdaCallableStatement(mockConnection, "CALL myFunction(?)", mockLambdaClient, mockLambdaConfig);
        });
    }
    
    @Test
    void testExecuteWithParameters() throws Exception {
        statement = new LambdaCallableStatement(mockConnection, "{call lambda:processOrder(?, ?, ?)}", mockLambdaClient, mockLambdaConfig);
        
        // Set IN parameters
        statement.setString(1, "order123");
        statement.setDouble(2, 99.99);
        statement.registerOutParameter(3, Types.VARCHAR);
        
        // Mock Lambda response
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("success", true);
        responseMap.put("outputParameters", Map.of("param3", "processed"));
        
        String responseJson = objectMapper.writeValueAsString(responseMap);
        InvokeResponse mockResponse = InvokeResponse.builder()
            .payload(SdkBytes.fromUtf8String(responseJson))
            .build();
        
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        
        // Execute
        boolean hasResultSet = statement.execute();
        assertThat(hasResultSet).isFalse();
        
        // Verify Lambda invocation
        ArgumentCaptor<InvokeRequest> requestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(mockLambdaClient).invoke(requestCaptor.capture());
        
        InvokeRequest request = requestCaptor.getValue();
        assertThat(request.functionName()).isEqualTo("processOrder");
        
        // Verify payload
        String payloadJson = request.payload().asUtf8String();
        Map<String, Object> payload = objectMapper.readValue(payloadJson, Map.class);
        assertThat(payload.get("action")).isEqualTo("processOrder");
        
        Map<String, Object> params = (Map<String, Object>) payload.get("parameters");
        assertThat(params.get("param1")).isEqualTo("order123");
        assertThat(params.get("param2")).isEqualTo(99.99);
        assertThat(params).doesNotContainKey("param3"); // OUT parameter not sent
        
        // Get OUT parameter
        String result = statement.getString(3);
        assertThat(result).isEqualTo("processed");
    }
    
    @Test
    void testExecuteQueryWithResultSet() throws Exception {
        statement = new LambdaCallableStatement(mockConnection, "{call lambda:getUsers(?)}", mockLambdaClient, mockLambdaConfig);
        
        statement.setString(1, "active");
        
        // Mock Lambda response with result set
        List<Map<String, Object>> users = Arrays.asList(
            Map.of("id", "user1", "name", "John Doe", "status", "active"),
            Map.of("id", "user2", "name", "Jane Smith", "status", "active")
        );
        
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("success", true);
        responseMap.put("resultSet", users);
        
        String responseJson = objectMapper.writeValueAsString(responseMap);
        InvokeResponse mockResponse = InvokeResponse.builder()
            .payload(SdkBytes.fromUtf8String(responseJson))
            .build();
        
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        
        // Execute query
        ResultSet rs = statement.executeQuery();
        assertNotNull(rs);
        
        // Verify result set
        assertTrue(rs.next());
        assertThat(rs.getString("id")).isEqualTo("user1");
        assertThat(rs.getString("name")).isEqualTo("John Doe");
        
        assertTrue(rs.next());
        assertThat(rs.getString("id")).isEqualTo("user2");
        assertThat(rs.getString("name")).isEqualTo("Jane Smith");
        
        assertFalse(rs.next());
    }
    
    @Test
    void testEnvironmentVariables() throws Exception {
        Map<String, String> connectionEnv = new HashMap<>();
        connectionEnv.put("ENVIRONMENT", "production");
        when(mockLambdaConfig.getEnvironmentVariables()).thenReturn(connectionEnv);
        
        statement = new LambdaCallableStatement(mockConnection, "{call lambda:processData()}", mockLambdaClient, mockLambdaConfig);
        
        // Set statement-level variables
        statement.setEnvironmentVariable("PRIORITY", "high");
        
        // Mock Lambda response
        Map<String, Object> responseMap = Map.of("success", true, "result", 1);
        String responseJson = objectMapper.writeValueAsString(responseMap);
        InvokeResponse mockResponse = InvokeResponse.builder()
            .payload(SdkBytes.fromUtf8String(responseJson))
            .build();
        
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        
        // Execute
        statement.executeUpdate();
        
        // Verify environment variables in payload
        ArgumentCaptor<InvokeRequest> requestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(mockLambdaClient).invoke(requestCaptor.capture());
        
        String payloadJson = requestCaptor.getValue().payload().asUtf8String();
        Map<String, Object> payload = objectMapper.readValue(payloadJson, Map.class);
        Map<String, String> env = (Map<String, String>) payload.get("environment");
        
        assertThat(env.get("ENVIRONMENT")).isEqualTo("production");
        assertThat(env.get("PRIORITY")).isEqualTo("high");
    }
    
    @Test
    void testFunctionAllowedList() throws SQLException {
        when(mockLambdaConfig.getAllowedFunctions()).thenReturn("processOrder,getUsers,validateData");
        
        // Allowed function - should succeed
        assertDoesNotThrow(() -> {
            new LambdaCallableStatement(mockConnection, "{call lambda:processOrder(?)}", mockLambdaClient, mockLambdaConfig);
        });
        
        // Not allowed function - should fail
        when(mockLambdaConfig.getAllowedFunctions()).thenReturn("processOrder,getUsers");
        SQLException ex = assertThrows(SQLException.class, () -> {
            new LambdaCallableStatement(mockConnection, "{call lambda:deleteAll()}", mockLambdaClient, mockLambdaConfig);
        });
        assertThat(ex.getMessage()).contains("not in the allowed list");
    }
    
    @Test
    void testLambdaFunctionError() throws Exception {
        statement = new LambdaCallableStatement(mockConnection, "{call lambda:failingFunction()}", mockLambdaClient, mockLambdaConfig);
        
        // Mock Lambda error response
        InvokeResponse mockResponse = InvokeResponse.builder()
            .functionError("Unhandled")
            .payload(SdkBytes.fromUtf8String("{\"errorMessage\":\"Something went wrong\"}"))
            .build();
        
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        
        // Execute should throw exception
        SQLException ex = assertThrows(SQLException.class, () -> {
            statement.execute();
        });
        assertThat(ex.getMessage()).contains("Lambda function error");
    }
    
    @Test
    void testNamedParameters() throws Exception {
        statement = new LambdaCallableStatement(mockConnection, "{call lambda:updateUser(?, ?)}", mockLambdaClient, mockLambdaConfig);
        
        // Register named parameters (in real usage, this would be done differently)
        // For now, test basic functionality
        statement.setString(1, "user123");
        statement.setInt(2, 25);
        
        // Test getter methods throw appropriate exceptions
        assertThrows(SQLException.class, () -> statement.getString("unknown"));
        assertThrows(SQLException.class, () -> statement.getInt("unknown"));
    }
}