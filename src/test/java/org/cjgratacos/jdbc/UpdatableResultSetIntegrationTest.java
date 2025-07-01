package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.testcontainers.dynamodb.DynaliteContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UpdatableResultSetIntegrationTest {

    private static final String TABLE_NAME = "UpdatableTestTable";

    private static DynaliteContainer dynamoDbContainer;
    private static DynamoDbClient dynamoDbClient;
    private static Connection connection;

    @BeforeAll
    static void setUpAll() throws SQLException {
        // Start DynamoDB container
        dynamoDbContainer = new DynaliteContainer();
        dynamoDbContainer.start();
        
        // Create DynamoDB client
        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(java.net.URI.create("http://" + dynamoDbContainer.getHost() + ":" + dynamoDbContainer.getFirstMappedPort()))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("dummy", "dummy")))
                .region(Region.US_EAST_1)
                .build();

        // Create test table
        createTestTable();

        // Create JDBC connection
        Properties props = new Properties();
        props.setProperty("aws.accessKeyId", "dummy");
        props.setProperty("aws.secretAccessKey", "dummy");

        String jdbcUrl = String.format("jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://%s:%d",
                dynamoDbContainer.getHost(), dynamoDbContainer.getFirstMappedPort());

        connection = DriverManager.getConnection(jdbcUrl, props);
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (dynamoDbClient != null) {
            try {
                dynamoDbClient.deleteTable(builder -> builder.tableName(TABLE_NAME));
            } catch (Exception e) {
                // Ignore
            }
            dynamoDbClient.close();
        }
        if (dynamoDbContainer != null) {
            dynamoDbContainer.stop();
        }
    }

    private static void createTestTable() {
        try {
            dynamoDbClient.createTable(builder -> builder
                    .tableName(TABLE_NAME)
                    .keySchema(
                            KeySchemaElement.builder()
                                    .attributeName("id")
                                    .keyType(KeyType.HASH)
                                    .build(),
                            KeySchemaElement.builder()
                                    .attributeName("version")
                                    .keyType(KeyType.RANGE)
                                    .build())
                    .attributeDefinitions(
                            AttributeDefinition.builder()
                                    .attributeName("id")
                                    .attributeType(ScalarAttributeType.S)
                                    .build(),
                            AttributeDefinition.builder()
                                    .attributeName("version")
                                    .attributeType(ScalarAttributeType.N)
                                    .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST));

            // Wait for table to be active
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test table", e);
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        // Clear table
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
            while (rs.next()) {
                stmt.execute(String.format(
                    "DELETE FROM %s WHERE id = '%s' AND version = %d",
                    TABLE_NAME, rs.getString("id"), rs.getInt("version")));
            }
        }

        // Insert test data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s VALUE {'id': 'user1', 'version': 1, 'name': 'John Doe', 'age': 30, 'active': true}",
                TABLE_NAME));
            stmt.execute(String.format(
                "INSERT INTO %s VALUE {'id': 'user2', 'version': 1, 'name': 'Jane Smith', 'age': 25, 'active': true}",
                TABLE_NAME));
            stmt.execute(String.format(
                "INSERT INTO %s VALUE {'id': 'user3', 'version': 1, 'name': 'Bob Johnson', 'age': 35, 'active': false}",
                TABLE_NAME));
        }
    }

    @Test
    @Order(1)
    void testUpdateRow() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            // Enable updatable result sets
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user1'");
            assertTrue(rs.next());
            
            // Verify current values
            assertThat(rs.getString("name")).isEqualTo("John Doe");
            assertThat(rs.getInt("age")).isEqualTo(30);
            
            // Update values
            rs.updateString("name", "John Updated");
            rs.updateInt("age", 31);
            rs.updateBoolean("active", false);
            rs.updateRow();
            
            rs.close();
        }
        
        // Verify the update
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user1'");
            assertTrue(rs.next());
            assertThat(rs.getString("name")).isEqualTo("John Updated");
            assertThat(rs.getInt("age")).isEqualTo(31);
            assertThat(rs.getBoolean("active")).isFalse();
        }
    }

    @Test
    @Order(2)
    void testDeleteRow() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user3'");
            assertTrue(rs.next());
            
            // Delete the row
            rs.deleteRow();
            
            rs.close();
        }
        
        // Verify the deletion
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user3'");
            assertFalse(rs.next());
        }
    }

    @Test
    @Order(3)
    void testInsertRow() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
            
            // Move to insert row
            rs.moveToInsertRow();
            
            // Set values for new row
            rs.updateString("id", "user4");
            rs.updateInt("version", 1);
            rs.updateString("name", "New User");
            rs.updateInt("age", 28);
            rs.updateBoolean("active", true);
            
            // Insert the row
            rs.insertRow();
            
            // Move back to current row
            rs.moveToCurrentRow();
            
            rs.close();
        }
        
        // Verify the insertion
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user4'");
            assertTrue(rs.next());
            assertThat(rs.getString("name")).isEqualTo("New User");
            assertThat(rs.getInt("age")).isEqualTo(28);
            assertThat(rs.getBoolean("active")).isTrue();
        }
    }

    @Test
    @Order(4)
    void testUpdateNull() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user2'");
            assertTrue(rs.next());
            
            // Set name to null
            rs.updateNull("name");
            rs.updateRow();
            
            rs.close();
        }
        
        // Verify the null update
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user2'");
            assertTrue(rs.next());
            assertNull(rs.getString("name"));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    @Order(5)
    void testCancelRowUpdates() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user1'");
            assertTrue(rs.next());
            
            // Get original values (for verification if needed)
            rs.getString("name");
            rs.getInt("age");
            
            // Make updates
            rs.updateString("name", "Should Not Be Saved");
            rs.updateInt("age", 99);
            
            // Cancel the updates
            rs.cancelRowUpdates();
            
            // updateRow should have no effect
            rs.updateRow();
            
            rs.close();
        }
        
        // Verify no changes were made
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user1'");
            assertTrue(rs.next());
            assertThat(rs.getString("name")).isNotEqualTo("Should Not Be Saved");
            assertThat(rs.getInt("age")).isNotEqualTo(99);
        }
    }

    @Test
    @Order(6)
    void testReadOnlyResultSet() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Default should be read-only
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 'user1'");
            assertTrue(rs.next());
            
            assertThat(rs.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            
            // Should throw exception when trying to update
            assertThrows(SQLException.class, () -> rs.updateString("name", "test"));
        }
    }

    @Test
    @Order(7)
    void testComplexQueryNotUpdatable() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            // Query with aggregation should not be updatable
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + TABLE_NAME);
            assertTrue(rs.next());
            
            // Should be read-only despite setting CONCUR_UPDATABLE
            assertThat(rs.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @Order(8)
    void testUpdateMultipleRows() throws SQLException {
        try (DynamoDbStatement stmt = (DynamoDbStatement) connection.createStatement()) {
            stmt.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
            
            int updateCount = 0;
            while (rs.next()) {
                // Update age by adding 10
                int currentAge = rs.getInt("age");
                rs.updateInt("age", currentAge + 10);
                rs.updateRow();
                updateCount++;
            }
            
            assertThat(updateCount).isGreaterThan(0);
            rs.close();
        }
        
        // Verify all ages were increased
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
            while (rs.next()) {
                int age = rs.getInt("age");
                assertThat(age).isGreaterThanOrEqualTo(35); // All original ages + 10
            }
        }
    }
}