package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import java.sql.ResultSetMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UpdatableResultSetTest {
    
    @Mock
    private DynamoDbClient mockClient;
    
    @Mock
    private DynamoDbStatement mockStatement;
    
    @Mock
    private OffsetTokenCache mockOffsetTokenCache;
    
    @Mock
    private ResultSetMetaData mockMetaData;
    
    private UpdatableResultSet resultSet;
    private Map<String, String> primaryKeyColumns;
    private List<Map<String, AttributeValue>> testItems;
    
    @BeforeEach
    void setUp() {
        // Set up primary key columns
        primaryKeyColumns = new HashMap<>();
        primaryKeyColumns.put("id", "S");
        primaryKeyColumns.put("timestamp", "N");
        
        // Set up test items
        testItems = new ArrayList<>();
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("id", AttributeValue.builder().s("user1").build());
        item1.put("timestamp", AttributeValue.builder().n("1000").build());
        item1.put("name", AttributeValue.builder().s("John Doe").build());
        item1.put("age", AttributeValue.builder().n("30").build());
        testItems.add(item1);
        
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("id", AttributeValue.builder().s("user2").build());
        item2.put("timestamp", AttributeValue.builder().n("2000").build());
        item2.put("name", AttributeValue.builder().s("Jane Smith").build());
        item2.put("age", AttributeValue.builder().n("25").build());
        testItems.add(item2);
        
        // Create ExecuteStatementResponse
        ExecuteStatementResponse response = ExecuteStatementResponse.builder()
            .items(testItems)
            .build();
        
        // Create the updatable result set
        resultSet = spy(new UpdatableResultSet(
            mockClient,
            "SELECT * FROM Users",
            response,
            100,
            new LimitOffsetInfo(null, null), // No limit or offset
            null,
            0,
            mockOffsetTokenCache,
            mockStatement,
            "Users",
            primaryKeyColumns
        ));
        
        // Mock getMetaData to return our mock metadata
        try {
            doReturn(mockMetaData).when(resultSet).getMetaData();
            when(mockMetaData.getColumnCount()).thenReturn(4);
            when(mockMetaData.getColumnName(1)).thenReturn("id");
            when(mockMetaData.getColumnName(2)).thenReturn("timestamp");
            when(mockMetaData.getColumnName(3)).thenReturn("name");
            when(mockMetaData.getColumnName(4)).thenReturn("age");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to setup mocks", e);
        }
    }
    
    @Test
    void testGetConcurrency() throws SQLException {
        assertThat(resultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_UPDATABLE);
    }
    
    @Test
    void testUpdateString() throws SQLException {
        // Move to first row
        assertTrue(resultSet.next());
        
        // Update name
        resultSet.updateString("name", "John Updated");
        
        // Verify update is pending but not applied yet
        assertThat(resultSet.getString("name")).isEqualTo("John Doe");
        
        // Apply the update
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        // Verify UPDATE statement was executed
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("Users") &&
            sql.contains("name") &&
            sql.contains("John Updated")
        ));
    }
    
    @Test
    void testUpdateInt() throws SQLException {
        assertTrue(resultSet.next());
        
        resultSet.updateInt("age", 31);
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("age") &&
            sql.contains("31")
        ));
    }
    
    @Test
    void testUpdateNull() throws SQLException {
        // First complete any pending stubbing from previous test
        reset(mockStatement);
        
        assertTrue(resultSet.next());
        
        resultSet.updateNull("name");
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("name") &&
            sql.contains("NULL")
        ));
    }
    
    @Test
    void testDeleteRow() throws SQLException {
        assertTrue(resultSet.next());
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.deleteRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("DELETE FROM") && 
            sql.contains("Users") &&
            sql.contains("WHERE") &&
            sql.contains("id") &&
            sql.contains("user1") &&
            sql.contains("timestamp") &&
            sql.contains("1000")
        ));
    }
    
    @Test
    void testInsertRow() throws SQLException {
        resultSet.moveToInsertRow();
        
        // Set values for new row
        resultSet.updateString(1, "user3"); // Use column index to test that path too
        resultSet.updateInt(2, 3000);
        resultSet.updateString("name", "New User");
        resultSet.updateInt("age", 35);
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.insertRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("INSERT INTO") && 
            sql.contains("Users") &&
            sql.contains("VALUE") &&
            sql.contains("user3") &&
            sql.contains("3000") &&
            sql.contains("New User") &&
            sql.contains("35")
        ));
    }
    
    @Test
    void testCancelRowUpdates() throws SQLException {
        assertTrue(resultSet.next());
        
        // Make some updates
        resultSet.updateString("name", "Should be cancelled");
        resultSet.updateInt("age", 99);
        
        // Cancel the updates
        resultSet.cancelRowUpdates();
        
        // updateRow should do nothing
        resultSet.updateRow();
        
        // Verify no execute was called
        verify(mockStatement, never()).execute(anyString());
    }
    
    @Test
    void testUpdateOnClosedResultSet() throws SQLException {
        resultSet.close();
        
        assertThrows(SQLException.class, () -> resultSet.updateString("name", "test"));
    }
    
    @Test
    void testUpdateWithInvalidColumnIndex() throws SQLException {
        assertTrue(resultSet.next());
        
        assertThrows(SQLException.class, () -> resultSet.updateString(99, "test"));
    }
    
    @Test
    void testInsertRowWithoutPrimaryKey() throws SQLException {
        resultSet.moveToInsertRow();
        
        // Set values but miss primary key
        resultSet.updateString("name", "Missing Key");
        resultSet.updateInt("age", 40);
        
        // Should throw exception because primary key is missing
        assertThrows(SQLException.class, () -> resultSet.insertRow());
    }
    
    @Test
    void testUpdateBoolean() throws SQLException {
        assertTrue(resultSet.next());
        
        resultSet.updateBoolean("active", true);
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("active") &&
            sql.contains("true")
        ));
    }
    
    @Test
    void testUpdateBigDecimal() throws SQLException {
        assertTrue(resultSet.next());
        
        resultSet.updateBigDecimal("balance", new java.math.BigDecimal("123.45"));
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("balance") &&
            sql.contains("123.45")
        ));
    }
    
    @Test
    void testUpdateBytes() throws SQLException {
        assertTrue(resultSet.next());
        
        byte[] data = {1, 2, 3, 4, 5};
        resultSet.updateBytes("data", data);
        
        when(mockStatement.execute(anyString())).thenReturn(true);
        resultSet.updateRow();
        
        verify(mockStatement).execute(argThat(sql -> 
            sql.contains("UPDATE") && 
            sql.contains("data")
        ));
    }
    
    @Test
    void testRefreshRow() throws SQLException {
        assertTrue(resultSet.next());
        
        // Create response for refresh query
        Map<String, AttributeValue> refreshedItem = new HashMap<>();
        refreshedItem.put("id", AttributeValue.builder().s("user1").build());
        refreshedItem.put("timestamp", AttributeValue.builder().n("1000").build());
        refreshedItem.put("name", AttributeValue.builder().s("Refreshed Name").build());
        refreshedItem.put("age", AttributeValue.builder().n("31").build());
        
        ExecuteStatementResponse refreshResponse = ExecuteStatementResponse.builder()
            .items(Arrays.asList(refreshedItem))
            .build();
        
        // Mock the refresh query
        when(mockStatement.executeQuery(anyString())).thenReturn(
            new DynamoDbResultSet(mockClient, "SELECT * FROM Users WHERE id = 'user1'", 
                refreshResponse, 100, new LimitOffsetInfo(null, null), null, 0, mockOffsetTokenCache)
        );
        
        // Make an update then refresh (should cancel the update)
        resultSet.updateString("name", "Should be refreshed");
        resultSet.refreshRow();
        
        // Verify refresh query was executed
        verify(mockStatement).executeQuery(argThat(sql -> 
            sql.contains("SELECT * FROM") && 
            sql.contains("Users") &&
            sql.contains("WHERE") &&
            sql.contains("id") &&
            sql.contains("user1")
        ));
    }
    
    @Test
    void testNonUpdatableResultSet() throws SQLException {
        // Create a non-updatable result set (no primary key columns)
        try (UpdatableResultSet nonUpdatable = new UpdatableResultSet(
            mockClient,
            "SELECT * FROM Users",
            ExecuteStatementResponse.builder().items(testItems).build(),
            100,
            new LimitOffsetInfo(null, null),
            null,
            0,
            mockOffsetTokenCache,
            mockStatement,
            "Users",
            new HashMap<>() // Empty primary key columns
        )) {
            assertThat(nonUpdatable.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            
            assertTrue(nonUpdatable.next());
            assertThrows(SQLException.class, () -> nonUpdatable.updateString("name", "test"));
        }
    }
}