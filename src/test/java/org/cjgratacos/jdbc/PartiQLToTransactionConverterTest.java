package org.cjgratacos.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@DisplayName("PartiQLToTransactionConverter Tests")
class PartiQLToTransactionConverterTest {
    
    private PartiQLToTransactionConverter converter;
    
    @BeforeEach
    void setUp() {
        converter = new PartiQLToTransactionConverter();
    }
    
    @Test
    @DisplayName("Should parse INSERT statement correctly")
    void testParseInsertStatement() throws SQLException {
        String sql = "INSERT INTO TransactionTestTable VALUE {'id': 'tx1', 'name': 'Item 1'}";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.INSERT);
        assertThat(op.getTableName()).isEqualTo("TransactionTestTable");
        assertThat(op.getItem()).containsEntry("id", AttributeValue.builder().s("tx1").build());
        assertThat(op.getItem()).containsEntry("name", AttributeValue.builder().s("Item 1").build());
    }
    
    @Test
    @DisplayName("Should parse DELETE statement correctly")
    void testParseDeleteStatement() throws SQLException {
        String sql = "DELETE FROM TransactionTestTable WHERE id = 'tx1'";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.DELETE);
        assertThat(op.getTableName()).isEqualTo("TransactionTestTable");
        assertThat(op.getKey()).containsEntry("id", AttributeValue.builder().s("tx1").build());
    }
    
    @Test
    @DisplayName("Should parse UPDATE statement correctly")
    void testParseUpdateStatement() throws SQLException {
        String sql = "UPDATE TransactionTestTable SET name = 'Updated' WHERE id = 'tx1'";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.UPDATE);
        assertThat(op.getTableName()).isEqualTo("TransactionTestTable");
        assertThat(op.getKey()).containsEntry("id", AttributeValue.builder().s("tx1").build());
        assertThat(op.getUpdateExpression()).contains("SET");
        assertThat(op.getExpressionAttributeNames()).isNotEmpty();
        assertThat(op.getExpressionAttributeValues()).isNotEmpty();
    }
    
    @Test
    @DisplayName("Should detect DML statements correctly")
    void testIsDMLStatement() {
        assertThat(converter.isDMLStatement("INSERT INTO table VALUE {}")).isTrue();
        assertThat(converter.isDMLStatement("UPDATE table SET x = 1")).isTrue();
        assertThat(converter.isDMLStatement("DELETE FROM table WHERE x = 1")).isTrue();
        assertThat(converter.isDMLStatement("SELECT * FROM table")).isFalse();
        assertThat(converter.isDMLStatement("CREATE TABLE foo")).isFalse();
    }
    
    @Test
    @DisplayName("Should handle complex INSERT with multiple attributes")
    void testParseComplexInsertStatement() throws SQLException {
        String sql = "INSERT INTO MyTable VALUE {'id': '123', 'name': 'Test', 'count': 42, 'active': true}";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.INSERT);
        assertThat(op.getTableName()).isEqualTo("MyTable");
        assertThat(op.getItem()).hasSize(4);
        assertThat(op.getItem()).containsEntry("id", AttributeValue.builder().s("123").build());
        assertThat(op.getItem()).containsEntry("name", AttributeValue.builder().s("Test").build());
        assertThat(op.getItem()).containsEntry("count", AttributeValue.builder().n("42").build());
        assertThat(op.getItem()).containsEntry("active", AttributeValue.builder().bool(true).build());
    }
    
    @Test
    @DisplayName("Should handle UPDATE with multiple SET clauses")
    void testParseComplexUpdateStatement() throws SQLException {
        String sql = "UPDATE MyTable SET name = 'New Name', count = 100 WHERE id = '123'";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.UPDATE);
        assertThat(op.getTableName()).isEqualTo("MyTable");
        assertThat(op.getKey()).containsEntry("id", AttributeValue.builder().s("123").build());
        assertThat(op.getUpdateExpression()).contains("SET");
        assertThat(op.getExpressionAttributeNames()).hasSize(2);
        assertThat(op.getExpressionAttributeValues()).hasSize(2);
    }
    
    @Test
    @DisplayName("Should handle DELETE with compound WHERE clause")
    void testParseComplexDeleteStatement() throws SQLException {
        String sql = "DELETE FROM MyTable WHERE id = '123' AND category = 'test'";
        
        PartiQLToTransactionConverter.DMLOperation op = converter.parseDMLStatement(sql);
        
        assertThat(op).isNotNull();
        assertThat(op.getType()).isEqualTo(PartiQLToTransactionConverter.DMLType.DELETE);
        assertThat(op.getTableName()).isEqualTo("MyTable");
        assertThat(op.getKey()).hasSize(2);
        assertThat(op.getKey()).containsEntry("id", AttributeValue.builder().s("123").build());
        assertThat(op.getKey()).containsEntry("category", AttributeValue.builder().s("test").build());
    }
    
    @Test
    @DisplayName("Should return null for non-DML statements")
    void testParseNonDMLStatement() throws SQLException {
        assertThat(converter.parseDMLStatement("SELECT * FROM table")).isNull();
        assertThat(converter.parseDMLStatement("CREATE TABLE foo")).isNull();
        assertThat(converter.parseDMLStatement("DROP TABLE bar")).isNull();
    }
}