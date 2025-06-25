-- DynamoDB PartiQL JDBC Driver - Pagination Examples
-- 
-- These examples show how to paginate through large result sets
-- in GUI clients like DbVisualizer

-- =============================================================================
-- BASIC PAGINATION WITH LIMIT AND OFFSET
-- =============================================================================

-- First page: rows 1-100 (default limit)
SELECT * FROM MyTable;

-- Or explicitly with LIMIT
SELECT * FROM MyTable LIMIT 100;

-- Second page: rows 101-200
SELECT * FROM MyTable LIMIT 100 OFFSET 100;

-- Third page: rows 201-300
SELECT * FROM MyTable LIMIT 100 OFFSET 200;

-- Fourth page: rows 301-400
SELECT * FROM MyTable LIMIT 100 OFFSET 300;

-- =============================================================================
-- PAGINATION WITH FILTERING
-- =============================================================================

-- Paginate through active users
SELECT * FROM Users 
WHERE status = 'active' 
LIMIT 50 OFFSET 0;

-- Next page of active users
SELECT * FROM Users 
WHERE status = 'active' 
LIMIT 50 OFFSET 50;

-- =============================================================================
-- PAGINATION WITH SPECIFIC COLUMNS
-- =============================================================================

-- Get just the essential fields, page by page
SELECT id, name, email, created_date 
FROM Users 
LIMIT 25 OFFSET 0;

-- Next page
SELECT id, name, email, created_date 
FROM Users 
LIMIT 25 OFFSET 25;

-- =============================================================================
-- COUNTING TOTAL ROWS
-- =============================================================================

-- Get total count (be careful with large tables!)
SELECT COUNT(*) as total_rows FROM MyTable;

-- Count with filter
SELECT COUNT(*) as active_users FROM Users WHERE status = 'active';

-- =============================================================================
-- PAGINATION PATTERNS FOR APPLICATIONS
-- =============================================================================

-- Page 1 (assuming page size of 20)
SELECT * FROM Products LIMIT 20 OFFSET 0;

-- Page 2
SELECT * FROM Products LIMIT 20 OFFSET 20;

-- Page 3
SELECT * FROM Products LIMIT 20 OFFSET 40;

-- Generic formula for page N (0-based):
-- OFFSET = (page_number * page_size)
-- LIMIT = page_size

-- =============================================================================
-- TIPS FOR DBVISUALIZER USERS
-- =============================================================================

-- 1. Set Max Rows in DbVisualizer preferences:
--    Tools → Tool Properties → General → SQL Commander → Max Rows
--    Set to desired page size (e.g., 500)

-- 2. Use connection properties for default limits:
--    jdbc:dynamodb:partiql:region=us-east-1;defaultMaxRows=500

-- 3. Create saved queries with parameters:
--    SELECT * FROM ${table} LIMIT ${limit} OFFSET ${offset}

-- 4. For very large tables, consider adding indexes on frequently filtered columns
--    to improve query performance

-- =============================================================================
-- PERFORMANCE CONSIDERATIONS
-- =============================================================================

-- Large OFFSET values can be slow as DynamoDB must skip records
-- For better performance with deep pagination, consider:

-- 1. Using a sort key with range queries:
SELECT * FROM MyTable 
WHERE pk = 'partition1' AND sk > 'last_seen_sort_key'
LIMIT 100;

-- 2. Using timestamps for pagination:
SELECT * FROM Events 
WHERE event_time > '2024-01-01T00:00:00Z'
LIMIT 100;

-- 3. Keeping track of the last item from previous page
-- and using it as a starting point for the next query