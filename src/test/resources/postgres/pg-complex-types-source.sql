-- Complex types test table for PostgreSQL TEXT COPY validation
-- Tests ARRAY, JSON, JSONB, XML, and INTERVAL types

CREATE TABLE IF NOT EXISTS t_complex_source (
    id INTEGER PRIMARY KEY,
    -- Array types
    int_array INTEGER[],
    text_array TEXT[],
    nested_array INTEGER[][],
    -- JSON types
    json_data JSON,
    jsonb_data JSONB,
    -- XML type
    xml_data XML,
    -- Interval type
    interval_data INTERVAL
);

-- Insert test data with edge cases
INSERT INTO t_complex_source VALUES 
(
    1,
    -- Simple integer array
    ARRAY[1, 2, 3, 4, 5],
    -- Text array with special characters
    ARRAY['hello', 'world', 'test "quoted"', 'backslash\\here'],
    -- Nested 2D array
    ARRAY[[1,2],[3,4]],
    -- JSON with Unicode and special chars
    '{"name": "Test User", "age": 30, "city": "San José", "special": "quote\"here"}',
    -- JSONB (binary JSON)
    '{"nested": {"key": "value"}, "array": [1, 2, 3], "unicode": "café ☕"}',
    -- XML with CDATA
    '<root><data><![CDATA[Special <chars> & "quotes"]]></data></root>',
    -- Interval
    INTERVAL '1 year 2 months 3 days 4 hours 5 minutes'
),
(
    2,
    -- Array with NULLs
    ARRAY[10, NULL, 30],
    -- Empty array
    ARRAY[]::TEXT[],
    -- Nested array with NULLs
    ARRAY[[5,NULL],[NULL,8]],
    -- Complex nested JSON
    '{"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], "meta": {"count": 2}}',
    -- JSONB with null values
    '{"key1": null, "key2": "value2", "key3": {"nested": null}}',
    -- XML with attributes and namespaces
    '<book xmlns="http://example.com" id="123"><title lang="en">Test</title></book>',
    -- Different interval format
    INTERVAL '100 days 5 hours'
),
(
    3,
    -- NULL array
    NULL,
    -- Array with empty strings
    ARRAY['', 'non-empty', ''],
    -- Single element nested array
    ARRAY[[999]],
    -- JSON null
    'null',
    -- Empty JSONB object
    '{}',
    -- XML with multiple elements
    '<items><item>First</item><item>Second</item><item>Third</item></items>',
    -- Negative interval
    INTERVAL '-30 days'
);
