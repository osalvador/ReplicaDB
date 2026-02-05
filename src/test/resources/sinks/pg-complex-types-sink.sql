-- Sink table for complex types test
CREATE TABLE IF NOT EXISTS t_complex_sink (
    id INTEGER PRIMARY KEY,
    int_array INTEGER[],
    text_array TEXT[],
    nested_array INTEGER[][],
    json_data JSON,
    jsonb_data JSONB,
    xml_data XML,
    interval_data INTERVAL
);
