CREATE OR REPLACE FUNCTION int_to_fixed_binary_real_array(n INT) RETURNS REAL[] AS $$
DECLARE
    binary_string TEXT;
    real_array REAL[] := '{}';
    i INT;
BEGIN
    binary_string := lpad(CAST(n::BIT(3) AS TEXT), 3, '0');
    FOR i IN 1..length(binary_string)
    LOOP
        real_array := array_append(real_array, CAST(substring(binary_string, i, 1) AS REAL));
    END LOOP;
    RETURN real_array;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE TABLE test_table (id INTEGER);
INSERT INTO test_table VALUES (0), (1), (7);
\set enable_seqscan = off;
CREATE INDEX ON test_table USING hnsw (int_to_fixed_binary_real_array(id)) WITH (M=2, dim=3);

SELECT id FROM test_table ORDER BY int_to_fixed_binary_real_array(id) <-> int_to_fixed_binary_real_array(0) LIMIT 2;