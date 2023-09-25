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

CREATE OR REPLACE FUNCTION int_to_dynamic_binary_real_array(n INT) RETURNS REAL[] AS $$
DECLARE
    binary_string TEXT;
    real_array REAL[] := '{}';
    i INT;
    result_length INT;
BEGIN
    binary_string := lpad(CAST(n::BIT(3) AS TEXT), 3, '0');
    
    -- Calculate the length of the result array
    result_length := 3 + n;

    FOR i IN 1..result_length
    LOOP
        IF i <= 3 THEN
            real_array := array_append(real_array, CAST(substring(binary_string, i, 1) AS REAL));
        ELSE
            real_array := array_append(real_array, CAST(i - 3 AS REAL));
        END IF;
    END LOOP;
    
    RETURN real_array;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION int_to_string(n INT) RETURNS TEXT AS $$
BEGIN
    RETURN lpad(CAST(n::BIT(3) AS TEXT), 3, '0');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE TABLE test_table (id INTEGER);
INSERT INTO test_table VALUES (0), (1), (7);

\set enable_seqscan = off;

-- This should success
CREATE INDEX ON test_table USING hnsw (int_to_fixed_binary_real_array(id)) WITH (M=2);

\set ON_ERROR_STOP off
-- This should result in an error that dimensions does not match
CREATE INDEX ON test_table USING hnsw (int_to_dynamic_binary_real_array(id)) WITH (M=2);

-- This should result in an error that data type text has no default operator class
CREATE INDEX ON test_table USING hnsw (int_to_string(id)) WITH (M=2);

-- This should result in error about multicolumn expressions support
CREATE INDEX ON test_table USING hnsw (int_to_fixed_binary_real_array(id), int_to_dynamic_binary_real_array(id)) WITH (M=2);

-- This currently results in an error about using the operator outside of index
-- This case should be fixed
SELECT id FROM test_table ORDER BY int_to_fixed_binary_real_array(id) <-> int_to_fixed_binary_real_array(0) LIMIT 2;
