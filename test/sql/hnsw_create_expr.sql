/*
This function, int_to_fixed_binary_real_array(n INT), will create a 3-dimensional float array (REAL[]).
It fills the array with the first 3 bits of the passed integer 'n' by converting 'n' to binary, 
left-padding it to 3 digits, and then converting each digit to a REAL value.
For example, int_to_fixed_binary_real_array(1); will result in the array {0,0,1},
and int_to_fixed_binary_real_array(2); will result in {0,1,0}.
*/
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

/*
This function, int_to_dynamic_binary_real_array(n INT), will create a 3+n dimensional float array (REAL[]).
It first fills the first 3 elements of the array with the first 3 bits of the passed integer 'n' 
(using a similar binary conversion as the previous function), and then adds elements sequentially from 4 to 'n+3'.
For example, int_to_dynamic_binary_real_array(3); will result in the array {0,1,1,1,2,3},
and int_to_dynamic_binary_real_array(4); will result in the array {1,0,0,1,2,3,4}.
*/
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

/*
This simple function, int_to_string(n INT), converts the integer 'n' to a 3-character text representation 
by converting 'n' to binary and left-padding it to 3 digits with '0's.
For example, int_to_string(1); will return '001', and int_to_string(2); will return '010'.
*/
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
SELECT _lantern_internal.validate_index('test_table_int_to_fixed_binary_real_array_idx', false);

\set ON_ERROR_STOP off
-- This should result in an error that dimensions does not match
CREATE INDEX ON test_table USING hnsw (int_to_dynamic_binary_real_array(id)) WITH (M=2);

-- This should result in an error that data type text has no default operator class
CREATE INDEX ON test_table USING hnsw (int_to_string(id)) WITH (M=2);

-- This should result in error about multicolumn expressions support
CREATE INDEX ON test_table USING hnsw (int_to_fixed_binary_real_array(id), int_to_dynamic_binary_real_array(id)) WITH (M=2);

SELECT id FROM test_table ORDER BY int_to_fixed_binary_real_array(id) <-> '{0,0,0}'::REAL[] LIMIT 2;
