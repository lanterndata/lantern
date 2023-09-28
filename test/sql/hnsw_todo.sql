-- THIS IS TODO TEST FILE
-- THIS TESTS WILL NOT PASS CURRENTLY BUT SHOULD BE FIXED LATER

CREATE TABLE small_world_l2 (
    id varchar(3),
    vector real[],
    vector_int integer[]
);

INSERT INTO small_world_l2 (id, vector) VALUES 
    ('000', '{0,0,0}'),
    ('001', '{0,0,1}'),
    ('010', '{0,1,0}'),
    ('011', '{0,1,1}'),
    ('100', '{1,0,0}'),
    ('101', '{1,0,1}'),
    ('110', '{1,1,0}'),
    ('111', '{1,1,1}');

SET enable_seqscan = false;

\set ON_ERROR_STOP off

CREATE INDEX ON small_world_l2 USING hnsw (vector dist_l2sq_ops);

-- this should be supported
CREATE INDEX ON small_world_l2 USING hnsw (vector_int dist_l2sq_int_ops);

-- this should use index
EXPLAIN (COSTS FALSE)
SELECT id, ROUND(l2sq_dist(vector_int, array[0,1,0])::numeric, 2) as dist
FROM small_world_l2
ORDER BY vector_int <-> array[0,1,0] LIMIT 7;

-- this result is not sorted correctly
CREATE TABLE small_world_ham (
    id SERIAL PRIMARY KEY,
    v INT[2]
);
INSERT INTO small_world_ham (v) VALUES ('{0,0}'), ('{1,1}'), ('{2,2}'), ('{3,3}');
CREATE INDEX ON small_world_ham USING hnsw (v dist_hamming_ops) WITH (dim=2);
SELECT ROUND(hamming_dist(v, '{0,0}')::numeric, 2) FROM small_world_ham ORDER BY v <-> '{0,0}';

--- Test scenarious ---
-----------------------------------------
-- Case:
-- Index is created externally.
-- More vectors are added to the table
-- CREATE INDEX is run on the table with the external file

SELECT array_fill(0, ARRAY[128]) AS v0 \gset

DROP TABLE IF EXISTS sift_base1k CASCADE;
\ir utils/sift1k_array.sql
INSERT INTO sift_base1k (id, v) VALUES 
(1001, array_fill(1, ARRAY[128])),
(1102, array_fill(2, ARRAY[128]));
SELECT v AS v1001 FROM sift_base1k WHERE id = 1001 \gset
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2.usearch');
-- The 1001 and 1002 vectors will be ignored in search, so the first row will not be 0 in result
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <-> :'v1001' LIMIT 1;

-- Case:
-- Index is created externally
-- Vectors updated
-- CREATE INDEX is run on the table with external file
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql
UPDATE sift_base1k SET v=:'v1001' WHERE id=777;
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2.usearch');
-- The first row will not be 0 now as the vector under id=777 was updated to 1,1,1,1... but it was indexed with different vector
-- So the usearch index can not find 1,1,1,1,1.. vector in the index and wrong results will be returned
-- This is an expected behaviour for now
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <-> :'v1001' LIMIT 1;

---- Query on expression based index is failing to check correct <-> operator usage --------
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
-- This currently results in an error about using the operator outside of index
-- This case should be fixed
SELECT id FROM test_table ORDER BY int_to_fixed_binary_real_array(id) <-> '{0,0,0}'::REAL[] LIMIT 2;

