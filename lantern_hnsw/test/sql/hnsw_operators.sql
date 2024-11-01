\set ON_ERROR_STOP off
CREATE TABLE op_test (v REAL[]);
INSERT INTO op_test (v) VALUES (ARRAY[0,0,0]), (ARRAY[1,1,1]);
CREATE INDEX cos_idx ON op_test USING lantern_hnsw(v dist_cos_ops);

-- Expect deprecation error due to use of the <?> operator
SELECT ARRAY[1,2,3] <?> ARRAY[3,2,1];

-- should not throw error
SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should not throw error
SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];

-- should not throw error
SELECT v <-> ARRAY[1,1,1] FROM op_test ORDER BY v <-> ARRAY[1,1,1];

SET enable_seqscan=OFF;
\set ON_ERROR_STOP on

-- one-off vector distance calculations should work with relevant operator
-- with integer arrays:
SELECT ARRAY[0,0,0] <-> ARRAY[2,3,-4];
-- with float arrays:
SELECT ARRAY[0,0,0] <-> ARRAY[2,3,-4]::real[];
SELECT ARRAY[0,0,0]::real[] <-> ARRAY[2,3,-4]::real[];
SELECT '{1,0,1}' <-> '{0,1,0}'::integer[];
SELECT '{1,0,1}' <=> '{0,1,0}'::integer[];
SELECT ROUND(num::NUMERIC, 2) FROM (SELECT '{1,1,1}' <=> '{0,1,0}'::INTEGER[] AS num) _sub;
SELECT ARRAY[.1,0,0] <=> ARRAY[0,.5,0];
SELECT cos_dist(ARRAY[.1,0,0]::real[], ARRAY[0,.5,0]::real[]);
SELECT ARRAY[1,0,0] <+> ARRAY[0,1,0];

-- should sort with index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should sort without index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];

-- should not throw error
\set ON_ERROR_STOP on

SELECT v <=> ARRAY[1,1,1] FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should not throw error
SELECT v::INTEGER[] <+> ARRAY[1,1,1] FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];

-- should not throw error
SELECT v <-> ARRAY[1,1,1] FROM op_test ORDER BY v <-> ARRAY[1,1,1];

RESET ALL;
-- Set false twice to verify that no crash is happening
\set ON_ERROR_STOP off
SET enable_seqscan=OFF;

CREATE INDEX hamming_idx ON op_test USING lantern_hnsw(cast(v as INTEGER[]) dist_hamming_ops);

-- should sort with cos_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should sort with hamming_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];
