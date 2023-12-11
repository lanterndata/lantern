-- Validate that lantern.pgvector_compat disables the operator rewriting hooks
CREATE TABLE op_test (v REAL[]);
INSERT INTO op_test (v) VALUES (ARRAY[0,0,0]), (ARRAY[1,1,1]);
CREATE INDEX cos_idx ON op_test USING hnsw(v dist_cos_ops);
-- should rewrite operator
SET lantern.pgvector_compat=FALSE;
SELECT * FROM op_test ORDER BY v <?> ARRAY[1,1,1];

\set ON_ERROR_STOP off
SET lantern.pgvector_compat=TRUE;
-- should throw error
SELECT * FROM op_test ORDER BY v <?> ARRAY[1,1,1];
-- should not throw error
SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should not throw error
SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];

-- should not throw error
SELECT v <-> ARRAY[1,1,1] FROM op_test ORDER BY v <-> ARRAY[1,1,1];

SET lantern.pgvector_compat=FALSE;
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

-- NOW THIS IS TRIGGERING INDEX SCAN AS WELL
-- BECAUSE WE ARE REGISTERING <?> FOR ALL OPERATOR CLASSES
-- IDEALLY THIS SHOULD NOT TRIGGER INDEX SCAN WHEN lantern.pgvector_compat=TRUE
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <?> ARRAY[1,1,1];

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
SET lantern.pgvector_compat=FALSE;
\set ON_ERROR_STOP off
-- should rewrite operator
SELECT * FROM op_test ORDER BY v <?> ARRAY[1,1,1];

SET enable_seqscan=OFF;

CREATE INDEX hamming_idx ON op_test USING hnsw(cast(v as INTEGER[]) dist_hamming_ops);

-- should sort with cos_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should sort with hamming_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];
