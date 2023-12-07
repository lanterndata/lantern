-- Validate that lantern.pgvector_compat disables the operator rewriting hooks
CREATE TABLE op_test (v REAL[]);
INSERT INTO op_test (v) VALUES (ARRAY[0,0,0]), (ARRAY[1,1,1]);
CREATE INDEX cos_idx ON op_test USING hnsw(v dist_cos_ops);
-- should rewrite operator
SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];

-- should throw error
\set ON_ERROR_STOP off
SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should throw error
SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];

-- should throw error
SELECT v <-> ARRAY[1,1,1] FROM op_test ORDER BY v <-> ARRAY[1,1,1];

SET lantern.pgvector_compat=TRUE;
SET enable_seqscan=OFF;
\set ON_ERROR_STOP on

-- NOW THIS IS TRIGGERING INDEX SCAN AS WELL
-- BECAUSE WE ARE REGISTERING <-> FOR ALL OPERATOR CLASSES
-- IDEALLY THIS SHOULD NOT TRIGGER INDEX SCAN WHEN lantern.pgvector_compat=TRUE
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];

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
SET lantern.pgvector_compat=FALSE;
\set ON_ERROR_STOP off
-- should rewrite operator
SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];

SET lantern.pgvector_compat=TRUE;
SET enable_seqscan=OFF;

CREATE INDEX hamming_idx ON op_test USING hnsw(cast(v as INTEGER[]) dist_hamming_ops);

-- should sort with cos_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v <=> ARRAY[1,1,1];

-- should sort with hamming_idx index
EXPLAIN (COSTS FALSE) SELECT * FROM op_test ORDER BY v::INTEGER[] <+> ARRAY[1,1,1];
