\ir utils/small_world_array.sql

-- Before the HNSW index is created, the parameter hnsw.init_k should not be available
\set ON_ERROR_STOP off
SHOW hnsw.init_k;
\set ON_ERROR_STOP on

-- Create an index and verify that it was created
CREATE INDEX ON small_world USING hnsw (v) WITH (dim=3);
SELECT * FROM ldb_get_indexes('small_world');

-- Verify that hnsw.init_k exists after index creation
SHOW hnsw.init_k;

-- Modify hnsw.init_k and verify that it was modified
SET hnsw.init_k = 45;
SHOW hnsw.init_k;

-- Reset all parameters and verify that hnsw.init_k was reset
RESET ALL;
SHOW hnsw.init_k;

-- Validate the index data structures
SELECT _lantern_internal.validate_index('small_world_v_idx', false);

-- Validate that lantern.pgvector_compat disables the operator rewriting hooks
CREATE TABLE op_test (v REAL[]);
INSERT INTO op_test (v) VALUES (ARRAY[0,0,0]), (ARRAY[1,1,1]);
CREATE INDEX ON op_test USING hnsw(v dist_cos_ops);
-- should rewrite operator
SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];
SET lantern.pgvector_compat=TRUE;
-- should throw error
\set ON_ERROR_STOP off
SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];
\set ON_ERROR_STOP on
RESET ALL;
-- should rewrite operator
SELECT * FROM op_test ORDER BY v <-> ARRAY[1,1,1];
