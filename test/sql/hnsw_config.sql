\ir utils/small_world_array.sql

-- Before the HNSW index is created, the parameter hnsw.init_k should not be available
\set ON_ERROR_STOP off
SHOW hnsw.init_k;
\set ON_ERROR_STOP on

-- Create an index and verify that it was created
CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3);
SELECT * FROM ldb_get_indexes('small_world');

-- Verify that hnsw.init_k exists after index creation
SHOW hnsw.init_k;

-- Modify hnsw.init_k and verify that it was modified
SET hnsw.init_k = 45;
SHOW hnsw.init_k;

-- Reset all parameters and verify that hnsw.init_k was reset
RESET ALL;
SHOW hnsw.init_k;
