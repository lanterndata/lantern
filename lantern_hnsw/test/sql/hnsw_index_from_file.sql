------------------------------------------------------------------------------
-- Test HNSW index creation from file
------------------------------------------------------------------------------
\ir utils/sift1k_array.sql

\set ON_ERROR_STOP off
-- Validate error on invalid path
-- Should throw deprecation error
CREATE INDEX hnsw_l2_index ON sift_base1k USING lantern_hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/invalid-path');
