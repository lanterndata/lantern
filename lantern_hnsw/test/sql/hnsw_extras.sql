------------------------------------------------------------------------------
-- Test Functions exported from lantern_extras extension
------------------------------------------------------------------------------

\ir utils/sift1k_array.sql

\set ON_ERROR_STOP off
CREATE EXTENSION lantern_extras;
-- Validate error on invalid params
SELECT lantern_create_external_index('v','sift_base1k', 'public', 'invalid_metric',  3, 10, 10, 10);
SELECT lantern_create_external_index('v','sift_base1k', 'public', 'l2sq',  3, -1, 10, 10);
SELECT lantern_create_external_index('v','sift_base1k', 'public', 'l2sq',  3, 10, -2, 10);
SELECT lantern_create_external_index('v','sift_base1k', 'public', 'l2sq',  3, 10, 10, -1);

-- Validate error on empty table
CREATE TABLE empty (v REAL[]);
SELECT lantern_create_external_index('v', 'empty');
\set ON_ERROR_STOP on

-- Create with defaults
SELECT lantern_create_external_index('v', 'sift_base1k');
SELECT _lantern_internal.validate_index('sift_base1k_v_idx', false);

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
-- Validate that using corresponding operator triggers index scan
EXPLAIN (COSTS FALSE) SELECT id FROM sift_base1k order by v <-> :'v777' LIMIT 10;
DROP INDEX sift_base1k_v_idx;

-- Create with params
SELECT lantern_create_external_index('v', 'sift_base1k', 'public', 'cos', 128, 10, 10, 10, false, 'hnsw_cos_index');
SELECT _lantern_internal.validate_index('hnsw_cos_index', false);
EXPLAIN (COSTS FALSE) SELECT id FROM sift_base1k order by v <=> :'v777' LIMIT 10;

-- -- Reindex external index
SELECT lantern_reindex_external_index('hnsw_cos_index');
SELECT _lantern_internal.validate_index('hnsw_cos_index', false);

-- Validate that using corresponding operator triggers index scan
EXPLAIN (COSTS FALSE) SELECT id FROM sift_base1k order by v <=> :'v777' LIMIT 10;

-- Create PQ Index
SET client_min_messages=ERROR;
DROP INDEX hnsw_cos_index;
-- Verify error that codebook does not exist
\set ON_ERROR_STOP off
SELECT lantern_create_external_index('v', 'sift_base1k', 'public', 'cos', 128, 10, 10, 10, true, 'hnsw_cos_index_pq');
\set ON_ERROR_STOP on
SELECT quantize_table('sift_base1k'::regclass, 'v', 10, 32, 'cos');
SELECT lantern_create_external_index('v', 'sift_base1k', 'public', 'cos', 128, 10, 10, 10, true, 'hnsw_cos_index_pq');
SELECT _lantern_internal.validate_index('hnsw_cos_index_pq', false);
SELECT lantern_reindex_external_index('hnsw_cos_index_pq');
SELECT _lantern_internal.validate_index('hnsw_cos_index_pq', false);
EXPLAIN (COSTS FALSE) SELECT id FROM sift_base1k order by v <=> :'v777' LIMIT 10;
