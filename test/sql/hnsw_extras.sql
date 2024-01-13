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
DROP INDEX sift_base1k_v_idx;

-- Create with params
SELECT lantern_create_external_index('v', 'sift_base1k', 'public', 'l2sq', 128, 10, 10, 10, 'hnsw_l2_index');
SELECT _lantern_internal.validate_index('hnsw_l2_index', false);

-- Reindex external index
SELECT lantern_reindex_external_index('hnsw_l2_index');
SELECT _lantern_internal.validate_index('hnsw_l2_index', false);
