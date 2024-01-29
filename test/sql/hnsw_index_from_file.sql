------------------------------------------------------------------------------
-- Test HNSW index creation from file
------------------------------------------------------------------------------

-- Index files were created with ldb-create-index program which source is under https://github.com/lanterndata/lantern_extras/
-- We have exported index files for sift1k dataset for cosine and l2sq distances
-- With the following params m=16 ef=64 ef_construction=128 dim=128
-- Validate that index creation works with a small number of vectors
\ir utils/sift1k_array.sql

\set ON_ERROR_STOP off
-- Validate error on invalid path
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/invalid-path');
-- Validate error on incompatible version
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2-0.0.0.usearch');
-- Validate error on more recent imcompatible version
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2.usearch');
-- Validate error on invalid file
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2-corrupted.usearch');
\set ON_ERROR_STOP on
-- Validate that creating an index from file works
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2sq-0.0.13.usearch');
SELECT _lantern_internal.validate_index('hnsw_l2_index', false);
SELECT * FROM ldb_get_indexes('sift_base1k');

SET enable_seqscan=FALSE;
SET lantern.pgvector_compat=FALSE;

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <?> :'v777' LIMIT 10;
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <?> :'v777' LIMIT 10;
-- Validate that inserting rows on index created from file works as expected
INSERT INTO sift_base1k (id, v) VALUES 
(1001, array_fill(1, ARRAY[128])),
(1002, array_fill(2, ARRAY[128]));
SELECT v AS v1001 FROM sift_base1k WHERE id = 1001 \gset
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

-- Drop and recreate table
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql

-- Validate that creating an index from file works with cosine distance function
CREATE INDEX hnsw_cos_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-cos-0.0.13.usearch');
SELECT _lantern_internal.validate_index('hnsw_cos_index', false);
SELECT * FROM ldb_get_indexes('sift_base1k');

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <?> :'v777' LIMIT 10;
SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <?> :'v777' LIMIT 10;

--- Test scenarious ---
-----------------------------------------

-- Case:
-- Index is created externally
-- Vectors are deleted from the table
-- CREATE INDEX is run on the table with external file
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql
DELETE FROM sift_base1k WHERE id=777;
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2sq-0.0.13.usearch');
SELECT _lantern_internal.validate_index('hnsw_l2_index', false);
-- This should not throw error, but the first result will not be 0 as vector 777 is deleted from the table
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <?> :'v777' LIMIT 10;

-- Should throw error when lantern_extras is not installed
SELECT lantern_reindex_external_index('hnsw_l2_index');
