------------------------------------------------------------------------------
-- Test HNSW index creation from file
------------------------------------------------------------------------------

-- Index files were created with ldb-create-index program which source is under https://github.com/lanterndata/lanterndb_extras/
-- We have exported index files for sift1k dataset for cosine and l2sq distances
-- With the following params m=16 ef=64 ef_construction=128 dims=128
-- Validate that index creation works with a small number of vectors
\ir utils/sift1k_array.sql

-- Validate that creating an index from file works
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-l2.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k');

SET enable_seqscan = false;

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;

DROP INDEX hnsw_l2_index;

-- Validate that creating an index from file works with cosine distance function
CREATE INDEX hnsw_cos_index ON sift_base1k USING hnsw (v dist_cos_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-cos.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k');

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;
SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;

