------------------------------------------------------------------------------
-- Test HNSW index creation from file
------------------------------------------------------------------------------

-- Index files were created with ldb-create-index program which source is under https://github.com/lanterndata/lanterndb_extras/
-- We have exported index files for sift1k dataset for cosine and l2sq distances
-- With the following params m=16 ef=64 ef_construction=128 dims=128
-- Validate that index creation works with a small number of vectors
\ir utils/sift1k_array.sql

-- Validate error on invalid path
\set ON_ERROR_STOP off
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/invalid-path');
\set ON_ERROR_STOP on
-- Validate that creating an index from file works
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-l2.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k');

SET enable_seqscan = false;

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;
-- Validate that inserting rows on index created from file works as expected
INSERT INTO sift_base1k (v) VALUES 
('{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'), 
('{2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2}');
SELECT v AS v1001 FROM sift_base1k WHERE id = 1001 \gset
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <-> :'v1001' LIMIT 10;

-- Drop and recreate table
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql

-- Validate that creating an index from file works with cosine distance function
CREATE INDEX hnsw_cos_index ON sift_base1k USING hnsw (v dist_cos_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-cos.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k');

SELECT v AS v777 FROM sift_base1k WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;
SELECT ROUND(cos_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;

--- Test scenarious ---
-----------------------------------------
-- Case 1:
-- Index is created externally.
-- More vectors are added to the table
-- CREATE INDEX is run on the table with the external file

DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql
INSERT INTO sift_base1k (v) VALUES 
('{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'), 
('{2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2}');
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-l2.usearch');
-- The 1001 and 1002 vectors will be ignored in search, so the first row will not be 0 in result
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <-> :'v1001' LIMIT 10;

-- Case 2:
-- Index is created externally
-- Vectors are deleted from the table
-- CREATE INDEX is run on the table with external file
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql
DELETE FROM sift_base1k WHERE id=777;
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-l2.usearch');
-- This should not throw error, but the first result will not be 0 as vector 777 is deleted from the table
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k order by v <-> :'v777' LIMIT 10;

-- Case 3:
-- Index is created externally
-- Vectors updated
-- CREATE INDEX is run on the table with external file
DROP TABLE sift_base1k CASCADE;
\ir utils/sift1k_array.sql
UPDATE  sift_base1k SET v=:'v1001' WHERE id=777;
CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v dist_l2sq_ops) WITH (dims=128, M=16, ef=64, ef_construction=128, _experimental_index_path='/tmp/lanterndb/files/index-sift1k-l2.usearch');
-- The first row will not be 0 now as the vector under id=777 was updated to 1,1,1,1... but it was indexed with different vector
-- So the usearch index can not find 1,1,1,1,1.. vector in the index and wrong results will be returned
-- This is an expected behaviour for now
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <-> :'v1001' LIMIT 10;
