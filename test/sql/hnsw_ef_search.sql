------------------------------------------------------------------------------
-- Test changing hnsw.ef variable at runtime 
------------------------------------------------------------------------------

\ir utils/sift1k_array.sql

CREATE INDEX hnsw_l2_index ON sift_base1k USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k');

INSERT INTO sift_base1k (id, v) VALUES 
(1001, array_fill(1, ARRAY[128])),
(1002, array_fill(2, ARRAY[128]));

-- Validate error on invalid ef_search values
\set ON_ERROR_STOP off
SET hnsw.ef = -1;
SET hnsw.ef = 0;
SET hnsw.ef = 401;
\set ON_ERROR_STOP on

-- Repeat the same query while varying ef parameter
-- NOTE: it is not entirely known if the results of these are deterministic
SET enable_seqscan=FALSE;
SET lantern.pgvector_compat=FALSE;
SELECT v AS v1001 FROM sift_base1k WHERE id = 1001 \gset

-- Queries below have the same result
SET hnsw.ef = 1;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 2;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 4;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 8;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 16;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

-- Queries below have the same result, which is different from above
SET hnsw.ef = 32;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 64;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 128;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 256;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;

SET hnsw.ef = 400;
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k order by v <?> :'v1001' LIMIT 10;
