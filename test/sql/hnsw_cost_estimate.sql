SET client_min_messages=debug5;

-- Goal: make sure query cost estimate is accurate
-- when index is created with varying costruction parameters.

-- Case 0, sanity check. No data.
CREATE TABLE sift_base10k_0 (
     id SERIAL PRIMARY KEY,
     v real[128]
);
CREATE INDEX hnsw_idx ON sift_base10k_0 USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=2, dims=2);
EXPLAIN ANALYZE SELECT * FROM sift_base10k_0 order by v <-> '{1, 2}'
LIMIT 10;
DROP INDEX hnsw_idx;

-- Case 1, more data in index.
-- Should see higher cost than Case 0.
CREATE TABLE sift_base10k_1 (
     id SERIAL PRIMARY KEY,
     v real[128]
);
\copy sift_base10k_1 (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
CREATE INDEX hnsw_idx ON sift_base10k_1 USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
SELECT V AS v4444  FROM sift_base10k_1 WHERE id = 4444 \gset
EXPLAIN ANALYZE SELECT * FROM sift_base10k_1 order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;

-- Case 2, higher M.
-- Should see higher cost than Case 1.
CREATE TABLE sift_base10k_2 (
     id SERIAL PRIMARY KEY,
     v real[128]
);
\copy sift_base10k_2 (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
CREATE INDEX hnsw_idx ON sift_base10k_2 USING hnsw (v dist_l2sq_ops) WITH (M=20, ef_construction=10, ef=4, dims=128);
SELECT V AS v4444  FROM sift_base10k_2 WHERE id = 4444 \gset
EXPLAIN ANALYZE SELECT * FROM sift_base10k_2 order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;

-- Case 3, higher ef.
-- Should see higher cost than Case 2.
CREATE TABLE sift_base10k_3 (
     id SERIAL PRIMARY KEY,
     v real[128]
);
\copy sift_base10k_3 (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
CREATE INDEX hnsw_idx ON sift_base10k_3 USING hnsw (v dist_l2sq_ops) WITH (M=20, ef_construction=10, ef=16, dims=128);
SELECT V AS v4444  FROM sift_base10k_3 WHERE id = 4444 \gset
EXPLAIN ANALYZE SELECT * FROM sift_base10k_3 order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;
