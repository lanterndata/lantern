-- Goal: make sure query cost estimate is accurate
-- when index is created with varying costruction parameters.

CREATE TABLE sift_base10k (
     id SERIAL PRIMARY KEY,
     v real[128]
);

-- Case 1, simple: dims=128, M=2
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
\copy sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
SELECT V AS v4444  FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM sift_base10k order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;

-- Case 2, higher M: dims=128, M=20
-- Should see higher cost than Case 20.
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=20, ef_construction=10, ef=4, dims=128);
\copy sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
SELECT V AS v4444  FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM sift_base10k order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;