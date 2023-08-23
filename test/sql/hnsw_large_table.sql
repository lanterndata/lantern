-- The goals here are:
-- - Test blockmap creation logic (triggered only after 2k vectors)
-- - Perhaps also do some lightweight benchmarking

 CREATE TABLE sift_base10k (
     id SERIAL PRIMARY KEY,
     v real[128]
);

 CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
 -- insert on an empty table/index
 \copy sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
SELECT V AS v4444  FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN SELECT * FROM sift_base10k order by v <-> :'v4444'
LIMIT 10;
DROP INDEX hnsw_idx;
-- build index on an existing table of 10k rows
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
EXPLAIN SELECT * FROM sift_base10k order by v <-> :'v4444'
LIMIT 10;
