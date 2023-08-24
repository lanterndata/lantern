-- We need a large enough table that postgres uses the index
 CREATE TABLE sift_base10k (
     id SERIAL PRIMARY KEY,
     v real[128]
);

\copy sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' with csv;
-- build index on an existing table of 10k rows
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v) WITH (M=2, ef_construction=10, ef=4, dims=128);
SELECT V AS v4444  FROM sift_base10k WHERE id = 4444 \gset
SELECT * FROM sift_base10k order by v <-> :'v4444' LIMIT 10;
SELECT * FROM sift_base10k order by v <-> :'v4444' LIMIT 50;
