------------------------------------------------------------------------------
-- Test HNSW index creation
------------------------------------------------------------------------------

-- Validate that index creation works with a small number of vectors
\ir utils/small_world_array.sql
\ir utils/sift1k_array.sql

-- Validate that creating a secondary index works
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dims=128, M=4);
SELECT * FROM ldb_get_indexes('sift_base1k');

-- Validate that index creation works with a larger number of vectors
CREATE TABLE sift_base10k (
    id SERIAL PRIMARY KEY,
    v REAL[128]
);
\COPY sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' WITH CSV;
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (COSTS FALSE) SELECT * FROM sift_base10k order by v <-> :'v4444' LIMIT 10;

--- Validate that M values inside the allowed range [2, 128] do not throw an error
CREATE INDEX ON small_world USING hnsw (v) WITH (M=2);
CREATE INDEX ON small_world USING hnsw (v) WITH (M=128);

---- Validate that M values outside the allowed range [2, 128] throw an error
\set ON_ERROR_STOP off
CREATE INDEX ON small_world USING hnsw (v) WITH (M=1);
CREATE INDEX ON small_world USING hnsw (v) WITH (M=129);
\set ON_ERROR_STOP on
