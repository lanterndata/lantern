---------------------------------------------------------------------
-- Test HNSW index inserts on empty table
---------------------------------------------------------------------

CREATE TABLE small_world (
    id SERIAL PRIMARY KEY,
    v REAL[2]
);
CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3);

-- Insert rows with valid vector data
INSERT INTO small_world (v) VALUES ('{0,0,1}'), ('{0,1,0}');
INSERT INTO small_world (v) VALUES (NULL);

-- Attempt to insert a row with an incorrect vector length
\set ON_ERROR_STOP off
INSERT INTO small_world (v) VALUES ('{1,1,1,1}');
\set ON_ERROR_STOP on

DROP TABLE small_world;

---------------------------------------------------------------------
-- Test HNSW index inserts on non-empty table
---------------------------------------------------------------------

\ir utils/small_world_array.sql

CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3);

SET enable_seqscan = false;

-- Inserting vectors of the same dimension and nulls should work
INSERT INTO small_world (v) VALUES ('{1,1,2}');
INSERT INTO small_world (v) VALUES (NULL);

-- Inserting vectors of different dimension should fail
\set ON_ERROR_STOP off
INSERT INTO small_world (v) VALUES ('{4,4,4,4}');
\set ON_ERROR_STOP on

-- Verify that the index works with the inserted vectors
SELECT
    id,
    ROUND(l2sq_dist(v, '{0,0,0}')::numeric, 2)
FROM
    small_world
ORDER BY
    v <-> '{0,0,0}';

-- Ensure the index size remains consistent after inserts
SELECT * from ldb_get_indexes('small_world');

-- Ensure the query plan remains consistent after inserts
EXPLAIN (COSTS FALSE)
SELECT
    id,
    ROUND(l2sq_dist(v, '{0,0,0}')::numeric, 2)
FROM
    small_world
ORDER BY
    v <-> '{0,0,0}'
LIMIT 10;

-- Test the index with a larger number of vectors
CREATE TABLE sift_base10k (
    id SERIAL PRIMARY KEY,
    v REAL[128]
);
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dims=128);
\COPY sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base_arrays.csv' WITH CSV;
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN SELECT * FROM sift_base10k order by v <-> :'v4444'
