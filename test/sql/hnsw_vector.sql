---------------------------------------------------------------------
-- Test HNSW index with pgvector dependency
---------------------------------------------------------------------

-- Verify basic functionality of pgvector
SELECT '[1,2,3]'::vector;

-- Test index creation x2 on empty table and subsequent inserts
CREATE TABLE items (id SERIAL PRIMARY KEY, trait_ai VECTOR(3));
INSERT INTO items (trait_ai) VALUES ('[1,2,3]'), ('[4,5,6]');
CREATE INDEX ON items USING hnsw (trait_ai dist_vec_l2sq_ops) WITH (dims=3, M=2);
INSERT INTO items (trait_ai) VALUES ('[6,7,8]');
CREATE INDEX ON items USING hnsw (trait_ai dist_vec_l2sq_ops) WITH (dims=3, M=4);
INSERT INTO items (trait_ai) VALUES ('[10,10,10]'), (NULL);
SELECT * FROM items ORDER BY trait_ai <-> '[0,0,0]' LIMIT 3;
SELECT * FROM ldb_get_indexes('items');

-- Test index creation on table with existing data
\ir utils/small_world_vector.sql
SET enable_seqscan = false;
CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3, M=5, ef=20, ef_construction=20);
SELECT * FROM ldb_get_indexes('small_world');
INSERT INTO small_world (v) VALUES ('[99,99,2]');
INSERT INTO small_world (v) VALUES (NULL);

-- Distance functions
SELECT id, ROUND(l2sq_dist(v, '[0,1,0]'::VECTOR)::numeric, 2) as dist
FROM small_world ORDER BY v <-> '[0,1,0]'::VECTOR LIMIT 7;
EXPLAIN SELECT id, ROUND(l2sq_dist(v, '[0,1,0]'::VECTOR)::numeric, 2) as dist
FROM small_world ORDER BY v <-> '[0,1,0]'::VECTOR LIMIT 7;

SELECT id, ROUND(vector_l2sq_dist(v, '[0,1,0]'::VECTOR)::numeric, 2) as dist
FROM small_world ORDER BY v <-> '[0,1,0]'::VECTOR LIMIT 7;
EXPLAIN SELECT id, ROUND(vector_l2sq_dist(v, '[0,1,0]'::VECTOR)::numeric, 2) as dist
FROM small_world ORDER BY v <-> '[0,1,0]'::VECTOR LIMIT 7;

-- Verify that index creation on a large vector produces an error
CREATE TABLE large_vector (v VECTOR(2001));
\set ON_ERROR_STOP off
CREATE INDEX ON large_vector USING hnsw (v);
\set ON_ERROR_STOP on

-- Validate that index creation works with a larger number of vectors
CREATE TABLE sift_base10k (
    id SERIAL PRIMARY KEY,
    v VECTOR(128)
);
\COPY sift_base10k (v) FROM '/tmp/lanterndb/vector_datasets/siftsmall_base.csv' WITH CSV;
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v);
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN SELECT * FROM sift_base10k ORDER BY v <-> :'v4444' LIMIT 10;

-- Ensure we can query an index for more elements than the value of init_k
SET client_min_messages TO DEBUG5;
SET hnsw.init_k = 4;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '[1,0,0]' LIMIT 3
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '[1,0,0]' LIMIT 15
) SELECT COUNT(*) from neighbors;
RESET client_min_messages;

\set ON_ERROR_STOP off

-- Expect error due to improper use of the <-> operator outside of its supported context
SELECT ARRAY[1,2,3] <-> ARRAY[3,2,1];

-- Expect error due to mismatching vector dimensions
SELECT 1 FROM small_world ORDER BY v <-> '[0,1,0,1]' LIMIT 1;
SELECT vector_l2sq_dist('[1,1]'::vector, '[0,1,0]'::vector);