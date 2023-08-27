---------------------------------------------------------------------
-- Test the distance functions used by the HNSW index
---------------------------------------------------------------------

\ir utils/small_world_array.sql

CREATE TABLE small_world_l2 (id VARCHAR(3), v REAL[]);
CREATE TABLE small_world_cos (id VARCHAR(3), v REAL[]);
CREATE TABLE small_world_ham (id VARCHAR(3), v INTEGER[]);

CREATE INDEX ON small_world_l2 USING hnsw (v dist_l2sq_ops) WITH (dims=3);
CREATE INDEX ON small_world_cos USING hnsw (v dist_cos_ops) WITH (dims=3);
CREATE INDEX ON small_world_ham USING hnsw (v dist_hamming_ops) WITH (dims=3);

INSERT INTO small_world_l2 SELECT id, v FROM small_world;
INSERT INTO small_world_cos SELECT id, v FROM small_world;
INSERT INTO small_world_ham SELECT id, v FROM small_world;

SET enable_seqscan = false;

-- Verify that the distance functions work (check distances)
SELECT ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 ORDER BY v <-> '{0,1,0}';
SELECT ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos ORDER BY v <-> '{0,1,0}';
SELECT ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham ORDER BY v <-> '{0,1,0}';

-- Verify that the distance functions work (check IDs)
SELECT ARRAY_AGG(id ORDER BY id), ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham GROUP BY 2 ORDER BY 2;

-- Verify that the indexes is being used
EXPLAIN SELECT id FROM small_world_l2 ORDER BY v <-> '{0,1,0}';
EXPLAIN SELECT id FROM small_world_cos ORDER BY v <-> '{0,1,0}';
EXPLAIN SELECT id FROM small_world_ham ORDER BY v <-> '{0,1,0}';

\set ON_ERROR_STOP off

-- Expect errors due to mismatching vector dimensions
SELECT 1 FROM small_world_l2 ORDER BY v <-> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_cos ORDER BY v <-> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_ham ORDER BY v <-> '{0,1,0,1}' LIMIT 1;
SELECT l2sq_dist('{1,1}', '{0,1,0}');
SELECT cos_dist('{1,1}', '{0,1,0}');
SELECT hamming_dist('{1,1}', '{0,1,0}');

-- Expect errors due to improper use of the <-> operator outside of its supported context
SELECT array[1,2,3] <-> array[3,2,1];
SELECT ROUND((v <-> array[0,1,0])::numeric, 2) FROM small_world_cos ORDER BY v <-> '{0,1,0}' LIMIT 7;
SELECT ROUND((v <-> array[0,1,0])::numeric, 2) FROM small_world_ham ORDER BY v <-> '{0,1,0}' LIMIT 7;

