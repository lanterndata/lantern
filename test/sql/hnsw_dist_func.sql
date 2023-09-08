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
SELECT ARRAY[1,2,3] <-> ARRAY[3,2,1];
SELECT ROUND((v <-> ARRAY[0,1,0])::numeric, 2) FROM small_world_cos ORDER BY v <-> '{0,1,0}' LIMIT 7;
SELECT ROUND((v <-> ARRAY[0,1,0])::numeric, 2) FROM small_world_ham ORDER BY v <-> '{0,1,0}' LIMIT 7;

-- More robust distance operator tests

CREATE TABLE test1 (id SERIAL, v REAL[]);
CREATE TABLE test2 (id SERIAL, v REAL[]);
INSERT INTO test1 (v) VALUES ('{5,3}');
INSERT INTO test2 (v) VALUES ('{5,4}');

-- Expect success
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;

-- Expect errors due to incorrect usage
INSERT INTO test1 (v) VALUES (ARRAY['{1,2}'::REAL[] <-> '{4,2}'::REAL[], 0]);
SELECT v <-> '{1,2}' FROM test1 ORDER BY v <-> '{1,3}';
SELECT v <-> '{1,2}' FROM test1;
WITH temp AS (SELECT v <-> '{1,2}' FROM test1) SELECT 1 FROM temp;
SELECT t.res FROM (SELECT v <-> '{1,2}' AS res FROM test1) t;
SELECT (SELECT v <-> '{1,2}' FROM test1 LIMIT 1) FROM test1;
SELECT COALESCE(v <-> '{1,2}', 0) FROM test1;
SELECT EXISTS (SELECT v <-> '{1,2}' FROM test1);
SELECT test1.v <-> test2.v FROM test1 JOIN test2 USING (id);
SELECT v <-> '{1,2}' FROM test1 UNION SELECT v <-> '{1,3}' FROM test1;
(SELECT v <-> '{1,2}' FROM test1 WHERE id < 5) UNION (SELECT v <-> '{1,3}' FROM test1 WHERE id >= 5);
SELECT MAX(v <-> '{1,2}') FROM test1;
SELECT * FROM test1 JOIN test2 ON test1.v <-> test2.v < 0.5;
SELECT test1.v FROM test1 JOIN test2 ON test1.v <-> '{1,2}' = test2.v <-> '{1,3}';
SELECT (v <-> '{1,2}') + (v <-> '{1,3}') FROM test1;
SELECT CASE WHEN v <-> '{1,2}' > 1 THEN 'High' ELSE 'Low' END FROM test1;
INSERT INTO test1 (v) VALUES ('{2,3}') RETURNING v <-> '{1,2}';
SELECT 1 FROM test1 GROUP BY v <-> '{1,3}';
SELECT 1 FROM test1 ORDER BY (('{1,2}'::real[] <-> '{3,4}'::real[]) - 0);
SELECT 1 FROM test1 ORDER BY '{1,2}'::REAL[] <-> '{3,4}'::REAL[];
SELECT 1 FROM test1 ORDER BY v <-> ARRAY[(SELECT '{1,4}'::REAL[] <-> '{4,2}'::REAL[]), 3];

-- Expect errors due to index not existing
SELECT id FROM test1 ORDER BY v <-> '{1,2}';
SELECT 1 FROM test1 ORDER BY v <-> (SELECT '{1,3}'::real[]);

-- Expect success
\set ON_ERROR_STOP on
CREATE INDEX ON test1 USING hnsw (v dist_l2sq_ops);
SELECT COALESCE(id, 0) FROM test1 ORDER BY v <-> '{1,2}';
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;
SELECT id FROM test1 ORDER BY '{1,2}'::REAL[] <-> v;
SELECT test1.id FROM test1 JOIN test2 USING (id) ORDER BY test1.v <-> test2.v LIMIT 1;