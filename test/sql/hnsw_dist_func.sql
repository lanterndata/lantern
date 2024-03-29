---------------------------------------------------------------------
-- Test the distance functions used by the HNSW index
---------------------------------------------------------------------

\ir utils/small_world_array.sql

CREATE TABLE small_world_l2 (id VARCHAR(3), v REAL[]);
CREATE TABLE small_world_cos (id VARCHAR(3), v REAL[]);
CREATE TABLE small_world_ham (id VARCHAR(3), v INTEGER[]);

CREATE INDEX ON small_world_l2 USING lantern_hnsw (v dist_l2sq_ops) WITH (dim=3);
CREATE INDEX ON small_world_cos USING lantern_hnsw (v dist_cos_ops) WITH (dim=3);
CREATE INDEX ON small_world_ham USING lantern_hnsw (v dist_hamming_ops) WITH (dim=3);

INSERT INTO small_world_l2 SELECT id, v FROM small_world;
INSERT INTO small_world_cos SELECT id, v FROM small_world;
INSERT INTO small_world_ham SELECT id, ARRAY[CAST(v[1] AS INTEGER), CAST(v[2] AS INTEGER), CAST(v[3] AS INTEGER)] FROM small_world;

SET enable_seqscan=FALSE;
SET lantern.pgvector_compat=FALSE;

-- Verify that the distance functions work (check distances)
SELECT ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 ORDER BY v <?> '{0,1,0}';
SELECT ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos ORDER BY v <?> '{0,1,0}';
SELECT ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham ORDER BY v <?> '{0,1,0}';

-- Verify that the distance functions work (check IDs)
SELECT ARRAY_AGG(id ORDER BY id), ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham GROUP BY 2 ORDER BY 2;

-- Verify that the indexes is being used
EXPLAIN (COSTS false) SELECT id FROM small_world_l2 ORDER BY v <?> '{0,1,0}';
EXPLAIN (COSTS false) SELECT id FROM small_world_cos ORDER BY v <?> '{0,1,0}';
EXPLAIN (COSTS false) SELECT id FROM small_world_ham ORDER BY v <?> '{0,1,0}';

\set ON_ERROR_STOP off

-- Expect errors due to mismatching vector dimensions
SELECT 1 FROM small_world_l2 ORDER BY v <?> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_cos ORDER BY v <?> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_ham ORDER BY v <?> '{0,1,0,1}' LIMIT 1;
SELECT l2sq_dist('{1,1}'::REAL[], '{0,1,0}'::REAL[]);
SELECT cos_dist('{1,1}'::real[], '{0,1,0}'::real[]);
-- the one below is umbiguous if pgvector's vector type is present
SELECT cos_dist('{1,1}', '{0,1,0}');
SELECT hamming_dist('{1,1}', '{0,1,0}');

-- Expect errors due to improper use of the <?> operator outside of its supported context
SELECT ARRAY[1,2,3] <?> ARRAY[3,2,1];
SELECT ROUND((v <?> ARRAY[0,1,0])::numeric, 2) FROM small_world_cos ORDER BY v <?> '{0,1,0}' LIMIT 7;
SELECT ROUND((v <?> ARRAY[0,1,0])::numeric, 2) FROM small_world_ham ORDER BY v <?> '{0,1,0}' LIMIT 7;

\set ON_ERROR_STOP on

-- More robust distance operator tests
CREATE TABLE test1 (id SERIAL, v REAL[]);
CREATE TABLE test2 (id SERIAL, v REAL[]);
INSERT INTO test1 (v) VALUES ('{5,3}');
INSERT INTO test2 (v) VALUES ('{5,4}');

-- Expect success
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;

\set ON_ERROR_STOP off

-- Expect errors due to incorrect usage
INSERT INTO test1 (v) VALUES (ARRAY['{1,2}'::REAL[] <?> '{4,2}'::REAL[], 0]);
SELECT v <?> '{1,2}' FROM test1 ORDER BY v <?> '{1,3}';
SELECT v <?> '{1,2}' FROM test1;
WITH temp AS (SELECT v <?> '{1,2}' FROM test1) SELECT 1 FROM temp;
SELECT t.res FROM (SELECT v <?> '{1,2}' AS res FROM test1) t;
SELECT (SELECT v <?> '{1,2}' FROM test1 LIMIT 1) FROM test1;
SELECT COALESCE(v <?> '{1,2}', 0) FROM test1;
SELECT EXISTS (SELECT v <?> '{1,2}' FROM test1);
SELECT test1.v <?> test2.v FROM test1 JOIN test2 USING (id);
SELECT v <?> '{1,2}' FROM test1 UNION SELECT v <?> '{1,3}' FROM test1;
(SELECT v <?> '{1,2}' FROM test1 WHERE id < 5) UNION (SELECT v <?> '{1,3}' FROM test1 WHERE id >= 5);
SELECT MAX(v <?> '{1,2}') FROM test1;
SELECT * FROM test1 JOIN test2 ON test1.v <?> test2.v < 0.5;
SELECT test1.v FROM test1 JOIN test2 ON test1.v <?> '{1,2}' = test2.v <?> '{1,3}';
SELECT (v <?> '{1,2}') + (v <?> '{1,3}') FROM test1;
SELECT CASE WHEN v <?> '{1,2}' > 1 THEN 'High' ELSE 'Low' END FROM test1;
INSERT INTO test1 (v) VALUES ('{2,3}') RETURNING v <?> '{1,2}';
SELECT 1 FROM test1 GROUP BY v <?> '{1,3}';
SELECT 1 FROM test1 ORDER BY (('{1,2}'::real[] <?> '{3,4}'::real[]) - 0);
SELECT 1 FROM test1 ORDER BY '{1,2}'::REAL[] <?> '{3,4}'::REAL[];
SELECT 1 FROM test1 ORDER BY v <?> ARRAY[(SELECT '{1,4}'::REAL[] <?> '{4,2}'::REAL[]), 3];

-- Expect errors due to index not existing
SELECT id FROM test1 ORDER BY v <?> '{1,2}';
SELECT 1 FROM test1 ORDER BY v <?> (SELECT '{1,3}'::real[]);
SELECT t2_results.id FROM test1 t1 JOIN LATERAL (SELECT t2.id FROM test2 t2 ORDER BY t1.v <?> t2.v LIMIT 1) t2_results ON TRUE;
WITH t AS (SELECT id FROM test1 ORDER BY v <?> '{1,2}' LIMIT 1) SELECT DISTINCT id FROM t;
WITH t AS (SELECT id FROM test1 ORDER BY v <?> '{1,2}' LIMIT 1) SELECT id, COUNT(*) FROM t GROUP BY 1;
WITH t AS (SELECT id FROM test1 ORDER BY v <?> '{1,2}') SELECT id FROM t UNION SELECT id FROM t;

-- issue #227
SELECT * from test2 JOIN LATERAL (SELECT * FROM (SELECT id FROM test2 ORDER BY v <?> '{1,2}') as forall) haha on TRUE;
-- more complex setup of the above
SELECT forall.id, nearest_per_id.* FROM
(SELECT * FROM
  test2) AS forall
  JOIN LATERAL (
    SELECT
      ARRAY_AGG(id ORDER BY id) AS near_ids,
      ARRAY_AGG(dist ORDER BY id) AS near_dists
    FROM
      (
        SELECT
          id,
          l2sq_dist(v, forall.v) as dist
        FROM
          test2
        ORDER BY
          v <?> forall.v
        LIMIT
          5
      ) as __unused_name
  ) nearest_per_id on TRUE
ORDER BY
  forall.id
LIMIT
  9;

\set ON_ERROR_STOP on
-- cross-lateral joins work as expected when appropriate index exists
-- nearest element for each vector
-- Note: The limit below is 4 to make sure all neighbors with distance 1 are included
-- and none of distance 2 are included. if we include some of distance 2, then we need
-- further sorting to make sure ties among nodes with distance 2 are broken consistently
SELECT forall.id, nearest_per_id.* FROM
(SELECT * FROM
  small_world_l2) AS forall
  JOIN LATERAL (
    SELECT
      ARRAY_AGG(id ORDER BY dist, id) AS near_ids,
      ARRAY_AGG(dist ORDER BY dist, id) AS near_dists
    FROM
      (
        SELECT
          id,
          l2sq_dist(v, forall.v) as dist
        FROM
          small_world_l2
        ORDER BY
          v <?> forall.v
        LIMIT
          4
      ) as __unused_name
  ) nearest_per_id on TRUE
ORDER BY
  forall.id
LIMIT
  9;

-- Check that hamming distance query results are sorted correctly
CREATE TABLE extra_small_world_ham (
    id SERIAL PRIMARY KEY,
    v INT[2]
);
INSERT INTO extra_small_world_ham (v) VALUES ('{0,0}'), ('{1,1}'), ('{2,2}'), ('{3,3}');
CREATE INDEX ON extra_small_world_ham USING lantern_hnsw (v dist_hamming_ops) WITH (dim=2);
SELECT ROUND(hamming_dist(v, '{0,0}')::numeric, 2) FROM extra_small_world_ham ORDER BY v <?> '{0,0}';

SELECT _lantern_internal.validate_index('small_world_l2_v_idx', false);
SELECT _lantern_internal.validate_index('small_world_cos_v_idx', false);
SELECT _lantern_internal.validate_index('small_world_ham_v_idx', false);
SELECT _lantern_internal.validate_index('extra_small_world_ham_v_idx', false);
