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

-- Verify that the distance functions work (check distances)
SELECT ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 ORDER BY v <-> '{0,1,0}';
SELECT ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos ORDER BY v <=> '{0,1,0}';
SELECT ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham ORDER BY v <+> '{0,1,0}';

-- Verify that the distance functions work (check IDs)
SELECT ARRAY_AGG(id ORDER BY id), ROUND(l2sq_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_l2 GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(cos_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_cos GROUP BY 2 ORDER BY 2;
SELECT ARRAY_AGG(id ORDER BY id), ROUND(hamming_dist(v, '{0,1,0}')::numeric, 2) FROM small_world_ham GROUP BY 2 ORDER BY 2;

-- Verify that the indexes is being used
EXPLAIN (COSTS false) SELECT id FROM small_world_l2 ORDER BY v <-> '{0,1,0}';
EXPLAIN (COSTS false) SELECT id FROM small_world_cos ORDER BY v <=> '{0,1,0}';
EXPLAIN (COSTS false) SELECT id FROM small_world_ham ORDER BY v <+> '{0,1,0}';

\set ON_ERROR_STOP off

-- Expect errors due to mismatching vector dimensions
SELECT 1 FROM small_world_l2 ORDER BY v <-> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_cos ORDER BY v <=> '{0,1,0,1}' LIMIT 1;
SELECT 1 FROM small_world_ham ORDER BY v <+> '{0,1,0,1}' LIMIT 1;
SELECT l2sq_dist('{1,1}'::REAL[], '{0,1,0}'::REAL[]);
SELECT cos_dist('{1,1}'::real[], '{0,1,0}'::real[]);
-- the one below is umbiguous if pgvector's vector type is present
SELECT cos_dist('{1,1}', '{0,1,0}');
SELECT hamming_dist('{1,1}', '{0,1,0}');


\set ON_ERROR_STOP on

-- More robust distance operator tests
CREATE TABLE test1 (id SERIAL, v REAL[]);
CREATE TABLE test2 (id SERIAL, v REAL[]);
INSERT INTO test1 (v) VALUES ('{5,3}');
INSERT INTO test2 (v) VALUES ('{5,4}');

-- Expect success
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;

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
          v <-> forall.v
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
SELECT ROUND(hamming_dist(v, '{0,0}')::numeric, 2) FROM extra_small_world_ham ORDER BY v <+> '{0,0}';

SELECT _lantern_internal.validate_index('small_world_l2_v_idx', false);
SELECT _lantern_internal.validate_index('small_world_cos_v_idx', false);
SELECT _lantern_internal.validate_index('small_world_ham_v_idx', false);
SELECT _lantern_internal.validate_index('extra_small_world_ham_v_idx', false);
