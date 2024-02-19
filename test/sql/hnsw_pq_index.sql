DROP TABLE IF EXISTS sift_base1k;

SET client_min_messages=ERROR;

\ir utils/sift1k_array.sql
\ir utils/sift1k_array_query.sql
\ir utils/random_array.sql
\ir utils/calculate_recall.sql
-- \ir ./utils/common.sql
DROP TABLE IF EXISTS small_world_pq;
CREATE TABLE small_world_pq (
    id SERIAL,
    v REAL[]
);

INSERT INTO small_world_pq (id,v) VALUES
(0, ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]),
(1, ARRAY[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]),
(2, ARRAY[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]),
(3, ARRAY[0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3]),
(4, ARRAY[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]),
(5, ARRAY[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]),
(6, ARRAY[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]),
(7, ARRAY[0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7]),
(8, ARRAY[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]),
(9, ARRAY[0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9]);

SELECT quantize_table('small_world_pq', 'v', 10, 4, 'l2sq');
SELECT COUNT(DISTINCT subvector_id) FROM _lantern_internal._codebook_small_world_pq_v;
SELECT COUNT(DISTINCT centroid_id) FROM _lantern_internal._codebook_small_world_pq_v;

ALTER TABLE small_world_pq ADD COLUMN v_pq_dec REAL[];
UPDATE small_world_pq SET v_pq_dec=decompress_vector(v_pq, '_lantern_internal._codebook_small_world_pq_v');

SET enable_seqscan=OFF;

SELECT v as v4 FROM small_world_pq WHERE id = 4 \gset

-- index without pq
CREATE INDEX hnsw_l2_index ON small_world_pq USING lantern_hnsw(v) WITH (pq=False);
EXPLAIN (COSTS FALSE) SELECT id, v, v_pq, v_pq_dec FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 1;
SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 1;
SELECT * FROM ldb_get_indexes('small_world_pq');
DROP INDEX hnsw_l2_index;

-- index with pq
CREATE INDEX hnsw_pq_l2_index ON small_world_pq USING lantern_hnsw(v) WITH (pq=True);
EXPLAIN (COSTS FALSE) SELECT id, v, v_pq, v_pq_dec, (v <-> :'v4') as dist, (v_pq_dec <-> :'v4') real_dist FROM small_world_pq ORDER BY dist LIMIT 1;
SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 1;

ALTER TABLE small_world_pq DROP COLUMN v_pq;
ALTER TABLE small_world_pq DROP COLUMN v_pq_dec;
DROP TABLE _lantern_internal._codebook_small_world_pq_v;
DROP INDEX hnsw_pq_l2_index;

SELECT quantize_table('small_world_pq', 'v', 4, 8, 'l2sq');
ALTER TABLE small_world_pq ADD COLUMN v_pq_dec REAL[]; --  GENERATED ALWAYS AS (decompress_vector("v_pq", '_lantern_codebook_small_world_pq')) STORED; -- << cannot do because genrated columns cannot refer to other generated columns
UPDATE small_world_pq SET v_pq_dec=decompress_vector(v_pq, '_lantern_internal._codebook_small_world_pq_v');
CREATE INDEX hnsw_pq_l2_index ON small_world_pq USING lantern_hnsw(v) WITH (pq=True);
EXPLAIN (COSTS FALSE) SELECT id, v, v_pq, v_pq_dec, (v <-> :'v4') as dist, (v_pq_dec <-> :'v4') real_dist FROM small_world_pq ORDER BY dist LIMIT 1;
SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 1;
-- add another entry with vector v4, and search for it again
INSERT INTO small_world_pq(id, v) VALUES (42, :'v4');
SELECT ARRAY_AGG(id ORDER BY id) FROM
  (SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 2) b;

ALTER TABLE small_world_pq DROP COLUMN v_pq;
ALTER TABLE small_world_pq DROP COLUMN v_pq_dec;
DROP TABLE _lantern_internal._codebook_small_world_pq_v;
DROP INDEX hnsw_pq_l2_index;

ALTER TABLE small_world_pq SET UNLOGGED;

SELECT quantize_table('small_world_pq', 'v', 7, 2, 'l2sq');
ALTER TABLE small_world_pq ADD COLUMN v_pq_dec REAL[];
UPDATE small_world_pq SET v_pq_dec=decompress_vector(v_pq, '_lantern_internal._codebook_small_world_pq_v');
CREATE INDEX hnsw_pq_l2_index ON small_world_pq USING lantern_hnsw(v) WITH (pq=True);
-- we had inserted a value with id=42 and vector=:'v4' above, before making the table unlogged
SELECT ARRAY_AGG(id ORDER BY id) FROM
  (SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 2) b;
-- add another entry with vector v4, and search for it again
INSERT INTO small_world_pq(id, v) VALUES (44, :'v4');
SELECT ARRAY_AGG(id ORDER BY id) FROM
  (SELECT id FROM small_world_pq ORDER BY v <-> :'v4' LIMIT 3) b;


-- Larger indexes
SELECT quantize_table('sift_base1k'::regclass, 'v', 200, 32, 'l2sq');
SELECT v as v1 FROM sift_base1k WHERE id=1 \gset
SELECT v_pq as v1_pq FROM sift_base1k WHERE id=1 \gset
ALTER TABLE sift_base1k ADD COLUMN v_pq_dec REAL[];
-- add trigger to auto-update v_pq_dec (cannot make this generated since the base of it - v_pq - is already generated and postgres does not allow using it in generated statements)
CREATE OR REPLACE FUNCTION v_pq_dec_update_trigger()
RETURNS TRIGGER AS $$
DECLARE
   v_pq pqvec;
BEGIN
  -- cannot use the generated column as it has not been created at this point
  v_pq = quantize_vector(NEW.v, '_lantern_internal._codebook_sift_base1k_v', 'l2sq');
  NEW.v_pq_dec :=  decompress_vector(v_pq, '_lantern_internal._codebook_sift_base1k_v');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER v_pq_dec_insert_update
BEFORE INSERT OR UPDATE ON sift_base1k
FOR EACH ROW
EXECUTE FUNCTION v_pq_dec_update_trigger();

UPDATE sift_base1k SET v_pq_dec=decompress_vector(v_pq, '_lantern_internal._codebook_sift_base1k_v');
-- this will always be one
-- SELECT calculate_table_recall('sift_base1k', 'sift_query1k', 'sift_truth1k', 'v', 10, 100) as recall \gset

SELECT calculate_table_recall('sift_base1k', 'sift_query1k', 'sift_truth1k', 'v_pq_dec', 10, 100) as recall_pq \gset
CREATE INDEX sift_base1k_pq_index ON sift_base1k USING lantern_hnsw(v) WITH (pq=True);
SELECT calculate_table_recall('sift_base1k', 'sift_query1k', 'sift_truth1k', 'v', 10, 100) as recall_pq_index \gset
SELECT (:'recall_pq'::float - :'recall_pq_index'::float)::float as recall_diff \gset
-- pq index costs no more than 10% in addition to what quantization has already cost us
-- recall diff must be positive but not large - the positive check sanity-checks that the index was used in calculate_table_recall
SELECT :recall_diff > 0 AND :recall_diff < 0.1 as recall_within_range;

-- inserts
SELECT v as v2 FROM sift_base1k WHERE id=2 \gset
SELECT random_array(128, 0.0, 5.0) as v1002 \gset
INSERT INTO sift_base1k(id, v) VALUES (1001, :'v2');
INSERT INTO sift_base1k(id, v) VALUES (1002, :'v1002');
-- check that the random vector is in the top 5 position
SELECT SUM(id1002::int) = 1 as contains_id_1002 FROM (SELECT id = 1002 as id1002 FROM sift_base1k ORDER BY v <-> :'v1002' LIMIT 5) b;
-- the top two results must be the vectors corresponding to v2
SELECT ARRAY_AGG(id ORDER BY id) FROM (SELECT id FROM sift_base1k ORDER BY v <-> :'v2' LIMIT 2) b;
-- since codebook are generated each time and are non deterministic, we cannot print them in regression tests.
-- run something like the following to view the results
-- SELECT id, v_pq, (v <-> :'v1002') as dist, (v_pq_dec <-> :'v1002') pq_dist FROM sift_base1k ORDER BY dist LIMIT 3;
