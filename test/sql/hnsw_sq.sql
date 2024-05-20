------------------------------------------------------------------------------
-- Test HNSW scalar and binary quantization
------------------------------------------------------------------------------

-- Validate that index creation works with a small number of vectors
\ir utils/small_world_array.sql
\ir utils/sift1k_array.sql

\set VERBOSITY default
SET enable_seqscan = off;

-- test failures on wrong option values
\set ON_ERROR_STOP off
CREATE INDEX ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=8, quant_bits=3);
CREATE INDEX ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=8, quant_bits=0);
\set ON_ERROR_STOP on
CREATE INDEX ind32 ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=8, quant_bits=32);
CREATE INDEX ind16 ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=8, quant_bits=16);
SELECT * FROM ldb_get_indexes('sift_base1k');

SELECT v as v42 from sift_base1k WHERE id = 42 \gset

BEGIN;
DROP INDEX ind16;
-- costs cause a regression in this query, so we disable it
EXPLAIN (COSTS FALSE) SELECT id, ROUND((v <-> :'v42')::numeric, 1) as dist FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;
                      SELECT id, ROUND((v <-> :'v42')::numeric, 1) as dist, l2sq_dist(v, :'v42') FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;
ROLLBACK;

DROP INDEX ind32, ind16;
-- create a transformed column that can be used for i8 uniform [-1-1]=>[-100,100] quantization and
-- binary > 0 quantization
ALTER TABLE sift_base1k ADD COLUMN v_transformed real[];
UPDATE sift_base1k SET v_transformed =  (
  SELECT array_agg((element - 50)/ 100.0)
  FROM unnest(v) AS t(element)
);
SELECT v_transformed as v_transformed  from sift_base1k WHERE id = 42 \gset
CREATE INDEX ind8 ON sift_base1k USING lantern_hnsw (v_transformed) WITH (dim=128, M=8, quant_bits=8);

SELECT * FROM ldb_get_indexes('sift_base1k');
EXPLAIN SELECT id, ROUND((v_transformed <-> :'v_transformed')::numeric, 1) as dist FROM sift_base1k ORDER BY v_transformed <-> :'v_transformed' LIMIT 10;
        SELECT id, ROUND((v_transformed <-> :'v_transformed')::numeric, 1) as dist FROM sift_base1k ORDER BY v_transformed <-> :'v_transformed' LIMIT 10;
DROP INDEX ind8;
SELECT * FROM ldb_get_indexes('sift_base1k');

SELECT v_transformed as v_transformed42 from sift_base1k WHERE id = 42 \gset
CREATE INDEX ind1 ON sift_base1k USING lantern_hnsw (v_transformed) WITH (dim=128, M=8, quant_bits=1);
SELECT * FROM ldb_get_indexes('sift_base1k');
EXPLAIN SELECT id, ROUND((v_transformed <-> :'v_transformed42')::numeric, 1) as dist FROM sift_base1k ORDER BY v_transformed <-> :'v_transformed42' LIMIT 10;
        SELECT id, ROUND((v_transformed <-> :'v_transformed42')::numeric, 1) as dist FROM sift_base1k ORDER BY v_transformed <-> :'v_transformed42' LIMIT 10;
-- test on 2000+ dim vectors
