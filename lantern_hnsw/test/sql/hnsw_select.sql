---------------------------------------------------------------------
-- Test HNSW index selects
---------------------------------------------------------------------
SET client_min_messages=debug5;

\ir utils/small_world_array.sql
CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (dim=3, M=5, ef=20, ef_construction=20);

\ir utils/sift1k_array.sql
CREATE INDEX ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=5, ef=20, ef_construction=20);

CREATE TABLE test1 (id SERIAL, v REAL[]);
CREATE TABLE test2 (id SERIAL, v REAL[]);
INSERT INTO test1 (v) VALUES ('{5,3}');
INSERT INTO test2 (v) VALUES ('{5,4}');
CREATE INDEX ON test1 USING lantern_hnsw (v);

SET enable_seqscan=FALSE;

-- Verify that basic queries still work given our query parser and planner hooks
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;

-- Verify that the index is being used
SET _lantern_internal.is_test = true;
EXPLAIN (COSTS FALSE) SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 1;

-- Verify that this does not use the index
EXPLAIN (COSTS FALSE) SELECT 1 FROM small_world WHERE v = '{0,0,0}';

-- Ensure we can query an index for more elements than the value of init_k
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 3
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 15
) SELECT COUNT(*) from neighbors;

-- Change default k and make sure the number of usearch_searchs makes sense
SET lantern_hnsw.init_k = 4;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 3
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 15
) SELECT COUNT(*) from neighbors;
RESET client_min_messages;

SET _lantern_internal.is_test = false;
-- Verify where condition works properly and still uses index
SELECT has_index_scan('EXPLAIN SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> ''{0,0,0}''');

-- Verify that the index is not being used when there is no order by
SELECT NOT has_index_scan('EXPLAIN SELECT COUNT(*) FROM small_world');

-- Verify swapping order doesn't change anything and still uses index
SELECT has_index_scan('EXPLAIN SELECT id FROM test1 ORDER BY ''{1,2}''::REAL[] <-> v');

-- Verify group by works and uses index
SELECT has_index_scan('EXPLAIN WITH t AS (SELECT id FROM test1 ORDER BY ''{1,2}''::REAL[] <-> v LIMIT 1) SELECT id, COUNT(*) FROM t GROUP BY 1');

-- Validate distinct works and uses index
SELECT has_index_scan('EXPLAIN WITH t AS (SELECT id FROM test1 ORDER BY v <-> ''{1,2}'' LIMIT 1) SELECT DISTINCT id FROM t');

-- Validate join lateral works and uses index
SELECT has_index_scan('EXPLAIN SELECT t1_results.id FROM test2 t2 JOIN LATERAL (SELECT t1.id FROM test1 t1 ORDER BY t2.v <-> t1.v LIMIT 1) t1_results ON TRUE');

-- Validate union works and uses index
SELECT has_index_scan('EXPLAIN (SELECT id FROM test1 ORDER BY v <-> ''{1,4}'') UNION (SELECT id FROM test1 ORDER BY v IS NOT NULL LIMIT 1)');

-- Validate CTEs work and still use index
SELECT has_index_scan('EXPLAIN WITH t AS (SELECT id FROM test1 ORDER BY v <-> ''{1,4}'') SELECT id FROM t UNION SELECT id FROM t');

set enable_indexscan = true;
set enable_seqscan = false;

-- test pagination in face of duplicates
-- Previously, usearch did not natively support pagination, so, we doubled number of elements we asked from it when more was needed.
-- this had issues in face of dupliates since consequitive search run could have slightly different order, resulting in some duplicate results and some missing results
-- the current approach of pagination that integrates streaming API into usearch, no longer has the issue, so we moved this test from hnsw_todo to here, to verify
-- pagination works correctly

DROP TABLE IF EXISTS small_world_repeat;
CREATE TABLE small_world_repeat (
    id SERIAL,
    v REAL[]
);

INSERT INTO small_world_repeat (id,v) VALUES
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

CREATE OR REPLACE FUNCTION fill_same() RETURNS VOID AS $$
BEGIN
FOR i in 1..1000 LOOP
  INSERT INTO small_world_repeat (id,v) VALUES (1000 + i, ARRAY[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]);
END LOOP;
END;
$$ LANGUAGE plpgsql;
SELECT fill_same();

CREATE INDEX hnsw_l2_index_repeat ON small_world_repeat USING lantern_hnsw(v);
set lantern_hnsw.init_k=3;
-- the query searches for the nearest 600 vectors closest to the duplicated constant vector above. It then aggregates all results in the outer query by number of times each id appears
-- if pagination worked correctly, we would expect all ids to appear at most once, but as you can see many of them appear 3 times below
explain (costs false) select id, ARRAY_AGG(dist) as dists, count(id) as cnt from (select id, (v <-> ARRAY[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]) as dist FROM small_world_repeat order by dist LIMIT 200) b group by id order by cnt DESC, dists, id limit 10;
        select case when s.cnt > 1 then 'incorrect' else 'correct' end from (
          select id, ARRAY_AGG(dist) as dists, count(id) as cnt from (select id, (v <-> ARRAY[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]) as dist FROM small_world_repeat order by dist LIMIT 200) b group by id order by cnt DESC, dists, id limit 10
        ) s;
set lantern_hnsw.init_k=200;
        select id, ARRAY_AGG(dist) as dists, count(id) as cnt from (select id, (v <-> ARRAY[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]) as dist FROM small_world_repeat order by dist LIMIT 200) b group by id order by cnt DESC, dists, id limit 10;

-- todo:: Verify joins work and still use index
-- todo:: Verify incremental sorts work

-- Validate index data structures
SELECT _lantern_internal.validate_index('small_world_v_idx', false);
SELECT _lantern_internal.validate_index('sift_base1k_v_idx', false);
SELECT _lantern_internal.validate_index('test1_v_idx', false);
