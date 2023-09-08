---------------------------------------------------------------------
-- Test HNSW index selects
---------------------------------------------------------------------
SET client_min_messages=debug5;

\ir utils/small_world_array.sql
CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3, M=5, ef=20, ef_construction=20);

\ir utils/sift1k_array.sql
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dims=128, M=5, ef=20, ef_construction=20);

CREATE TABLE test1 (id SERIAL, v REAL[]);
CREATE TABLE test2 (id SERIAL, v REAL[]);
INSERT INTO test1 (v) VALUES ('{5,3}');
INSERT INTO test2 (v) VALUES ('{5,4}');
CREATE INDEX ON test1 USING hnsw (v);

SET enable_seqscan = false;

-- Verify that basic queries still work
SELECT 0 + 1;
SELECT 1 FROM test1 WHERE id = 0 + 1;

-- Verify that the index is being used
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
SET hnsw.init_k = 4;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 3
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 15
) SELECT COUNT(*) from neighbors;
RESET client_min_messages;

-- Verify where condition works properly and still uses index
SELECT has_index_scan('SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> ''{0,0,0}''');

-- Verify that the index is not being used when there is no order by
SELECT NOT has_index_scan('SELECT COUNT(*) FROM small_world');

-- Verify swapping order doesn't change anything and still uses index
SELECT has_index_scan('SELECT id FROM test1 ORDER BY ''{1,2}''::REAL[] <-> v');

-- Verify group by works and uses index
SELECT has_index_scan('WITH t AS (SELECT id FROM test1 ORDER BY ''{1,2}''::REAL[] <-> v LIMIT 1) SELECT id, COUNT(*) FROM t GROUP BY 1');

-- Validate distinct works and uses index
SELECT has_index_scan('WITH t AS (SELECT id FROM test1 ORDER BY ''{1,2}''::REAL[] <-> v LIMIT 1) SELECT DISTINCT id FROM t');

-- Validate join lateral works and uses index
SELECT has_index_scan('SELECT t1_results.id FROM test2 t2 JOIN LATERAL (SELECT t1.id FROM test1 t1 ORDER BY t2.v <-> t1.v LIMIT 1) t1_results ON TRUE');

-- Validate union works and uses index
SELECT has_index_scan('(SELECT id FROM test1 ORDER BY v <-> ''{1,4}'') UNION (SELECT id FROM test1 ORDER BY v IS NOT NULL LIMIT 1)');

-- todo:: Verify joins work and still use index
-- todo:: Verify incremental sorts work