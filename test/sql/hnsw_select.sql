---------------------------------------------------------------------
-- Test HNSW index selects
---------------------------------------------------------------------

\ir utils/small_world_array.sql
CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3, M=5, ef=20, ef_construction=20);

\ir utils/sift1k_array.sql
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dims=128, M=5, ef=20, ef_construction=20);

SET enable_seqscan = false;

-- Verify that the index is being used
EXPLAIN (COSTS FALSE) SELECT * FROM small_world order by v <-> '{1,0,0}' LIMIT 1;

-- Verify that this does not use the index
EXPLAIN (COSTS FALSE) SELECT 1 FROM small_world WHERE v = '{0,0,0}';

-- Ensure we can query an index for more elements than the value of init_k
SET client_min_messages TO DEBUG5;
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
SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> '{0,0,0}';
EXPLAIN (COSTS FALSE) SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> '{0,0,0}';

-- Verify that the index is not being used when there is no order by
EXPLAIN (COSTS FALSE) SELECT COUNT(*) FROM small_world;

-- todo:: Verify joins work and still use index
-- todo:: Verify incremental sorts work