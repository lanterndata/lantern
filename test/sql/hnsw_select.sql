---------------------------------------------------------------------
-- Test HNSW index selects
---------------------------------------------------------------------

\ir utils/small_world_array.sql

-- Verify where condition works properly and still uses index
SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> '{0,0,0}';
EXPLAIN (COSTS FALSE) SELECT * FROM small_world WHERE b IS TRUE ORDER BY v <-> '{0,0,0}';

-- todo:: Verify joins work and still use index
-- todo:: Verify incremental sorts work