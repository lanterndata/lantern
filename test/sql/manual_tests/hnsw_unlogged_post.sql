-- INSTRUCTIONS
-- this test is only to be run after running the `hnsw_unlogged_pre.sql` test and crashing postgres

-- Validate recovered unlogged index structure (postgres should have moved the init fork data for these indexes to their main forks)
SELECT _lantern_internal.validate_index('unlogged_world1_hnsw_idx', true); 
--SELECT _lantern_internal.validate_index('unlogged_world2_hnsw_idx', true); 
SELECT _lantern_internal.validate_index('unlogged_world3_hnsw_idx', true); 
SELECT _lantern_internal.validate_index('unlogged_world4_hnsw_idx', true); 
SELECT _lantern_internal.validate_index('morph_world_hnsw_idx', true); 
SELECT _lantern_internal.validate_index('morph_world2_hnsw_idx', true); 

-- Verify that the tables are now in fact empty after the crash, since tables are unlogged 
SELECT * from unlogged_world1;
--SELECT * from unlogged_world2;
SELECT * from unlogged_world3;
SELECT * from unlogged_world4;
SELECT * from morph_world;
SELECT * from morph_world2;

-- Verify that the indexes are operational
set enable_seqscan = false;
set enable_indexscan = true;

-- These should use an index scan and return nothing (since table is empty)
EXPLAIN SELECT * FROM unlogged_world1 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
SELECT * FROM unlogged_world1 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

--EXPLAIN SELECT * FROM unlogged_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
--SELECT * FROM unlogged_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

EXPLAIN SELECT * FROM unlogged_world3 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
SELECT * FROM unlogged_world3 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

EXPLAIN SELECT * FROM unlogged_world4 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
SELECT * FROM unlogged_world4 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

EXPLAIN SELECT * FROM morph_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
SELECT * FROM morph_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

EXPLAIN SELECT * FROM morph_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10;
SELECT * FROM morph_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 

-- Insert data into each one 
INSERT INTO unlogged_world1 (id, vector) VALUES ('101', '{1,2,3,4}');
--INSERT INTO unlogged_world2 (id, vector) VALUES ('101', '{1,2,3,4}');
INSERT INTO unlogged_world3 (id, vector) VALUES ('101', '{1,2,3,4}');
INSERT INTO unlogged_world4 (id, vector) VALUES ('101', '{1,2,3,4}');
INSERT INTO morph_world (id, vector) VALUES ('101', '{1,2,3,4}');
INSERT INTO morph_world2 (id, vector) VALUES ('101', '{1,2,3,4}');


-- Test queries after new data inserted
SELECT * FROM unlogged_world1 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
--SELECT * FROM unlogged_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
SELECT * FROM unlogged_world3 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
SELECT * FROM unlogged_world4 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
SELECT * FROM morph_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
SELECT * FROM morph_world2 ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 
