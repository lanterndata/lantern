---------------------------------------------------------------------
-- Test support for unlogged tables
---------------------------------------------------------------------

-- Test insertion and index creation on an empty unlogged table
CREATE UNLOGGED TABLE small_world_unlogged (
    id varchar(3),
    vector real[]
);

INSERT INTO small_world_unlogged (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');

CREATE INDEX small_world4_hnsw_idx ON small_world_unlogged USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Creating the index should not prevent further insertions
INSERT INTO small_world_unlogged (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}');

-- Attempt to insert a row with an incorrect vector length
\set ON_ERROR_STOP off
INSERT INTO small_world_unlogged (id, vector) VALUES ('111', '{1,1,1}');
\set ON_ERROR_STOP on

-- Verify that the index works
SET enable_seqscan = false;
SELECT * FROM ldb_get_indexes('small_world_unlogged');
SELECT vector AS vector000 FROM small_world_unlogged WHERE id = '000';
EXPLAIN (COSTS FALSE) SELECT ROUND(l2sq_dist(vector, '{1,0,0,0}')::numeric, 2) FROM small_world_unlogged;
SELECT ROUND(l2sq_dist(vector, '{1,0,0,0}')::numeric, 2) FROM small_world_unlogged;

DROP TABLE small_world_unlogged;

-- Validate that creating an index from file works
\ir utils/sift1k_array_unlogged.sql
CREATE INDEX hnsw_l2_index ON sift_base1k_unlogged USING hnsw (v) WITH (_experimental_index_path='/tmp/lantern/files/index-sift1k-l2.usearch');
SELECT * FROM ldb_get_indexes('sift_base1k_unlogged');
SELECT v AS v777 FROM sift_base1k_unlogged WHERE id = 777 \gset
EXPLAIN (COSTS FALSE) SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k_unlogged order by v <-> :'v777' LIMIT 10;
SELECT ROUND(l2sq_dist(v, :'v777')::numeric, 2) FROM sift_base1k_unlogged order by v <-> :'v777' LIMIT 10;

-- Validate that inserting rows on index created from file works as expected
INSERT INTO sift_base1k_unlogged (id, v) VALUES 
(1001, array_fill(1, ARRAY[128])),
(1002, array_fill(2, ARRAY[128]));
SELECT v AS v1001 FROM sift_base1k_unlogged WHERE id = 1001 \gset
SELECT ROUND(l2sq_dist(v, :'v1001')::numeric, 2) FROM sift_base1k_unlogged order by v <-> :'v1001' LIMIT 10;

DROP TABLE sift_base1k_unlogged CASCADE;

-- Verify that unlogged tables work with the l2sq_ops distance function
\ir utils/small_world_array_unlogged.sql
CREATE UNLOGGED TABLE small_world_l2_unlogged (id VARCHAR(3), vector REAL[]);
CREATE INDEX ON small_world_l2_unlogged USING hnsw (vector dist_l2sq_ops) WITH (dim=3);
INSERT INTO small_world_l2_unlogged SELECT id, v FROM small_world_unlogged;
SELECT ROUND(l2sq_dist(vector, '{0,1,0}')::numeric, 2) FROM small_world_l2_unlogged ORDER BY vector <-> '{0,1,0}';
SELECT ARRAY_AGG(id ORDER BY id), ROUND(l2sq_dist(vector, '{0,1,0}')::numeric, 2) FROM small_world_l2_unlogged GROUP BY 2 ORDER BY 2;
EXPLAIN SELECT id FROM small_world_l2_unlogged ORDER BY vector <-> '{0,1,0}';
