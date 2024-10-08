-- Test changing tables from logged to unlogged, and from unlogged to logged

-- --------------------------
-- Start with logged table
-- --------------------------
CREATE TABLE small_world (
    id varchar(3),
    vector real[]
);

-- Insert (we insert data such that each vector has a unique distance from (0,0,0)
INSERT INTO small_world (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,1,1,0}'),
('011', '{1,1,1,1}'),
('100', '{2,1,0,0}'),
('101', '{1,2,0,1}'),
('110', '{1,2,1,1}'),
('111', '{2,2,2,0}');


-- Create an index
CREATE INDEX small_world_idx ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate index
SELECT _lantern_internal.validate_index('small_world_idx', false);

-- Query
SET enable_seqscan = false;
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector  FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 


-- Switch table to be unlogged
ALTER TABLE small_world SET UNLOGGED;

-- Create a new index
CREATE INDEX small_world_idx2 ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate indexes
SELECT _lantern_internal.validate_index('small_world_idx', false);
SELECT _lantern_internal.validate_index('small_world_idx2', false);

-- Insert
INSERT INTO small_world (id, vector) VALUES ('002', '{0,3,1,1}');

-- Query
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector  FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 


-- Switch table to be logged again
ALTER TABLE small_world SET LOGGED;

-- Create a new index
CREATE INDEX small_world_idx3 ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate indexes
SELECT _lantern_internal.validate_index('small_world_idx', false);
SELECT _lantern_internal.validate_index('small_world_idx2', false);
SELECT _lantern_internal.validate_index('small_world_idx3', false);

-- Insert
INSERT INTO small_world (id, vector) VALUES ('020', '{0,0,4,0}');

-- Query
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector  FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 


-- --------------------------
-- Start with unlogged table
-- --------------------------
DROP TABLE small_world;

CREATE UNLOGGED TABLE small_world (
    id varchar(3),
    vector real[]
);

-- Insert (we insert data such that each vector has a unique distance from (0,0,0)
INSERT INTO small_world (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,1,1,0}'),
('011', '{1,1,1,1}'),
('100', '{2,1,0,0}'),
('101', '{1,2,0,1}'),
('110', '{1,2,1,1}'),
('111', '{2,2,2,0}');


-- Create an index
CREATE INDEX small_world_idx ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate index
SELECT _lantern_internal.validate_index('small_world_idx', false);

-- Query
SET enable_seqscan = false;
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 


-- Switch table to be logged
ALTER TABLE small_world SET LOGGED;

-- Create a new index
CREATE INDEX small_world_idx2 ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate indexes
SELECT _lantern_internal.validate_index('small_world_idx', false);
SELECT _lantern_internal.validate_index('small_world_idx2', false);

-- Insert
INSERT INTO small_world (id, vector) VALUES ('002', '{0,3,1,1}');

-- Query
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 



-- Switch table to be unlogged again
ALTER TABLE small_world SET UNLOGGED;

-- Create a new index
CREATE INDEX small_world_idx3 ON small_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);

-- Validate indexes
SELECT _lantern_internal.validate_index('small_world_idx', false);
SELECT _lantern_internal.validate_index('small_world_idx2', false);
SELECT _lantern_internal.validate_index('small_world_idx3', false);

-- Insert
INSERT INTO small_world (id, vector) VALUES ('020', '{0,0,4,0}');

-- Query
SELECT id, l2sq_dist(vector, '{0, 0, 0, 0}'), vector FROM small_world ORDER BY vector <-> ARRAY[0, 0, 0, 0] LIMIT 10; 



