-- INSTRUCTIONS
-- run this file first, and then crash 
-- then, run the `hnsw_unlogged_post.sql` test

DROP TABLE IF EXISTS unlogged_world1;
DROP TABLE IF EXISTS unlogged_world2;
DROP TABLE IF EXISTS unlogged_world3;
DROP TABLE IF EXISTS unlogged_world4;

-- Explanation of tables
-- unlogged_world1: empty, dimension specified in index
-- unlogged_world2: empty, dimension not specified in index (this will error for now, ignored at the moment)
-- unlogged_world3: non-empty, dimension specified in index
-- unlogged_world4: non-empty, dimension not specified in index

-- morph_world: will start as unlogged and then be altered to logged; non-empty, dimension not specified 
-- morph_world2: will start as logged and then be altered to unlogged; non-empty, dimension not specified 


CREATE UNLOGGED TABLE unlogged_world1 (
    id varchar(3),
    vector real[]
);

/*
CREATE UNLOGGED TABLE unlogged_world2 (
    id varchar(3),
    vector real[]
);
*/

CREATE UNLOGGED TABLE unlogged_world3 (
    id varchar(3),
    vector real[]
);

CREATE UNLOGGED TABLE unlogged_world4 (
    id varchar(3),
    vector real[]
);

CREATE UNLOGGED TABLE morph_world (
    id varchar(3),
    vector real[]
);

CREATE TABLE morph_world2 (
    id varchar(3),
    vector real[]
);

-- Insert data into some tables 

INSERT INTO unlogged_world3 (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');

INSERT INTO unlogged_world4 (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');

INSERT INTO morph_world (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');

INSERT INTO morph_world2 (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');


-- Change table status
ALTER TABLE morph_world SET LOGGED;

ALTER TABLE morph_world2 SET UNLOGGED;


-- Verify contents of unlogged tables pre-crash
SELECT * from unlogged_world1;
--SELECT * from unlogged_world2;
SELECT * from unlogged_world3;
SELECT * from unlogged_world4;
SELECT * from morph_world;
SELECT * from morph_world2;


-- Create indexes
CREATE INDEX unlogged_world1_hnsw_idx ON unlogged_world1 USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2, dim=4);
--CREATE INDEX unlogged_world2_hnsw_idx ON unlogged_world2 USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
CREATE INDEX unlogged_world3_hnsw_idx ON unlogged_world3 USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2, dim=4);
CREATE INDEX unlogged_world4_hnsw_idx ON unlogged_world4 USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
CREATE INDEX morph_world_hnsw_idx ON morph_world USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
CREATE INDEX morph_world2_hnsw_idx ON morph_world2 USING lantern_hnsw (vector) WITH (M=14, ef=22, ef_construction=2);



-- Validate indexes pre-crash
SELECT _lantern_internal.validate_index('unlogged_world1_hnsw_idx', true);
--SELECT _lantern_internal.validate_index('unlogged_world2_hnsw_idx', true);
SELECT _lantern_internal.validate_index('unlogged_world3_hnsw_idx', true);
SELECT _lantern_internal.validate_index('unlogged_world4_hnsw_idx', true);
SELECT _lantern_internal.validate_index('morph_world_hnsw_idx', true);
SELECT _lantern_internal.validate_index('morph_world2_hnsw_idx', true);



-- Now, we crash the database (todo:: find a way to do this programatically from within this .sql file?)
-- We can do this in one of two ways. Either:
-- 1. Find pid of master pg process using `ps aux | grep postgres` and then kill it with `kill -9`
-- OR
-- 2. `pg_ctl stop -D {PGDATA DIRECTORY} -m immediate`

-- After crashing, restart it with:
-- sudo systemctl restart postgresql

-- Then, run `hnsw_unlogged_post.sql` 