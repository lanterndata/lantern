CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

\ir test_helpers/small_world.sql
\ir test_helpers/sift.sql

CREATE INDEX ON small_world USING hnsw (vector);
CREATE INDEX ON sift_base1k USING hnsw (v);

SET enable_seqscan = off;

insert into small_world (id, vector) values ('xxx', '[0,0,0]');
insert into small_world (id, vector) values ('x11', '[0,0,110]');
INSERT INTO small_world (id, vector) VALUES 
('000', '[0,0,0]'),
('001', '[0,0,1]'),
('010', '[0,1,0]'),
('011', '[0,1,1]'),
('100', '[1,0,0]'),
('101', '[1,0,1]'),
('110', '[1,1,0]'),
('111', '[1,1,1]');

SELECT * FROM (
    SELECT id, ROUND( (vector <-> '[0,1,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,1,0]' LIMIT 7
) v ORDER BY v.dist, v.id;

INSERT INTO small_world (id, vector) VALUES 
('000', '[0,0,0]'),
('001', '[0,0,1]'),
('010', '[0,1,0]'),
('011', '[0,1,1]'),
('100', '[1,0,0]'),
('101', '[1,0,1]'),
('110', '[1,1,0]'),
('111', '[1,1,1]');

SELECT * FROM (
    SELECT id, ROUND( (vector <-> '[0,1,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,1,0]' LIMIT 7
) v ORDER BY v.dist, v.id;

SELECT v as v42 FROM sift_base1k WHERE id = 42 \gset 

-- no index scan
BEGIN;
DROP INDEX IF EXISTS sift_base1k_hnsw_idx;
EXPLAIN SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;
SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;
ROLLBACK;


-- index scan
EXPLAIN SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;
SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v42' LIMIT 10;

-- todo:: craft an SQL query to compare the results of the two above so I do not have to do it manually