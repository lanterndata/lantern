CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

\qecho
\set ON_ERROR_STOP on

-- this take should be made more robust in the following ways
-- 1. make sure selected distances are not so close that numeric rounding
--  would result in different answers on different platforms
-- 2. make sure all columns are sorted so output order is deterministic
--  Make sure pgvector is present
SELECT '[1,2,3]'::vector;

CREATE TABLE items (id bigserial PRIMARY KEY, trait_ai vector(3));
INSERT INTO items (trait_ai) VALUES ('[1,2,3]'), ('[4,5,6]');
SELECT * FROM items ORDER BY trait_ai <-> '[3,1,2]' LIMIT 5;
CREATE INDEX ON items USING hnsw (trait_ai vector_l2_ops);

CREATE TABLE large_vector (v vector(2001));
\set ON_ERROR_STOP off
CREATE INDEX ON large_vector USING hnsw (v);
\set ON_ERROR_STOP on

CREATE TABLE small_world (
    id varchar(3),
    vector vector(3)
);


INSERT INTO small_world (id, vector) VALUES 
('000', '[0,0,0]'),
('001', '[0,0,1]'),
('010', '[0,1,0]'),
('011', '[0,1,1]'),
('100', '[1,0,0]'),
('101', '[1,0,1]'),
('110', '[1,1,0]'),
('111', '[1,1,1]');

SET enable_seqscan = off;

begin;
CREATE INDEX ON small_world USING hnsw (vector);
\d+ small_world
SELECT id, ROUND( (vector <-> '[0,0,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,0,0]' LIMIT 5;
SELECT id, ROUND( (vector <-> '[0,1,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,1,0]' LIMIT 5;
rollback;


begin;
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=2, ef=11, ef_construction=12);
\d+ small_world
SELECT id, ROUND( (vector <-> '[0,0,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,0,0]' LIMIT 5;
SELECT id, ROUND( (vector <-> '[0,1,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,1,0]' LIMIT 5;
rollback;

begin;
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=11, ef=2, ef_construction=2);
\d+ small_world
SELECT id, ROUND( (vector <-> '[0,0,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,0,0]' LIMIT 5;
SELECT id, ROUND( (vector <-> '[0,1,0]')::numeric, 2) as dist
FROM small_world 
ORDER BY vector <-> '[0,1,0]' LIMIT 5;
rollback;

\echo "Done with hnsw.sql test!"