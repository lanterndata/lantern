CREATE EXTENSION IF NOT EXISTS lanterndb;

\ir sql/test_helpers/common.sql
\ir sql/test_helpers/small_world_arrays.sql
\ir sql/test_helpers/sift_arrays.sql

CREATE INDEX ON small_world USING hnsw (vector);
CREATE INDEX ON sift_base1k_arr USING hnsw (v) WITH (dims=128);

SET enable_seqscan = off;

INSERT INTO small_world (id, vector) VALUES ('xxx', '{0,0,0}');
INSERT INTO small_world (id, vector) VALUES ('x11', '{0,0,110}');
INSERT INTO small_world (id, vector) VALUES 
('000', NULL),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');

SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

INSERT INTO small_world (id, vector) VALUES 
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');

SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

SELECT v as v42 FROM sift_base1k_arr WHERE id = 42 \gset 

-- no index scan
BEGIN;
DROP INDEX IF EXISTS sift_base1k_hnsw_idx;
EXPLAIN (COSTS FALSE) SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k_arr ORDER BY v <-> :'v42' LIMIT 10;
SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k_arr ORDER BY v <-> :'v42' LIMIT 10;
ROLLBACK;

-- index scan
EXPLAIN (COSTS FALSE) SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k_arr ORDER BY v <-> :'v42' LIMIT 10;
SELECT id, ROUND((v <-> :'v42')::numeric, 2) FROM sift_base1k_arr ORDER BY v <-> :'v42' LIMIT 10;
-- todo:: craft an SQL query to compare the results of the two above so I do not have to do it manually


-- another insert test
DROP TABLE IF EXISTS new_small_world;
CREATE TABLE new_small_world as SELECT * from small_world;
CREATE INDEX ON new_small_world USING hnsw (vector);

INSERT INTO new_small_world (id, vector) VALUES
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');

--make sure insert of the wrong length fails
\set ON_ERROR_STOP off
INSERT INTO new_small_world (id, vector) VALUES
('000', '{4,4,4,4}');
\set ON_ERROR_STOP on
--make sure the value was actually not inserted
select * from new_small_world where id = '000';

-- index scan
SELECT '{0,0,0}'::real[] as v42  \gset
EXPLAIN (COSTS FALSE) SELECT id, ROUND((vector <-> :'v42')::numeric, 2) FROM new_small_world ORDER BY vector <-> :'v42' LIMIT 10;
SELECT id, ROUND((vector <-> :'v42')::numeric, 2) FROM new_small_world ORDER BY vector <-> :'v42' LIMIT 10;

SELECT count(*) from sift_base1k_arr;
SELECT * from ldb_get_indexes('sift_base1k_arr');
INSERT INTO sift_base1k_arr(v)
SELECT v FROM sift_base1k_arr WHERE id <= 444 AND v IS NOT NULL;
SELECT count(*) from sift_base1k_arr;
SELECT * from ldb_get_indexes('sift_base1k_arr');

-- make sure NULL inserts into the index are handled correctly
INSERT INTO small_world (id, vector) VALUES ('xxx', NULL);
CREATE UNLOGGED TABLE unlogged_small_world AS TABLE small_world;
\set ON_ERROR_STOP off
INSERT INTO small_world (id, vector) VALUES ('xxx', '{1,1,1,1}');
SELECT '{1,1,1,1}'::real[] <-> '{1,1,1}'::real[];
CREATE INDEX ON unlogged_small_world USING hnsw (vector);
