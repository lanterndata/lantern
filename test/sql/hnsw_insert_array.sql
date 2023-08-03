CREATE EXTENSION IF NOT EXISTS lanterndb;

\ir test_helpers/small_world_arrays.sql

CREATE INDEX ON small_world USING hnsw (vector);

SET enable_seqscan = off;

INSERT INTO small_world (id, vector) VALUES ('xxx', '{0,0,0}');
INSERT INTO small_world (id, vector) VALUES ('x11', '{0,0,110}');
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




-- another insert test
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
-- index scan
EXPLAIN SELECT id, ROUND((vector <-> array[0,0,0])::numeric, 2) FROM new_small_world ORDER BY vector <-> array[0,0,0] LIMIT 10;
SELECT id, ROUND((vector <-> array[0,0,0])::numeric, 2) FROM new_small_world ORDER BY vector <-> array[0,0,0] LIMIT 10;

