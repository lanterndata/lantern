-- create a table like in small world
DROP TABLE IF EXISTS small_world;
CREATE TABLE small_world (
    id VARCHAR(3),
    b BOOLEAN,
    v REAL[3]
);

CREATE INDEX ON small_world USING hnsw (v) WITH (dims=3, M=5, ef=20, ef_construction=20);

SET enable_seqscan = off;

INSERT INTO small_world (id, b, v) VALUES
    ('000', TRUE,  '{0,0,0}'),
    ('001', TRUE,  '{0,0,1}'),
    ('010', FALSE, '{0,1,0}'),
    ('011', TRUE,  '{0,1,1}'),
    ('100', FALSE, '{1,0,0}'),
    ('101', FALSE, '{1,0,1}'),
    ('110', FALSE, '{1,1,0}'),
    ('111', TRUE,  '{1,1,1}');

SELECT * FROM small_world ORDER BY v <-> '{0,0,0}' LIMIT 3;
EXPLAIN (COSTS FALSE) SELECT * FROM small_world ORDER BY v <-> '{0,0,0}' LIMIT 3;
SELECT * FROM small_world ORDER BY v <-> '{0,0,0}' LIMIT 10;