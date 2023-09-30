CREATE TABLE small_world (
    id int,
    b BOOLEAN,
    v REAL[3]
);

CREATE INDEX test ON small_world USING hnsw (v) INCLUDE (id, b) WITH (dim=3, M=5);

INSERT INTO small_world (id, b, v) VALUES
    (1, TRUE,  '{0,0,0}'),
    (2, TRUE,  '{0,0,1}'),
    (3, FALSE, '{0,1,0}'),
    (4, TRUE,  '{0,1,1}'),
    (5, FALSE, '{1,0,0}'),
    (6, FALSE, '{1,0,1}'),
    (7, FALSE, '{1,1,0}'),
    (8, TRUE,  '{1,1,1}');

SET enable_seqscan = off;
EXPLAIN (COSTS FALSE) SELECT * FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
-- TODO this works but row ordering is non deterministic and a second order by forces seq scan
-- SELECT n, v FROM small_world ORDER BY v <-> '{1,1,1}' ASC;

-- build an index on an existing table
DROP TABLE small_world;
CREATE TABLE small_world (
    id int,
    b BOOLEAN,
    v REAL[3]
);

INSERT INTO small_world (id, b, v) VALUES
    (1, TRUE,  '{0,0,0}'),
    (2, TRUE,  '{0,0,1}'),
    (3, FALSE, '{0,1,0}'),
    (4, TRUE,  '{0,1,1}'),
    (5, FALSE, '{1,0,0}'),
    (6, FALSE, '{1,0,1}'),
    (7, FALSE, '{1,1,0}'),
    (8, TRUE,  '{1,1,1}');

CREATE INDEX test ON small_world USING hnsw (v) INCLUDE (b) WITH (dim=3, M=5);
EXPLAIN (COSTS FALSE) SELECT * FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
EXPLAIN (COSTS FALSE) SELECT b,v FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
SELECT b,v FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
-- modify the table
UPDATE small_world SET v = '{1,1,1}' WHERE id = 1;
DELETE FROM small_world WHERE id = 7;
INSERT INTO small_world (id, b, v) VALUES (9, TRUE, '{1,1,1}');
EXPLAIN (ANALYZE, COSTS FALSE) SELECT b,v FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
SELECT b,v FROM small_world ORDER BY v <-> '{1,1,1}' ASC LIMIT 3;
-- vacuum to set visibility table
VACUUM small_world;
EXPLAIN (ANALYZE, COSTS FALSE) SELECT b,v FROM small_world ORDER BY v <-> '{1,1,1}' ASC;
-- TODO add test for when included columns exceed page size
