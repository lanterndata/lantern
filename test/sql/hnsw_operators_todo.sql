-- THIS IS TODO TEST FILE
-- THIS TESTS WILL NOT PASS CURRENTLY BUT SHOULD BE FIXED LATER
CREATE TABLE small_world_l2 (
    id varchar(3),
    vector real[] vector_int integer[]
);

INSERT INTO small_world_l2 (id, vector)
    VALUES ('000', '{0,0,0}'),
    ('001', '{0,0,1}'),
    ('010', '{0,1,0}'),
    ('011', '{0,1,1}'),
    ('100', '{1,0,0}'),
    ('101', '{1,0,1}'),
    ('110', '{1,1,0}'),
    ('111', '{1,1,1}');

SET enable_seqscan = FALSE;

-- this should not throw error as it is string
SELECT
    'array[1,2,3] <-> array[4,5,6]';

\set ON_ERROR_STOP off
-- this should  throw error as it is out of index usage
SELECT
    ARRAY[1, 2, 3] <-> ARRAY[4, 5, 6];

-- this should  throw error as it is out of index usage
SELECT
    '{1,2,3}'::real[] <-> '{4,5,6}'::real[];

-- this should throw error, as it is out of index usage
SELECT
    *
FROM (
    SELECT
        id,
        ROUND(l2sq_dist (vector, ARRAY[0, 1, 0])::numeric, 2) AS dist
    FROM
        small_world_l2
    ORDER BY
        vector <-> ARRAY[0, 1, 0]
    LIMIT 7) v
ORDER BY
    v.dist,
    v.id;

\set ON_ERROR_STOP on
CREATE INDEX ON small_world_l2 USING hnsw (vector dist_l2sq_ops);

-- this should not throw error, as it is index usage
SELECT
    *
FROM (
    SELECT
        id,
        ROUND(l2sq_dist (vector, ARRAY[0, 1, 0])::numeric, 2) AS dist
    FROM
        small_world_l2
    ORDER BY
        vector <-> ARRAY[0, 1, 0]
    LIMIT 7) v
ORDER BY
    v.dist,
    v.id;

-- this should use index
EXPLAIN
SELECT
    *
FROM (
    SELECT
        id,
        ROUND(l2sq_dist (vector, ARRAY[0, 1, 0])::numeric, 2) AS dist
    FROM
        small_world_l2
    ORDER BY
        vector <-> ARRAY[0, 1, 0]
    LIMIT 7) v
ORDER BY
    v.dist,
    v.id;

CREATE INDEX ON small_world_l2 USING hnsw (vector_int dist_l2sq_int_ops);

INSERT INTO small_world_l2 (id, vector_int)
    VALUES ('000', '{0,0,0}'),
    ('001', '{0,0,1}'),
    ('010', '{0,1,0}'),
    ('011', '{0,1,1}'),
    ('100', '{1,0,0}'),
    ('101', '{1,0,1}'),
    ('110', '{1,1,0}'),
    ('111', '{1,1,1}');

-- this should not throw error, as it is index usage
SELECT
    *
FROM (
    SELECT
        id,
        ROUND(l2sq_dist (vector_int, ARRAY[0, 1, 0])::numeric, 2) AS dist
    FROM
        small_world_l2
    ORDER BY
        vector_int <-> ARRAY[0, 1, 0]
    LIMIT 7) v
ORDER BY
    v.dist,
    v.id;

-- this should use index
EXPLAIN
SELECT
    *
FROM (
    SELECT
        id,
        ROUND(l2sq_dist (vector_int, ARRAY[0, 1, 0])::numeric, 2) AS dist
    FROM
        small_world_l2
    ORDER BY
        vector_int <-> ARRAY[0, 1, 0]
    LIMIT 7) v
ORDER BY
    v.dist,
    v.id;

