-- THIS IS TODO TEST FILE
-- THIS TESTS WILL NOT PASS CURRENTLY BUT SHOULD BE FIXED LATER

CREATE TABLE small_world_l2 (
    id varchar(3),
    vector real[],
    vector_int integer[]
);

INSERT INTO small_world_l2 (id, vector) VALUES 
    ('000', '{0,0,0}'),
    ('001', '{0,0,1}'),
    ('010', '{0,1,0}'),
    ('011', '{0,1,1}'),
    ('100', '{1,0,0}'),
    ('101', '{1,0,1}'),
    ('110', '{1,1,0}'),
    ('111', '{1,1,1}');

SET enable_seqscan = false;

\set ON_ERROR_STOP off

-- this should throw error, as it is out of index usage (no index)
SELECT 1 FROM small_world_l2 order by vector <-> '{1,0,0}' LIMIT 3;
EXPLAIN (COSTS FALSE) SELECT 1 FROM small_world_l2 order by vector <-> '{1,0,0}' LIMIT 3;

-- this should throw error, as it is out of index usage (in select)
SELECT * FROM (
    SELECT id, ROUND((vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_l2
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

CREATE INDEX ON small_world_l2 USING hnsw (vector dist_l2sq_ops);

-- this should be supported
CREATE INDEX ON small_world_l2 USING hnsw (vector_int dist_l2sq_int_ops);

-- this should use index
EXPLAIN (COSTS FALSE)
SELECT id, ROUND(l2sq_dist(vector_int, array[0,1,0])::numeric, 2) as dist
FROM small_world_l2
ORDER BY vector_int <-> array[0,1,0] LIMIT 7;

-- this result is not sorted correctly
CREATE TABLE small_world_ham (
    id SERIAL PRIMARY KEY,
    v INT[2]
);
INSERT INTO small_world_ham (v) VALUES ('{0,0}'), ('{1,1}'), ('{2,2}'), ('{3,3}');
CREATE INDEX ON small_world_ham USING hnsw (v dist_hamming_ops) WITH (dims=2);
SELECT ROUND(hamming_dist(v, '{0,0}')::numeric, 2) FROM small_world_ham ORDER BY v <-> '{0,0}';
