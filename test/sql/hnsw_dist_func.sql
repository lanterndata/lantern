CREATE EXTENSION IF NOT EXISTS lanterndb;


CREATE TABLE small_world_l2 (
    id varchar(3),
    vector real[]
);

CREATE TABLE small_world_cos (
    id varchar(3),
    vector real[]
);

CREATE TABLE small_world_ham (
    id varchar(3),
    vector real[]
);

CREATE INDEX ON small_world_l2 USING hnsw (vector);
CREATE INDEX ON small_world_cos USING hnsw (vector ann_cos_ops);
CREATE INDEX ON small_world_ham USING hnsw (vector ann_ham_ops);

INSERT INTO small_world_l2 (id, vector) VALUES 
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');

INSERT INTO small_world_cos (id, vector) VALUES 
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');

INSERT INTO small_world_ham (id, vector) VALUES 
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');


-- l2
SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_l2
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should use index
EXPLAIN SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_l2
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- cos
SELECT * FROM (
    SELECT id, ROUND( cos_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_cos
    ORDER BY cos_dist(vector, array[0,1,0]) LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should not use index as dist function is not specified
EXPLAIN SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_cos
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- ham
SELECT * FROM (
    SELECT id, ROUND( ham_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_ham
    ORDER BY ham_dist(vector, array[0,1,0]) LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should not use index as wrong dist function is specified
EXPLAIN SELECT * FROM (
    SELECT id, ROUND( (vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_ham
    ORDER BY cos_dist(vector, array[0,1,0]) LIMIT 7
) v ORDER BY v.dist, v.id;
