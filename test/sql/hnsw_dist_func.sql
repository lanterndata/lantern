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

CREATE TABLE small_world_l2_vec (
    id varchar(3),
    vector vector(3)
);

CREATE INDEX ON small_world_l2_vec USING hnsw (vector);
CREATE INDEX ON small_world_l2 USING hnsw (vector ann_l2_ops);
CREATE INDEX ON small_world_cos USING hnsw (vector ann_cos_ops);
CREATE INDEX ON small_world_ham USING hnsw (vector ann_ham_ops);

INSERT INTO small_world_l2_vec (id, vector) VALUES 
('000', '[0,0,0]'),
('001', '[0,0,1]'),
('010', '[0,1,0]'),
('011', '[0,1,1]'),
('100', '[1,0,0]'),
('101', '[1,0,1]'),
('110', '[1,1,0]'),
('111', '[1,1,1]');

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

SET enable_seqscan = false;

-- l2
SELECT * FROM (
    SELECT id, ROUND(l2sq_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_l2
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should use index
EXPLAIN SELECT * FROM (
    SELECT id, ROUND(l2sq_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_l2
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- l2 vector
SELECT * FROM (
    SELECT id, ROUND(l2sq_dist(vector, '[0,1,0]'::vector)::numeric, 2) as dist
    FROM small_world_l2_vec
    ORDER BY vector <-> '[0,1,0]'::vector LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should use index
EXPLAIN SELECT * FROM (
    SELECT id, ROUND(l2sq_dist(vector, '[0,1,0]'::vector)::numeric, 2) as dist
    FROM small_world_l2_vec
    ORDER BY vector <-> '[0,1,0]'::vector LIMIT 7
) v ORDER BY v.dist, v.id;

-- cos
SELECT * FROM (
    SELECT id, ROUND(cos_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_cos
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should use index
EXPLAIN SELECT * FROM (
    SELECT id, ROUND(cos_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_cos
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- ham
SELECT * FROM (
    SELECT id, ROUND(hamming_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_ham
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

-- this should use index
EXPLAIN SELECT * FROM (
    SELECT id, ROUND(hamming_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world_ham
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

\set ON_ERROR_STOP off
-- this should throw error about standalone usage of the operator
SELECT array[1,2,3] <-> array[3,2,1];
\set ON_ERROR_STOP on

-- the dis column in select should be null(empty), as the function 
-- is being called during index scan, so we can not throw error there
SELECT * FROM (
    SELECT id, ROUND((vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world_ham
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;
