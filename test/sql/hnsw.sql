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
SELECT * FROM items ORDER BY trait_ai <-> '[3,1,2]' LIMIT 7;
CREATE INDEX ON items USING hnsw (trait_ai dist_vec_l2sq_ops);

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
SELECT * FROM ldb_get_indexes('small_world');
SELECT * FROM (
	SELECT id, ROUND(vector_l2sq_dist(vector, '[0,0,0]')::numeric, 2) as dist
	FROM small_world
	ORDER BY vector <-> '[0,0,0]' LIMIT 7
) v ORDER BY v.dist, v.id;
SELECT * FROM (
	SELECT id, ROUND(vector_l2sq_dist(vector, '[0,1,0]')::numeric, 2) as dist
	FROM small_world
	ORDER BY vector <-> '[0,1,0]' LIMIT 7
) v ORDER BY v.dist, v.id;
rollback;


begin;
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=2, ef=11, ef_construction=12);
SELECT * FROM ldb_get_indexes('small_world');
-- Equidistant points from the given vector appear in different order in the output of the inner query
-- depending on postgres version and platform. The outder query forces a deterministic order.
-- Unfortunately, outer query resorts distances as well so if the index sorted them in a wrong order,
-- that would be hidden by the outer query.

-- For that reason we first run a query that only outputs distances so we can see vectors are in fact in the right (approximate)
-- order. Then, we run the second query which outputs id, dist pairs and we sort ids for equal distances in the outer query to get
-- deterministic output.
SELECT ROUND(vector_l2sq_dist(vector, '[0,0,0]')::numeric, 2) as dist
FROM small_world
ORDER BY vector <-> '[0,0,0]' LIMIT 7;
SELECT * FROM (
    SELECT id, ROUND(vector_l2sq_dist(vector, '[0,0,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,0,0]' LIMIT 7
) v ORDER BY v.dist, v.id;
SELECT * FROM (
    SELECT id, ROUND(vector_l2sq_dist(vector, '[0,1,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,1,0]' LIMIT 7
) v ORDER BY v.dist, v.id;
rollback;

begin;
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=11, ef=2, ef_construction=2);
SELECT * FROM ldb_get_indexes('small_world');
SELECT * FROM (
    SELECT id, ROUND(vector_l2sq_dist(vector, '[0,0,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,0,0]' LIMIT 7
) v ORDER BY v.dist, v.id;

SELECT * FROM (
    SELECT id, ROUND(vector_l2sq_dist(vector, '[0,1,0]')::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> '[0,1,0]' LIMIT 7
) v ORDER BY v.dist, v.id;
rollback;

-- Make sure the index can handle having multiple indexes on the same table
-- attempts to makes sure that hnsw index requires no extension-global state
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=5, ef=20, ef_construction=20);
CREATE INDEX ON small_world USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
INSERT INTO small_world (id, vector) VALUES
('000', '[0,0,0]'),
('001', '[0,0,1]'),
('010', '[0,1,0]'),
('011', '[0,1,1]'),
('100', '[1,0,0]'),
('101', '[1,0,1]'),
('110', '[1,1,0]'),
('111', '[1,1,1]');

-- Ensure we can query an index for more elements than the value of init_k
SET client_min_messages TO DEBUG5;
WITH neighbors AS (
    SELECT * FROM small_world order by vector <-> '[1,0,0]' LIMIT 10
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by vector <-> '[1,0,0]' LIMIT 50
) SELECT COUNT(*) from neighbors;

-- change default k and make sure the number of usearch_searchs makes sense
SET hnsw.init_k = 4;
WITH neighbors AS (
    SELECT * FROM small_world order by vector <-> '[1,0,0]' LIMIT 3
) SELECT COUNT(*) from neighbors;
WITH neighbors AS (
    SELECT * FROM small_world order by vector <-> '[1,0,0]' LIMIT 15
) SELECT COUNT(*) from neighbors;
RESET client_min_messages;

\echo "Done with hnsw.sql test!"
