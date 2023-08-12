CREATE EXTENSION IF NOT EXISTS lanterndb;

\ir test_helpers/small_world_arrays.sql


CREATE INDEX ON small_world USING hnsw (vector);

SET enable_seqscan = off;

SELECT cos_dist(array[0,0,1], array[0,1,0]);
SELECT l2sq_dist(array[0,0,1], array[0,1,0]);
\set ON_ERROR_STOP off
-- <-> is reserved for index operations
SELECT array[0,0,1] <-> array[0,1,0];
\set ON_ERROR_STOP on


SELECT * FROM (
    SELECT id, ROUND( l2sq_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

drop index small_world_vector_idx;

CREATE INDEX ON small_world USING hnsw (vector ann_cos_ops);
-- this query should now use cosine distance function
-- as it was given on index creation
SELECT * FROM (
    SELECT id, ROUND(cos_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;

drop index small_world_vector_idx;

-- the below query now can use l2sq dist if that was chosen when creating the index
-- and can use cos dist if that was chosen when creating the index
SELECT * FROM (
    SELECT id, ROUND(l2sq_dist(vector, array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;


--todo:: the bad case is that, I can still forget and use l2sq_dist instead of <->, in SELECT from table
-- query and the function will just return null
-- todo:: this has to fail, but currently does not
SELECT * FROM (
    SELECT id, ROUND((vector <-> array[0,1,0])::numeric, 2) as dist
    FROM small_world
    ORDER BY vector <-> array[0,1,0] LIMIT 7
) v ORDER BY v.dist, v.id;
