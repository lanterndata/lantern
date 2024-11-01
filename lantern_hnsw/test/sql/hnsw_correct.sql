------------------------------------------------------------------------------
-- Test HNSW index correctness
------------------------------------------------------------------------------

-- Create table with values that have unique Euclidean distances
CREATE TABLE small_world (
    id SERIAL PRIMARY KEY,
    v REAL[2]
);
INSERT INTO small_world (v) VALUES ('{0,0}'), ('{1,1}'), ('{2,2}'), ('{3,3}');

-- Create index
CREATE INDEX ON small_world USING lantern_hnsw (v dist_l2sq_ops) WITH (dim=2, M=4);
SET enable_seqscan=FALSE;


-- Get the results without the index
CREATE TEMP TABLE results_wo_index AS
SELECT
    ROW_NUMBER() OVER (ORDER BY l2sq_dist(v, '{0,0}')) AS row_num,
    id,
    l2sq_dist(v, '{0,0}') AS dist
FROM
    small_world;

-- Get the results with the index
CREATE TEMP TABLE results_w_index AS
SELECT
    ROW_NUMBER() OVER (ORDER BY v <-> '{0,0}') AS row_num,
    id,
    l2sq_dist(v, '{0,0}') AS dist
FROM
    small_world;

-- Validate that the results are same with and without the index (should be empty)
SELECT
    a.row_num,
    a.id as id_with_index,
    b.id as id_without_index,
    a.dist as dist_with_index,
    b.dist as dist_without_index
FROM 
    results_w_index a
JOIN 
    results_wo_index b
USING (row_num)
WHERE
    a.id != b.id;

-- Validate the index data structures
SELECT _lantern_internal.validate_index('small_world_v_idx', false);
