---------------------------------------------------------------------
-- Test HNSW index inserts on empty table
---------------------------------------------------------------------
-- set an artificially low work_mem to make sure work_mem exceeded warnings are printed
set work_mem = '64kB';
-- We do not actually print the warnings generated for exceeding work_mem because the work_mem
-- check does not work for postgres 13 and lower.So, if we printed the warnings, we would get a regression
-- failure in older postgres versions. We still reduce workmem to exercise relevant codepaths for coverage
set client_min_messages = 'ERROR';

CREATE TABLE small_world (
    id SERIAL PRIMARY KEY,
    v REAL[2] -- this demonstates that postgres actually does not enforce real[] length as we actually insert vectors of length 3
);

CREATE TABLE small_world_int (
    id SERIAL PRIMARY KEY,
    v INTEGER[]
);

CREATE INDEX ON small_world USING hnsw (v) WITH (dim=3);
SELECT _lantern_internal.validate_index('small_world_v_idx', false);

-- Insert rows with valid vector data
INSERT INTO small_world (v) VALUES ('{0,0,1}'), ('{0,1,0}');
INSERT INTO small_world (v) VALUES (NULL);

-- Attempt to insert a row with an incorrect vector length
\set ON_ERROR_STOP off
-- Cannot create an hnsw index with implicit typecasts (trying to cast integer[] to real[], in this case)
CREATE INDEX ON small_world_int USING hnsw (v dist_l2sq_ops) WITH (dim=3);
INSERT INTO small_world (v) VALUES ('{1,1,1,1}');
\set ON_ERROR_STOP on

DROP TABLE small_world;

-- set work_mem to a value that is enough for the tests
set client_min_messages = 'WARNING';
set work_mem = '10MB';

---------------------------------------------------------------------
-- Test HNSW index inserts on non-empty table
---------------------------------------------------------------------

\ir utils/small_world_array.sql

CREATE INDEX ON small_world USING hnsw (v) WITH (dim=3);

SET enable_seqscan = false;

-- Inserting vectors of the same dimension and nulls should work
INSERT INTO small_world (v) VALUES ('{1,1,2}');
INSERT INTO small_world (v) VALUES (NULL);

-- Inserting vectors of different dimension should fail
\set ON_ERROR_STOP off
INSERT INTO small_world (v) VALUES ('{4,4,4,4}');
\set ON_ERROR_STOP on

-- Verify that the index works with the inserted vectors
SELECT
    ROUND(l2sq_dist(v, '{0,0,0}')::numeric, 2)
FROM
    small_world
ORDER BY
    v <-> '{0,0,0}';

-- Ensure the index size remains consistent after inserts
SELECT * from ldb_get_indexes('small_world');

-- Ensure the query plan remains consistent after inserts
EXPLAIN (COSTS FALSE)
SELECT
    ROUND(l2sq_dist(v, '{0,0,0}')::numeric, 2)
FROM
    small_world
ORDER BY
    v <-> '{0,0,0}'
LIMIT 10;

SELECT _lantern_internal.validate_index('small_world_v_idx', false);

-- Test the index with a larger number of vectors
CREATE TABLE sift_base10k (
    id SERIAL PRIMARY KEY,
    v REAL[128]
);
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dim=128);
\COPY sift_base10k (v) FROM '/tmp/lantern/vector_datasets/siftsmall_base_arrays.csv' WITH CSV;
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (COSTS FALSE) SELECT * FROM sift_base10k order by v <-> :'v4444';

SELECT _lantern_internal.validate_index('hnsw_idx', false);
