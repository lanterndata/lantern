\set ON_ERROR_STOP on
\timing
DROP EXTENSION IF EXISTS vector CASCADE;

CREATE EXTENSION IF NOT EXISTS vector;

DROP EXTENSION IF EXISTS lanterndb CASCADE;

CREATE EXTENSION IF NOT EXISTS lanterndb;

-- Create SIFT tables for benchmarking
DROP TABLE IF EXISTS sift_base10k;

CREATE TABLE sift_base10k (
    id serial PRIMARY KEY,
    v vector (128)
);

\copy sift_base10k (v) FROM 'base10k.csv' with csv;
--  CREATE TABLE sift_base1m (
--      id SERIAL PRIMARY KEY,
--      v vector(128));
--  CREATE TABLE gist_base1m (
--      id SERIAL PRIMARY KEY,
--      v vector(960));
--  CREATE TABLE sift_base1b (
--      id SERIAL PRIMARY KEY,
--      v vector(128));
--  \copy sift_base1m (v) FROM 'base1m.csv' with csv;
SELECT
    v AS v4444
FROM
    sift_base10k
WHERE
    id = 4444 \gset

EXPLAIN (
    ANALYZE,
    TIMING FALSE
)
SELECT
    *
FROM
    sift_base10k
ORDER BY
    v <-> :'v4444'
LIMIT 10;

SELECT
    id,
    vector_l2sq_dist (v, :'v4444') AS dist
FROM
    sift_base10k
ORDER BY
    dist
LIMIT 10;

\set GROUP_LIMIT 10000
-- CREATE INDEX ON sift_base1m USING hnsw (v vector_l2_ops) WITH (M=2, ef_construction=14, alg="diskann");
CREATE INDEX ON sift_base10k USING hnsw (v dist_vec_l2sq_ops) WITH (M = 2, ef_construction = 10, ef = 4, alg = "diskann");

CREATE INDEX ON sift_base10k USING ivfflat (v vector_l2_ops);

\echo "running" v4444 "vector queries"
\echo "@@@@@@@@@@@@@@@@@@@@ ivfflat index is also created @@@@@@@@@@@@@@"
BEGIN;
DROP INDEX sift_base10k_v_idx;
EXPLAIN (
    ANALYZE,
    buffers
)
SELECT
    q.id AS query_id,
    ARRAY_AGG(b.id ORDER BY q.v <-> b.v) AS base_ids
FROM
    sift_base10k q
    JOIN LATERAL (
        SELECT
            id,
            v
        FROM
            sift_base10k
        ORDER BY
            q.v <-> v
        LIMIT 10) b ON TRUE
GROUP BY
    q.id
LIMIT :GROUP_LIMIT;
ROLLBACK;

\echo "^^^^^^^^^^^^^^^^^^^^ ivfflat performance above ^^^^^^^^^^^^^^"
EXPLAIN (
    ANALYZE,
    buffers
)
SELECT
    q.id AS query_id,
    ARRAY_AGG(b.id ORDER BY q.v <-> b.v) AS base_ids
FROM
    sift_base10k q
    JOIN LATERAL (
        SELECT
            id,
            v
        FROM
            sift_base10k
        ORDER BY
            q.v <-> v
        LIMIT 10) b ON TRUE
GROUP BY
    q.id
LIMIT :GROUP_LIMIT;

