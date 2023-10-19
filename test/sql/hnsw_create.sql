------------------------------------------------------------------------------
-- Test HNSW index creation
------------------------------------------------------------------------------

-- Validate that index creation works with a small number of vectors
\ir utils/small_world_array.sql
\ir utils/sift1k_array.sql

-- Validate that creating a secondary index works
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dim=128, M=4);
SELECT * FROM ldb_get_indexes('sift_base1k');

-- Validate that index creation works with a larger number of vectors
\ir utils/sift10k_array.sql
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dim=128);
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (COSTS FALSE) SELECT * FROM sift_base10k order by v <-> :'v4444' LIMIT 10;

--- Validate that M values inside the allowed range [2, 128] do not throw an error

CREATE INDEX ON small_world USING hnsw (v) WITH (M=2);
CREATE INDEX ON small_world USING hnsw (v) WITH (M=128);

---- Validate that M values outside the allowed range [2, 128] throw an error
\set ON_ERROR_STOP off
CREATE INDEX ON small_world USING hnsw (v) WITH (M=1);
CREATE INDEX ON small_world USING hnsw (v) WITH (M=129);
\set ON_ERROR_STOP on

-- Validate index dimension inference
CREATE TABLE small_world4 (
    id varchar(3),
    vector real[]
);

-- Test postponing of index creation on an empty table
-- no options and single insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}');
rollback;
DROP INDEX small_world4_idx1;
-- We need to vacuum or else we won't detect that the table is empty in ambuild
VACUUM small_world4;

SELECT * FROM small_world4;

-- no options and batch insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}'),
('001', '{1,0,0,1}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;


-- some options but no dim and single insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;


-- some options but no dim and batch insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}'),
('001', '{1,0,0,1}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;


-- dim specified and single insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2, dim=4);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;

-- dim specified and batch insert
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2, dim=4);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', '{0,1,2,0}'),
('001', '{1,0,0,1}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;

-- Test cases where a NULL vector is inserted, and dim is not specified

-- Create postponed index on empty table with batch insert where one vector is NULL
-- this should ignore all NULL vectors and build index upon encountering insertion of first non-NULL entry
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', NULL),
('001', '{1,0,0,1}');
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;

-- Empty table with first insert where vector is NULL
-- This should ignore the NULL vector and NOT build the postponed index
CREATE INDEX small_world4_idx1 ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', NULL);
rollback;
DROP INDEX small_world4_idx1;
VACUUM small_world4;

-- If the first row is NULL and index is not postponed (non-empty table) then we can't infer dimension and this will error
\set ON_ERROR_STOP off
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', NULL),
('001', '{1,0,0,1}');
CREATE INDEX ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
rollback;
\set ON_ERROR_STOP on
VACUUM small_world4;

INSERT INTO small_world4 (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}'),
('011', '{1,0,1,1}'),
('100', '{1,1,0,0}'),
('101', '{1,1,0,1}'),
('110', '{1,1,1,0}'),
('111', '{1,1,1,1}');
CREATE INDEX small_world4_hnsw_idx ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
SELECT * FROM ldb_get_indexes('small_world4');
-- the index will not allow changing the dimension of a vector element
\set ON_ERROR_STOP off
UPDATE small_world4 SET vector = '{0,0,0}' WHERE id = '000';
UPDATE small_world4 SET vector = '{0,0,0}' WHERE id = '001';
\set ON_ERROR_STOP on

INSERT INTO small_world4 (id, vector) VALUES
('000', '{1,0,0,0}'),
('001', '{1,0,0,1}'),
('010', '{1,0,1,0}');

-- without the index, I can change the dimension of a vector element
DROP INDEX small_world4_hnsw_idx;
UPDATE small_world4 SET vector = '{0,0,0}' WHERE id = '001';
-- but then, I cannot create the same dimension-inferred index
\set ON_ERROR_STOP off
CREATE INDEX ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
\set ON_ERROR_STOP on
