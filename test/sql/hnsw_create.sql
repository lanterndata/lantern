------------------------------------------------------------------------------
-- Test HNSW index creation
------------------------------------------------------------------------------

-- Validate that index creation works with a small number of vectors
\ir utils/small_world_array.sql
\ir utils/sift1k_array.sql

-- Validate that creating a secondary index works
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dim=128, M=4);
SELECT * FROM ldb_get_indexes('sift_base1k');
SELECT _lantern_internal.validate_index('sift_base1k_v_idx', false);

-- Validate that index creation works with a larger number of vectors
\ir utils/sift10k_array.sql
SET lantern.pgvector_compat=FALSE;

CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dim=128);
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
EXPLAIN (COSTS FALSE) SELECT * FROM sift_base10k order by v <?> :'v4444' LIMIT 10;
SELECT _lantern_internal.validate_index('hnsw_idx', false);

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
-- If the first inserted row is NULL: we can create an index but we can't infer the dimension from the first inserted row (since it is null)
\set ON_ERROR_STOP off
CREATE INDEX first_row_null_idx ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
begin;
INSERT INTO small_world4 (id, vector) VALUES
('000', NULL),
('001', '{1,0,0,1}');
CREATE INDEX ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
rollback;
\set ON_ERROR_STOP on
DROP INDEX first_row_null_idx;

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

SELECT _lantern_internal.validate_index('small_world4_hnsw_idx', false);

-- without the index, I can change the dimension of a vector element
DROP INDEX small_world4_hnsw_idx;
UPDATE small_world4 SET vector = '{0,0,0}' WHERE id = '001';
-- but then, I cannot create the same dimension-inferred index
\set ON_ERROR_STOP off
CREATE INDEX ON small_world4 USING hnsw (vector) WITH (M=14, ef=22, ef_construction=2);
\set ON_ERROR_STOP on

-- Test index creation on empty table and no dimension specified
CREATE TABLE small_world5 (
    id SERIAL PRIMARY KEY,
    v REAL[]
);

-- We can still create an index despite having an empty table and not specifying a dimension during index creation
CREATE INDEX small_world5_hnsw_idx ON small_world5 USING hnsw (v dist_l2sq_ops);

begin;
-- Inserting a NULL vector should only insert it into the table and not into our index
-- So, our index is still empty after and is yet to pick up a dimension
INSERT INTO small_world5 (id, v) VALUES ('200', NULL);

-- Our index then infers the dimension from the first inserted non-NULL row
INSERT INTO small_world5 (id, v) VALUES
('000', '{1,0,0,0,1}'),
('001', '{1,0,0,1,2}'),
('010', '{1,0,1,0,3}');
rollback;

-- Test that upon infering the dimension from the first inserted row, we do not allow subsequent rows with different dimensions 
\set ON_ERROR_STOP off
INSERT INTO small_world5 (id, v) VALUES
('100', '{2,0,0,0,1}'),
('101', '{2,0,0}'),
('110', '{2,0,1,0}');
\set ON_ERROR_STOP on


