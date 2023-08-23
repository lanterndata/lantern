-- initially, before we create an index, the variable does not exist
\ir test_helpers/small_world.sql
\set ON_ERROR_STOP off
SHOW hnsw.init_k;
\set ON_ERROR_STOP on

CREATE INDEX ON small_world USING hnsw (vector);
-- verify that the index was created
SELECT * FROM ldb_get_indexes('small_world');

-- it exists after we create an index
SHOW hnsw.init_k;

SET hnsw.init_k = 45;
SHOW hnsw.init_k;
RESET ALL;
SHOW hnsw.init_k;
