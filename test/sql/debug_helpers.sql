CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

-- initially, before we create an index, the variable does not exist
\ir sql/test_helpers/small_world.sql
SHOW hnsw.init_k;

CREATE INDEX ON small_world USING hnsw (vector);
-- verify that the index was created
SELECT * FROM ldb_get_indexes('small_world');

-- it exists after we create an index
SHOW hnsw.init_k;

SET hnsw.init_k = 45;
SHOW hnsw.init_k;
RESET ALL;
SHOW hnsw.init_k;
