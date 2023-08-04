-- here for reference of directory file structure
DROP OPERATOR CLASS IF EXISTS vector_l2_ops using embedding CASCADE;

DROP ACCESS METHOD IF EXISTS embedding;

DROP FUNCTION IF EXISTS hnsw_handler;

CREATE FUNCTION hnsw_handler (internal) RETURNS index_am_handler AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnsw_handler;

COMMENT ON ACCESS METHOD hnsw IS 'LanternDB vector index access method. Can be configured to use various strategies such hs hnsw, graph-based, disk-optimized etc.';

-- taken from pgvector so our index can work with pgvector types
CREATE OPERATOR CLASS vector_l2_ops DEFAULT FOR TYPE vector USING hnsw AS OPERATOR 1 < - > (vector, vector) FOR
ORDER BY
  float_ops,
  FUNCTION 1 vector_l2_squared_distance (vector, vector),
  FUNCTION 3 l2_distance (vector, vector);
