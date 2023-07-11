-- Definitions concerning our hnsw-based index data strucuture

CREATE FUNCTION embedding_handler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD embedding TYPE INDEX HANDLER embedding_handler;

COMMENT ON ACCESS METHOD embedding IS 'LanternDB vector index access method. Can be configured to use various strategies such hs hnsw, graph-based, disk-optimized etc.';

-- taken from pgvector so our index can work with pgvector types
CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING embedding AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 l2_distance(vector, vector);
