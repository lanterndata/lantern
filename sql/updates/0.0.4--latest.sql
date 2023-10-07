
DO $BODY$
DECLARE
	pgvector_exists boolean;
BEGIN
	-- replace is with overloaded version
	-- Check if the vector type from pgvector exists
	SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname = 'vector'
	) INTO pgvector_exists;

	IF pgvector_exists THEN
		CREATE FUNCTION l2sq_dist(vector, vector) RETURNS float8
		AS 'MODULE_PATHNAME', 'vector_l2sq_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

		-- change the operator class to use the new function
		DROP OPERATOR CLASS dist_vec_l2sq_ops USING lantern_hnsw;
		CREATE OPERATOR CLASS dist_vec_l2sq_ops
			DEFAULT FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 l2sq_dist(vector, vector);

		-- drop the old implementation
		DROP FUNCTION IF EXISTS vector_l2sq_dist(vector, vector);

	END IF;
END;
$BODY$
LANGUAGE plpgsql;