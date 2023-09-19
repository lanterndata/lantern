-- drop the old implementation
DROP FUNCTION IF EXISTS vector_l2sq_dist(vector, vector);

-- replace is with overloaded version
BEGIN
	-- Check if the vector type from pgvector exists
	SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname = 'vector'
	) INTO pgvector_exists;

	IF pgvector_exists THEN
                CREATE FUNCTION l2sq_dist(vector, vector) RETURNS float8
                        AS 'MODULE_PATHNAME', 'vector_l2sq_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
        END IF
END
