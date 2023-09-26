-- drop the old implementation
DROP FUNCTION IF EXISTS vector_l2sq_dist(vector, vector);

-- replace is with overloaded version
-- Check if the vector type from pgvector exists
SELECT EXISTS (
	SELECT 1
	FROM pg_type
	WHERE typname = 'vector'
) INTO pgvector_exists;

CREATE OR REPLACE FUNCTION _create_ldb_operator_classes(access_method_name TEXT) RETURNS BOOLEAN AS $$
DECLARE
    dist_l2sq_ops TEXT;
    dist_cos_ops TEXT;
    dist_hamming_ops TEXT;
BEGIN
    -- Construct the SQL statement to create the operator classes dynamically.
    dist_l2sq_ops := '
        CREATE OPERATOR CLASS dist_l2sq_ops
        DEFAULT FOR TYPE real[] USING ' || access_method_name || ' AS
        OPERATOR 1 <-> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 l2sq_dist(real[], real[]);
    ';
    
    dist_cos_ops := '
        CREATE OPERATOR CLASS dist_cos_ops
        FOR TYPE real[] USING ' || access_method_name || ' AS
        OPERATOR 1 <-> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 cos_dist(real[], real[]);
    ';
    
    dist_hamming_ops := '
        CREATE OPERATOR CLASS dist_hamming_ops
        FOR TYPE integer[] USING ' || access_method_name || ' AS
        OPERATOR 1 <-> (integer[], integer[]) FOR ORDER BY float_ops,
        FUNCTION 1 hamming_dist(integer[], integer[]);
    ';

    -- Execute the dynamic SQL statement.
    EXECUTE dist_l2sq_ops;
    EXECUTE dist_cos_ops;
    EXECUTE dist_hamming_ops;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql VOLATILE;

IF pgvector_exists THEN
			CREATE FUNCTION l2sq_dist(vector, vector) RETURNS float8
					AS 'MODULE_PATHNAME', 'vector_l2sq_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
 			PERFORM _create_ldb_operator_classes('lantern_hnsw');
END IF
