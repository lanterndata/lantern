-- Definitions concerning our hnsw-based index data strucuture
CREATE FUNCTION hnsw_handler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- functions
CREATE FUNCTION ldb_generic_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
	
CREATE FUNCTION l2sq_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- this function is needed, as we should also use <-> operator
-- with integer[] type (to overwrite hamming dist function in our hooks)
-- and if we do not create l2sq_dist for integer[] type it will fail to cast in pgvector_compat mode
CREATE FUNCTION l2sq_dist(integer[], integer[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cos_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- functions _with_guard suffix are used to forbid operator usage
-- if operator hooks are enabled (lantern.pgvector_compat=FALSE)
CREATE FUNCTION cos_dist_with_guard(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION hamming_dist(integer[], integer[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


CREATE FUNCTION hamming_dist_with_guard(integer[], integer[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
	
-- operators
CREATE OPERATOR <-> (
	LEFTARG = real[], RIGHTARG = real[], PROCEDURE = l2sq_dist,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <-> (
	LEFTARG = integer[], RIGHTARG = integer[], PROCEDURE = l2sq_dist,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <=> (
	LEFTARG = real[], RIGHTARG = real[], PROCEDURE = cos_dist_with_guard,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = integer[], RIGHTARG = integer[], PROCEDURE = hamming_dist_with_guard,
	COMMUTATOR = '<+>'
);


CREATE SCHEMA _lantern_internal;

CREATE FUNCTION _lantern_internal.validate_index(index regclass, print_info boolean DEFAULT true) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_validate_index' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.failure_point_enable(func TEXT, name TEXT, dont_trigger_first_nr INTEGER DEFAULT 0) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_failure_point_enable' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.continue_blockmap_group_initialization(index regclass) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_continue_blockmap_group_initialization' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

-- operator classes
CREATE OR REPLACE FUNCTION _lantern_internal._create_ldb_operator_classes(access_method_name TEXT) RETURNS BOOLEAN AS $$
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
        FUNCTION 1 cos_dist(real[], real[]),
        -- it is important to set the function with guard the second
        -- as op rewriting hook takes the first function to use
        OPERATOR 2 <=> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 2 cos_dist_with_guard(real[], real[]);
    ';
    
    dist_hamming_ops := '
        CREATE OPERATOR CLASS dist_hamming_ops
        FOR TYPE integer[] USING ' || access_method_name || ' AS
        OPERATOR 1 <-> (integer[], integer[]) FOR ORDER BY float_ops,
        FUNCTION 1 hamming_dist(integer[], integer[]),
        OPERATOR 2 <+> (integer[], integer[]) FOR ORDER BY integer_ops,
        FUNCTION 2 hamming_dist_with_guard(integer[], integer[]);
    ';

    -- Execute the dynamic SQL statement.
    EXECUTE dist_l2sq_ops;
    EXECUTE dist_cos_ops;
    EXECUTE dist_hamming_ops;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql VOLATILE;


-- Create access method
DO $BODY$
DECLARE
	hnsw_am_exists boolean;
	pgvector_exists boolean;
BEGIN
	-- Check if another extension already created an access method named 'hnsw'
	SELECT EXISTS (
		SELECT 1
		FROM pg_am
		WHERE amname = 'hnsw'
	) INTO hnsw_am_exists;

	-- Check if the vector type from pgvector exists
	SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname = 'vector'
	) INTO pgvector_exists;

	IF pgvector_exists OR hnsw_am_exists THEN
		-- RAISE NOTICE 'hnsw access method already exists. Creating lantern_hnsw access method';
		CREATE ACCESS METHOD lantern_hnsw TYPE INDEX HANDLER hnsw_handler;
		COMMENT ON ACCESS METHOD lantern_hnsw IS 'LanternDB access method for vector embeddings, based on the hnsw algorithm';
	END IF;

	IF pgvector_exists THEN
		-- taken from pgvector so our index can work with pgvector types
		CREATE FUNCTION l2sq_dist(vector, vector) RETURNS float8
			AS 'MODULE_PATHNAME', 'vector_l2sq_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

		CREATE FUNCTION cos_dist(vector, vector) RETURNS float8
			AS 'MODULE_PATHNAME', 'vector_cos_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
			
		CREATE FUNCTION hamming_dist(vector, vector) RETURNS float8
			AS 'MODULE_PATHNAME', 'vector_hamming_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
			
		CREATE OPERATOR <+> (
			LEFTARG = vector, RIGHTARG = vector, PROCEDURE = hamming_dist,
			COMMUTATOR = '<+>'
		);

		CREATE OPERATOR CLASS dist_vec_l2sq_ops
			DEFAULT FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 l2sq_dist(vector, vector);
			
		CREATE OPERATOR CLASS dist_vec_cos_ops
			FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 cos_dist(vector, vector),
			OPERATOR 2 <=> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 2 cos_dist(vector, vector);
			
		CREATE OPERATOR CLASS dist_vec_hamming_ops
			FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 hamming_dist(vector, vector),
			OPERATOR 2 <+> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 2 hamming_dist(vector, vector);
	END IF;


	IF hnsw_am_exists THEN
		PERFORM _lantern_internal._create_ldb_operator_classes('lantern_hnsw');
		RAISE WARNING 'Access method(index type) "hnsw" already exists. Creating lantern_hnsw access method';
	ELSE
		-- create access method
		CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnsw_handler;
		COMMENT ON ACCESS METHOD hnsw IS 'LanternDB access method for vector embeddings, based on the hnsw algorithm';
		PERFORM _lantern_internal._create_ldb_operator_classes('hnsw');
	END IF;
END;
$BODY$
LANGUAGE plpgsql;

-- Database updates
-- Must be run in update scripts every time index storage format changes and a finer-grained update
-- method is not shipped for the format change
CREATE OR REPLACE FUNCTION _lantern_internal.reindex_lantern_indexes()
RETURNS VOID AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT indexname FROM pg_indexes
            WHERE indexdef ILIKE '%USING hnsw%' OR indexdef ILIKE '%USING lantern_hnsw%'
    LOOP
        RAISE NOTICE 'Reindexing index: %', r.indexname;
        EXECUTE 'REINDEX INDEX ' || quote_ident(r.indexname) || ';';
        RAISE NOTICE 'Reindexed index: %', r.indexname;
    END LOOP;
END $$ LANGUAGE plpgsql VOLATILE;
