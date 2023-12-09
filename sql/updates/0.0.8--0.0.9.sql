DO $BODY$
DECLARE
	pgvector_exists boolean;
	am_name TEXT;
  r pg_indexes%ROWTYPE;
  indexes_cursor REFCURSOR;
  index_names TEXT[] := '{}';
  index_definitions TEXT[] := '{}';
BEGIN
	-- Function to recreate operator classes for specified access method
	CREATE OR REPLACE FUNCTION _lantern_internal._recreate_ldb_operator_classes(access_method_name TEXT) RETURNS BOOLEAN AS $$
	DECLARE
    	dist_l2sq_ops TEXT;
    	dist_l2sq_ops_drop TEXT;
    	dist_cos_ops TEXT;
    	dist_cos_ops_drop TEXT;
    	dist_hamming_ops TEXT;
    	dist_hamming_ops_drop TEXT;
	BEGIN
	  	
    	-- Construct the SQL statement to create the operator classes dynamically.
    	dist_l2sq_ops_drop := 'DROP OPERATOR CLASS IF EXISTS dist_l2sq_ops USING ' || access_method_name || ' CASCADE;'; 
    	dist_l2sq_ops := '
        	CREATE OPERATOR CLASS dist_l2sq_ops
        	DEFAULT FOR TYPE real[] USING ' || access_method_name || ' AS
        	OPERATOR 1 <-> (real[], real[]) FOR ORDER BY float_ops,
        	FUNCTION 1 l2sq_dist(real[], real[]);
    	';
    	
    	dist_cos_ops_drop := 'DROP OPERATOR CLASS IF EXISTS dist_cos_ops USING ' || access_method_name || ' CASCADE;';
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
    
    	
    	dist_hamming_ops_drop := 'DROP OPERATOR CLASS IF EXISTS dist_hamming_ops USING ' || access_method_name || ' CASCADE;';
    dist_hamming_ops := '
        CREATE OPERATOR CLASS dist_hamming_ops
        FOR TYPE integer[] USING ' || access_method_name || ' AS
        OPERATOR 1 <-> (integer[], integer[]) FOR ORDER BY float_ops,
        FUNCTION 1 hamming_dist(integer[], integer[]),
        OPERATOR 2 <+> (integer[], integer[]) FOR ORDER BY integer_ops,
        FUNCTION 2 hamming_dist_with_guard(integer[], integer[]);
    ';


    	-- Execute the dynamic SQL statement.
    	EXECUTE dist_l2sq_ops_drop;
    	EXECUTE dist_l2sq_ops;
    	EXECUTE dist_cos_ops_drop;
    	EXECUTE dist_cos_ops;
    	EXECUTE dist_hamming_ops_drop;
    	EXECUTE dist_hamming_ops;

    	RETURN TRUE;
	END;
	$$ LANGUAGE plpgsql VOLATILE;

	-- Check if the vector type from pgvector exists
	SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname = 'vector'
	) INTO pgvector_exists;
	
	am_name := 'hnsw';
		

	IF pgvector_exists THEN
		CREATE FUNCTION cos_dist(vector, vector) RETURNS float8
			AS 'MODULE_PATHNAME', 'vector_cos_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
			
		CREATE FUNCTION hamming_dist(vector, vector) RETURNS float8
			AS 'MODULE_PATHNAME', 'vector_hamming_dist' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
			
			
		CREATE OPERATOR <+> (
			LEFTARG = vector, RIGHTARG = vector, PROCEDURE = hamming_dist,
			COMMUTATOR = '<+>'
		);

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

		am_name := 'lantern_hnsw';
	END IF;

	-- this function is needed, as we should also use <-> operator
	-- with integer[] type (to overwrite hamming dist function in our hooks)
	-- and if we do create l2sq_dist for integer[] type it will fail to cast in pgvector_compat mode
	CREATE OR REPLACE FUNCTION l2sq_dist(integer[], integer[]) RETURNS real
		AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

	-- functions _with_guard suffix are used to forbid operator usage
	-- if operator hooks are enabled (lantern.pgvector_compat=FALSE)
	CREATE FUNCTION cos_dist_with_guard(real[], real[]) RETURNS real
		AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

	CREATE FUNCTION hamming_dist_with_guard(integer[], integer[]) RETURNS integer
		AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


		-- keep existing indexes to reindex as we should drop indexes in order to change operator classes
  OPEN indexes_cursor FOR SELECT * FROM pg_indexes WHERE indexdef ILIKE '%USING ' || am_name || '%';
  -- Fetch index names into the array
  LOOP
      FETCH indexes_cursor INTO r;
      EXIT WHEN NOT FOUND;

      -- Append index name to the array
      index_names := array_append(index_names, r.indexname);
      index_definitions := array_append(index_definitions, r.indexdef);
  END LOOP;

  CLOSE indexes_cursor;

	-- operators
	DROP OPERATOR <->(real[], real[]) CASCADE;
	CREATE OPERATOR <-> (
		LEFTARG = real[], RIGHTARG = real[], PROCEDURE = l2sq_dist,
		COMMUTATOR = '<->'
	);

	DROP OPERATOR <->(integer[], integer[]) CASCADE;
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

	PERFORM _lantern_internal._recreate_ldb_operator_classes(am_name);

  SET client_min_messages TO NOTICE;
	-- reindex indexes
	FOR i IN 1..coalesce(array_length(index_names, 1), 0) LOOP
      RAISE NOTICE 'Reindexing index %', index_names[i];
      EXECUTE index_definitions[i];
      RAISE NOTICE 'Reindexed index: %', index_names[i];
  END LOOP;
END;
$BODY$
LANGUAGE plpgsql;
