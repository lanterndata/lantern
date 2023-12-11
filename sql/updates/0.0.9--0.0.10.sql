--
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
        OPERATOR 1 <?> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 l2sq_dist(real[], real[]),
        OPERATOR 2 <-> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 2 l2sq_dist(real[], real[]);
    	';
    	
    	dist_cos_ops_drop := 'DROP OPERATOR CLASS IF EXISTS dist_cos_ops USING ' || access_method_name || ' CASCADE;';
    dist_cos_ops := '
        CREATE OPERATOR CLASS dist_cos_ops
        FOR TYPE real[] USING ' || access_method_name || ' AS
        OPERATOR 1 <?> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 cos_dist(real[], real[]),
        OPERATOR 2 <=> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 2 cos_dist(real[], real[]);
    ';
    
    	
    	dist_hamming_ops_drop := 'DROP OPERATOR CLASS IF EXISTS dist_hamming_ops USING ' || access_method_name || ' CASCADE;';
    dist_hamming_ops := '
        CREATE OPERATOR CLASS dist_hamming_ops
        FOR TYPE integer[] USING ' || access_method_name || ' AS
        OPERATOR 1 <?> (integer[], integer[]) FOR ORDER BY float_ops,
        FUNCTION 1 hamming_dist(integer[], integer[]),
        OPERATOR 2 <+> (integer[], integer[]) FOR ORDER BY integer_ops,
        FUNCTION 2 hamming_dist(integer[], integer[]);
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
		am_name := 'lantern_hnsw';
    -- these go for good.
    DROP OPERATOR CLASS IF EXISTS dist_vec_hamming_ops USING hnsw CASCADE;
    DROP FUNCTION IF EXISTS hamming_dist(vector, vector);
    DROP OPERATOR <+> (vector, vector) CASCADE;
	END IF;


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
  
	IF pgvector_exists THEN
		CREATE FUNCTION ldb_generic_dist(vector, vector) RETURNS real
			AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
			
			
		CREATE OPERATOR <?> (
			LEFTARG = vector, RIGHTARG = vector, PROCEDURE = ldb_generic_dist,
			COMMUTATOR = '<?>'
		);

	 -- pgvecor's vector type requires floats and we cannot define hamming distance for floats
		CREATE OPERATOR CLASS dist_vec_l2sq_ops
			DEFAULT FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <?> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 l2sq_dist(vector, vector),
			OPERATOR 2 <-> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 2 l2sq_dist(vector, vector);
			
		CREATE OPERATOR CLASS dist_vec_cos_ops
			FOR TYPE vector USING lantern_hnsw AS
			OPERATOR 1 <?> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 1 cos_dist(vector, vector),
			OPERATOR 2 <=> (vector, vector) FOR ORDER BY float_ops,
			FUNCTION 2 cos_dist(vector, vector);

		am_name := 'lantern_hnsw';
	END IF;

	-- operators
	DROP OPERATOR <->(integer[], integer[]) CASCADE;
	DROP OPERATOR <->(real[], real[]) CASCADE;
	DROP OPERATOR <=>(real[], real[]) CASCADE;
	DROP OPERATOR <+>(integer[], integer[]) CASCADE;
	
	DROP FUNCTION IF EXISTS cos_dist_with_guard CASCADE;
	DROP FUNCTION IF EXISTS hamming_dist_with_guard CASCADE;
	  
  CREATE OR REPLACE FUNCTION ldb_generic_dist(integer[], integer[]) RETURNS real
	  AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

  CREATE OPERATOR <?> (
	  LEFTARG = real[], RIGHTARG = real[], PROCEDURE = ldb_generic_dist,
	  COMMUTATOR = '<?>'
  );

  CREATE OPERATOR <?> (
	  LEFTARG = integer[], RIGHTARG = integer[], PROCEDURE = ldb_generic_dist,
	  COMMUTATOR = '<?>'
  );

  CREATE OPERATOR <-> (
	  LEFTARG = real[], RIGHTARG = real[], PROCEDURE = l2sq_dist,
	  COMMUTATOR = '<->'
  );

  CREATE OPERATOR <=> (
	  LEFTARG = real[], RIGHTARG = real[], PROCEDURE = cos_dist,
	  COMMUTATOR = '<=>'
  );

  CREATE OPERATOR <+> (
	  LEFTARG = integer[], RIGHTARG = integer[], PROCEDURE = hamming_dist,
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

