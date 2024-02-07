-- Definitions concerning our hnsw-based index data strucuture
CREATE FUNCTION hnsw_handler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION lantern_reindex_external_index(index regclass) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_reindex_external_index' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

-- functions
CREATE FUNCTION ldb_generic_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
	
CREATE FUNCTION ldb_generic_dist(integer[], integer[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
	
CREATE FUNCTION l2sq_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cos_dist(real[], real[]) RETURNS real
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION hamming_dist(integer[], integer[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
	
-- operators
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


CREATE SCHEMA _lantern_internal;

CREATE FUNCTION _lantern_internal.validate_index(index regclass, print_info boolean DEFAULT true) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_validate_index' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.failure_point_enable(func TEXT, name TEXT, dont_trigger_first_nr INTEGER DEFAULT 0) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_failure_point_enable' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.continue_blockmap_group_initialization(index regclass) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_continue_blockmap_group_initialization' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.create_pq_codebook(REGCLASS, NAME, INT, INT, TEXT) RETURNS REAL[][][]
	AS 'MODULE_PATHNAME', 'create_pq_codebook' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;
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
        OPERATOR 1 <?> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 l2sq_dist(real[], real[]),
        OPERATOR 2 <-> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 2 l2sq_dist(real[], real[]);
    ';
    
    dist_cos_ops := '
        CREATE OPERATOR CLASS dist_cos_ops
        FOR TYPE real[] USING ' || access_method_name || ' AS
        OPERATOR 1 <?> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 1 cos_dist(real[], real[]),
        OPERATOR 2 <=> (real[], real[]) FOR ORDER BY float_ops,
        FUNCTION 2 cos_dist(real[], real[]);
    ';
    
    dist_hamming_ops := '
        CREATE OPERATOR CLASS dist_hamming_ops
        FOR TYPE integer[] USING ' || access_method_name || ' AS
        OPERATOR 1 <?> (integer[], integer[]) FOR ORDER BY float_ops,
        FUNCTION 1 hamming_dist(integer[], integer[]),
        OPERATOR 2 <+> (integer[], integer[]) FOR ORDER BY integer_ops,
        FUNCTION 2 hamming_dist(integer[], integer[]);
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

-------------------------------------
-------- Product Quantization -------
-------------------------------------

CREATE FUNCTION ldb_pqvec_in(cstring, oid, integer) RETURNS pqvec AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_pqvec_out(pqvec) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_pqvec_recv(internal, oid, integer) RETURNS pqvec AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_pqvec_send(pqvec) RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_array_pqvec(int[], integer, boolean) RETURNS pqvec	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION ldb_cast_pqvec_array(pqvec, integer, boolean) RETURNS int[]	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE pqvec (
	INPUT     = ldb_pqvec_in,
	OUTPUT    = ldb_pqvec_out,
	RECEIVE   = ldb_pqvec_recv,
	SEND      = ldb_pqvec_send,
	STORAGE   = extended
);

CREATE CAST (integer[] AS pqvec)
	WITH FUNCTION ldb_cast_array_pqvec(integer[], integer, boolean) AS ASSIGNMENT;
	
CREATE CAST (pqvec AS integer[])
	WITH FUNCTION ldb_cast_pqvec_array(pqvec, integer, boolean) AS ASSIGNMENT;
	
CREATE OR REPLACE FUNCTION create_codebook(tbl REGCLASS, col NAME, cluster_cnt INT, subset_count INT, distance_metric TEXT)
RETURNS NAME AS $$
DECLARE
  stmt TEXT;
  res REAL[];
  codebooks REAL[][][];
  i INT;
  subset_len INT;
  end_idx INT;
  codebook_table NAME;
  dim INT;
BEGIN
  
  stmt := format('SELECT array_length(%I, 1) FROM %I WHERE %1$I IS NOT NULL LIMIT 1', col, tbl);
  EXECUTE stmt INTO dim;

	-- Get codebooks
	codebooks := _lantern_internal.create_pq_codebook(tbl, col, cluster_cnt, subset_count, distance_metric);

	-- Create codebook table
  codebook_table := format('_lantern_codebook_%s', tbl);
  stmt := format('DROP TABLE IF EXISTS %s CASCADE', codebook_table);
  EXECUTE stmt;
  
  stmt:= format('CREATE TABLE %s(subvector_id INT, centroid_id INT, c REAL[]);', codebook_table);
  EXECUTE stmt;
  
  stmt:= format('CREATE INDEX ON %s USING BTREE(subvector_id, centroid_id);', codebook_table);
  EXECUTE stmt;
  
  -- Iterate over codebooks and insert into table
  FOR i IN 1..subset_count loop
  	FOR k IN 1..cluster_cnt loop
  	  -- centroid_id is k-1 because k is in range[0,255] but postgres arrays start from index 1
      stmt := format('INSERT INTO %I(subvector_id, centroid_id, c) VALUES (%s, %s, ARRAY(SELECT * FROM unnest(''%s''::REAL[])))', codebook_table, i, k - 1, codebooks[i:i][k:k]);
      EXECUTE stmt;
  	END LOOP;
  END LOOP;

  return codebook_table;
END;
$$ LANGUAGE plpgsql;

-- Compress vector using codebook
CREATE OR REPLACE FUNCTION _lantern_internal.compress_vector(v REAL[], subset_count INTEGER, codebook regclass, distance_metric TEXT)
RETURNS pqvec AS $$
DECLARE
  subset_center INT;
  start_idx INT;
  end_idx INT;
  dim INT;
  subset_len INT;
  res INT[];
  subvector_id INT;
BEGIN
  dim := array_length(v, 1);
  res := '{}'::INT[];
  subset_len := dim/subset_count;
  subvector_id := 1;

  FOR i IN 1..dim BY subset_len LOOP
    IF i = dim THEN
      end_idx := dim;
    ELSE
      end_idx := i + subset_len - 1;
    END IF;
    EXECUTE format('SELECT centroid_id FROM %I WHERE subvector_id=%s ORDER BY %s_dist(c, %L) LIMIT 1', codebook, subvector_id, distance_metric, v[i:end_idx]) INTO subset_center;
    res := array_append(res, subset_center);
    subvector_id := subvector_id + 1;
  END LOOP;
  
  RETURN res::pqvec;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION compress_vector(v REAL[], codebook regclass, distance_metric TEXT)
RETURNS pqvec AS $$
DECLARE
  subset_count INT;
  stmt TEXT;
BEGIN

  stmt := format('SELECT COUNT(centroid_id) FROM %I WHERE centroid_id=1', codebook);
  EXECUTE stmt INTO subset_count;
  RETURN _lantern_internal.compress_vector(v, subset_count, codebook, distance_metric);
END;
$$ LANGUAGE plpgsql;

-- Decompress vector using codebook
CREATE OR REPLACE FUNCTION decompress_vector(v pqvec, codebook regclass)
RETURNS REAL[] AS $$
DECLARE
  res REAL[];
  subset REAL[];
  centroid_id INT;
  subvector_id INT;
BEGIN
  res := '{}'::REAL[];
  subvector_id := 1;
  FOREACH centroid_id in array v::INT[]
  LOOP
     EXECUTE format('SELECT c FROM %I WHERE subvector_id=%L AND centroid_id=%L', codebook, subvector_id, centroid_id) INTO subset;
     res := res || subset;
     subvector_id := subvector_id + 1;
  END LOOP;

  RETURN res;
END;
$$ LANGUAGE plpgsql;

-- Quantize table
CREATE OR REPLACE FUNCTION quantize_table(tbl regclass, col NAME, cluster_cnt INT,subset_count INT, distance_metric TEXT)
RETURNS VOID AS $$
DECLARE
  subset REAL[];
  id INT;
  stmt TEXT;
  pq_col_name NAME;
  codebook_table NAME;
  trigger_func_name NAME;
  insert_trigger_name NAME;
  update_trigger_name NAME;
  pg_version INT;
  column_exists BOOLEAN;
BEGIN
  pg_version := (SELECT setting FROM pg_settings WHERE name = 'server_version_num');
  pq_col_name := format('%I_pq', col);
  
  column_exists := (SELECT true FROM pg_attribute WHERE attrelid = tbl AND attname = pq_col_name AND NOT attisdropped);

  IF column_exists THEN
    RAISE EXCEPTION 'Column % already exists in table', pq_col_name;
  END IF;
  -- Create codebook
  codebook_table := create_codebook(tbl, col, cluster_cnt, subset_count, distance_metric);

  -- Compress vectors
  RAISE INFO 'Compressing vectors...';

  IF pg_version >= 120000 THEN
    stmt := format('ALTER TABLE %I ADD COLUMN %I PQVEC GENERATED ALWAYS AS (_lantern_internal.compress_vector(%I, %L, %L, %L)) STORED', tbl, pq_col_name, col, subset_count, codebook_table, distance_metric);
    EXECUTE stmt;
  ELSE
    stmt := format('ALTER TABLE %I ADD COLUMN %I PQVEC', tbl, pq_col_name);
    EXECUTE stmt;

    stmt := format('UPDATE %1$I SET %2$I_pq=_lantern_internal.compress_vector(%2$I, %3$L, %4$L::regclass, %5$L)', tbl, col, subset_count, codebook_table, distance_metric);
    EXECUTE stmt;

    -- Create trigger to update pq values based on vector value
    trigger_func_name := format('_set_pq_col_%s', md5(tbl || col));
    stmt := format('
      CREATE OR REPLACE FUNCTION %I()
        RETURNS trigger
        LANGUAGE plpgsql AS
      $body$
      DECLARE
        stmt TEXT;
      BEGIN
        NEW.%I := _lantern_internal.compress_vector(NEW.%I, %L, %L::regclass, %L);
        RETURN NEW;
      END
      $body$;
      ', trigger_func_name, pq_col_name, col, subset_count, codebook_table, distance_metric);
    EXECUTE stmt;
    
    insert_trigger_name := format('_pq_trigger_in_%s', md5(tbl || col));
    update_trigger_name := format('_pq_trigger_up_%s', md5(tbl || col));
    
    stmt := format('DROP TRIGGER IF EXISTS %I ON %I', insert_trigger_name, tbl);
    EXECUTE stmt;
    
    stmt := format('DROP TRIGGER IF EXISTS %I ON %I', update_trigger_name, tbl);
    EXECUTE stmt;
    
    stmt := format('CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW WHEN (NEW.%I IS NOT NULL) EXECUTE FUNCTION %I()', 
      insert_trigger_name,
      tbl,
      col,
      trigger_func_name
    );

    EXECUTE stmt;

    stmt := format('CREATE TRIGGER %1$I BEFORE UPDATE OF %2$I ON %3$I FOR EACH ROW WHEN (NEW.%2$I IS NOT NULL) EXECUTE FUNCTION %4$I()', 
      update_trigger_name,
      col,
      tbl,
      trigger_func_name
    );
    EXECUTE stmt;
  END IF;
END;
$$ LANGUAGE plpgsql;
