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

CREATE FUNCTION _lantern_internal.create_pq_codebook(REGCLASS, NAME, INT, INT, TEXT, INT) RETURNS REAL[][][]
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
	-- Check if the vector type from pgvector exists
	SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname = 'vector'
	) INTO pgvector_exists;

	-- create access method
	CREATE ACCESS METHOD lantern_hnsw TYPE INDEX HANDLER hnsw_handler;
	COMMENT ON ACCESS METHOD lantern_hnsw IS 'Hardware-accelerated Lantern access method for vector embeddings, based on the hnsw algorithm, with various compression techniques';
	PERFORM _lantern_internal._create_ldb_operator_classes('lantern_hnsw');

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
    FOR r IN SELECT indexname, indexdef FROM pg_indexes
            WHERE indexdef ILIKE '%USING lantern_hnsw%'
    LOOP
        RAISE NOTICE 'Reindexing index: %', r.indexname;
        IF POSITION('_experimental_index_path' in r.indexdef) > 0 THEN
          PERFORM lantern_reindex_external_index(r.indexname::regclass);
        ELSE
          EXECUTE 'REINDEX INDEX ' || quote_ident(r.indexname) || ';';
        END IF;
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
	
CREATE FUNCTION _lantern_internal.forbid_table_change()
  RETURNS TRIGGER
AS
$$
BEGIN
  RAISE EXCEPTION 'Cannot modify readonly table.';
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_pq_codebook(p_tbl REGCLASS, p_col NAME, cluster_cnt INT, subvector_count INT, distance_metric TEXT, dataset_size_limit INT DEFAULT 0)
RETURNS NAME AS $$
DECLARE
  tbl NAME;
  col NAME;
  stmt TEXT;
  res REAL[];
  codebooks REAL[][][];
  i INT;
  end_idx INT;
  codebook_table NAME;
  dim INT;
BEGIN
  tbl := regexp_replace(trim(both '"' FROM p_tbl::TEXT), '^.*\.', '');
  col := trim(both '"' FROM p_col);

  stmt := format('SELECT array_length(%I, 1) FROM %I WHERE %1$I IS NOT NULL LIMIT 1', col, tbl);
  EXECUTE stmt INTO dim;

	-- Get codebooks
	codebooks := _lantern_internal.create_pq_codebook(p_tbl, col, cluster_cnt, subvector_count, distance_metric, dataset_size_limit);

	-- Create codebook table
  codebook_table := format('_lantern_internal."_codebook_%s_%s"', tbl, col);
  stmt := format('DROP TABLE IF EXISTS %s CASCADE', codebook_table);
  EXECUTE stmt;
  
  stmt:= format('CREATE UNLOGGED TABLE %s(subvector_id INT, centroid_id INT, c REAL[]);', codebook_table);
  EXECUTE stmt;
  
  stmt:= format('CREATE INDEX ON %s USING BTREE(subvector_id, centroid_id);', codebook_table);
  EXECUTE stmt;
  
  -- Iterate over codebooks and insert into table
  FOR i IN 1..subvector_count loop
  	FOR k IN 1..cluster_cnt loop
  	  -- centroid_id is k-1 because k is in range[0,255] but postgres arrays start from index 1
      stmt := format('INSERT INTO %s(subvector_id, centroid_id, c) VALUES (%s, %s, ARRAY(SELECT * FROM unnest(''%s''::REAL[])))', codebook_table, i - 1, k - 1, codebooks[i:i][k:k]);
      EXECUTE stmt;
  	END LOOP;
  END LOOP;

  -- Make table logged and readonly
  stmt := format('ALTER TABLE %s SET LOGGED', codebook_table);
  EXECUTE stmt;

  stmt := format('CREATE TRIGGER readonly_guard BEFORE INSERT OR UPDATE OR DELETE ON %s EXECUTE PROCEDURE _lantern_internal.forbid_table_change()', codebook_table);
  EXECUTE stmt;

  return codebook_table;
END;
$$ LANGUAGE plpgsql;

-- Compress vector using codebook
CREATE OR REPLACE FUNCTION _lantern_internal.quantize_vector(v REAL[], subvector_count INTEGER, codebook regclass, distance_metric TEXT)
RETURNS pqvec AS $$
DECLARE
  subvector_center INT;
  start_idx INT;
  end_idx INT;
  dim INT;
  subvector_len INT;
  res INT[];
  subvector_id INT;
BEGIN
  dim := array_length(v, 1);
  res := '{}'::INT[];
  subvector_len := dim/subvector_count;
  subvector_id := 0;

  IF v IS NULL THEN
    RETURN NULL;
  END IF;

  FOR i IN 1..dim BY subvector_len LOOP
    IF i = dim THEN
      end_idx := dim;
    ELSE
      end_idx := i + subvector_len - 1;
    END IF;
    EXECUTE format('SELECT centroid_id FROM %s WHERE subvector_id=%s ORDER BY %s_dist(c, %L) LIMIT 1', codebook, subvector_id, distance_metric, v[i:end_idx]) INTO subvector_center;
    res := array_append(res, subvector_center);
    subvector_id := subvector_id + 1;
  END LOOP;
  
  RETURN res::pqvec;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION quantize_vector(v REAL[], codebook regclass, distance_metric TEXT)
RETURNS pqvec AS $$
DECLARE
  subvector_count INT;
  stmt TEXT;
BEGIN

  stmt := format('SELECT COUNT(centroid_id) FROM %s WHERE centroid_id=0', codebook);
  EXECUTE stmt INTO subvector_count;

  IF subvector_count = 0 THEN
    RAISE EXCEPTION 'Empty codebook';
  END IF;

  RETURN _lantern_internal.quantize_vector(v, subvector_count, codebook, distance_metric);
END;
$$ LANGUAGE plpgsql;

-- Dequantize vector using codebook
CREATE OR REPLACE FUNCTION dequantize_vector(v pqvec, codebook regclass)
RETURNS REAL[] AS $$
DECLARE
  res REAL[];
  subvector REAL[];
  centroid_id INT;
  subvector_id INT;
  subvector_count INT;
  v_len INT;
BEGIN
  -- Validate arguments
  EXECUTE format('SELECT COUNT(DISTINCT subvector_id) FROM %s', codebook) INTO subvector_count;
  v_len := array_length(v::INT[], 1);

  IF subvector_count != v_len THEN
    RAISE EXCEPTION 'Codebook has % subvectors, but vector is quantized in % subvectors', subvector_count, v_len;
  END IF;
  
  res := '{}'::REAL[];
  subvector_id := 0;
  FOREACH centroid_id in array v::INT[]
  LOOP
     EXECUTE format('SELECT c FROM %s WHERE subvector_id=%L AND centroid_id=%L', codebook, subvector_id, centroid_id) INTO subvector;
     res := res || subvector;
     subvector_id := subvector_id + 1;
  END LOOP;

  RETURN res;
END;
$$ LANGUAGE plpgsql;

-- Quantize table
CREATE OR REPLACE FUNCTION quantize_table(p_tbl regclass, p_col NAME, cluster_cnt INT,subvector_count INT, distance_metric TEXT, dataset_size_limit INT DEFAULT 0)
RETURNS VOID AS $$
DECLARE
  subvector REAL[];
  id INT;
  stmt TEXT;
  tbl NAME;
  col NAME;
  pq_col_name NAME;
  codebook_table NAME;
  trigger_func_name NAME;
  insert_trigger_name NAME;
  update_trigger_name NAME;
  pg_version INT;
  column_exists BOOLEAN;
BEGIN
  tbl := regexp_replace(trim(both '"' FROM p_tbl::TEXT), '^.*\.', '');
  col := trim(both '"' FROM p_col);

  pg_version := (SELECT setting FROM pg_settings WHERE name = 'server_version_num');
  pq_col_name := format('%s_pq', col);
  
  column_exists := (SELECT true FROM pg_attribute WHERE attrelid = p_tbl AND attname = pq_col_name AND NOT attisdropped);

  IF column_exists THEN
    RAISE EXCEPTION 'Column % already exists in table', pq_col_name;
  END IF;
  -- Create codebook
  codebook_table := create_pq_codebook(p_tbl, col, cluster_cnt, subvector_count, distance_metric, dataset_size_limit);

  -- Compress vectors
  RAISE INFO 'Compressing vectors...';

  IF pg_version >= 120000 THEN
    stmt := format('ALTER TABLE %I ADD COLUMN %I PQVEC GENERATED ALWAYS AS (_lantern_internal.quantize_vector(%I, %L, %L, %L)) STORED', tbl, pq_col_name, col, subvector_count, codebook_table, distance_metric);
    EXECUTE stmt;
  ELSE
    stmt := format('ALTER TABLE %I ADD COLUMN %I PQVEC', tbl, pq_col_name);
    EXECUTE stmt;

    stmt := format('UPDATE %1$I SET "%2$s_pq"=_lantern_internal.quantize_vector(%2$I, %3$L, %4$L::regclass, %5$L)', tbl, col, subvector_count, codebook_table, distance_metric);
    EXECUTE stmt;

    -- Create trigger to update pq values based on vector value
    trigger_func_name := format('"_lantern_internal"._set_pq_col_%s', md5(tbl || col));
    stmt := format('
      CREATE OR REPLACE FUNCTION %s()
        RETURNS trigger
        LANGUAGE plpgsql AS
      $body$
      DECLARE
        stmt TEXT;
      BEGIN
        NEW.%I := _lantern_internal.quantize_vector(NEW.%I, %L, %L::regclass, %L);
        RETURN NEW;
      END
      $body$;
      ', trigger_func_name, pq_col_name, col, subvector_count, codebook_table, distance_metric);
    EXECUTE stmt;
    
    insert_trigger_name := format('_pq_trigger_in_%s', md5(tbl || col));
    update_trigger_name := format('_pq_trigger_up_%s', md5(tbl || col));
    
    stmt := format('DROP TRIGGER IF EXISTS %I ON %I', insert_trigger_name, tbl);
    EXECUTE stmt;
    
    stmt := format('DROP TRIGGER IF EXISTS %I ON %I', update_trigger_name, tbl);
    EXECUTE stmt;
    
    stmt := format('CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW WHEN (NEW.%I IS NOT NULL) EXECUTE FUNCTION %s()', 
      insert_trigger_name,
      tbl,
      col,
      trigger_func_name
    );

    EXECUTE stmt;

    stmt := format('CREATE TRIGGER %1$I BEFORE UPDATE OF %2$I ON %3$I FOR EACH ROW WHEN (NEW.%2$I IS NOT NULL) EXECUTE FUNCTION %4$s()', 
      update_trigger_name,
      col,
      tbl,
      trigger_func_name
    );
    EXECUTE stmt;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION drop_quantization(p_tbl regclass, p_col NAME)
RETURNS VOID AS $$
DECLARE
  tbl NAME;
  col NAME;
  pq_col_name NAME;
  codebook_table NAME;
  trigger_func_name NAME;
BEGIN
  tbl := regexp_replace(trim(both '"' FROM p_tbl::TEXT), '^.*\.', '');
  col := trim(both '"' FROM p_col);
  codebook_table := format('_lantern_internal."_codebook_%s_%s"', tbl, col);
  pq_col_name := format('%s_pq', col);
  trigger_func_name := format('"_lantern_internal"._set_pq_col_%s', md5(tbl || col));
  
  EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', codebook_table);
  
  EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS %I', tbl, pq_col_name);

  EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE',  trigger_func_name);
END;
$$ LANGUAGE plpgsql;
