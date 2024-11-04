-- Definitions concerning our hnsw-based index data strucuture
CREATE FUNCTION hnsw_handler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

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
CREATE SCHEMA lantern;
GRANT USAGE ON SCHEMA lantern TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA lantern TO PUBLIC;


CREATE FUNCTION _lantern_internal.validate_index(index regclass, print_info boolean DEFAULT true) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_validate_index' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

CREATE FUNCTION _lantern_internal.failure_point_enable(func TEXT, name TEXT, dont_trigger_first_nr INTEGER DEFAULT 0) RETURNS VOID
	AS 'MODULE_PATHNAME', 'lantern_internal_failure_point_enable' LANGUAGE C STABLE STRICT PARALLEL UNSAFE;

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
  codebook_table TEXT;
  dim INT;
BEGIN
  tbl := regexp_replace(trim(both '"' FROM p_tbl::TEXT), '^.*\.', '');
  col := trim(both '"' FROM p_col);
  codebook_table := format('pq_%s_%s', tbl, col);

  IF length(codebook_table) > 63 THEN
    RAISE EXCEPTION 'Codebook table name "%" exceeds 63 char limit', codebook_table;
  END IF;

  codebook_table := format('_lantern_internal."%s"', codebook_table);

  stmt := format('SELECT array_length(%I, 1) FROM %I WHERE %1$I IS NOT NULL LIMIT 1', col, tbl);
  EXECUTE stmt INTO dim;

	-- Get codebooks
	codebooks := _lantern_internal.create_pq_codebook(p_tbl, col, cluster_cnt, subvector_count, distance_metric, dataset_size_limit);

	-- Create codebook table
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
  codebook_table := format('_lantern_internal."pq_%s_%s"', tbl, col);
  pq_col_name := format('%s_pq', col);
  trigger_func_name := format('"_lantern_internal"._set_pq_col_%s', md5(tbl || col));
  
  EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', codebook_table);
  
  EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS %I', tbl, pq_col_name);

  EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE',  trigger_func_name);
END;
$$ LANGUAGE plpgsql;

  -- Asynchronous task scheduling BEGIN
CREATE OR REPLACE FUNCTION _lantern_internal.maybe_setup_lantern_tasks() RETURNS VOID AS
$async_tasks_related$
BEGIN
  IF NOT (SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cron'))
  THEN
    RAISE NOTICE 'pg_cron extension not found. Skipping lantern async task setup';
    RETURN;
  END IF;
  GRANT USAGE ON SCHEMA cron TO PUBLIC;

  CREATE TABLE lantern.tasks (
	  jobid bigserial primary key,
	  query text not null,
	  pg_cron_job_name text default null, -- initially null, because it will be ready after job insertion
	  job_name text default null,
	  username text not null default current_user,
    started_at timestamp with time zone not null default now(),
    duration interval,
    status text,
    error_message text
  );

  GRANT SELECT, INSERT, UPDATE, DELETE ON lantern.tasks TO public;
  GRANT USAGE, SELECT ON SEQUENCE lantern.tasks_jobid_seq TO public;
  ALTER TABLE lantern.tasks ENABLE ROW LEVEL SECURITY;
  CREATE POLICY lantern_tasks_policy ON lantern.tasks USING (username OPERATOR(pg_catalog.=) current_user);

  -- create a trigger and added to cron.job_run_details
  CREATE OR REPLACE FUNCTION _lantern_internal.async_task_finalizer_trigger() RETURNS TRIGGER AS $$
  DECLARE
    res RECORD;
  BEGIN
    -- if NEW.status is one of "starting", "running", "sending, "connecting", return
    IF NEW.status IN ('starting', 'running', 'sending', 'connecting') THEN
      RETURN NEW;
    END IF;

    IF NEW.status NOT IN ('succeeded', 'failed') THEN
      RAISE WARNING 'Lantern Async tasks: Unexpected status %', NEW.status;
    END IF;

    -- Get the job name from the jobid
    -- Call the job finalizer if corresponding job exists BOTH in lantern async tasks AND
    -- active cron jobs
    UPDATE lantern.tasks t SET
        (duration, status, error_message, pg_cron_job_name) = (run.end_time - t.started_at, NEW.status,
        CASE WHEN NEW.status = 'failed' THEN return_message ELSE NULL END,
        c.jobname )
    FROM cron.job c
    LEFT JOIN cron.job_run_details run
    ON c.jobid = run.jobid
    WHERE
       t.pg_cron_job_name = c.jobname AND
       c.jobid = NEW.jobid
    -- using returning as a trick to run the unschedule function as a side effect
    -- Note: have to unschedule by jobid because of pg_cron#320 https://github.com/citusdata/pg_cron/issues/320
    RETURNING cron.unschedule(NEW.jobid) INTO res;

    RETURN NEW;

  EXCEPTION
     WHEN OTHERS THEN
          RAISE WARNING 'Lantern Async tasks: Unknown job failure in % % %', NEW, SQLERRM, SQLSTATE;
          PERFORM cron.unschedule(NEW.jobid);
          RETURN NEW;
  END
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER status_change_trigger
  AFTER UPDATE OF status
  ON cron.job_run_details
  FOR EACH ROW
  WHEN (OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION _lantern_internal.async_task_finalizer_trigger();


  CREATE OR REPLACE FUNCTION lantern.async_task(query text, job_name text) RETURNS INTEGER AS $$
  DECLARE
    _job_id integer;
    _pg_cron_job_name text;
    start_time timestamptz;
  BEGIN
    start_time := clock_timestamp();
    job_name := COALESCE(job_name, '');

    INSERT INTO lantern.tasks (query, job_name, started_at)
    VALUES (query, job_name, start_time) RETURNING jobid INTO _job_id;

    _pg_cron_job_name := 'async_task_' || _job_id;

    UPDATE lantern.tasks t SET
      pg_cron_job_name = _pg_cron_job_name
    WHERE jobid = _job_id;

    -- Schedule the job. Note: The original query execution is moved to the finalizer.
    PERFORM cron.schedule(_pg_cron_job_name, '1 seconds', query);
    RAISE NOTICE 'Job scheduled with pg_cron name: %', quote_literal(_pg_cron_job_name);
    RETURN _job_id;
  END
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION lantern.async_task(query text) RETURNS INTEGER AS $$
  BEGIN
    RETURN lantern.async_task(query, NULL);
  END
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION lantern.cancel_all_async_tasks() RETURNS void AS $$
  BEGIN
    PERFORM cron.unschedule(pg_cron_job_name) FROM lantern.tasks
      WHERE duration IS NULL;

    UPDATE lantern.tasks t SET
        duration = clock_timestamp() - t.started_at,
        status = 'canceled',
        error_message = COALESCE(error_message, '') || 'Canceled by user'
      WHERE duration is NULL;
  END
  $$ LANGUAGE plpgsql;
END
$async_tasks_related$ LANGUAGE plpgsql;

SELECT _lantern_internal.maybe_setup_lantern_tasks();
DROP FUNCTION _lantern_internal.maybe_setup_lantern_tasks();

-- ^^^^
-- Asynchronous task scheduling END

-- Weighted vector search

CREATE OR REPLACE FUNCTION _lantern_internal.mask_arrays(arr text)
RETURNS text AS $$
BEGIN
-- match:
--    single quote (escaped by doubling it)
--    opening square bracket (escaped with a backslash)
--    any character (as few as possible, via *?)
--    closing square bracket (escaped with a backslash)
--    single quote (escaped by doubling it)
--    the string ::vector literally
arr := regexp_replace(arr, '''\[.*?\]''::vector', '''[MASKED_VECTOR]''::vector','g');
-- same as above, but for non-explain context where the explicit cast is missing
arr := regexp_replace(arr, '''\[.*?\]''', '''[MASKED_VECTOR]''','g');

RETURN arr;
END
$$ LANGUAGE plpgsql;


-- Helper function that takes in the output of EXPLAIN (FORMAT JSON) and masks long vectors in ORDER BY clauses
CREATE OR REPLACE FUNCTION _lantern_internal.mask_order_by_in_plan(json_data jsonb)
RETURNS jsonb AS $$
DECLARE
    key TEXT;
    value JSONB;
BEGIN
    -- Check if the input is null
    IF json_data IS NULL THEN
        RETURN NULL;
    END IF;

    -- Check if the input is a JSON object
    IF jsonb_typeof(json_data) = 'object' THEN
        -- Loop through each key-value pair in the JSON object
        FOR key, value IN SELECT * FROM jsonb_each(json_data) LOOP
            -- If the key is "Order By", set the value to null
            IF key = 'Order By' OR key = 'Filter' OR key = 'Sort Key' THEN
                value = _lantern_internal.mask_arrays(value::text);
                json_data = jsonb_set(json_data, ARRAY[key], value);
            ELSE
                -- Recursively call the function for nested JSON objects or arrays
                json_data = jsonb_set(json_data, ARRAY[key], _lantern_internal.mask_order_by_in_plan(value));
            END IF;
        END LOOP;
    -- Check if the input is a JSON array
    ELSIF jsonb_typeof(json_data) = 'array' THEN
        -- Loop through each element in the JSON array
        FOR idx IN 0 .. jsonb_array_length(json_data) - 1 LOOP
            -- Recursively call the function for elements of the array
            json_data = jsonb_set(json_data, ARRAY[idx::text], _lantern_internal.mask_order_by_in_plan(json_data->idx));
        END LOOP;
    END IF;

    RETURN json_data;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _lantern_internal.maybe_setup_weighted_vector_search() RETURNS VOID AS
$weighted_vector_search$
DECLARE
  pgvector_exists boolean;
BEGIN
  -- Check if the vector type from pgvector exists
  SELECT EXISTS (
    SELECT 1
    FROM pg_type
    WHERE typname = 'vector'
  ) INTO pgvector_exists;

  IF NOT pgvector_exists THEN
    RAISE NOTICE 'pgvector extension not found. Skipping lantern weighted vector search setup';
    RETURN;
  END IF;

  CREATE OR REPLACE FUNCTION lantern.weighted_vector_search(
    relation_type anyelement,
    w1 numeric,
    col1 text,
    vec1 vector,
    w2 numeric= 0,
    col2 text = NULL,
    vec2 vector = NULL,
    w3 numeric = 0,
    col3 text = NULL,
    vec3 vector = NULL,
    ef integer = 100,
    max_dist numeric = NULL,
    -- set l2 (pgvector) and l2sq (lantern) as default, as we do for lantern index.
    distance_operator text = '<->',
    id_col text = 'id',
    exact boolean = false,
    debug_output boolean = false,
    analyze_output boolean = false
    )
    -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
    -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
    -- then that single "column" is properly spread with source table's column names
    -- but, when returning ("row" anyelement, "anothercol" integer), things fall all oaver the place
    -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
    RETURNS TABLE ("row" anyelement) AS
  $$
  DECLARE
    joint_condition text;
    query_base text;
    query_final_where text = '';
    query1 text;
    query2 text;
    query3 text;
    -- variables for weighted columns
    wc1 text = NULL;
    wc2 text = NULL;
    wc3 text = NULL;
    cte_query text;
    maybe_unions_query text;
    final_query text;
    explain_query text;
    explain_output jsonb;
    old_hnsw_ef_search numeric;
    debug_count integer;
    maybe_analyze text = '';
  BEGIN
    -- TODO:: better validate inputs to throw nicer errors in case of wrong input:
    --   1. only allow valid distance_operator stirngs (<->, <=>, but not abracadabra)
    --   2. only allow valid column names
    --   3. throw an error on negative weights
    --   4. check that id_col column exists before proceeding

    IF analyze_output THEN
      maybe_analyze := 'ANALYZE, BUFFERS,';
    END IF;
    -- Joint similarity metric condition
    -- the cast ::vector is necessary for cases when the column is not of type vector
    -- and for some reason in those cases cast does not happen automatically
    wc1 := format('(%s * (%I %s %L::vector))', w1, col1, distance_operator, vec1);
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
      wc2 := format(' (%s * (%I %s %L::vector))', w2, col2, distance_operator, vec2);
    END IF;
    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
      wc3 := format(' (%s * (%I %s %L::vector))', w3, col3, distance_operator, vec3);
    END IF;

    joint_condition := wc1 || COALESCE('+' || wc2, '') || COALESCE('+' || wc3, '');

    -- Base query with joint similarity metric
    query_base := format('SELECT * FROM %s ', pg_typeof(relation_type));
    IF max_dist IS NOT NULL THEN
      query_final_where := format(' WHERE %s < %L', joint_condition, max_dist);
    END IF;

    IF exact THEN
      final_query := query_base || query_final_where || format(' ORDER BY %s', joint_condition);
      IF debug_output THEN
        explain_query := format('EXPLAIN (%s COSTS FALSE, FORMAT JSON) %s', maybe_analyze, final_query);
        EXECUTE explain_query INTO explain_output;

        RAISE WARNING 'Query: %', _lantern_internal.mask_arrays(final_query);

        explain_output := _lantern_internal.mask_order_by_in_plan(explain_output);
        RAISE WARNING 'weighted vector search explain(exact=true): %', jsonb_pretty(explain_output);
      END IF;
      RETURN QUERY EXECUTE final_query;
      -- the empty return below is crucial, to make sure the rest of the function is not executed after the return query above
      RETURN;
    END IF;

    EXECUTE format('SET LOCAL hnsw.ef_search TO %L', ef);
    -- UNION ALL.. part of the final query that aggregates results from individual vector search queries
    maybe_unions_query := '';

    -- Query 1: Order by first condition's weighted similarity
    query1 := format('%s ORDER BY %I %s %L::vector LIMIT %L', query_base || query_final_where, col1, distance_operator, vec1, ef);

    IF debug_output THEN
      EXECUTE format('SELECT count(*) FROM (%s) t', query1) INTO debug_count;
      RAISE WARNING 'col1 yielded % rows', debug_count;
    END IF;

    cte_query = format('WITH query1 AS (%s) ', query1);

    -- Query 2: Order by other conditions' weighted similarity, if applicable
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
      query2 := format('%s ORDER BY %I %s %L::vector LIMIT %L', query_base || query_final_where, col2, distance_operator, vec2, ef);
      cte_query := cte_query || format(', query2 AS (%s)', query2);
      maybe_unions_query := maybe_unions_query || format(' UNION ALL (SELECT * FROM query2) ');
      IF debug_output THEN
        EXECUTE format('SELECT count(*) FROM (%s) t', query2) INTO debug_count;
        RAISE WARNING 'col2 yielded % rows', debug_count;
      END IF;
    END IF;

    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
      query3 := format('%s ORDER BY %I %s %L::vector LIMIT %L', query_base || query_final_where, col3, distance_operator, vec3, ef);
      cte_query := cte_query || format(', query3 AS (%s)', query3);
      maybe_unions_query := maybe_unions_query || format(' UNION ALL (SELECT * FROM query3) ');
      IF debug_output THEN
        EXECUTE format('SELECT count(*) FROM (%s) t', query3) INTO debug_count;
        RAISE WARNING 'col3 yielded % rows', debug_count;
      END IF;
    END IF;

    final_query := cte_query || format($final_cte_query$SELECT * FROM (
      SELECT DISTINCT ON (%I) * FROM (
          (SELECT * FROM query1)
          %s
      ) t
    )
    tt %s ORDER BY %s$final_cte_query$,
    id_col, maybe_unions_query, query_final_where, joint_condition);

  IF debug_output THEN
    explain_query := format('EXPLAIN (%s COSTS FALSE, FORMAT JSON) %s', maybe_analyze, final_query);
    EXECUTE explain_query INTO explain_output;

    RAISE WARNING 'Query: %', _lantern_internal.mask_arrays(final_query);

    explain_output := _lantern_internal.mask_order_by_in_plan(explain_output);
    RAISE WARNING ' weighted vector search explain: %', jsonb_pretty(explain_output);
  END IF;
  RETURN QUERY EXECUTE final_query;
  END
  $$ LANGUAGE plpgsql;

-- setup API shortcuts
  CREATE OR REPLACE FUNCTION lantern.weighted_vector_search_cos(
    relation_type anyelement,
    w1 numeric,
    col1 text,
    vec1 vector,
    w2 numeric= 0,
    col2 text = NULL,
    vec2 vector = NULL,
    w3 numeric = 0,
    col3 text = NULL,
    vec3 vector = NULL,
    ef integer = 100,
    max_dist numeric = NULL,
    id_col text = 'id',
    exact boolean = false,
    debug_output boolean = false,
    analyze_output boolean = false
    )
    -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
    -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
    -- then that single "column" is properly spread with source table's column names
    -- but, when returning ("row" anyelement, "anothercol" integer), things fall all oaver the place
    -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
    RETURNS TABLE ("row" anyelement) AS $$

BEGIN
  RETURN QUERY SELECT * FROM lantern.weighted_vector_search(relation_type, w1, col1, vec1, w2, col2, vec2, w3, col3, vec3, ef, max_dist, '<=>', id_col, exact, debug_output, analyze_output);
END $$ LANGUAGE plpgsql;

 CREATE OR REPLACE FUNCTION lantern.weighted_vector_search_l2sq(
    relation_type anyelement,
    w1 numeric,
    col1 text,
    vec1 vector,
    w2 numeric= 0,
    col2 text = NULL,
    vec2 vector = NULL,
    w3 numeric = 0,
    col3 text = NULL,
    vec3 vector = NULL,
    ef integer = 100,
    max_dist numeric = NULL,
    id_col text = 'id',
    exact boolean = false,
    debug_output boolean = false,
    analyze_output boolean = false
    )
    -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
    -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
    -- then that single "column" is properly spread with source table's column names
    -- but, when returning ("row" anyelement, "anothercol" integer), things fall all oaver the place
    -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
    RETURNS TABLE ("row" anyelement) AS $$

BEGIN
  RETURN QUERY SELECT * FROM lantern.weighted_vector_search(relation_type, w1, col1, vec1, w2, col2, vec2, w3, col3, vec3, ef, max_dist, '<->', id_col, exact, debug_output, analyze_output);
END $$ LANGUAGE plpgsql;


END
$weighted_vector_search$ LANGUAGE plpgsql;

SELECT _lantern_internal.maybe_setup_weighted_vector_search();
DROP FUNCTION _lantern_internal.maybe_setup_weighted_vector_search;

-- helper function to mask large vectors in explain outputs of queries containing vectors
CREATE OR REPLACE FUNCTION lantern.masked_explain(
        query text,
        do_analyze boolean = true,
        buffers boolean = true,
        costs boolean = true,
        timing boolean = true
) RETURNS text AS $$
DECLARE
    explain_query text;
    explain_output jsonb;
    flags text = '';
BEGIN
    IF do_analyze THEN
      flags := flags || 'ANALYZE, ';
    END IF;
    IF buffers THEN
      flags := flags || 'BUFFERS, ';
    END IF;
    IF costs THEN
      flags := flags || 'COSTS, ';
    END IF;
    IF timing THEN
      flags := flags || 'TIMING ';
    END IF;
    explain_query := format('EXPLAIN (%s, FORMAT JSON) %s', flags, query);
    EXECUTE explain_query INTO explain_output;
    RETURN jsonb_pretty(_lantern_internal.mask_order_by_in_plan(explain_output));
END $$ LANGUAGE plpgsql;

-- Get vector type oid
CREATE FUNCTION _lantern_internal.get_vector_type_oid() RETURNS OID AS $$
DECLARE
  type_oid OID;
BEGIN
  type_oid := (SELECT pg_type.oid FROM pg_type
                JOIN pg_depend ON pg_type.oid = pg_depend.objid
                JOIN pg_extension ON pg_depend.refobjid = pg_extension.oid 
                WHERE typname='vector' AND extname='vector'
                LIMIT 1);
  RETURN COALESCE(type_oid, 0);
END;
$$ LANGUAGE plpgsql;
