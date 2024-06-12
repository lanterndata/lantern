-- Create Lantern schema
CREATE SCHEMA lantern;
GRANT USAGE ON SCHEMA lantern TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA lantern TO PUBLIC;

-- Asynchronous task scheduling BEGIN
CREATE OR REPLACE FUNCTION _lantern_internal.maybe_setup_lantern_tasks() RETURNS VOID AS
$async_tasks_related$
BEGIN
  IF NOT (SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cron'))
  THEN
    RAISE NOTICE 'pg_cron extension not found. Skipping lantern async task setup';
    RETURN;
  END IF;

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

  GRANT SELECT ON lantern.tasks TO public;
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
    RETURNING cron.unschedule(t.pg_cron_job_name) INTO res;

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
      maybe_analyze := 'ANALYZE,';
    END IF;
    -- Joint similarity metric condition
    wc1 := format('(%s * (%I %s %L))', w1, col1, distance_operator, vec1);
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
      wc2 := format(' (%s * (%I %s %L))', w2, col2, distance_operator, vec2);
    END IF;
    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
      wc3 := format(' (%s * (%I %s %L))', w3, col3, distance_operator, vec3);
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
    query1 := format('%s ORDER BY %I %s %L LIMIT %L', query_base || query_final_where, col1, distance_operator, vec1, ef);

    IF debug_output THEN
      EXECUTE format('SELECT count(*) FROM (%s) t', query1) INTO debug_count;
      RAISE WARNING 'col1 yielded % rows', debug_count;
    END IF;

    cte_query = format('WITH query1 AS (%s) ', query1);

    -- Query 2: Order by other conditions' weighted similarity, if applicable
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
      query2 := format('%s ORDER BY %I %s %L LIMIT %L', query_base || query_final_where, col2, distance_operator, vec2, ef);
      cte_query := cte_query || format(', query2 AS (%s)', query2);
      maybe_unions_query := maybe_unions_query || format(' UNION ALL (SELECT * FROM query2) ');
      IF debug_output THEN
        EXECUTE format('SELECT count(*) FROM (%s) t', query2) INTO debug_count;
        RAISE WARNING 'col2 yielded % rows', debug_count;
      END IF;
    END IF;

    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
      query3 := format('%s ORDER BY %I %s %L LIMIT %L', query_base || query_final_where, col3, distance_operator, vec3, ef);
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
    debug_output boolean = false
    )
    -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
    -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
    -- then that single "column" is properly spread with source table's column names
    -- but, when returning ("row" anyelement, "anothercol" integer), things fall all oaver the place
    -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
    RETURNS TABLE ("row" anyelement) AS $$

BEGIN
  RETURN QUERY SELECT * FROM lantern.weighted_vector_search(relation_type, w1, col1, vec1, w2, col2, vec2, w3, col3, vec3, ef, max_dist, '<=>', id_col, exact, debug_output);
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
    debug_output boolean = false
    )
    -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
    -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
    -- then that single "column" is properly spread with source table's column names
    -- but, when returning ("row" anyelement, "anothercol" integer), things fall all oaver the place
    -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
    RETURNS TABLE ("row" anyelement) AS $$

BEGIN
  RETURN QUERY SELECT * FROM lantern.weighted_vector_search(relation_type, w1, col1, vec1, w2, col2, vec2, w3, col3, vec3, ef, max_dist, '<->', id_col, exact, debug_output);
END $$ LANGUAGE plpgsql;


END
$weighted_vector_search$ LANGUAGE plpgsql;

SELECT _lantern_internal.maybe_setup_weighted_vector_search();
DROP FUNCTION _lantern_internal.maybe_setup_weighted_vector_search;

