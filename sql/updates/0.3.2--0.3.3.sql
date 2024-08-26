CREATE OR REPLACE FUNCTION _lantern_internal.maybe_setup_weighted_vector_search() RETURNS VOID AS
$weighted_vector_search$
DECLARE
  pgvector_exists boolean;
  pgvector_sparsevec_exists boolean;

    -- required type exist, v1 input type, v2 input type with defaults, v3 input type with defaults, v1 input type, v2 input type, v3 input type
  search_inputs text[4][] := ARRAY[
    ARRAY['vector', 'vector', 'vector = NULL', 'vector = NULL'],
    ARRAY['sparsevec', 'sparsevec', 'vector', 'vector'],
    ARRAY['sparsevec', 'vector', 'sparsevec', 'vector'],
    ARRAY['sparsevec', 'vector', 'vector', 'sparsevec'],
    ARRAY['sparsevec', 'sparsevec', 'sparsevec', 'vector'],
    ARRAY['sparsevec', 'sparsevec', 'vector', 'sparsevec = NULL'],
    ARRAY['sparsevec', 'vector', 'sparsevec', 'sparsevec = NULL'],
    ARRAY['sparsevec', 'sparsevec', 'sparsevec = NULL', 'sparsevec = NULL']
  ];

  -- function suffix, function default operator
  utility_functions text[2][] := ARRAY[
    ARRAY['', '<->'],
    ARRAY['_cos', '<->'],
    ARRAY['_l2sq', '<=>']
  ];
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

  -- Check if the sparsevec type from pgvector exists
  SELECT EXISTS (
    SELECT 1
    FROM pg_type
    WHERE typname = 'sparsevec'
  ) INTO pgvector_sparsevec_exists;

  CREATE OR REPLACE FUNCTION _lantern_internal.weighted_vector_search_helper(
    table_name regtype,
    w1 numeric,
    col1 text,
    vec1 text,
    w2 numeric = 0,
    col2 text = NULL,
    vec2 text = NULL,
    w3 numeric = 0,
    col3 text = NULL,
    vec3 text = NULL,
    ef integer = 100,
    max_dist numeric = NULL,
    distance_operator text = '<->',
    id_col text = 'id',
    exact boolean = false,
    debug_output boolean = false,
    analyze_output boolean = false
  ) RETURNS TEXT AS $$
  DECLARE
    joint_condition text;
    query_base text;
    query_final_where text = '';
    query1 text;
    query2 text;
    query3 text;
    parsed_schema_name text;
    parsed_table_name text;
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

    -- Start: Validate inputs

    -- 1. only allow valid distance_operator strings (<->, <=>, but not abracadabra)
    IF distance_operator NOT IN ('<->', '<=>', '<#>', '<+>') THEN
      RAISE EXCEPTION 'Invalid distance operator: %', distance_operator;
    END IF;

    -- 2. only allow valid column names, i.e., column names that exist in the table
    SELECT n.nspname, c.relname INTO parsed_schema_name, parsed_table_name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.reltype = table_name::oid;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name = parsed_table_name AND table_schema = parsed_schema_name AND column_name = id_col) THEN
      RAISE EXCEPTION 'Invalid column name: %', id_col;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name = parsed_table_name AND table_schema = parsed_schema_name AND column_name = col1) THEN
      RAISE EXCEPTION 'Invalid column name: %', col1;
    END IF;
    IF col2 IS NOT NULL AND NOT EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name = parsed_table_name AND table_schema = parsed_schema_name AND column_name = col2) THEN
      RAISE EXCEPTION 'Invalid column name: %', col2;
    END IF;
    IF col3 IS NOT NULL AND NOT EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name = parsed_table_name AND table_schema = parsed_schema_name AND column_name = col3) THEN
      RAISE EXCEPTION 'Invalid column name: %', col3;
    END IF;

    -- 3. throw an error on negative weights
    IF w1 < 0 OR w2 < 0 OR w3 < 0 THEN
      RAISE EXCEPTION 'Invalid weight: %', w1;
    END IF;

    -- End: Validate inputs

    IF analyze_output THEN
        maybe_analyze := 'ANALYZE, BUFFERS,';
    END IF;

    -- Joint similarity metric condition
    wc1 := format('(%s * (%I %s %s))', w1, col1, distance_operator, vec1);
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
        wc2 := format(' (%s * (%I %s %s))', w2, col2, distance_operator, vec2);
    END IF;
    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
        wc3 := format(' (%s * (%I %s %s))', w3, col3, distance_operator, vec3);
    END IF;

    joint_condition := wc1 || COALESCE('+' || wc2, '') || COALESCE('+' || wc3, '');

    -- Base query with joint similarity metric
    query_base := format('SELECT * FROM %s ', table_name);
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

      RETURN final_query;
    END IF;

    EXECUTE format('SET LOCAL hnsw.ef_search TO %L', ef);
    -- UNION ALL.. part of the final query that aggregates results from individual vector search queries
    maybe_unions_query := '';

    -- Query 1: Order by first condition's weighted similarity
    query1 := format('%s ORDER BY %I %s %s LIMIT %L', query_base || query_final_where, col1, distance_operator, vec1, ef);

    IF debug_output THEN
      EXECUTE format('SELECT count(*) FROM (%s) t', query1) INTO debug_count;
      RAISE WARNING 'col1 yielded % rows', debug_count;
    END IF;

    cte_query = format('WITH query1 AS (%s) ', query1);

    -- Query 2: Order by other conditions' weighted similarity, if applicable
    IF w2 > 0 AND col2 IS NOT NULL AND vec2 IS NOT NULL THEN
      query2 := format('%s ORDER BY %I %s %s LIMIT %L', query_base || query_final_where, col2, distance_operator, vec2, ef);
      cte_query := cte_query || format(', query2 AS (%s)', query2);
      maybe_unions_query := maybe_unions_query || format(' UNION ALL (SELECT * FROM query2) ');
      IF debug_output THEN
        EXECUTE format('SELECT count(*) FROM (%s) t', query2) INTO debug_count;
        RAISE WARNING 'col2 yielded % rows', debug_count;
      END IF;
    END IF;

    -- Query 3: Order by third condition's weighted similarity, if applicable
    IF w3 > 0 AND col3 IS NOT NULL AND vec3 IS NOT NULL THEN
      query3 := format('%s ORDER BY %I %s %s LIMIT %L', query_base || query_final_where, col3, distance_operator, vec3, ef);
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
    
    RETURN final_query;
    
    END
  $$ LANGUAGE plpgsql;

  FOR i IN 1 .. array_length(search_inputs, 1) LOOP
    FOR j IN 1 .. array_length(utility_functions, 1) LOOP
      IF search_inputs[i][1] = 'sparsevec' AND NOT pgvector_sparsevec_exists THEN
        RAISE NOTICE 'pgvector sparsevec type not found. Skipping lantern weighted vector search setup for sparsevec';
        CONTINUE;
      END IF;

      EXECUTE format($create_weighted_vector_search_functions$
        CREATE OR REPLACE FUNCTION lantern.weighted_vector_search%s(
          relation_type anyelement,
          w1 numeric,
          col1 text,
          vec1 %s,
          w2 numeric %s,
          col2 text %s,
          vec2 %s,
          w3 numeric %s,
          col3 text %s,
          vec3 %s,
          ef integer = 100,
          max_dist numeric = NULL,
          distance_operator text = %L,
          id_col text = 'id',
          exact boolean = false,
          debug_output boolean = false,
          analyze_output boolean = false
          )
          -- N.B. Something seems strange about PL/pgSQL functions that return table with anyelement
          -- when there is single "anylement column" being returned (e.g. returns table ("row" anylement))
          -- then that single "column" is properly spread with source table's column names
          -- but, when returning ("row" anyelement, "anothercol" integer), things fall all over the place
          -- now, the returned table always has 2 columns one row that is a record of sorts, and one "anothercol"
          RETURNS TABLE ("row" anyelement) AS
        $$
        DECLARE
          query text;
          vec1_string text = CASE WHEN vec1 IS NULL THEN '' ELSE format('%%L::%s', vec1) END;
          vec2_string text = CASE WHEN vec2 IS NULL THEN '' ELSE format('%%L::%s', vec2) END;
          vec3_string text = CASE WHEN vec3 IS NULL THEN '' ELSE format('%%L::%s', vec3) END;
        BEGIN
          query := _lantern_internal.weighted_vector_search_helper(pg_typeof(relation_type), w1, col1, vec1_string, w2, col2, vec2_string, w3, col3, vec3_string, ef, max_dist, distance_operator, id_col, exact, debug_output, analyze_output);
          RETURN QUERY EXECUTE query;
        END
        $$ LANGUAGE plpgsql;
      $create_weighted_vector_search_functions$,
      utility_functions[j][1],
      search_inputs[i][2],
      CASE WHEN search_inputs[i][2] LIKE '%NULL' THEN ' = 0' ELSE '' END,
      CASE WHEN search_inputs[i][2] LIKE '%NULL' THEN ' = NULL' ELSE '' END,
      search_inputs[i][3],
      CASE WHEN search_inputs[i][3] LIKE '%NULL' THEN ' = 0' ELSE '' END,
      CASE WHEN search_inputs[i][3] LIKE '%NULL' THEN ' = NULL' ELSE '' END,
      search_inputs[i][4],
      utility_functions[j][2],
      CASE WHEN search_inputs[i][2] LIKE 'sparsevec%%' THEN 'sparsevec' ELSE 'vector' END,
      CASE WHEN search_inputs[i][3] LIKE 'sparsevec%%' THEN 'sparsevec' ELSE 'vector' END,
      CASE WHEN search_inputs[i][4] LIKE 'sparsevec%%' THEN 'sparsevec' ELSE 'vector' END);
    END LOOP;
  END LOOP;
END
$weighted_vector_search$ LANGUAGE plpgsql;

SELECT _lantern_internal.maybe_setup_weighted_vector_search();
DROP FUNCTION _lantern_internal.maybe_setup_weighted_vector_search;