SET client_min_messages=debug5;

\ir utils/sift10k_array.sql

-- This function executes the given SQL query and returns its estimated total cost.
-- It parses the EXPLAIN output to retrieve the outermost (top-level) cost estimation.
-- Example EXPLAIN line: "Limit  (cost=0.00..0.47 rows=10 width=40)"
-- The regex captures the cost range and returns the higher end.
-- Returns NULL if no cost is found or if the provided query doesn't match the expected format.
CREATE OR REPLACE FUNCTION get_cost_estimate(explain_query text) RETURNS real AS $$
DECLARE
    explain_output text;
    cost_match text;
    line text;
BEGIN
    EXECUTE explain_query INTO explain_output;
    FOR line IN (
        SELECT
            unnest(string_to_array(explain_output, E'\n')))
        LOOP
            IF position(' ' IN
            LEFT (line, 1)) = 0 AND line LIKE '%cost=%' THEN
                cost_match :=(regexp_matches(line, 'cost=\d+\.\d+..\d+\.\d+'))[1];
                -- Adjust regex to capture both costs
                RETURN split_part(split_part(cost_match, '=', 2), '..', 2)::real;
                -- Extract the total cost
            END IF;
        END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- This function checks if val2 is within some error margin of val1.
CREATE OR REPLACE FUNCTION is_within_error(val1 real, val2 real, error_margin real) RETURNS boolean AS $$
BEGIN
    RETURN val2 BETWEEN val1 * (1 - error_margin) AND val1 * (1 + error_margin);
END;
$$ LANGUAGE plpgsql;

-- This function checks if the cost estimate for the given query is within some error margin of the expected cost.
CREATE OR REPLACE FUNCTION is_cost_estimate_within_error(explain_query text, expected_cost real, error_margin real DEFAULT 0.05) RETURNS boolean AS $$
BEGIN
    RETURN is_within_error(get_cost_estimate(explain_query), expected_cost, error_margin);    
END;
$$ LANGUAGE plpgsql;

SET lantern.pgvector_compat=FALSE;

-- Goal: make sure query cost estimate is accurate
-- when index is created with varying costruction parameters.
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
\set explain_query_template 'EXPLAIN SELECT * FROM sift_base10k ORDER BY v <?> ''%s'' LIMIT 10'
\set enable_seqscan = off;

-- Case 0, sanity check. No data.
CREATE TABLE empty_table(id SERIAL PRIMARY KEY, v REAL[2]);
CREATE INDEX empty_idx ON empty_table USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=2, dim=2);
SET _lantern_internal.is_test = true;
SELECT is_cost_estimate_within_error('EXPLAIN SELECT * FROM empty_table ORDER BY v <?> ''{1,2}'' LIMIT 10', 0.47);
SELECT _lantern_internal.validate_index('empty_idx', false);
DROP INDEX empty_idx;

-- Case 1, more data in index.
-- Should see higher cost than Case 0.
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=2, ef_construction=10, ef=4, dim=128);
SELECT is_cost_estimate_within_error(format(:'explain_query_template', :'v4444'), 3.00);
SELECT _lantern_internal.validate_index('hnsw_idx', false);
DROP INDEX hnsw_idx;

-- Case 2, higher M.
-- Should see higher cost than Case 1.
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=20, ef_construction=10, ef=4, dim=128);
SELECT is_cost_estimate_within_error(format(:'explain_query_template', :'v4444'), 3.27);
SELECT _lantern_internal.validate_index('hnsw_idx', false);
DROP INDEX hnsw_idx;

-- Case 3, higher ef.
-- Should see higher cost than Case 2.
CREATE INDEX hnsw_idx ON sift_base10k USING hnsw (v dist_l2sq_ops) WITH (M=20, ef_construction=10, ef=16, dim=128);
SELECT is_cost_estimate_within_error(format(:'explain_query_template', :'v4444'), 3.91);
SELECT _lantern_internal.validate_index('hnsw_idx', false);
DROP INDEX hnsw_idx;


-- Goal: Test cost estimation when number of pages in index is likely less than number of blockmaps allocated
-- this is relevant in this check in estimate_number_blocks_accessed in hnsw.c:
-- const uint64 num_datablocks = Max(num_pages - 1 - num_blockmap_allocated, 1);

-- One place where this happens is on partial indexes where the filter is rare (empirically, matching <2.5% of the entire table)
-- This is what we test below
\ir utils/views_vec10k.sql

-- This is important to make sure that index selectivity calculations from genericcostestimate are accurate (which we test below)
SET client_min_messages=ERROR;
VACUUM ANALYZE;
SET client_min_messages=debug5;

SET hnsw.init_k = 10;

-- Note that the (views < 100) condition is quite rare (out of 10,000 rows)
SELECT COUNT(*) FROM views_vec10k WHERE views < 100;

-- Create partial lantern index with (views < 100) filter
CREATE INDEX hnsw_partial_views_100 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 100;

-- This should use the partial index we just created, since it is an exact filter match
EXPLAIN (COSTS FALSE) SELECT id, views FROM views_vec10k WHERE views < 100 ORDER BY vec<->'{0,1,2,3,4,5}' LIMIT 10;

-- Goal: Test that the index selectivity being calculated for partial indexes is correct
-- note that these boundaries are selected so that mac num_pages and cost values align
CREATE INDEX hnsw_partial_views_1000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 1000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_1000', false);

CREATE INDEX hnsw_partial_views_2000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 2000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_2000', false);

CREATE INDEX hnsw_partial_views_3000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 3000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_3000', false);

CREATE INDEX hnsw_partial_views_4000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 4000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_4000', false);

CREATE INDEX hnsw_partial_views_6000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 6000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_6000', false);

CREATE INDEX hnsw_partial_views_8000 ON views_vec10k USING hnsw (vec dist_l2sq_ops) WITH (dim=6) WHERE views < 8000;
SELECT _lantern_internal.validate_index('hnsw_partial_views_8000', false);

-- Trigger each partial index by using its exact filter in a filtered query
-- Each indexSelectivity value for a partial index with the filter (views < N) should be around N/20000
-- (in other words, the fraction of rows from the table that is in the partial index, since views ~ Unif[0, 20,000])

-- note that all partial indexes whose filter is a superset of the filter in the query will output indexSelectivity to ldb_dlog below
-- so, it suffices to call the "smallest" filter, and we will get the selectivity for all the other indices since their filters are nested subsets of each other
EXPLAIN (COSTS FALSE) SELECT id, views FROM views_vec10k WHERE views < 1000 ORDER BY vec<->'{0,1,2,3,4,5}' LIMIT 10;
