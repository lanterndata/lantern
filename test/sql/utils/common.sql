-- N.B.: This file shall be maintained such that it can safely be rerun without throwing an error
--      This is because in upgrade tests we may run this multiple times in preparation for sequential
--      and parallel upgrade tests

-- test helper functions that should exist in all test runs live here
-- there is no need to explicitly include this file in other tests as the test runner will
-- run this before running the actual test

CREATE EXTENSION IF NOT EXISTS pageinspect;

\set ON_ERROR_STOP on

-- retrieves details for all indices associated with a given table, similar to \di+
-- the output of \di+ is not consistent across postgres versions
-- todo:: add a columns to this function which returning number of used DB pages
CREATE OR REPLACE FUNCTION ldb_get_indexes(tblname text)
RETURNS TABLE(
    indexname name,
    size text,
    indexdef text,
    total_index_size text
) AS
$BODY$
BEGIN
    RETURN QUERY
    WITH total_size_data AS (
        SELECT
            SUM(pg_relation_size(indexrelid)) as total_size
        FROM
            pg_index 
        WHERE
            indisvalid
            AND indrelid = tblname::regclass
    )
    SELECT
        idx.indexname,
        pg_size_pretty(pg_relation_size(idx.indexname::REGCLASS)) as size,
        idx.indexdef,
        pg_size_pretty(total_size_data.total_size) as total_index_size
    FROM
        pg_indexes idx,
        total_size_data
    WHERE
        idx.tablename = tblname;
END;
$BODY$
LANGUAGE plpgsql;

-- Determines if the provided SQL query (with an EXPLAIN prefix) uses an "Index Scan" 
-- by examining its execution plan. This function helps ensure consistent analysis 
-- across varying Postgres versions where EXPLAIN output may differ.
CREATE OR REPLACE FUNCTION has_index_scan(explain_query text) RETURNS boolean AS $$
DECLARE
    plan_row RECORD;
    found boolean := false;
BEGIN
    FOR plan_row IN EXECUTE explain_query LOOP
        IF position('Index Scan' in plan_row."QUERY PLAN") > 0 THEN
            found := true;
            EXIT;
        END IF;
    END LOOP;
    RETURN found;
END;
$$ LANGUAGE plpgsql;

-- Determine if the two  queries provided return the same results
-- At the moment this only works on queries that return rows with the same entries as one another
-- if you try to compare uneven numbers of columns or columns of different types it will generate an error
CREATE OR REPLACE FUNCTION results_match(left_query text, right_query text) RETURNS boolean AS $$
DECLARE
    left_cursor REFCURSOR;
    left_row RECORD;

    right_cursor REFCURSOR;
    right_row RECORD;
BEGIN
    OPEN left_cursor FOR EXECUTE left_query;
    OPEN right_cursor FOR EXECUTE right_query;
    LOOP
        FETCH NEXT FROM left_cursor INTO left_row;
        FETCH NEXT FROM right_cursor INTO right_row;
        IF left_row != right_row THEN
            RETURN false;
        ELSEIF left_row IS NULL AND right_row IS NULL THEN
            RETURN true;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
