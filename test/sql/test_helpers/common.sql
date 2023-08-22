--test helper functions that should exist in all test runs live here
-- there is no need to explicitly include this file in other tests as the test runner will
-- run this before running the actual test
CREATE EXTENSION pageinspect;

--todo:: add a columns to this function which returning number of used DB pages and total index size
\set ON_ERROR_STOP on
CREATE OR REPLACE FUNCTION ldb_get_indexes (tblname text)
    RETURNS TABLE (
        indexname name,
        size text,
        indexdef text
    )
    AS $BODY$
BEGIN
    RETURN QUERY
    SELECT
        pg_indexes.indexname,
        pg_size_pretty(pg_relation_size(pg_indexes.indexname::regclass)) AS size,
        pg_indexes.indexdef
    FROM
        pg_indexes
    WHERE
        tablename = tblname;
END;
$BODY$
LANGUAGE plpgsql;

