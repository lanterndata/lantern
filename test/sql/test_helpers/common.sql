-- test helper functions that should exist in all test runs live here
-- there is no need to explicitly include this file in other tests as the test runner will
-- run this before running the actual test

CREATE EXTENSION pageinspect;

\set ON_ERROR_STOP on

-- retrieves details for all indices associated with a given table, similar to \di+
-- the output of \di+ is not consistent across postgres versions
-- todo:: add a columns to this function which returning number of used DB pages and total index size
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
