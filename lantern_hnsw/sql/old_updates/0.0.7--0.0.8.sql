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

-- Storage format changelog:
-- Initial mechanism of this changelog
-- Change index header magic to check the mechanism works
SELECT _lantern_internal.reindex_lantern_indexes();
