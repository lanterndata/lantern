-- Database updates
-- Must be run in update scripts every time index storage format changes and a finer-grained update
-- method is not shipped for the format change
CREATE OR REPLACE FUNCTION _lantern_internal.reindex_lantern_indexes()
RETURNS VOID AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT indexname, indexdef FROM pg_indexes
            WHERE indexdef ILIKE '%USING hnsw%' OR indexdef ILIKE '%USING lantern_hnsw%'
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
