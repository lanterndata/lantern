-----------------------------------------------------------
-- Test HNSW blockmap creation after failures in the middle
-----------------------------------------------------------

-- create a table and fill the first blockmap group
CREATE FUNCTION prepare() RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS small_world;
    CREATE TABLE small_world (id SERIAL PRIMARY KEY, v real[]);
    CREATE INDEX ON small_world USING hnsw (v) WITH (dim=3);
    -- let's insert HNSW_BLOCKMAP_BLOCKS_PER_PAGE (2000) record to fill the first blockmap page
    BEGIN
        FOR i IN 1..2000 LOOP
            INSERT INTO small_world (v) VALUES (array_replace(ARRAY[0,0,-1], -1, i));
        END LOOP;
    END;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- enable a failure point and run an insert to trigger new blockmap group initialization
CREATE FUNCTION trigger_failure(func TEXT, name TEXT, dont_trigger_first_nr INTEGER) RETURNS VOID AS $$
BEGIN
    PERFORM _lantern_internal.failure_point_enable(func, name, dont_trigger_first_nr);
    BEGIN
        INSERT INTO small_world (v) VALUES ('{2,2,2}');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Exception caught: %', SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql VOLATILE;

DO $$
DECLARE
    failure_points TEXT[][]:= '{
        {"At this point no changes have been made to the index header, so validate_index() should succeed.",
        "ContinueBlockMapGroupInitialization",
         "just_before_writing_the_intent_to_init", 0, false},
        {"It''s not know if the header will be updated before WAL flush, so it''s not clear if validate_index() will succeed.",
        "UpdateHeaderBlockMapGroupDesc", "just_before_wal_flush", 0, NULL},
        {"After updating the header validate_index() must fail, because the header has 0 initialized blockmaps for the last blockmap group.",
        "UpdateHeaderBlockMapGroupDesc", "just_after_wal_flush", 0, true},
        {"The same reason to fail as before.",
        "ContinueBlockMapGroupInitialization",
         "just_after_writing_the_intent_to_init", 0, true},
        {"The validate_index() will fail at the same place, because the check for unused blocks is after the check for the number of initialize blockmap blocks.",
        "ContinueBlockMapGroupInitialization",
         "just_after_extending_the_index_relation", 0, true},
        {"Here blockmap blocks are initialized, but the header may or may not be updated to reflect this.",
        "ContinueBlockMapGroupInitialization",
         "just_before_updating_header_at_the_end", 0, NULL},
        {"It''s not know if the header will be updated for the second (last) time before WAL flush, so it''s not clear if validate_index() will succeed.",
        "UpdateHeaderBlockMapGroupDesc", "just_before_wal_flush", 1, NULL},
        {"After updating the header validate_index() must succeed, because the blockmap group is fully initialized and the header is updated.",
        "UpdateHeaderBlockMapGroupDesc", "just_after_wal_flush", 1, false},
        {"Blockmaps are initilized, the header is updated. validate_index() should not fail.",
        "ContinueBlockMapGroupInitialization",
         "just_after_updating_header_at_the_end", 0, false}
                    }';
    fp TEXT[];
BEGIN
    FOREACH fp SLICE 1 IN ARRAY failure_points
    LOOP
        PERFORM prepare();
        PERFORM _lantern_internal.validate_index('small_world_v_idx', false);
        PERFORM trigger_failure(fp[2], fp[3], fp[4]::integer);
        RAISE INFO '%', fp[1];
        -- If it's not known if the data is written to WAL (and the validate_index()
        -- may find issues) or if we know that validate_index() will definitely
        -- find an issue then catch the exception
        IF fp[5]::boolean IS NULL OR fp[5]::boolean THEN
            BEGIN
                PERFORM _lantern_internal.validate_index('small_world_v_idx', false);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Exception caught: %', SQLERRM;
            END;
        ELSE
            PERFORM _lantern_internal.validate_index('small_world_v_idx', false);
        END IF;
    END LOOP;
    RAISE INFO 'The test is complete.';
END $$;