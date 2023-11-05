------------------------------
-- Test HNSW failure points --
------------------------------

CREATE TABLE small_world (
    id SERIAL PRIMARY KEY,
    v REAL[2]
);
CREATE INDEX ON small_world USING hnsw (v) WITH (dim=3);

-- let's insert HNSW_BLOCKMAP_BLOCKS_PER_PAGE (2000) record to fill the first blockmap page

do $$
BEGIN
    FOR i IN 1..2000 LOOP
        INSERT INTO small_world (v) VALUES (array_replace(ARRAY[0,0,-1], -1, i));
    END LOOP;
END $$;

-- everything is fine, the index is valid
SELECT _lantern_internal.validate_index('small_world_v_idx', false);

-- now let's crash after a buffer for a blockmap is allocated during insert,
-- but it hasn't been recorded yet
SELECT _lantern_internal.failure_point_enable('ContinueBlockMapGroupInitialization', 'just_after_extending_the_index_relation');

-- here is the insert where the crash happens
\set ON_ERROR_STOP off
INSERT INTO small_world (v) VALUES ('{2,2,2}');
\set ON_ERROR_STOP on

-- now we see that the index has an extra free page, so the index validation fails
SELECT _lantern_internal.validate_index('small_world_v_idx', false);
