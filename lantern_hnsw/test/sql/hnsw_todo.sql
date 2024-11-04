-- THIS IS TODO TEST FILE
-- THIS TESTS WILL NOT PASS CURRENTLY BUT SHOULD BE FIXED LATER

CREATE TABLE small_world_l2 (
    id varchar(3),
    vector real[],
    vector_int integer[]
);

INSERT INTO small_world_l2 (id, vector) VALUES 
    ('000', '{0,0,0}'),
    ('001', '{0,0,1}'),
    ('010', '{0,1,0}'),
    ('011', '{0,1,1}'),
    ('100', '{1,0,0}'),
    ('101', '{1,0,1}'),
    ('110', '{1,1,0}'),
    ('111', '{1,1,1}');

SET enable_seqscan=FALSE;
\set ON_ERROR_STOP off

CREATE INDEX ON small_world_l2 USING lantern_hnsw (vector dist_l2sq_ops);
SELECT _lantern_internal.validate_index('small_world_l2_vector_idx', false);

-- this should be supported
CREATE INDEX ON small_world_l2 USING lantern_hnsw (vector_int dist_l2sq_int_ops);
SELECT _lantern_internal.validate_index('small_world_l2_vector_int_idx', false);
