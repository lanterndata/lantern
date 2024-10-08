\ir utils/small_world_array.sql

SELECT * FROM small_world;
CREATE INDEX ON small_world USING lantern_hnsw (v) WITH (M=128) WHERE b = FALSE;
set enable_seqscan=FALSE;
SELECT * FROM small_world WHERE b = FALSE order by v <-> '{1,0,0}' LIMIT 3;
DELETE FROM small_world WHERE v <> '{1,0,0}';
SELECT * FROM small_world WHERE b = FALSE order by v <-> '{1,0,0}' LIMIT 3;
VACUUM small_world;
INSERT INTO small_world (id, b, v) VALUES (3, TRUE, '{4,4,4}'), (3, TRUE, '{4,4,4}'), (4, TRUE, '{4,4,4}'),
                                          (5, TRUE, '{5,5,5}'),
                                          (6, TRUE, '{6,6,6}'),
                                          (7, TRUE, '{7,7,7}');
SELECT * FROM small_world WHERE b = FALSE order by v <-> '{1,0,0}' LIMIT 3;
