
\ir utils/sift1k_array.sql

-- 6 closest vectors to the vector with ID 444
SELECT v as v444 from sift_base1k wHERE id = 444 LIMIT 1 \gset
SELECT id, ROUND((v <-> :'v444')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v444' LIMIT 6;
CREATE INDEX to_be_reindexed ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=8);
SELECT * FROM ldb_get_indexes('sift_base1k');
SELECT _lantern_internal.validate_index('to_be_reindexed', false);
REINDEX INDEX CONCURRENTLY to_be_reindexed;
SELECT _lantern_internal.validate_index('to_be_reindexed', false);
SELECT * FROM ldb_get_indexes('sift_base1k');
set enable_seqscan=FALSE;
-- 6 closest vectors to the vector with ID 444. note all the duplicate results because of bad handling of REINDEX
SELECT id, ROUND((v <-> :'v444')::numeric, 2) FROM sift_base1k ORDER BY v <-> :'v444' LIMIT 6;
