-- This file handles initializing the database before parallel tests are run
ALTER SYSTEM SET work_mem = '128MB';
SELECT pg_reload_conf();

\ir utils/sift10k_array.sql
\ir utils/random_array.sql

CREATE SEQUENCE serial START 10001;
SELECT v as v444 from sift_base10k wHERE id = 444 LIMIT 1 \gset
SELECT id, ROUND((v <-> :'v444')::numeric, 2) FROM sift_base10k ORDER BY v <-> :'v444' LIMIT 6;
CREATE INDEX to_be_reindexed ON sift_base10k  USING lantern_hnsw (v) WITH (M=7, ef=20, ef_construction=20);
SELECT id, ROUND((v <-> :'v444')::numeric, 2) FROM sift_base10k ORDER BY v <-> :'v444' LIMIT 6;
REINDEX INDEX CONCURRENTLY to_be_reindexed;
