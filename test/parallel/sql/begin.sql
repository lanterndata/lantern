-- This file handles initializing the database before parallel tests are run
\ir utils/sift10k_array.sql
\ir utils/random_array.sql

CREATE SEQUENCE serial START 10001;
CREATE INDEX ON sift_base10k  USING lantern_hnsw (v) WITH (M=5, ef=20, ef_construction=20);
