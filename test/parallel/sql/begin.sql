\ir utils/sift10k_array.sql

CREATE INDEX ON sift_base10k  USING HNSW (v) WITH (M=5, ef=20, ef_construction=20);
