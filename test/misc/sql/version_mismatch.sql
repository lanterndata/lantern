-- Validate that creating an index with mismatched versions fails
CREATE INDEX ON sift_base1k USING hnsw (v) WITH (dim=128, M=4);
