-- Validate that creating an index with mismatched versions fails

-- There is a warning that prints specific versions in the mismatch
-- We do not want that for regression tests
SET client_min_messages=ERROR;
CREATE INDEX ON sift_base1k USING lantern_hnsw (v) WITH (dim=128, M=4);
