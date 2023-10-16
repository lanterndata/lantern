CREATE UNLOGGED TABLE IF NOT EXISTS sift_base1k_unlogged (
    id SERIAL,
    v REAL[]
);

COPY sift_base1k_unlogged (v) FROM '/tmp/lantern/vector_datasets/sift_base1k_arrays.csv' WITH csv;
