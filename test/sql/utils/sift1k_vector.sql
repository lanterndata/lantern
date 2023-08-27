CREATE TABLE IF NOT EXISTS sift_base1k (
    id SERIAL,
    v VECTOR(128)
);

COPY sift_base1k (v) FROM '/tmp/lanterndb/vector_datasets/sift_base1k.csv' WITH CSV;
