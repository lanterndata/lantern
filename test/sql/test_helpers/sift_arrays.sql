
CREATE TABLE IF NOT EXISTS sift_base1k (
    id SERIAL PRIMARY KEY,
    v real[]);
COPY sift_base1k (v) FROM '/tmp/lanterndb/vector_datasets/sift_base1k.csv' with csv;
