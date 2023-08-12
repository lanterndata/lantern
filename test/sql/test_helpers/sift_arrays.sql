CREATE TABLE IF NOT EXISTS sift_base1k_arr (
    id SERIAL PRIMARY KEY,
    v real[]);
COPY sift_base1k_arr (v) FROM '/tmp/lanterndb/vector_datasets/sift_base1k_arrays.csv' with csv;
