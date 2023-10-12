CREATE TABLE IF NOT EXISTS sift_base10k (
     id SERIAL PRIMARY KEY,
     v REAL[128]
);
\copy sift_base10k (v) FROM '/tmp/lantern/vector_datasets/siftsmall_base_arrays.csv' with csv;