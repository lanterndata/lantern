CREATE TABLE IF NOT EXISTS views_vec10k (
     id INTEGER,
     views INTEGER,
     vec REAL[]
);

\copy views_vec10k (id, views, vec) FROM '/tmp/lantern/vector_datasets/views_vec10k.csv' WITH (FORMAT CSV, HEADER);