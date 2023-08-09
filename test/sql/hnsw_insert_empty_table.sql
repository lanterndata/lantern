CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

CREATE TABLE small_world (
    id varchar(3),
    vector vector(3)
);

CREATE INDEX ON small_world USING hnsw (vector);

INSERT INTO small_world (id, vector) VALUES ('xxx', '[0,0,0]');
