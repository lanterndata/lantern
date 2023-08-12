CREATE EXTENSION IF NOT EXISTS lanterndb;

\set ON_ERROR_STOP off
DROP TABLE IF EXISTS small_world;
CREATE TABLE small_world (
    id varchar(3),
    vector real[]
);

INSERT INTO small_world (id, vector) VALUES ('001', '{0,0,0,0}');

CREATE INDEX ON small_world USING hnsw (vector) WITH (dims=4);

-- should throw dimension error
INSERT INTO small_world (id, vector) VALUES ('002', '{0,0,0}');

-- should insert successfully
INSERT INTO small_world (id, vector) VALUES ('003', '{0,0,0,0}');
