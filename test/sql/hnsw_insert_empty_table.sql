CREATE TABLE small_world (
    id varchar(3),
    vector real[]
);

CREATE INDEX ON small_world USING hnsw (vector);

SELECT * FROM small_world WHERE 1=1;

INSERT INTO small_world (id, vector) VALUES ('xxx', '{0,0,0}');

-- make sure NULL inserts into the index are handled correctly
INSERT INTO small_world (id, vector) VALUES ('xxx', NULL);
\set ON_ERROR_STOP off
INSERT INTO small_world (id, vector) VALUES ('xxx', '{1,1,1,1}');
\set ON_ERROR_STOP on
