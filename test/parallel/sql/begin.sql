CREATE TABLE small_world (
    id varchar(3),
    b boolean,
    v real[3]
);

CREATE INDEX ON small_world USING HNSW (v) WITH (dim=3, M=5, ef=20, ef_construction=20);
