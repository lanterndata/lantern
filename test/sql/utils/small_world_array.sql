CREATE TABLE small_world (
    id VARCHAR(3),
    b BOOLEAN,
    v REAL[3]
);

INSERT INTO small_world (id, b, v) VALUES
    ('000', TRUE,  '{0,0,0}'),
    ('001', TRUE,  '{0,0,1}'),
    ('010', FALSE, '{0,1,0}'),
    ('011', TRUE,  '{0,1,1}'),
    ('100', FALSE, '{1,0,0}'),
    ('101', FALSE, '{1,0,1}'),
    ('110', FALSE, '{1,1,0}'),
    ('111', TRUE,  '{1,1,1}');