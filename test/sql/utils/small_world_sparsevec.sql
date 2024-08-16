CREATE TABLE small_world (
    id VARCHAR(3),
    b BOOLEAN,
    v VECTOR(3),
    s SPARSEVEC(3)
);

INSERT INTO small_world (id, b, v, s) VALUES
    ('000', TRUE,  '[0,0,0]', '{}/3'),
    ('001', TRUE,  '[0,0,1]', '{3:1}/3'),
    ('010', FALSE, '[0,1,0]' , '{2:1}/3'),
    ('011', TRUE,  '[0,1,1]', '{2:1,3:1}/3'),
    ('100', FALSE, '[1,0,0]', '{1:1}/3'),
    ('101', FALSE, '[1,0,1]', '{1:1,3:1}/3'),
    ('110', FALSE, '[1,1,0]', '{1:1,2:1}/3'),
    ('111', TRUE,  '[1,1,1]', '{1:1,2:1,3:1}/3');