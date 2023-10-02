\ir ../utils/random_array.sql

INSERT INTO small_world (id, b, v) VALUES
    ('000', TRUE,  random_int_array(3,1,10)),
    ('001', TRUE,  random_int_array(3,1,10)),
    ('010', FALSE, random_int_array(3,1,10)),
    ('011', TRUE,  random_int_array(3,1,10)),
    ('100', FALSE, random_int_array(3,1,10)),
    ('101', FALSE, random_int_array(3,1,10)),
    ('110', FALSE, random_int_array(3,1,10)),
    ('111', TRUE,  random_int_array(3,1,10));
