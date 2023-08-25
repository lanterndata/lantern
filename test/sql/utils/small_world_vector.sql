CREATE TABLE small_world (
    id SERIAL,
    b BOOLEAN,
    v VECTOR(2)
);

INSERT INTO small_world (b, v) VALUES
    (TRUE, '[1,1]'),
    (TRUE, '[2,2]'),
    (TRUE, '[3,3]'),
    (TRUE, '[4,4]'),
    (TRUE, '[1,9]');