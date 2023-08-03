-- creates a simple vector table
CREATE TABLE small_world (
    id varchar(3),
    vector real[]
);

INSERT INTO small_world (id, vector) VALUES 
('000', '{0,0,0}'),
('001', '{0,0,1}'),
('010', '{0,1,0}'),
('011', '{0,1,1}'),
('100', '{1,0,0}'),
('101', '{1,0,1}'),
('110', '{1,1,0}'),
('111', '{1,1,1}');
