-- SET work_mem='10MB';
BEGIN;
INSERT INTO sift_base10k (id, v) VALUES 
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128)),
    (nextval('serial'), random_array(128, 0, 128));
COMMIT;
