SET work_mem='10MB';
BEGIN;
INSERT INTO sift_base10k (id, v) VALUES 
    (10002, random_array(128, 0, 128)),
COMMIT;
