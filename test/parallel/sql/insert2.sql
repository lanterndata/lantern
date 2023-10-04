\ir utils/random_array.sql
DO $$
BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO sift_base10k (v) VALUES (random_array(128, 0, 128));
    END LOOP;
END; $$
