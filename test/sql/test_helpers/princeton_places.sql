CREATE TABLE princeton_places (
    name text,
    street text,
    long float,
    lat float,
    v vector(2) 
);
\copy pton_area(name, street, long, lat) FROM '/tmp/lanterndb/vector_datasets/sift_base1k.csv' DELIMITER E',';
