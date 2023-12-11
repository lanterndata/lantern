-- these go for good.

DROP OPERATOR CLASS IF EXISTS dist_vec_hamming_ops USING hnsw CASCADE;
DROP FUNCTION IF EXISTS cos_dist(vector, vector);
DROP OPERATOR <+>(vector, vector) CASCADE