SELECT
   b.id, 
   ARRAY(SELECT id FROM sift_base1k b2 ORDER BY l2sq_dist(b.v, b2.v) LIMIT 10)::INT[] as indices
INTO sift_truth1k
FROM sift_base1k b
WHERE id IN (SELECT id FROM sift_base1k ORDER BY id LIMIT 100);

SELECT id, v INTO sift_query1k FROM sift_base1k ORDER BY id LIMIT 100;
