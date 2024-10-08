SELECT v AS v1111 FROM sift_base10k WHERE id = 1111 \gset
SELECT v AS v2222 FROM sift_base10k WHERE id = 2222 \gset
SELECT v AS v3333 FROM sift_base10k WHERE id = 3333 \gset
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
SELECT v as v444 from sift_base10k wHERE id = 444 LIMIT 1 \gset
-- Make sure that our index queries will actually run against the index
EXPLAIN (COSTS false) SELECT id FROM sift_base10k ORDER BY  v <-> :'v1111'  ASC LIMIT 1;


-- Do the queries

-- Make sure the new delete hook works to fix concurrent builds in 0.2.5->0.2.6
-- The delete hook exists in 0.2.6, but if this index was built on the previous version of the binary,
-- reindex, this query will return duplicates.
-- This checks that the two binaries are compatible and queries can run
-- after an upgrade with new binaries and old indexes
SELECT id, ROUND((v <-> :'v444')::numeric, 2) FROM sift_base10k ORDER BY v <-> :'v444' LIMIT 6;

SELECT id FROM sift_base10k ORDER BY  v <-> :'v1111'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v2222'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v3333'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v4444'  ASC LIMIT 1;
