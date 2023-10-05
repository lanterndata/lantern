SELECT v AS v1111 FROM sift_base10k WHERE id = 1111 \gset
SELECT v AS v2222 FROM sift_base10k WHERE id = 2222 \gset
SELECT v AS v3333 FROM sift_base10k WHERE id = 3333 \gset
SELECT v AS v4444 FROM sift_base10k WHERE id = 4444 \gset
SELECT id FROM sift_base10k ORDER BY  v <-> :'v1111'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v2222'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v3333'  ASC LIMIT 1;
SELECT id FROM sift_base10k ORDER BY  v <-> :'v4444'  ASC LIMIT 1;
