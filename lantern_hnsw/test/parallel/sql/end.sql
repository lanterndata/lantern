-- This file contains invariants to be checked after the parallel tests have run
SELECT COUNT(*) FROM sift_base10k;
SELECT * from sift_base10k WHERE id=4444;
