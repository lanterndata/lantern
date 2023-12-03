---------------------------------------------------------------------
-- Test Database Crashes which were caused by operator rewriting logic
---------------------------------------------------------------------


-- This case were causing Segfault from
-- post_parse_analyze_hook_with_operator_check() -> ldb_get_operator_oids() -> ... LookupOperName() ... -> GetRealCmin()
BEGIN;
DROP EXTENSION IF EXISTS lantern CASCADE;
CREATE EXTENSION lantern;
\set ON_ERROR_STOP off
SELECT ARRAY[1,1] <-> ARRAY[1,1];
ROLLBACK;
