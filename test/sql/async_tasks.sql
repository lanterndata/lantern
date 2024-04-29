-- Note: We drop the Lantern extension and re-create it because Lantern only supports
-- async tasks if pg_cron is loaded before lantern

-- connect to postgres database to run this test as pg_cron can only be installed there
\c postgres
SET client_min_messages TO ERROR;
-- needed because we connected to a different DB which does not have the helper functions
\ir utils/common.sql
DROP EXTENSION IF EXISTS lantern CASCADE;
DROP EXTENSION IF EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION lantern;

SELECT lantern.async_task($$SELECT pg_sleep(0.1);$$, 'Lantern job name');
SELECT lantern.async_task($$SELECT pg_sleep(70);$$::text);
-- will fail since the task is not valid SQL
SELECT lantern.async_task($$SELECT pg_sleep(haha);$$, 'Lantern job name');
SELECT jobid, query, pg_cron_job_name, job_name, duration IS NOT NULL AS is_done, status, error_message FROM lantern.tasks;
SELECT pg_sleep(3);
SELECT jobid, query, pg_cron_job_name, job_name, duration IS NOT NULL AS is_done, status, error_message FROM lantern.tasks;
SELECT lantern.cancel_all_async_tasks();


-- test async tasks on index creation

DROP TABLE IF EXISTS small_world;
DROP TABLE IF EXISTS sift_base1k;
DROP TABLE IF EXISTS "sift_base1k_UpperCase";

\ir utils/sift1k_array.sql

-- add uppercase symbols to table name to make sure those are handlered properly in async_task function
ALTER TABLE sift_base1k RENAME TO "sift_base1k_UpperCase";

SELECT lantern.async_task($$CREATE INDEX idx ON "sift_base1k_UpperCase" USING lantern_hnsw (v) WITH (dim=128, M=6);$$, 'Indexing Job');
-- blocks DB deletions that is why it is disabled for now
-- SELECT lantern.async_task($$CREATE INDEX CONCURRENTLY idx_concurrent ON "sift_base1k_UpperCase" USING lantern_hnsw (v) WITH (dim=128, M=6);$$, 'Indexing Job');
SELECT pg_sleep(5);
SELECT * FROM ldb_get_indexes('sift_base1k_UpperCase');
SELECT _lantern_internal.validate_index('idx', false);

SELECT jobid, query, pg_cron_job_name, job_name, duration IS NOT NULL AS is_done, status, error_message FROM lantern.tasks;
-- NOTE: the test finishes but the async index creation may still be in progress

-- create non superuser and test the function
SET client_min_messages = WARNING;
-- suppress NOTICE:  role "test_user" does not exist, skipping
DROP USER IF EXISTS test_user_async;
SET client_min_messages = NOTICE;
CREATE USER test_user_async WITH PASSWORD 'test_password';
GRANT SELECT ON "sift_base1k_UpperCase" TO test_user_async;
GRANT SELECT ON sift_base1k_id_seq TO test_user_async;

SET ROLE test_user_async;

SELECT lantern.async_task($$SELECT 1$$, 'simple job');

SELECT lantern.async_task($$CREATE INDEX idx2 ON "sift_base1k_UpperCase" USING lantern_hnsw (v) WITH (dim=128, M=6);$$, 'Indexing Job');
-- this should fail since test_user does not have permission to drop the table
-- sql line for do not stop on error
SELECT lantern.async_task('DROP TABLE "sift_base1k_UpperCase";', 'Dropping Table Job');

-- lantern.tasks jobid is disitinct and independent from cron.jobid, even though they may often overlap
-- make sure everything works even when they are out of sync
SELECT nextval('lantern.tasks_jobid_seq');
SELECT lantern.async_task($$SELECT 42$$, 'Life');

SELECT pg_sleep(4);
SELECT jobid, query, pg_cron_job_name, job_name, duration IS NOT NULL AS is_done, status, error_message FROM lantern.tasks ORDER BY jobid;
