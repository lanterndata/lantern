  -- Asynchronous task scheduling BEGIN
CREATE OR REPLACE FUNCTION _lantern_internal.maybe_setup_lantern_tasks() RETURNS VOID AS
$async_tasks_related$
BEGIN
  IF NOT (SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cron'))
  THEN
    RAISE NOTICE 'pg_cron extension not found. Skipping lantern async task setup';
    RETURN;
  END IF;

  CREATE TABLE lantern.tasks (
	  jobid bigserial primary key,
	  query text not null,
	  pg_cron_job_name text default null, -- initially null, because it will be ready after job insertion
	  job_name text default null,
	  username text not null default current_user,
    started_at timestamp with time zone not null default now(),
    duration interval,
    status text,
    error_message text
  );

  GRANT SELECT ON lantern.tasks TO public;
  ALTER TABLE lantern.tasks ENABLE ROW LEVEL SECURITY;
  CREATE POLICY lantern_tasks_policy ON lantern.tasks USING (username OPERATOR(pg_catalog.=) current_user);

  -- create a trigger and added to cron.job_run_details
  CREATE OR REPLACE FUNCTION _lantern_internal.async_task_finalizer_trigger() RETURNS TRIGGER AS $$
  DECLARE
    res RECORD;
  BEGIN
    -- if NEW.status is one of "starting", "running", "sending, "connecting", return
    IF NEW.status IN ('starting', 'running', 'sending', 'connecting') THEN
      RETURN NEW;
    END IF;

    IF NEW.status NOT IN ('succeeded', 'failed') THEN
      RAISE WARNING 'Lantern Async tasks: Unexpected status %', NEW.status;
    END IF;

    -- Get the job name from the jobid
    -- Call the job finalizer if corresponding job exists BOTH in lantern async tasks AND
    -- active cron jobs
  UPDATE lantern.tasks t SET
        (duration, status, error_message, pg_cron_job_name) = (run.end_time - t.started_at, NEW.status,
        CASE WHEN NEW.status = 'failed' THEN return_message ELSE NULL END,
        c.jobname )
    FROM cron.job c
    LEFT JOIN cron.job_run_details run
    ON c.jobid = run.jobid
    WHERE
       t.pg_cron_job_name = c.jobname AND
       c.jobid = NEW.jobid
    -- using returning as a trick to run the unschedule function as a side effect
    RETURNING cron.unschedule(t.pg_cron_job_name) INTO res;

    RETURN NEW;
  END
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER status_change_trigger
  AFTER UPDATE OF status
  ON cron.job_run_details
  FOR EACH ROW
  WHEN (OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION _lantern_internal.async_task_finalizer_trigger();


  CREATE OR REPLACE FUNCTION lantern.async_task(query text, job_name text) RETURNS INTEGER AS $$
  DECLARE
    _job_id integer;
    _pg_cron_job_name text;
    start_time timestamptz;
  BEGIN
    start_time := clock_timestamp();
    job_name := COALESCE(job_name, '');

    INSERT INTO lantern.tasks (query, job_name, started_at)
    VALUES (query, job_name, start_time) RETURNING jobid INTO _job_id;

    _pg_cron_job_name := 'async_task_' || _job_id;

    UPDATE lantern.tasks t SET
      pg_cron_job_name = _pg_cron_job_name
    WHERE jobid = _job_id;

    -- Schedule the job. Note: The original query execution is moved to the finalizer.
    PERFORM cron.schedule(_pg_cron_job_name, '1 seconds', query);
    RAISE NOTICE 'Job scheduled with pg_cron name: %', quote_literal(_pg_cron_job_name);
    RETURN _job_id;
  END
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION lantern.async_task(query text) RETURNS INTEGER AS $$
  BEGIN
    RETURN lantern.async_task(query, NULL);
  END
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION lantern.cancel_all_async_tasks() RETURNS void AS $$
  BEGIN
    PERFORM cron.unschedule(pg_cron_job_name) FROM lantern.tasks
      WHERE duration IS NULL;

    UPDATE lantern.tasks t SET
        duration = clock_timestamp() - t.started_at,
        status = 'canceled',
        error_message = COALESCE(error_message, '') || 'Canceled by user'
      WHERE duration is NULL;
  END
  $$ LANGUAGE plpgsql;
END
$async_tasks_related$ LANGUAGE plpgsql;

SELECT _lantern_internal.maybe_setup_lantern_tasks();
DROP FUNCTION _lantern_internal.maybe_setup_lantern_tasks();

-- Asynchronous task scheduling BEGIN
