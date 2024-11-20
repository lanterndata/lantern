DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'lantern' AND table_name = 'tasks') THEN
        ALTER TABLE lantern.tasks ADD COLUMN pg_cron_jobid bigint DEFAULT NULL;
    END IF;
END $$;
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

  -- Update pg_cron_jobid on lantern.tasks table before the job is unscheduled
  -- This is necessary because under some circumstances jobs continue changing status even after they no longer
  -- appear in cron.job. The easiest way to trigger this case is to schedule a multi-statement job
  -- where the second statement causes a failure, e.g. async_task('select 1; select haha;')
  UPDATE lantern.tasks t SET
      pg_cron_jobid = c.jobid
  FROM cron.job c
  LEFT JOIN cron.job_run_details run
  ON c.jobid = run.jobid
  WHERE
      t.pg_cron_job_name = c.jobname AND
      c.jobid = NEW.jobid
  -- using returning as a trick to run the unschedule function as a side effect
  -- Note: have to unschedule by jobid because of pg_cron#320 https://github.com/citusdata/pg_cron/issues/320
  -- Note2: unscheduling happens here since the update below may run multiple times for the same async task
  --        and unscheduling same job multiple times is not allowed
  --        At least experimentally so far, this update runs once per async task
  RETURNING cron.unschedule(NEW.jobid) INTO res;

  -- Get the job name from the jobid
  -- Call the job finalizer if corresponding job exists BOTH in lantern async tasks AND
  -- active cron jobs
  UPDATE lantern.tasks t SET
      (duration, status, error_message) = (run.end_time - t.started_at, NEW.status,
      CASE WHEN NEW.status = 'failed' THEN return_message ELSE NULL END)
  FROM cron.job_run_details run
  WHERE
      t.pg_cron_jobid = NEW.jobid
      AND t.pg_cron_jobid = run.jobid;

  RETURN NEW;

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Lantern Async tasks: Unknown job failure in % % %', NEW, SQLERRM, SQLSTATE;
        PERFORM cron.unschedule(NEW.jobid);
        RETURN NEW;
END
$$ LANGUAGE plpgsql;

