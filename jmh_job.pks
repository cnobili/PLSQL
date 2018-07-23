create or replace package jmh_job as
-- --------------------------------------------------------------------------
--
-- module name: jmh_job
--
-- description: Encapsulates routines for managaing jobs created on the fly
--   for concurrent processing.
--
-- Uses the following packages:
--   jmh_log_pkg   - logging
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:    03-05-2012
-- author:  Craig Nobili
-- desc:    original
--
-- --------------------------------------------------------------------------

--
-- Global Constants
--

SUCCESS_MSG_PREFIX constant varchar2(7) := 'SUCCESS';
FAILURE_MSG_PREFIX constant varchar2(7) := 'FAILURE';
--
-- Types
--

type job_rec_t is record
(
  job_name     varchar2(30)
, plsql_block  varchar2(4000)
, ignore_error boolean
);

type job_tab_t is table of job_rec_t index by pls_integer;

type alert_tab_t is table of number index by varchar2(30);

-- ---------------------------------------------------------------------------
--
-- procedure: execute_jobs
--
-- purpose: Submits jobs to dbms_scheduler.
--
-- ---------------------------------------------------------------------------
procedure execute_jobs(p_jobs in job_tab_t);

-- ---------------------------------------------------------------------------
--
-- procedure: signal_completion
--
-- purpose: Send an alert to signal job completion.
--
-- ---------------------------------------------------------------------------
procedure signal_completion(p_job_name in varchar2, p_msg in varchar2);

-- ---------------------------------------------------------------------------
--
-- procedure: wait_for_jobs
--
-- purpose: Waits for the completion of all parallel jobs in passed in array.
--
-- ---------------------------------------------------------------------------
procedure wait_for_jobs(p_jobs in job_tab_t);

end jmh_job;
/ 
show errors
