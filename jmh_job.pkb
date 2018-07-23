create or replace package body jmh_job as
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
-- Private Methods
--

-- ---------------------------------------------------------------------------
--
-- function: get_array_index
--
-- purpose: Returns the index for value in the jobs associative array.
--
-- ---------------------------------------------------------------------------
function get_array_index(p_array in job_tab_t, p_value in varchar2)
return number
is
  l_index pls_integer := null;
begin

  for i in p_array.first .. p_array.last loop
  
    if (p_array(i).job_name = p_value) then
      l_index := i;
      exit;
    end if;
    
  end loop;
  
  return l_index;
  
end get_array_index;

-- ---------------------------------------------------------------------------
--
-- procedure: execute_jobs
--
-- purpose: Submits jobs to dbms_scheduler.
--
-- ---------------------------------------------------------------------------
procedure execute_jobs(p_jobs in job_tab_t)
is
begin
  
  for i in p_jobs.first .. p_jobs.last loop
  
    dbms_scheduler.create_job
    (
      job_name        => p_jobs(i).job_name
    , job_type        => 'plsql_block'    
    , job_action      => p_jobs(i).plsql_block
    , start_date      => systimestamp
    , repeat_interval=>  null
    , end_date        => null
    , enabled         => false
    , comments        => 'Run ' || p_jobs(i).plsql_block
    , auto_drop       => true
    );

    dbms_scheduler.enable(name => p_jobs(i).job_name);    
    dbms_scheduler.run_job(job_name => p_jobs(i).job_name, use_current_session => FALSE);
    
    jmh_log_pkg.wlog('execute_jobs: submitted job = ' || p_jobs(i).job_name, jmh_log_pkg.LOG_NORM); 
    
  end loop;
  
exception

 when others then
   rollback;
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in execute_jobs'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
  
end execute_jobs;

-- ---------------------------------------------------------------------------
--
-- procedure: signal_completion
--
-- purpose: Send an alert to signal job completion.
--
-- ---------------------------------------------------------------------------
procedure signal_completion(p_job_name in varchar2, p_msg in varchar2)
is
begin

  dbms_alert.signal(p_job_name, p_msg);
  commit;
  
end signal_completion;

-- ---------------------------------------------------------------------------
--
-- procedure: wait_for_jobs
--
-- purpose: Waits for the completion of all parallel jobs in passed in array.
--
-- ---------------------------------------------------------------------------
procedure wait_for_jobs(p_jobs in job_tab_t)
is
  l_alert_tab alert_tab_t;
  l_alert     varchar2(30);
  l_job_name  varchar2(30);
  l_message   varchar2(4000);
  l_status    pls_integer;
begin

  if p_jobs.count = 0 then
    return;
  end if;

  --
  -- Build array of alerts.
  --
  for i in p_jobs.first .. p_jobs.last loop
    l_alert_tab(p_jobs(i).job_name) := i;
  end loop;

  --
  -- Register for alerts.
  --
  for i in p_jobs.first .. p_jobs.last loop
    dbms_alert.register(p_jobs(i).job_name);
  end loop;

  --
  -- Wait until all of the alerts are received.
  --
  while (l_alert_tab.count > 0) loop
    dbms_alert.waitany(l_alert, l_message, l_status);
    
    if l_status = 0 then
      dbms_alert.remove(l_alert);
      l_alert_tab.delete(l_alert);
      jmh_log_pkg.wlog('jmh_job_pkg.wait_for_jobs - Received alert: ' || l_alert || ' with message: ' || l_message , jmh_log_pkg.LOG_NORM);
      
      -- Check if return message is successful and whether we want to raise an error
      if l_message like FAILURE_MSG_PREFIX || '%' and
          p_jobs(get_array_index(p_jobs, l_alert)).ignore_error = false
      then   
        raise_application_error(-20000, l_message);   
      end if;
      
    end if;
    
  end loop;
 
end wait_for_jobs;

end jmh_job;
/ 
show errors

