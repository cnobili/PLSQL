create or replace procedure test_proc(p_job_name in varchar, p_secs in number)
is
begin

  dbms_lock.sleep(p_secs);

  if (p_secs = 5) then
    raise_application_error(-20000, 'test error handling');
  end if;

  jmh_job.signal_completion(p_job_name, jmh_job.SUCCESS_MSG_PREFIX || ' test message');
  
exception

  when others then
  
    jmh_job.signal_completion(p_job_name, jmh_job.FAILURE_MSG_PREFIX || ' test message');
    
end test_proc;
/
