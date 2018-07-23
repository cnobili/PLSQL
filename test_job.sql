set serveroutput on
set time on
set timing on

declare
  l_jobs jmh_job.job_tab_t;
begin

  l_jobs(1).job_name := dbms_scheduler.generate_job_name(prefix => user);
  l_jobs(1).plsql_block := 'begin test_proc(' || '''' || l_jobs(1).job_name || '''' || ',6); end;';
  l_jobs(1).ignore_error := false;
  
  l_jobs(2).job_name := dbms_scheduler.generate_job_name(prefix => user);
  l_jobs(2).plsql_block := 'begin test_proc(' || '''' || l_jobs(2).job_name || '''' || ',5); end;';
  l_jobs(2).ignore_error := false;

  jmh_job.execute_jobs(l_jobs);
  jmh_job.wait_for_jobs(l_jobs);
  
end;
/
