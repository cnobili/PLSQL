-- --------------------------------------------------------------------------
--
-- name:  jmh_process_log.sql
--
-- description: Creates jmh_process_log table
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:   07-20-2010
-- author:  Craig Nobili
-- desc: original
--
-- --------------------------------------------------------------------------
--
create table jmh_process_log
(
  process_log_seq number
, db_name         varchar2(255)  
, instance_name   varchar2(30)  
, client_host     varchar2(255)
, os_user         varchar2(255)
, sessionid       number
, username        varchar2(30)
, module_owner    varchar2(30)
, module_type     varchar2(30)
, module_name     varchar2(128)
, module_lineno   number
, run_dt          date
, log_dt          timestamp
, log_msg         varchar2(4000)
, err_code        number
, err_msg         varchar2(4000)
, constraint process_log_detail_pk
  primary key (process_log_seq)  
);

comment on table jmh_process_log is 'Logging table.';
comment on column jmh_process_log.process_log_seq is 'sequence generated primary key';
comment on column jmh_process_log.db_name is 'database name';
comment on column jmh_process_log.instance_name is 'database instance name';
comment on column jmh_process_log.client_host is 'machine name of client process';
comment on column jmh_process_log.os_user is 'os user of process if local to machine';
comment on column jmh_process_log.sessionid is 'session process id';
comment on column jmh_process_log.username is 'username of logged on process';
comment on column jmh_process_log.module_owner is 'owner of module that logging';
comment on column jmh_process_log.module_type is 'type of module that logging';
comment on column jmh_process_log.module_name is 'name of module that logged';
comment on column jmh_process_log.module_lineno is 'line number in module where logging occurred';
comment on column jmh_process_log.run_dt is 'session run date';
comment on column jmh_process_log.log_dt is 'date logged';
comment on column jmh_process_log.log_msg is 'message logged';
comment on column jmh_process_log.err_code is 'sqlcode when logged';
comment on column jmh_process_log.err_msg is 'sqlerrm when logged';

grant select on jmh_process_log to public;
