create or replace package body jmh_log_pkg
-- --------------------------------------------------------------------------
--
-- module name: jmh_pkg_log
--
-- description: Logging package.
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:    07-20-2010
-- author:  Craig Nobili
-- desc:
--
-- date:    03-05-2012
-- author:  Craig Nobili
-- desc:    Added default parameter p_module
--
-- --------------------------------------------------------------------------
as

-- 
-- Private Globals
--
g_log_level number;
g_run_dt date;

--
-- Public Methods
--
-- Note: You must commit or rollback before returning from an autonomous
-- transction procedure.

procedure wlog
(
  p_log_msg   in jmh_process_log.log_msg%type
, p_log_level in number := LOG_NORM
, p_module    in varchar2 := null
, p_err_code  in number := sqlcode
, p_err_msg   in varchar2 := sqlerrm
)
is
  pragma autonomous_transaction;
  
  l_module_owner  varchar2(30);
  l_module_type   user_objects.object_type%type;
  l_module_name   user_objects.object_name%type;
  l_module_lineno number;
begin

  -- Get module that called logging package
  owa_util.who_called_me
  (
    owner    => l_module_owner
  , name     => l_module_name
  , lineno   => l_module_lineno
  , caller_t => l_module_type
  );
    
  -- Show progress in v$session and v$sqlarea
  dbms_application_info.set_module(l_module_name, p_log_msg);
    
  insert into jmh_process_log
  (
    process_log_seq
  , db_name
  , instance_name
  , client_host
  , os_user
  , sessionid
  , username
  , module_owner
  , module_type
  , module_name
  , module_lineno
  , run_dt
  , log_dt
  , log_msg
  , err_code
  , err_msg
  )
  select
    jmh_process_log_seq.nextval
  , sys_context('userenv', 'db_name')
  , sys_context('userenv', 'instance_name')
  , sys_context('userenv', 'host')
  , sys_context('userenv', 'os_user')
  , sys_context('userenv', 'sessionid')
  --, sys_context('userenv', 'current_user')
  , sys_context('userenv', 'session_user')
  , l_module_owner
  , l_module_type
  , nvl(p_module, l_module_name)
  , l_module_lineno
  , g_run_dt
  , systimestamp
  , p_log_msg
  , p_err_code
  , p_err_msg
  from
    dual
  where p_log_level >= g_log_level -- only log if higher than default logging level
  ;
  commit;

end wlog;

begin
 --
 -- Package Initialization: Get logging level
 --
 g_run_dt := sysdate;
 g_log_level := 
 to_number
 (
   jmh_app_parameters_pkg.get_value
   (
     p_parameter     => 'JMH_LOG_LEVEL'
   , p_default_value => LOG_NORM
   )
 );

end jmh_log_pkg;
/
show errors
