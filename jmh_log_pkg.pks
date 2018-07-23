create or replace package jmh_log_pkg
-- --------------------------------------------------------------------------
--
-- module name: jmh_log_pkg
--
-- description: Logging package.
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:    07-20-2010
-- author:  Craig Nobili
-- desc:    original
--
-- date:    03-05-2012
-- author:  Craig Nobili
-- desc:    Added default parameter p_module
--
-- --------------------------------------------------------------------------
as
  --
  -- The logging levels
  --
  LOG_DEBUG constant pls_integer := 0; -- lowest level for debugging statements
  LOG_NORM  constant pls_integer := 1; -- for normal production runs
  LOG_MUST  constant pls_integer := 2; -- highest level that only logs errors
  
  --
  -- The default logging level is in JMH_APP_PARAMETERS.  You can adjust the logging
  -- level dynamically by changing the value of this parameter(JMH_LOG_LEVEL).  Logs only
  -- messages where the log level is greater than or equal to the value of
  -- this parameter.
  --
  
procedure wlog
(
  p_log_msg   in jmh_process_log.log_msg%type
, p_log_level in number := LOG_NORM
, p_module    in varchar2 := null
, p_err_code  in number := sqlcode
, p_err_msg   in varchar2 := sqlerrm
);

end jmh_log_pkg;
/
show errors
