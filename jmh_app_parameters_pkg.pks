create or replace package jmh_app_parameters_pkg is
-- --------------------------------------------------------------------------
--
-- name:  jmhs_app_parameters_pkg.pks
--
-- description: Package containing methods to get parameters from
--   the JMH_APP_PARAMETERS table.
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:  7-20-2010
-- author: Craig Nobili
-- desc: original
--
-- --------------------------------------------------------------------------
      
-- --------------------------------------------------------------------------
--
-- function: get_value
--
-- description:  Returns the value associated with a given parameter.
--
-- --------------------------------------------------------------------------
function get_value
(
  p_parameter     in jmh_app_parameters.parameter%type
, p_default_value in jmh_app_parameters.value%type := null
)
return jmh_app_parameters.value%type;

-- --------------------------------------------------------------------------
--
-- procedure: set_value
--
-- description:  Assigns a key/value pair.
--
-- --------------------------------------------------------------------------
procedure set_value
(
  p_parameter in jmh_app_parameters.parameter%type
, p_value     in jmh_app_parameters.value%type 
, p_desc      in jmh_app_parameters.description%type := null 
);
    
end jmh_app_parameters_pkg;
/

show errors
