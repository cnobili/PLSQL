create or replace package body jmh_app_parameters_pkg is
-- --------------------------------------------------------------------------
--
-- name:  jmh_app_parameters_pkg.pkb
--
-- description: Package containing methods to get parameters from
--   the JMH_APP_PARAMETERS table.
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:   7-20-2010
-- author: Craig Nobili
-- desc:   original
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
return jmh_app_parameters.value%type
is
  l_value jmh_app_parameters.value%type;
begin
  
  select nvl(value, p_default_value)
  into l_value
  from jmh_app_parameters 
  where app_schema = sys_context('userenv', 'current_schema')
    and parameter = upper(p_parameter); 
            
  return l_value;
    
exception
  when no_data_found then
    return p_default_value;
    
end get_value;

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
)
is
begin

  merge into jmh_app_parameters p
  using
  (
    select 
      sys_context('userenv', 'current_schema') app_schema
    , upper(p_parameter)                       parameter
    , p_value                                  value
    , sysdate                                  chg_date
    , p_desc                                   description
    from dual
  ) n
  on 
  (p.app_schema = n.app_schema and p.parameter = n.parameter)
  when matched then
  update set
    p.value = n.value
  , p.chg_date = n.chg_date
  , p.description = nvl(p_desc, p.description)
  when not matched then
  insert
  values
  (
    n.app_schema
  , n.parameter
  , n.value
  , n.chg_date
  , n.description
  );
  commit;

end set_value;
    
end jmh_app_parameters_pkg;
/

show errors
