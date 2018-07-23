create or replace trigger source_hist_change
--after create on schema
after create on database
--
-- ---------------------------------------------------------------------------
--
-- trigger: source_hist_change
--
-- purpose:  Keeps a history of PL/SQL source code.
--
-- author: Craig Nobili
--
-- ---------------------------------------------------------------------------
--
declare
begin

  if sys.dictionary_obj_type in 
  ('PROCEDURE', 'FUNCTION', 'TRIGGER', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'JAVA SOURCE')
  then
    -- Store old code in source_hist table
    insert into source_hist
    select 
      sysdate
    , sys.login_user
    , sys_context('USERENV', 'OS_USER')
    , u.*
    from
      dba_source u
    where type = sys.dictionary_obj_type
      and name = sys.dictionary_obj_name
      and owner = sys.dictionary_obj_owner
    ;
  end if;
       
end source_hist_change;
/
