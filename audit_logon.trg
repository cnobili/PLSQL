create or replace trigger jmdba.audit_logon
after logon on database 
--
-- ---------------------------------------------------------------------------
--
-- trigger: audit_logon
--
-- purpose:  Keeps a history of logons to the database.
--   
-- Note: jmdba needs administer database trigger system privilege.
--
-- author: Craig Nobili
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- 02/16/2012 - Craig Nobili - Creation.
--
--
-- ---------------------------------------------------------------------------
begin

   if ( user not in ('SYS', 'SYSTEM', 'DBSNMP') ) then
   
     insert into jmdba.audit_logon
     (
       audit_logon_seq
     , ora_user
     , os_user
     , session_id
     , logon_date
     , host
     , ip_address
     , terminal
     , module
     )
     values
     (
       jmdba.audit_logon_seq.nextval
     , sys_context('USERENV', 'SESSION_USER')
     , sys_context('USERENV', 'OS_USER')
     , sys_context('USERENV', 'SESSIONID')
     , sysdate
     , sys_context('USERENV', 'HOST')
     , sys_context('USERENV', 'IP_ADDRESS')
     , sys_context('USERENV', 'TERMINAL')
     , sys_context('USERENV', 'MODULE')
     )
     ;
     
   end if;

exception
  when others then null; -- swallow any exceptions to allow login in case of an error
  
end;
/
