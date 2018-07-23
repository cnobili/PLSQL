create or replace trigger jmdba.audit_logoff
before logoff on database
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
  
    update jmdba.audit_logon a
      set
      a.logoff_date = sysdate
    where a.session_id = sys_context('USERENV', 'SESSIONID')
      and a.ora_user = sys_context('USERENV', 'SESSION_USER')
      and a.os_user = sys_context('USERENV','OS_USER')
      and a.logon_date = (select max(a2.logon_date) from jmdba.audit_logon a2
                          where a2.session_id = sys_context('USERENV', 'SESSIONID')
                            and a2.ora_user = sys_context('USERENV', 'SESSION_USER')
                            and a2.os_user = sys_context('USERENV','OS_USER')
                         );
  end if;

exception
  when others then null; -- swallow any exceptions to allow login in case of an error
   
end;
/
