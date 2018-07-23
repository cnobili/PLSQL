declare
  -- exceptions
  table_not_exist_excep  EXCEPTION;

  -- pragmas
  pragma exception_init(table_not_exist_excep, -942);

begin
  execute immediate 'drop table audit_logon';

exception
  when table_not_exist_excep then
     null;
end;
/

create table jmdba.audit_logon
(
  audit_logon_seq      number
, ora_user             varchar2(30)
, os_user              varchar2(255)
, session_id           number
, logon_date           date
, logoff_date          date
, host                 varchar2(255)
, ip_address           varchar2(30)
, terminal             varchar2(255)
, module               varchar2(255)
, constraint audit_logon_pk primary key (audit_logon_seq) 
)
;

comment on table  audit_logon is 'Audit table to track user logons.';
comment on column audit_logon.audit_logon_seq is 'sequence id';
comment on column audit_logon.ora_user is 'Oracle logon id';
comment on column audit_logon.os_user is 'OS user id';
comment on column audit_logon.session_id is 'Session Id, audsid in v$session';
comment on column audit_logon.logon_date is 'Logon date';
comment on column audit_logon.host is 'Machine logged on from';
comment on column audit_logon.ip_address is 'IP address logged on from';
comment on column audit_logon.terminal is 'Terminal user logged on from';

-- The user jmdba needs to have the administer database trigger system privlege
