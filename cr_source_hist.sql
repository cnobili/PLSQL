create table source_hist
as
select
  sysdate                           as change_date
, user                              as username
, sys_context('USERENV', 'OS_USER') as osuser
, u.*
from
  all_source u
where 1 = 2
;

