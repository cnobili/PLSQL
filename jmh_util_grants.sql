-- Must run this as sys
--
-- Explicit grants need by jmdba user
-- in order to compile utility package jmh_util_pkg.
--
grant select on dba_users to jmdba;
grant select on v_$session to jmdba;
grant select on v_$process to jmdba;
grant select on v_$sqlarea to jmdba;
grant select on dba_objects to jmdba;
grant select on dba_extents to jmdba;
