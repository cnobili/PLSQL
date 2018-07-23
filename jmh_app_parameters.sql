-- -----------------------------------------------------------------------------
--
-- script:  jmh_app_parameters.sql
--
-- purpose:  create jmh_app_parameters table
--
-- -----------------------------------------------------------------------------
--
-- rev log
--
-- date:   7-20-2010
-- author: Craig Nobili
-- desc:   original
--
-- -----------------------------------------------------------------------------

create table jmh_app_parameters
( 
  app_schema  varchar2(30)
, parameter   varchar2(255)
, value       varchar2(4000)
, chg_date    date
, description varchar2(4000)
, constraint nwps_app_parameters_pk 
    primary key (app_schema, parameter)
)
;
