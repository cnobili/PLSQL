drop table hpf_mrn_ext purge;

create table hpf_mrn_ext
( 
  MRN               varchar2(500)
, OLD_MRN           varchar2(500)
, NAME              varchar2(500)
, SSN               varchar2(500)
, ADDRESS           varchar2(500)
, CITY              varchar2(500)
, STATE             varchar2(500)
, ZIP               varchar2(500)
, HM_PHONE          varchar2(500)
, WK_PHONE          varchar2(500)
, SEX               varchar2(500)
, DOB               varchar2(500)
, AGE               varchar2(500)
, RACE              varchar2(500)
, MARITAL           varchar2(500)
, EMPLOYER          varchar2(500)
, SMOKER            varchar2(500)
, RELIGION          varchar2(500)
, CHURCH            varchar2(500)
, EMERGENCY_CONTACT varchar2(500)
, BIRTH_PLACE       varchar2(500)
, LOCKOUT           varchar2(500)
, REMARK            varchar2(500)
, FACILITY          varchar2(500)
, LNSSN             varchar2(500)
, GPI               varchar2(500)
, ENTERPRISE_PI     varchar2(500)
, ENTERPRISE_AI     varchar2(500)
, ADDRESS2          varchar2(500)
, ADDRESS3          varchar2(500)
, DOMAIN_ID         varchar2(500)
, EPN               varchar2(500)
, NAME_LAST         varchar2(500)
, NAME_FIRST        varchar2(500)
, NAME_MIDDLE       varchar2(500)
, NAME_SUFFIX       varchar2(500)
, NAME_PREFIX       varchar2(500)
, rec_num             number
)
organization external
(
  type oracle_loader
  default directory JMH_EPIC_CONV
  access parameters
  (
    records delimited by NEWLINE
    skip 2
    nobadfile
    nodiscardfile
    nologfile
    fields
    missing field values are null
    reject rows with all null fields
    (
      MRN               position(1:20)
    , OLD_MRN           position(22:41)
    , NAME              position(43:82)
    , SSN               position(84:110)
    , ADDRESS           position(112:161)
    , CITY              position(163:212)
    , STATE             position(214:263)
    , ZIP               position(265:284)
    , HM_PHONE          position(286:305)
    , WK_PHONE          position(307:326)
    , SEX               position(328:331)
    , DOB               position(333:355)
    , AGE               position(357:361)
    , RACE              position(363:367)
    , MARITAL           position(369:375)
    , EMPLOYER          position(377:401)
    , SMOKER            position(403:408)
    , RELIGION          position(410:417)
    , CHURCH            position(419:443)
    , EMERGENCY_CONTACT position(445:484)
    , BIRTH_PLACE       position(486:500)
    , LOCKOUT           position(502:508)
    , REMARK            position(510:549)
    , FACILITY          position(551:560)
    , LNSSN             position(562:566)
    , GPI               position(568:587)
    , ENTERPRISE_PI     position(589:608)
    , ENTERPRISE_AI     position(610:629)
    , ADDRESS2          position(631:680)
    , ADDRESS3          position(682:731)
    , DOMAIN_ID         position(733:741)
    , EPN               position(743:792)
    , NAME_LAST         position(794:833)
    , NAME_FIRST        position(835:874)
    , NAME_MIDDLE       position(876:915)
    , NAME_SUFFIX       position(917:927)
    , NAME_PREFIX       position(929:939)
    , rec_num           recnum      
    )
  )
  location ('HPF_MRN_LIST.txt')
)
reject limit unlimited
parallel
;
