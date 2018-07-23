create table etl_ext_tab
(
  c1 number
, c2 number
, c3 varchar2(20)
)
organization external
(
  type oracle_loader
  default directory ETL_DIR
  access parameters
  (
    records delimited by '\r\n'
    nologfile
		nodiscardfile
		badfile       'etl.bad'
    fields terminated by ','
    missing field values are null
    reject rows with all null fields
    (
      c1
    , c2
    , c3
    )
  )
  location ('etl.dat')
)
reject limit unlimited
parallel 10
;

