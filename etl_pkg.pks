create or replace package etl_pkg 
--
-- No complex transformations done, just
-- an example package of using pipelined
-- function with external table.
--
-- The output of the etl function in this package
-- can then be the input to an:
-- insert /* parallel(f, 10) append */ into fact f
-- select * from table(etl_pkg.etl(cursor(select /* parallel(s, 10) */ * from etl_ext_tab s)));
--
as

type etl_ext_tab_cur is ref cursor return etl_ext_tab%rowtype;
  
type etl_rec is record
(
  c1   number
, c2   number
, c3   varchar2(20)
, calc number
);

type etl_tab is table of etl_rec;

function etl(p_cur in etl_ext_tab_cur)
return etl_tab
pipelined
parallel_enable(partition p_cur by any)
;

end etl_pkg;
/

  