select *
from
  table(etl_pkg.etl(cursor(select /* parallel(s, 10) */ * from etl_ext_tab s)))
;
