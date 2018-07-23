create or replace package body etl_pkg 
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

function etl(p_cur in etl_ext_tab_cur)
return etl_tab
pipelined
parallel_enable(partition p_cur by any)
is
  l_out_rec etl_rec;
  l_inp etl_ext_tab%rowtype;
begin

  loop
    fetch p_cur into l_inp;
    exit when p_cur%notfound;
    
    l_out_rec.c1 := l_inp.c1;
    l_out_rec.c2 := l_inp.c2;
    l_out_rec.c3 := l_inp.c3;
    l_out_rec.calc := l_inp.c1 + l_inp.c2;
    pipe row (l_out_rec);
  end loop;
  
end etl;

end etl_pkg;
/
