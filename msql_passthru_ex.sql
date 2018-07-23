set serveroutput on

declare
  l_mrn varchar2(100); 
  l_cur integer; 
  l_rs  integer; 
begin

  l_cur := dbms_hs_passthrough.open_cursor@dg4msql_apta; 
  
  dbms_hs_passthrough.parse@dg4msql_apta
  (
    l_cur
  , 'select mrn from patient with(nolock)'
  );
  
  loop
    l_rs := dbms_hs_passthrough.fetch_row@dg4msql_apta(l_cur);
    exit when l_rs = 0;
    dbms_hs_passthrough.get_value@dg4msql_apta(l_cur, 1, l_mrn);
    dbms_output.put_line(l_mrn);
  end loop;
  
  dbms_hs_passthrough.close_cursor@dg4msql_apta(l_cur); 
  
end;
/
