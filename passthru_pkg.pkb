create or replace package body passthru_pkg
as

-- ---------------------------------------------------------------------------
--
-- procedure:  get_phs_data
--
-- purpose: Gets data using pass thru query (i.e. send native SQL
--   statement to the external system) and writes to flat file.
--
-- ---------------------------------------------------------------------------
--
procedure get_phs_data
(
  p_passthru_query in varchar2
, p_num_cols       in integer 
, p_dir            in varchar2
, p_filename       in varchar2
, p_delim          in varchar2 := '|'
)
is
  l_cur   integer;
  l_rs    integer; 
  l_col   varchar2(4000);
  l_row   varchar2(32767);
  l_delim varchar2(1);
  l_handle utl_file.file_type;
  l_maxline constant integer := 32767;
begin

  jmh_log_pkg.wlog('Begin get_phs_data' , jmh_log_pkg.LOG_NORM);
  
  l_handle := utl_file.fopen(p_dir, p_filename, 'w', l_maxline);
  
  l_cur := dbms_hs_passthrough.open_cursor@dg4msql_phs; 
  
  dbms_hs_passthrough.parse@dg4msql_phs(l_cur, p_passthru_query);
  
  loop
  
    l_rs := dbms_hs_passthrough.fetch_row@dg4msql_phs(l_cur);
    exit when l_rs = 0;
    l_row := null;
    l_delim := null;
    
    for i in 1 .. p_num_cols loop
      dbms_hs_passthrough.get_value@dg4msql_phs(l_cur, i, l_col);
      l_row := l_row || l_delim || l_col;
      l_delim := p_delim;
    end loop;
    
    utl_file.put_line(l_handle, l_row);
    
  end loop;
  
  dbms_hs_passthrough.close_cursor@dg4msql_phs(l_cur); 
  utl_file.fclose(l_handle);
  
  jmh_log_pkg.wlog('End get_phs_data' , jmh_log_pkg.LOG_NORM);

exception

  when others then
  
    if ( utl_file.is_open(l_handle) ) then
      utl_file.fclose(l_handle);
    end if;
    raise;
    
end get_phs_data;


-- ---------------------------------------------------------------------------
--
-- procedure:  get_hsm_data
--
-- purpose: Gets data using pass thru query (i.e. send native SQL
--   statement to the external system) and writes to flat file.
--
-- ---------------------------------------------------------------------------
--
procedure get_hsm_data
(
  p_passthru_query in varchar2
, p_num_cols       in integer 
, p_dir            in varchar2
, p_filename       in varchar2
, p_delim          in varchar2 := '|'
)
is
  l_cur   integer;
  l_rs    integer; 
  l_col   varchar2(4000);
  l_row   varchar2(32767);
  l_delim varchar2(1);
  l_handle utl_file.file_type;
  l_maxline constant integer := 32767;
begin

  jmh_log_pkg.wlog('Begin get_hsm_data' , jmh_log_pkg.LOG_NORM);
  
  l_handle := utl_file.fopen(p_dir, p_filename, 'w', l_maxline);
  
  l_cur := dbms_hs_passthrough.open_cursor@dg4msql_hsm; 
  
  dbms_hs_passthrough.parse@dg4msql_hsm(l_cur, p_passthru_query);
  
  loop
  
    l_rs := dbms_hs_passthrough.fetch_row@dg4msql_hsm(l_cur);
    exit when l_rs = 0;
    l_row := null;
    l_delim := null;
    
    for i in 1 .. p_num_cols loop
      dbms_hs_passthrough.get_value@dg4msql_hsm(l_cur, i, l_col);
      l_row := l_row || l_delim || l_col;
      l_delim := p_delim;
    end loop;
    
    utl_file.put_line(l_handle, l_row);
    
  end loop;
  
  dbms_hs_passthrough.close_cursor@dg4msql_hsm(l_cur); 
  utl_file.fclose(l_handle);
  
  jmh_log_pkg.wlog('End get_hsm_data' , jmh_log_pkg.LOG_NORM);

exception

  when others then
  
    if ( utl_file.is_open(l_handle) ) then
      utl_file.fclose(l_handle);
    end if;
    raise;
    
end get_hsm_data;

-- ---------------------------------------------------------------------------
--
-- procedure:  get_hemm_data
--
-- purpose: Gets data using pass thru query (i.e. send native SQL
--   statement to the external system) and writes to flat file.
--
-- ---------------------------------------------------------------------------
--
procedure get_hemm_data
(
  p_passthru_query in varchar2
, p_num_cols       in integer 
, p_dir            in varchar2
, p_filename       in varchar2
, p_delim          in varchar2 := '|'
)
is
  l_cur   integer;
  l_rs    integer; 
  l_col   varchar2(4000);
  l_row   varchar2(32767);
  l_delim varchar2(1);
  l_handle utl_file.file_type;
  l_maxline constant integer := 32767;
begin

  jmh_log_pkg.wlog('Begin get_hemmm_data' , jmh_log_pkg.LOG_NORM);
  
  l_handle := utl_file.fopen(p_dir, p_filename, 'w', l_maxline);
  
  l_cur := dbms_hs_passthrough.open_cursor@dg4msql_hemm; 
  
  dbms_hs_passthrough.parse@dg4msql_hemm(l_cur, p_passthru_query);
  
  loop
  
    l_rs := dbms_hs_passthrough.fetch_row@dg4msql_hemm(l_cur);
    exit when l_rs = 0;
    l_row := null;
    l_delim := null;
    
    for i in 1 .. p_num_cols loop
      dbms_hs_passthrough.get_value@dg4msql_hemm(l_cur, i, l_col);
      l_row := l_row || l_delim || l_col;
      l_delim := p_delim;
    end loop;
    
    utl_file.put_line(l_handle, l_row);
    
  end loop;
  
  dbms_hs_passthrough.close_cursor@dg4msql_hemm(l_cur); 
  utl_file.fclose(l_handle);
  
  jmh_log_pkg.wlog('End get_hemmm_data' , jmh_log_pkg.LOG_NORM);

exception

  when others then
  
    if ( utl_file.is_open(l_handle) ) then
      utl_file.fclose(l_handle);
    end if;
    raise;
    
end get_hemm_data;

  
end passthru_pkg;
/
