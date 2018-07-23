create or replace package body jmh_util_pkg
as
--
-- ---------------------------------------------------------------------------
--
-- package:  jmh_util_pkg
--
-- purpose:  Misc general routines
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  05-AUG-2010
-- author:  Craig Nobili
-- desc: original
--
-- date:   17-JAN-2014
-- author: Craig Nobili
-- desc:   Added webservice function
--
-- ---------------------------------------------------------------------------

--
-- Private Module Variables
--

g_NEW_LINE constant varchar2(2) := '0A';

g_ALPHA   constant varchar2(26) := 'abcdefghijklmnopqrstuvwxyz';
g_DIGIT   constant varchar2(10) := '0123456789';
g_SPECIAL constant varchar2(3)  := '_#$';

--
-- Private Methods
--

-- ---------------------------------------------------------------------------
--
-- procedure:  writeln
--
-- purpose: Writes out a string.
--
-- ---------------------------------------------------------------------------
--
procedure writeln(p_dir in varchar2, p_handle in utl_file.file_type, p_str in varchar2)
is
begin
  
  if (p_dir is not null) then
    utl_file.put_line(p_handle, p_str);
  else
    dbms_output.put_line(p_str);
  end if;

end writeln;

-- ---------------------------------------------------------------------------
--
-- procedure:  output_controlfile
--
-- purpose: Outputs a SQL*Loader controlfile.
--
-- ---------------------------------------------------------------------------
--
procedure output_controlfile
(
  p_desc_tab in dbms_sql.desc_tab
, p_tablename in varchar2
, p_delim in varchar2 
, p_dir in varchar2
)
is
  l_utl_handle utl_file.file_type;
  l_comma varchar2(10) := ' ';
begin

  if (p_dir is not null) then
    l_utl_handle := utl_file.fopen(p_dir, p_tablename || '.ctl', 'w');
  end if;

  writeln(p_dir, l_utl_handle, 'load data');
  writeln(p_dir, l_utl_handle, 'infile ' || '''' || p_tablename || '.dat''');
  writeln(p_dir, l_utl_handle, 'truncate');
  writeln(p_dir, l_utl_handle, 'into table ' || p_tablename);
  writeln(p_dir, l_utl_handle, 'fields terminated by ' || '''' || p_delim || '''' || ' optionally enclosed by ''"''');
  writeln(p_dir, l_utl_handle, 'trailing nullcols');
  writeln(p_dir, l_utl_handle, '(');

  -- output columns and the datatypes   
  for i in 1 .. p_desc_tab.count loop
    if (p_desc_tab(i).col_type = 12) then
      writeln
      (
        p_dir
      , l_utl_handle
      , l_comma || ' ' || rpad(p_desc_tab(i).col_name, 30) || ' date "' || sys_context('userenv', 'nls_date_format') || '"'
      );
    else
      writeln
      (
        p_dir
      , l_utl_handle
      , l_comma || ' ' || rpad(p_desc_tab(i).col_name, 30)|| ' char(' || to_char(p_desc_tab(i).col_max_len) || ')' 
      );
    end if;
    l_comma := ',';
  end loop;

  writeln(p_dir, l_utl_handle, ')' || chr(10));

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;  
    raise;
    
end output_controlfile;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_constraints_ddl
--
-- purpose: Generates DDL to drop, enable, disable table constraints.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_constraints_ddl
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_constraint_type in all_constraints.constraint_type%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_owner all_tables.owner%type := upper(p_owner);
  l_table_name all_tables.table_name%type := upper(p_table_name);
  l_constraint_type all_constraints.constraint_type%type := upper(p_constraint_type);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);
begin
  
  if (p_dir is not null) then
    l_filename := replace(p_action, ' ', '_') || 's.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select owner, table_name, constraint_type, constraint_name
    from all_constraints
    where table_name = decode(l_table_name, null, table_name, l_table_name)
      and constraint_type = decode(l_constraint_type, null, constraint_type, l_constraint_type)
      and owner = l_owner
    order by table_name, constraint_type, constraint_name
  ) 
  loop
    l_str := 'alter table ' || rec.owner || '.' || rec.table_name || ' ' || p_action || ' ' || rec.constraint_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;

end gen_constraints_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_disable_tab_triggers
--
-- purpose: Generates DDL to enable or diable table triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all tables.
--
-- ---------------------------------------------------------------------------
--
procedure enable_disable_tab_triggers
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_owner all_tables.owner%type := upper(p_owner);
  l_table_name all_tables.table_name%type := upper(p_table_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);  
begin

  if (p_dir is not null) then
    l_filename := replace(p_action, ' ', '_') || 'triggers.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select owner, table_name, trigger_name
    from all_triggers
    where table_name = decode(l_table_name, null, table_name, l_table_name)
      and owner = l_owner
    order by table_name, trigger_name
  ) 
  loop
    l_str := 'alter table ' || rec.owner || '.' || rec.table_name || ' ' || p_action || ' all triggers ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;

end enable_disable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_disable_triggers
--
-- purpose: Generates DDL to enable or diable triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all triggers.
--
-- ---------------------------------------------------------------------------
--
procedure enable_disable_triggers
(
  p_owner in all_tables.owner%type := null
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_owner all_tables.owner%type := upper(p_owner);
  l_table_name all_tables.table_name%type := upper(p_table_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);  
begin

  if (p_dir is not null) then
    l_filename := replace(p_action, ' ', '_') || 'triggers.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select owner, table_name, trigger_name
    from all_triggers
    where table_name = decode(l_table_name, null, table_name, l_table_name)
      and owner = l_owner
    order by table_name, trigger_name
  ) 
  loop
    l_str := 'alter trigger ' || rec.trigger_name || ' ' || p_action || ' ;' ;
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;

end enable_disable_triggers;

--
-- Public Methods
--

-- ---------------------------------------------------------------------------
--
-- function:  get_spid
--
-- purpose: Gets the session server process Id
--
--  Invoker/Owner of package must have the following privileges:
--    select on v_$process
--    select on v_$session
--
-- ---------------------------------------------------------------------------
--
function get_spid
return v$process.spid%type
is
  l_spid v$process.spid%type;
begin

  select
    p.spid
  into
    l_spid
  from
    v$process p
    inner join
    v$session s
    on
    (p.addr = s.paddr)
  where s.audsid = sys_context('userenv', 'sessionid')
  ; 

  return l_spid;

end get_spid;

-- ---------------------------------------------------------------------------
--
-- function:  get_session_trace_filename
--
-- purpose: Gets the session's trace file.  You need to turn tracing on in 
--   your session first, i.e. alter session set sql_trace=true;
--
-- You need to have select access on v$process (i.e. v_$process) and v$session (v_$session)
-- granted directly by sys.
--
-- ---------------------------------------------------------------------------
--
function get_session_trace_filename 
return varchar2
is
begin
  
  return sys_context('USERENV', 'INSTANCE_NAME') || '_ora_' || get_spid || '.trc' ;
  
end get_session_trace_filename;

-- ---------------------------------------------------------------------------
--
-- function:  display_file
--
-- purpose: Displays the contents of an OS file as a virtual table.  The file
--   must be in a directory that is referenced in by a Oracle directory object.
--
-- Note:
-- This function can take a call to get_session_trace_filename to
-- display a trace file's contents as a virtual table (assuming you have
-- set tracing on in your session at some point).  Thus, you don't need
-- physical OS access to trace file directories, utl_file_dir ... to
-- view file contents.
--
-- ---------------------------------------------------------------------------
--
function display_file
(
  p_dir in varchar2
, p_filename in varchar2
) return char_array_t pipelined
is
  l_binfile bfile := bfilename(p_dir, p_filename);
  l_last_pos pls_integer := 1;
  l_curr pls_integer := -1;
begin

  dbms_lob.fileopen(l_binfile);
  loop
    l_curr := dbms_lob.instr(l_binfile, g_NEW_LINE, l_last_pos, 1);
    exit when nvl(l_curr, 0) = 0;
    pipe row
    (
      utl_raw.cast_to_varchar2(dbms_lob.substr(l_binfile, l_curr - l_last_pos + 1, l_last_pos))
    ); 
    l_last_pos := l_curr + 1;
  end loop;

  return;

end display_file;

-- ---------------------------------------------------------------------------
--
-- function:  list_to_rows
--
-- purpose: Takes a comman delimited string and turns it into a set of rows.
--
-- ---------------------------------------------------------------------------
--
function list_to_rows
(
  p_string in varchar2
) return char_array_t pipelined
is
  /* Can only use regular expressions if on 10g 
  cursor c_parse (str varchar2) 
  is
  select regexp_substr(str, '[^,]+', 1, level) token
  from dual
  connect by level <= length(regexp_replace(str,'[^,]+',''))+ 1
  ; */
  cursor c_parse (str varchar2)
  is
  select
     substr
     (
       ',' || x.input_string || ','
     , instr(',' || x.input_string || ',', ',', 1, y.seq) + 1
     , instr(',' || x.input_string || ',', ',', 1, y.seq + 1) - instr(',' || x.input_string || ',', ',', 1, y.seq) - 1
     ) token
  from
      (select str input_string from dual) x
    , (select rownum seq from all_objects
       where rownum <= length(str)
      ) y
  where
      instr(',' || x.input_string || ',', ',', 1, y.seq + 1) - instr(',' || x.input_string || ',', ',', 1, y.seq) - 1 > 0
  ;
  
begin

  for rec in c_parse(p_string) loop
    pipe row (rec.token);
  end loop;

  return;

end list_to_rows;

-- ---------------------------------------------------------------------------
--
-- procedure:  show_resultset_by_cols
--
-- purpose: Displays the result of a query by listing out the columns and the
--  corresponding column value.  Need to have set serveroutput on in session.
--
-- ---------------------------------------------------------------------------
--
procedure show_resultset_by_cols
(
  p_query in varchar2
)
is
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);

  -- Define all columns as varchar2
  for i in 1 .. l_num_cols loop
    dbms_sql.define_column(l_cur, i, l_col_value, 4000);
  end loop;

  l_retcode := dbms_sql.execute(l_cur);

  while (dbms_sql.fetch_rows(l_cur) > 0) loop
    for i in 1 .. l_num_cols loop
      dbms_sql.column_value(l_cur, i, l_col_value);
      dbms_output.put_line(rpad(l_desc_tab(i).col_name, 30) || ': ' || l_col_value);
    end loop;
    dbms_output.put_line('****************************************************');
  end loop;

  dbms_sql.close_cursor(l_cur);

end show_resultset_by_cols;

-- ---------------------------------------------------------------------------
--
-- function:  get_resultset_by_cols
--
-- purpose: Turns a query resultset into virtual table with the columns
--   and column values rotated into rows.
--
-- ---------------------------------------------------------------------------
--
function get_resultset_by_cols
(
  p_query in varchar2
) return by_cols_t pipelined
is
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
  l_row_num pls_integer := 1;
  l_rec by_cols_rec;
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);

  -- Define all columns as varchar2
  for i in 1 .. l_num_cols loop
    dbms_sql.define_column(l_cur, i, l_col_value, 4000);
  end loop;

  l_retcode := dbms_sql.execute(l_cur);

  while (dbms_sql.fetch_rows(l_cur) > 0) loop
    for i in 1 .. l_num_cols loop
      dbms_sql.column_value(l_cur, i, l_col_value);
      l_row_num := l_row_num;
      l_rec.col_name := l_desc_tab(i).col_name;
      l_rec.col_value := l_col_value;
      pipe row(l_rec);
    end loop;
    l_row_num := l_row_num + 1; 
  end loop;

  dbms_sql.close_cursor(l_cur);
  return;

end get_resultset_by_cols;

-- ---------------------------------------------------------------------------
--
-- function:  generate_rows
--
-- purpose: Generates a pivot table of row numbers using pipelined table function.
--
-- This could also be done via the following methods:
--
--   * Create an actual table and insert into it, but this requires a physical table.
--
--   * select level from dual connect by level <= :n
--
--   * select rownum from all_objects where rownum <= :n
--
--   * recursive CTE - 11g
--     with numbers(n) as 
--     (  
--       --The "anchor member." It contains exactly one row (N = 1).  
--       select 1 AS N  
--       from dual  
--       union all 
--       -- The "recursive member." Notice that it references the name of the recursive  
--       -- CTE as a placeholder for the results of the anchor member or the previous  
--       -- execution of the recursive CTE. Each iteration of the recursive member  
--       -- produces the next value of N. Recursive execution stops when N = 9.  
--       select N + 1 AS N  
--       from numbers Numbers  
--       where N < 9  
--       )  
--       select * from numbers 
--
-- ---------------------------------------------------------------------------
--
function generate_rows
(
  p_num in number
) return num_array_t pipelined
is
begin
  for n in 1 .. p_num loop
    pipe row (n);
  end loop;
  return;
end generate_rows;

-- ---------------------------------------------------------------------------
--
-- procedure:  dump_resultset
--
-- purpose: Dumps a result set to a file or the screen (if dir and filename
--   are not passed in ).  If delim is specified the columns are delimited. 
--
-- Note: If no directory and filename are passed in, the output will go
--   to the screen, assuming you have set serveroutput on in your session.
--
--   If tablename is passed in it writes out a SQL*Loader controlfile.  You
--   can set the date format required by alter session set nls_date_format ...
--
--   If p_add_header_rec is true, the first record will contain the list
--   of column headers.
--
-- ---------------------------------------------------------------------------
--
procedure dump_resultset
(
  p_query in varchar2
, p_dir in varchar2 := null
, p_filename in varchar2 := null
, p_delim in varchar2 := null
, p_tablename in varchar2 := null
, p_add_header_rec in boolean := false
)
is
  l_utl_handle utl_file.file_type;
  l_line varchar2(32767);
  l_delimiter varchar2(10);
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);
  
  if (p_tablename is not null) then
    output_controlfile(l_desc_tab, p_tablename, p_delim, p_dir);
  end if;

  -- Define all columns as varchar2
  for i in 1 .. l_num_cols loop
    dbms_sql.define_column(l_cur, i, l_col_value, 4000);
  end loop;

  l_retcode := dbms_sql.execute(l_cur);

  if (p_dir is not null) then
    l_utl_handle := utl_file.fopen(p_dir, p_filename, 'w');
  end if;
  
  if (p_add_header_rec) then
    l_line := null;
    l_delimiter := null;
    for i in 1 .. l_num_cols loop
      l_line := l_line || l_delimiter || l_desc_tab(i).col_name;
      l_delimiter := p_delim;
    end loop;
    writeln(p_dir, l_utl_handle, l_line);
  end if;
  
  while (dbms_sql.fetch_rows(l_cur) > 0) loop
    l_line := null;
    l_delimiter := null;
    for i in 1 .. l_num_cols loop
      dbms_sql.column_value(l_cur, i, l_col_value);
      l_line := l_line || l_delimiter || l_col_value;
      l_delimiter := p_delim;
    end loop;
    writeln(p_dir, l_utl_handle, l_line);
   
  end loop;

  dbms_sql.close_cursor(l_cur);
  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
  
end dump_resultset;

-- ---------------------------------------------------------------------------
--
-- procedure:  dump_resultset2
--
-- purpose: Dumps a result set to a file.  Buffers IO for performance.
--
--   If p_add_header_rec is true, the first record will contain the list
--   of column headers.
--
-- ---------------------------------------------------------------------------
--
procedure dump_resultset2
(
  p_query in varchar2
, p_dir in varchar2
, p_filename in varchar2
, p_delim in varchar2
, p_add_header_rec in boolean := false
)
is
  l_utl_handle utl_file.file_type;
  l_line varchar2(32767);
  l_delimiter varchar2(10);
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
  
  l_buffer varchar2(32767);
  l_eol     constant varchar2(1) := chr(10);                                    
  l_eollen  constant pls_integer := length(l_eol);                              
  l_maxline constant pls_integer := 32767;     
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);
 
  -- Define all columns as varchar2
  for i in 1 .. l_num_cols loop
    dbms_sql.define_column(l_cur, i, l_col_value, 4000);
  end loop;

  l_retcode := dbms_sql.execute(l_cur);

  l_utl_handle := utl_file.fopen(p_dir, p_filename, 'w', l_maxline);
   
  if (p_add_header_rec) then
    l_line := null;
    l_delimiter := null;
    for i in 1 .. l_num_cols loop
      l_line := l_line || l_delimiter || l_desc_tab(i).col_name;
      l_delimiter := p_delim;
    end loop;
    utl_file.put_line(l_utl_handle, l_line);
  end if;
  
  while (dbms_sql.fetch_rows(l_cur) > 0) loop
    l_line := null;
    l_delimiter := null;
    for i in 1 .. l_num_cols loop
      dbms_sql.column_value(l_cur, i, l_col_value);
      l_line := l_line || l_delimiter || l_col_value;
      l_delimiter := p_delim;
    end loop;
    
    if length(l_buffer) + l_eollen + length(l_line) <= l_maxline then
      l_buffer := l_buffer || l_eol || l_line;
    else
      if l_buffer is not null then
        utl_file.put_line(l_utl_handle, l_buffer);
      end if;
      l_buffer := l_line;
    end if;
       
  end loop;

  dbms_sql.close_cursor(l_cur);
  utl_file.fclose(l_utl_handle);
  
exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
  
end dump_resultset2;

-- ---------------------------------------------------------------------------
--
-- function:  dump_resultset
--
-- purpose: Dumps a result set to a file and displays file stats in
--   select statement via pipelined table.  It will dump the files in 
--   parallel, adding the sid to the filename.  Also, buffers IO for performance.
--
-- Example call (you must concatenate select list with delimiter):
--
-- select * 
-- from
--   table
--   (
--     jmh_util_pkg.dump_result
--     (
--       cursor
--       (
--          select /*+ parallel(o,2) */ o.object_type || '|' || o.object_name from all_objects o
--       )
--     , p_dir
--     , p_filename
--     , p_delim
--     )
--   )
-- 
-- ---------------------------------------------------------------------------
--
function dump_resultset
(
  p_query in sys_refcursor
, p_dir in varchar2
, p_filename in varchar2
)
return dump_tab pipelined parallel_enable (partition p_query by any)
is
  l_rec dump_rec;
  l_sid number;
  l_filename varchar2(256);
  l_buffer varchar2(32767);
  
  type row_tab is table of varchar2(32767);
  l_rows row_tab;
  
  l_eol     constant varchar2(1) := chr(10);                                    
  l_eollen  constant pls_integer := length(l_eol);                              
  l_maxline constant pls_integer := 32767;   
  l_handle utl_file.file_type;
begin

  l_sid := sys_context('USERENV', 'sid');
  l_filename := p_filename || '_' || l_sid;
  
  l_rec.dir := p_dir;
  l_rec.filename := l_filename;
  l_rec.num_recs := 0;
  l_rec.session_id := l_sid;
  l_rec.start_tm := sysdate;

  l_handle := utl_file.fopen(p_dir, l_filename, 'w', l_maxline);
  
  loop                                                                          
    fetch p_query bulk collect into l_rows limit 1000;                          

      for i in 1 .. l_rows.count loop                                            

        if length(l_buffer) + l_eollen + length(l_rows(i)) <= l_maxline then    
          l_buffer := l_buffer || l_eol || l_rows(i);                          
        else                                                                    
          if l_buffer is not null then
            utl_file.put_line(l_handle, l_buffer);                              
          end if;                                                              
          l_buffer := l_rows(i);                                               
        end if;                                                                 

      end loop;                                                                  

      l_rec.num_recs := l_rec.num_recs + l_rows.count;                                         

      exit when p_query%notfound; 
      
  end loop;                                                                     
  
  close p_query;                                                             

  utl_file.put_line(l_handle, l_buffer);                                          
  utl_file.fclose(l_handle);     
  
  l_rec.end_tm := sysdate; 

  pipe row (l_rec);                                   
  return;                                     

end dump_resultset;

-- ---------------------------------------------------------------------------
--
-- procedure:  unload
--
-- purpose: Unloads a result set to a file, buffers IO for performance.
--  
-- Note: The query passed in (p_query) needs to have the columns separated by
--  a delimiter.
--
-- Example call (note the use of quoted # string to include ' in select statement):
--
-- jmh_util_pkg.unload
-- (
--   q'#select /*+ parallel(4) */ object_type || '|' || object_name from user_objects#'
-- , p_dir
-- , p_filename
-- );
--
-- ---------------------------------------------------------------------------
--
procedure unload
(
  p_query    in varchar2
, p_dir      in varchar2
, p_filename in varchar2
)
is
  l_cur sys_refcursor;
  l_buffer varchar2(32767);
  
  type row_tab is table of varchar2(32767);
  l_rows row_tab;
  
  l_eol     constant varchar2(1) := chr(10);  -- utl_file handles OS end of line differences                                  
  l_eollen  constant pls_integer := length(l_eol);                              
  l_maxline constant pls_integer := 32767;   
  l_handle utl_file.file_type;
  
begin

  l_handle := utl_file.fopen(p_dir, p_filename, 'w', l_maxline);
  
  open l_cur for p_query;
  
  loop                                                                          
    fetch l_cur bulk collect into l_rows limit 1000;                          

      for i in 1 .. l_rows.count loop                                            

        if length(l_buffer) + l_eollen + length(l_rows(i)) <= l_maxline then    
          l_buffer := l_buffer || l_eol || l_rows(i);                          
        else                                                                    
          if l_buffer is not null then
            utl_file.put_line(l_handle, l_buffer);                              
          end if;                                                              
          l_buffer := l_rows(i);                                               
        end if;                                                                 

      end loop;                                                                  

      exit when l_cur%notfound; 
      
  end loop;                                                                     
  
  close l_cur;                                                             

  utl_file.put_line(l_handle, l_buffer);                                          
  utl_file.fclose(l_handle);     
  
end unload;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_drop_objects_ddl
--
-- purpose: Generates DDL to drop schema objects.
--   If p_object_name is null (i.e. not passed in when called), it generates
--   the SQL DDL to drop all objects for the passed in object_type.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_drop_objects_ddl
(
  p_owner in all_objects.owner%type
, p_object_type in all_objects.object_type%type 
, p_object_name in all_objects.object_name%type := null
, p_dir in varchar2 := null
)
is
  l_owner all_objects.owner%type := upper(p_owner);
  l_object_type all_objects.object_type%type := upper(p_object_type);
  l_object_name all_objects.object_name%type := upper(p_object_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);
begin
  
  if (p_dir is not null) then
    l_filename := 'drop_' || l_owner || '_' || l_object_type || '_ddl.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select owner, object_type, object_name
    from all_objects
    where object_type = l_object_type
      and object_name = decode(l_object_name, null, object_name, l_object_name)
      and owner = l_owner
    order by object_type, object_name
  ) 
  loop
    l_str := 'drop ' || rec.object_type || ' ' || rec.owner || '.' || rec.object_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;

end gen_drop_objects_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_drop_constraints_ddl
--
-- purpose: Generates DDL to drop schema table constraints.
--  If p_table_name is null (i.e. not passed in when called), it generates
--  the SQL DLL for all tables.
--  If p_constraint_type is null (i.e. not passed in when called), it generates
--  the SQL DLL for all constraints.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_drop_constraints_ddl
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_constraint_type in all_constraints.constraint_type%type := null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_owner
   , p_table_name
   , p_constraint_type
   , p_dir
   , 'drop constraint'
   );

end gen_drop_constraints_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_disable_constraints_ddl
--
-- purpose: Generates DDL to disable schema table constraints.
--  If p_table_name is null (i.e. not passed in when called), it generates
--  the SQL DLL for all tables.
--  If p_constraint_type is null (i.e. not passed in when called), it generates
--  the SQL DLL for all constraints.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_disable_constraints_ddl
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_constraint_type in all_constraints.constraint_type%type := null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_owner
   , p_table_name
   , p_constraint_type
   , p_dir
   , 'disable constraint'
   );

end gen_disable_constraints_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_enable_constraints_ddl
--
-- purpose: Generates DDL to enable schema table constraints.
--  If p_table_name is null (i.e. not passed in when called), it generates
--  the SQL DLL for all tables.
--  If p_constraint_type is null (i.e. not passed in when called), it generates
--  the SQL DLL for all constraints.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_enable_constraints_ddl
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_constraint_type in all_constraints.constraint_type%type :=null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_owner
   , p_table_name
   , p_constraint_type
   , p_dir
   , 'enable constraint'
   );

end gen_enable_constraints_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_tab_triggers
--
-- purpose: Generates DDL to enable table triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all tables.
--
-- ---------------------------------------------------------------------------
--
procedure enable_tab_triggers
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_tab_triggers
 (
   p_owner
 , p_table_name
 , p_dir
 , 'enable'
 );

end enable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  disable_tab_triggers
--
-- purpose: Generates DDL to disable table triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all tables.
--
-- ---------------------------------------------------------------------------
--
procedure disable_tab_triggers
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_tab_triggers
 (
   p_owner
 , p_table_name
 , p_dir
 , 'disable'
 );

end disable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_triggers
--
-- purpose: Generates DDL to enable triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all triggers.
--
-- ---------------------------------------------------------------------------
--
procedure enable_triggers
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_triggers
 (
   p_owner
 , p_table_name
 , p_dir
 , 'enable'
 );

end enable_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  disable_triggers
--
-- purpose: Generates DDL to disable triggers.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all triggers.
--
-- ---------------------------------------------------------------------------
--
procedure disable_triggers
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_triggers
 (
   p_owner
 , p_table_name
 , p_dir
 , 'disable'
 );

end disable_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  truncate_tables
--
-- purpose: Generates DDL to truncate tables.
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, does it for all tables.
--
-- ---------------------------------------------------------------------------
--
procedure truncate_tables
(
  p_owner in all_tables.owner%type 
, p_table_name in all_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
  l_owner all_tables.owner%type := upper(p_owner);
  l_table_name all_tables.table_name%type := upper(p_table_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);
begin
  
  if (p_dir is not null) then
    l_filename := 'truncate_tables.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select owner, table_name
    from all_tables
    where table_name = decode(l_table_name, null, table_name, l_table_name)
      and owner = l_owner
    order by table_name
  ) 
  loop
    l_str := 'truncate table ' || rec.owner || '.' || rec.table_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;

end truncate_tables;

-- -----------------------------------------------------------------------------
--
-- function: get_token
--
-- purpose: Returns a token in a delimited string.
--
-- -----------------------------------------------------------------------------
--
function get_token
(
  p_str varchar2
, p_delim varchar2
, p_num number
) 
return varchar2
is
  l_str varchar2(4000);
  l_token varchar2(4000);
begin
  l_str := p_delim || p_str || p_delim;

  l_token :=
    substr
    (
      l_str
    , instr(l_str, p_delim, 1, p_num) + 1
    , instr(l_str, p_delim, 1, p_num + 1) - instr(l_str, p_delim, 1, p_num) - 1
    );

  return l_token; 
  
end get_token;

-- -----------------------------------------------------------------------------
--
-- function:  get_long.fnc
--
-- purpose: Gets a long column value
--
-- description:  Extracts a long column value and returns a varchar2;
--               gets up to 4000 characters the max varchar2 size.
--
-- -----------------------------------------------------------------------------
--
function get_long
(
  p_table_name in varchar2
, p_column in varchar2
, p_rowid in rowid
)
return varchar2
is
  l_BUFSIZE constant integer := 4000;
  l_id   integer;
  l_str  varchar2(4000);
  l_len integer;
begin
  l_id := dbms_sql.open_cursor;
  dbms_sql.parse
  (
    l_id
  , 'select ' || p_column || ' from ' || p_table_name || ' where rowid = :x'
  , dbms_sql.native
  );
  dbms_sql.bind_variable(l_id, ':x', p_rowid);
  dbms_sql.define_column_long(l_id, 1);
  if (dbms_sql.execute_and_fetch(l_id) > 0) then
    dbms_sql.column_value_long(l_id, 1, l_BUFSIZE, 0 , l_str, l_len);
  end if;
  dbms_sql.close_cursor(l_id);

  return l_str;
  
end get_long;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_plsql_code
--
-- purpose: Generates procedure, function, package, and trigger code.
--
-- If p_type is null, it does all PL/SQL objects types. If p_name is null, it
-- does all objects within the type.
--
-- ---------------------------------------------------------------------------
--
procedure gen_plsql_code
(
  p_dir in varchar2
, p_owner in all_source.owner%type  
, p_type in all_source.type%type := null
, p_name in all_source.name%type := null  
)
is
  l_owner all_source.owner%type := upper(p_owner);
  l_type all_source.type%type := upper(p_type);
  l_name all_source.name%type := upper(p_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_bytes number;
  l_line_len number;
  l_BUFFER_LIMIT constant number := 32767;
begin

  for proc in 
  (
    select distinct 
      owner
    , type
    , name
    , name || '.' || 
      case type 
      when 'PACKAGE' then 'pks'
      when 'PACKAGE BODY' then 'pkb'
      when 'PROCEDURE' then 'prc'
      when 'FUNCTION' then 'fnc'
      when 'TRIGGER' then 'tri'
      when 'TYPE' then 'obs'
      when 'TYPE' then 'obb'
      end as filename
    from all_source
    where type = decode(l_type, null, type, l_type)
      and name = decode(l_name, null, name, l_name)
      and owner = l_owner
    order by type, name
  )
  loop
    
    l_filename := proc.filename;
    l_utl_handle := utl_file.fopen
                    (
                      location => p_dir
                    , filename => l_filename
                    , open_mode => 'w'
                    );
    dbms_output.put_line('Writing out ' || l_filename);
    
    utl_file.put(l_utl_handle, 'create or replace ');
    l_bytes := length('create or replace ');
    
    for line in
    (
      select text
      from all_source
      where type = proc.type
        and name = proc.name
        and owner = proc.owner
      order by line
    )
    loop
      l_line_len := length(line.text);
      l_bytes := l_bytes + l_line_len;
      if (l_bytes > l_BUFFER_LIMIT) then
        utl_file.fflush(l_utl_handle);
        l_bytes := l_line_len;
      end if;
      utl_file.put(l_utl_handle, line.text);
    end loop;
    
    utl_file.new_line(l_utl_handle);
    utl_file.put_line(l_utl_handle, '/');
    utl_file.fclose(l_utl_handle);
      
  end loop;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
    
end gen_plsql_code;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_views
--
-- purpose: Generates views
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_view_name is null, it generates all views for the user.
--
-- ---------------------------------------------------------------------------
--
procedure gen_views
(
  p_owner in all_views.owner%type
, p_view_name in all_views.view_name%type := null  
, p_dir in varchar2 := null
)
is
  l_owner all_views.owner%type := upper(p_owner);
  l_view_name all_views.view_name%type := upper(p_view_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);  
begin

  for vw in 
  (
    select view_name ,text
    from all_views
    where view_name = decode(l_view_name, null, view_name, l_view_name)
      and owner = l_owner
  )
  loop
  
    if (p_dir is not null) then
      l_filename := vw.view_name || '.vw' ;
      l_utl_handle := utl_file.fopen
                      (
                        location => p_dir
                      , filename => l_filename
                      , open_mode => 'w'
                      , max_linesize => 32767
                      );
    end if;
    
    writeln(p_dir, l_utl_handle, 'create or replace view ' || vw.view_name);
    writeln(p_dir, l_utl_handle, 'as');
    writeln(p_dir, l_utl_handle, vw.text);
    writeln(p_dir, l_utl_handle, '/');
       
    if (p_dir is not null) then
      utl_file.fclose(l_utl_handle);
    end if;
      
  end loop;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
  
end gen_views;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_create_users
--
-- purpose: Generates users
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- Need direct select access on DBA_USERS view.
--
-- If p_username is null, it generates all users.
--
-- Note: This procedure will not work in 11g.  In 11g the password
--   column in dba_users is null.  You can get the password from sys.user$
--   via columns: password, spare4 (salt + other ...).
--
--   In 11g passwords are case sensitive, if using sys.user$ to change
--   users password via identified by values 'xyz', must use both
--   spare4 and password to retain password case sensitivity when resetting it.
--   If just use password it won't be case sensitive any longer.  This is the 
--   case even if database parameter sec_case_sensitive_logon=true.
--
--   So to change user password, issue this to retain case sensitive password:
--     alter user foo identified by 'spare4;password';
--   
--  Going forward, better to use DBMS_METADATA.GET_DDL function without going
--  to any other dictionary tables.
--
-- ---------------------------------------------------------------------------
--
procedure gen_create_users
(
  p_username in dba_users.username%type := null  
, p_dir in varchar2 := null
)
is
  l_username dba_users.username%type := upper(p_username);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);  
begin

  if (p_dir is not null) then
    l_utl_handle := utl_file.fopen(p_dir, 'create_users.sql', 'w');
  end if;

  for rec in
  (
    select username, password, default_tablespace, temporary_tablespace
    from dba_users
    where username = decode(l_username, null, username, l_username)
    order by username
  )
  loop
    writeln(p_dir, l_utl_handle, 'create user ' || rec.username);
    writeln(p_dir, l_utl_handle, ' identified by values ' || '''' || rec.password || '''');
    writeln(p_dir, l_utl_handle, ' default tablespace ' || rec.default_tablespace);
    writeln(p_dir, l_utl_handle, ' temporary tablespace ' || rec.temporary_tablespace);
    writeln(p_dir, l_utl_handle, '/');
  end loop;
  
  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
 
end gen_create_users;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_create_users2
--
-- purpose: Generates users
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_create_users2
(
  p_username in dba_users.username%type := null  
, p_dir in varchar2 := null
)
is
  l_username dba_users.username%type := upper(p_username);
  l_utl_handle utl_file.file_type;
begin

  for rec in
  (
    select username, dbms_metadata.get_ddl('USER', username) cr_user_ddl
    from dba_users
    where username = decode(l_username, null, username, l_username)
    order by username
  )
  loop
  
    if (p_dir is not null) then
      clob2file(p_dir, 'cr_user_' || rec.username || '.sql', rec.cr_user_ddl, true);
    else
      dbms_output.put_line(rec.cr_user_ddl);
    end if;
   
  end loop;
 
end gen_create_users2;

-- ---------------------------------------------------------------------------
--
-- procedure:  clob2file
--
-- purpose: Writes clob data passed in to file.
--
-- Note: If the clob has more than a stream of 32K of data without a newline,
-- UTL_FILE will generate a PL/SQL: numeric or value error with put_line, and
-- UTL_RAW.cast_to_raw will generate this error if input is > 32K.
-- So it won't work for clobs that don't have a newline at least every
-- 32K of data.
--
--
-- ---------------------------------------------------------------------------
--
procedure clob2file
(
  p_dir              in varchar2
, p_filename         in varchar2
, p_clob_data        in out nocopy clob
, p_add_slash_to_eof in boolean
)
is
  l_output utl_file.file_type;
  l_amt    integer := 32767;
  l_offset integer := 1;
  l_length number := nvl(dbms_lob.getlength(p_clob_data), 0);
begin

  l_output := utl_file.fopen(p_dir, p_filename, 'w', 32767);
  
  while ( l_offset < l_length ) loop
    utl_file.put(l_output, dbms_lob.substr(p_clob_data, l_amt, l_offset) );
    utl_file.fflush(l_output);
    l_offset := l_offset + l_amt;
  end loop;
  
  --utl_file.new_line(l_output);
  if (p_add_slash_to_eof) then
    utl_file.put_line(l_output, '/');
  end if;
  
  utl_file.fclose(l_output);
 
exception

  when others then
  
    if ( utl_file.is_open(l_output) ) then
      utl_file.fclose(l_output);
    end if;
    raise;
    
end clob2file;

-- ---------------------------------------------------------------------------
--
-- procedure:  clob2file
--
-- purpose: Writes clob data passed in to file.
--
-- Note: If the clob has more than a stream of 32K of data without a newline,
-- UTL_FILE will generate a PL/SQL: numeric or value error with put_line, and
-- UTL_RAW.cast_to_raw will generate this error if input is > 32K.
-- So it won't work for clobs that don't have a newline at least every
-- 32K of data.
--
-- Solution: first convert clob to blob, then just write it as it (i.e. as binary data).
--
-- ---------------------------------------------------------------------------
--
procedure clob2file
(
  p_dir       in varchar2
, p_filename  in varchar2
, p_clob_data in out nocopy clob
)
is 
  l_output_file utl_file.file_type;
  l_buffer_size constant pls_integer := 32767;
  l_buf         raw(32767);
  l_amt         pls_integer;
  l_offset      pls_integer;
  -- for converting clob to blob
  l_blob        blob;
  l_lob_len     number;
  l_dest_offset integer := 1;
  l_src_offset  integer := 1;
  l_lang        integer := dbms_lob.default_lang_ctx;
  l_warning     integer;
begin
  
  l_lob_len := dbms_lob.getlength(p_clob_data);
  dbms_lob.createtemporary(l_blob, TRUE);
  
  dbms_lob.converttoblob
  (
    dest_lob     => l_blob
  , src_clob     => p_clob_data
  , amount       => l_lob_len
  , dest_offset  => l_dest_offset
  , src_offset   => l_src_offset
  , blob_csid    => dbms_lob.default_csid
  , lang_context => l_lang
  , warning      => l_warning
  );
  
  l_output_file := utl_file.fopen
  (
    location  => p_dir
  , filename  => p_filename
  , open_mode => 'wb'              -- open in binary mode for writing
  , max_linesize => l_buffer_size
  );
  
  l_amt := l_buffer_size;
  l_offset := 1;
  
  while (l_amt >= l_buffer_size) loop
    
    dbms_lob.read
    (
      lob_loc => l_blob
    , amount  => l_amt
    , offset  => l_offset
    , buffer  => l_buf
    );
    l_offset := l_offset + l_amt;
    
    utl_file.put_raw
    (
      file   => l_output_file
    , buffer => l_buf
    );
    
    utl_file.fflush(l_output_file);
    
  end loop;
    
  utl_file.fclose(l_output_file);
  
exception

  when others then
  
    if ( utl_file.is_open(l_output_file) ) then
      utl_file.fclose(l_output_file);
    end if;
    raise;
  
end clob2file;  

-- ---------------------------------------------------------------------------
--
-- procedure:  xplan2file
--
-- purpose: Writes explain plan to file.
--
-- ---------------------------------------------------------------------------
--
procedure xplan2file
(
  p_dir              in varchar2
, p_filename         in varchar2
, p_sql_id           in varchar2
, p_sql_child_number in number
)
is
  l_utl_handle utl_file.file_type;
begin

  if (p_dir is not null) then
    l_utl_handle := utl_file.fopen(p_dir, p_filename, 'w', 32767);
  end if;

  for rec in
  (
    select * from table(dbms_xplan.display_cursor(p_sql_id, p_sql_child_number))
  )
  loop
    writeln(p_dir, l_utl_handle, rec.plan_table_output);
  end loop;
  
  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;
  
exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;
    raise;
  
end xplan2file;

-- ---------------------------------------------------------------------------
--
-- procedure:  vsqlarea2file
--
-- purpose: Writes sql statements in v$sqlarea to a file.  Each sql statement
-- will go into its own file.  File will be named as follows:
--   osuser_orauser_sqlid.sql
--
-- Note: must have explicit select on v$sqlarea (v_$sqlarea) and v$session.
--
-- ---------------------------------------------------------------------------
--
procedure vsqlarea2file
(
  p_dir       in varchar2
, p_username  in varchar2 := null
)
is
begin
  
  for rec in
  (
    select
      s.osuser
    , s.username
    , s.sql_id
    , s.sql_child_number
    , a.sql_fulltext
    from
      v$sqlarea  a
      inner join
      v$session s
      on s.sql_hash_value = a.hash_value
      and s.sql_address = a.address
    where s.username is not null
      and s.username = nvl(p_username, s.username)
  )
  loop
  
    clob2file
    (
      p_dir       => p_dir
    , p_filename  => rec.osuser || '_' || rec.username || '_' || rec.sql_id || '.sql'
    , p_clob_data => rec.sql_fulltext
    );
    
    xplan2file
    (
      p_dir              => p_dir
    , p_filename         => rec.osuser || '_' || rec.username || '_' || rec.sql_id || '_xplan.sql'
    , p_sql_id           => rec.sql_id
    , p_sql_child_number => rec.sql_child_number
    );
    
  end loop;

end vsqlarea2file;

-- -----------------------------------------------------------------------------
--
-- function: delim_str
--
-- purpose: Returns the string of characters delimited by specified deliminator,
--          defaults to using a comma if not specified.
--
-- -----------------------------------------------------------------------------
--
function delim_str
(
  p_str varchar2
, p_delim varchar2 := ','
)
return varchar2
is

  l_len pls_integer := length(p_str);
  l_cnt pls_integer := 1;
  l_delim varchar2(1) := null;
  l_result varchar2(32767) := null;
  
begin

  while (l_cnt <= l_len) loop
    l_result := l_result || l_delim || substr(p_str, l_cnt, 1);
    l_cnt := l_cnt + 1;
    l_delim := p_delim;
  end loop;
    
  return l_result;
  
end delim_str;

-- -----------------------------------------------------------------------------
--
-- function: gen_permutations
--
-- purpose: Returns all the permutations of size specified for characters in
--          specified domain.  If no domain specified, defaults to: alphabetic
--          characters (lower case), digits, and special characters (_, #, $).
--
-- -----------------------------------------------------------------------------
--
function gen_permutations
(
  p_num_of_chars integer
, p_char_domain  varchar2 := null
)
return char_array_t pipelined
is
  l_char_domain   varchar2(500);
  l_select_clause varchar2(1000) := 'select';
  l_from_clause   varchar2(1000) := 'from ';
  l_concat_op     varchar2(2)    := ' ';
  l_join          varchar2(20)   := ' ';
  l_sqlstmt       varchar2(4000);
  
  l_cur           pls_integer;
  l_desc_tab      dbms_sql.desc_tab;
  l_num_cols      pls_integer;
  l_col_value     varchar2(4000);
  l_retcode       pls_integer;
begin

  if (p_char_domain is null) then
    l_char_domain := delim_str(g_ALPHA, ',') || ',' || delim_str(g_DIGIT, ',') || ',' || delim_str(g_SPECIAL);
  else
    l_char_domain := delim_str(p_char_domain, ',');
  end if;

  
  for i in 1 .. p_num_of_chars loop
    l_select_clause := l_select_clause || l_concat_op || 't' || i || '.column_value';
    l_concat_op := '||';
  end loop;
  l_select_clause := l_select_clause || ' as perm ';
  
  for i in 1 .. p_num_of_chars loop
    l_from_clause := l_from_clause || l_join || 'table(jmdba.jmh_util_pkg.list_to_rows(''' || l_char_domain || ''')) t' || i || ' ';
    l_join := ' cross join ';
  end loop;
  
  l_sqlstmt := l_select_clause || l_from_clause;
  
  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, l_sqlstmt, dbms_sql.native);
  dbms_sql.define_column(l_cur, 1, l_col_value, 4000);
  
  l_retcode := dbms_sql.execute(l_cur);
  
  while (dbms_sql.fetch_rows(l_cur) > 0) loop
    dbms_sql.column_value(l_cur, 1, l_col_value);
    pipe row (l_col_value);
  end loop;
  dbms_sql.close_cursor(l_cur);
  
  return;

end gen_permutations;

-- ---------------------------------------------------------------------------
--
-- procedure:  put_blob
--
-- purpose: Writes an OS file to blob column in table.  Assumes that record has
--   been inserted into table with blob column populating non-blob columns.
--   The query (p_query parameter) should update blob column setting it
--   equal to emptry_blob() returning the blob column into bind variable.
--
-- For example, assume a table called blob_table with a blob column called
-- blob_col:
--
-- p_query = update blob_table 
--             set blob_col = empty_blob()
--           where primary_key_col = primary_key_val
--           returning blob_col into :1
--
-- ---------------------------------------------------------------------------
--
procedure put_blob
(
  p_query    in varchar2
, p_dir      in varchar2
, p_filename in varchar2
)
is
  l_blob_loc blob;
  l_bfile_handle bfile := bfilename(p_dir, p_filename);
begin
  
  execute immediate p_query returning into l_blob_loc;
  
  dbms_lob.fileopen(l_bfile_handle);
  dbms_lob.loadfromfile(l_blob_loc, l_bfile_handle, dbms_lob.getlength(l_bfile_handle));
  dbms_lob.fileclose(l_bfile_handle);
  commit;

exception
  
  when others then
    
    if (dbms_lob.isopen(l_bfile_handle) = 1) then
      dbms_lob.fileclose(l_bfile_handle);
    end if;
    raise;
    
end put_blob;

-- ---------------------------------------------------------------------------
--
-- procedure:  get_blob
--
-- purpose: Extracts blob column from table and writes it to OS file.
--
-- Parameter p_query is the SQL query to extract blob data.
--
-- For example, assume a table called blob_table with a blob column called
-- blob_col:
--
-- p_query = select blob_col from blob_table
--           where primary_key_col = primary_key_val
--
-- ---------------------------------------------------------------------------
--
procedure get_blob
(
  p_query    in varchar2
, p_dir      in varchar2
, p_filename in varchar2
)
is
  LINE_SIZE   constant integer := 32767;
  l_handle    utl_file.file_type;
  l_buffer    raw(32767);
  l_amount    binary_integer := LINE_SIZE;
  l_pos       integer := 1;
  l_blob_loc  blob;
  l_blob_len  integer;
begin

  -- Get LOB locator
  execute immediate p_query into l_blob_loc;
  
  l_blob_len := dbms_lob.getlength(l_blob_loc);
    
  l_handle := utl_file.fopen
              (
                location  => p_dir
              , filename  => p_filename
              , open_mode => 'wb'       -- binary mode write
              , max_linesize => LINE_SIZE
              );

  -- Read chunks of the BLOB and write them to the file
  while l_pos < l_blob_len loop
    dbms_lob.read (l_blob_loc, l_amount, l_pos, l_buffer);
    utl_file.put_raw(l_handle, l_buffer, true);
    l_pos := l_pos + l_amount;
  end loop;
  
  utl_file.fclose(l_handle);

exception

  when others then
  
    if ( utl_file.is_open(l_handle) ) then
      utl_file.fclose(l_handle);
    end if;
    raise;
 
end get_blob;

-- ---------------------------------------------------------------------------
--
-- function: replace_clob
--
-- purpose: Replaces a string inside a clob.  The source clob (p_src_clob) is 
--   passed in and it returns a new clob with p_what replaced by p_with.
--
-- ---------------------------------------------------------------------------
function replace_clob
(
  p_src_clob in clob
, p_what     in varchar2
, p_with     in varchar2
)
return clob
as
  l_new_clob clob := empty_clob();
  l_src_pos  number;
  l_offset   number := 1;
  l_len_what pls_integer := length(p_what);
  l_len_with pls_integer := length(p_with);
  l_first    boolean := true;
begin
  
  dbms_lob.createtemporary(l_new_clob, true);
  
  loop
  
    l_src_pos := dbms_lob.instr(p_src_clob, p_what, l_offset);
       
    if (nvl(l_src_pos, 0) = 0 and l_first) then
      return p_src_clob;
    elsif (nvl(l_src_pos, 0) = 0) then
	    exit;
    end if;
    
    -- copy data preceding string matched from source to dest
    if (l_src_pos != 1) then
      dbms_lob.copy
      (
        dest_lob    => l_new_clob
      , src_lob     => p_src_clob
      , amount      => l_src_pos - l_offset 
      , dest_offset => dbms_lob.getlength(l_new_clob) + 1
      , src_offset  => l_offset
      );
    end if;
    -- add replacement string to dest       
    dbms_lob.write
    (
      lob_loc => l_new_clob
    , amount  => l_len_with
    , offset  => dbms_lob.getlength(l_new_clob) + 1
    , buffer  => p_with
    );
           
    l_offset := l_src_pos + l_len_what;
    l_first := false;
      
  end loop;
  
  -- copy any remaining characters
  if (l_offset <= dbms_lob.getlength(p_src_clob)) then
    dbms_lob.copy
    (
      dest_lob    => l_new_clob
    , src_lob     => p_src_clob
    , amount      => dbms_lob.getlength(p_src_clob) - l_offset + 1
    , dest_offset => dbms_lob.getlength(l_new_clob) + 1
    , src_offset  => l_offset
    );
  end if;
    
  return l_new_clob;

end replace_clob;

-- ---------------------------------------------------------------------------
--
-- procedure:  send_email
--
-- purpose: Sends an email.
--
-- Note: Need to install the following as sys first:
--
-- SQL>$ORACLE_HOME/rdbms/admin/utlmail.sql
-- SQL>$ORACLE_HOME/rdbms/admin/prvtmail.plb
-- 
-- Grant privileges:

-- SQL>grant execute on utl_mail to public;
--
-- In 11g, need to setup ACL (Access control lists), as sys run:
--
-- create ACL
--
-- begin 
--   dbms_network_acl_admin.create_acl
--   (  
--     acl         => 'utl_mail.xml',  
--     description => 'Allow mail to be send',  
--     principal   => 'JMDBA',  
--     is_grant    => TRUE,  
--     privilege   => 'connect' 
--   );  
--   commit;  
-- end; 
--
-- Add Privilege
--begin 
--  dbms_network_acl_admin.add_privilege
--  (  
--    acl       => 'utl_mail.xml',  
--    principal => 'JMDBA',  
--    is_grant  => TRUE,  
--    privilege => 'resolve' 
--  );  
--  commit;  
--end; 
--
-- Assign ACL
--begin 
--  dbms_network_acl_admin.assign_acl  
--  (
--    acl  => 'utl_mail.xml',  
--    host => 'mr1.hsys.local' 
--  );  
--  commit;  
--end; 
--
-- ---------------------------------------------------------------------------
procedure send_email
(
  p_sender          in varchar2
, p_recipients      in varchar2
, p_subject         in varchar2
, p_message         in varchar2
, p_smtp_out_server in varchar2 default 'mr1.hsys.local'
, p_cc              in varchar2 default null
, p_bcc             in varchar2 default null
, p_mime_type       in varchar2 default 'text; charset=us-ascii'
)
is
begin

 -- Can also set parameter smtp_out_server in spfile
  execute immediate 'alter session set smtp_out_server =''' || p_smtp_out_server || '''';

  utl_mail.send
  (
    sender     => p_sender
  , recipients => p_recipients
  , cc         => p_cc
  , bcc        => p_bcc
  , subject    => p_subject
  , message    => p_message
  , mime_type  => p_mime_type
  , priority   => null
  );

end send_email;

-- ---------------------------------------------------------------------------
--
-- procedure:  send_email_attach_32k
--
-- purpose: Sends an email with an attachment. The attachment cannot be
--          more than 32K in size.
--
-- ---------------------------------------------------------------------------
procedure send_email_attach_32k
(
  p_sender          in varchar2
, p_recipients      in varchar2
, p_subject         in varchar2
, p_message         in varchar2
, p_dir             in varchar2
, p_filename        in varchar2
, p_smtp_out_server in varchar2 default 'mr1.hsys.local'
, p_cc              in varchar2 default null
, p_bcc             in varchar2 default null
)
is
  l_handle utl_file.file_type;
  l_rawfile raw(32767);
  l_size    number;
  l_block   number;
  l_bool    boolean;
begin

  l_handle := utl_file.fopen(p_dir, p_filename, 'rb');
  utl_file.fgetattr(p_dir, p_filename, l_bool, l_size, l_block);
  utl_file.get_raw(l_handle, l_rawfile, l_size);
  utl_file.fclose(l_handle);

  -- Can also set parameter smtp_out_server in spfile
  execute immediate 'alter session set smtp_out_server =''' || p_smtp_out_server || '''';

  utl_mail.send_attach_raw
  (
    sender       => p_sender
  , recipients   => p_recipients
  , cc           => p_cc
  , bcc          => p_bcc
  , subject      => p_subject
  , message      => p_message
  , attachment   => l_rawfile
  , att_inline   => false
  , att_filename => p_filename
  );
  
end send_email_attach_32k;

-- ---------------------------------------------------------------------------
--
-- procedure:  send_email_text_attach_gt_32k
--
-- purpose: Sends an email with an attachment of any size. The attachment cannot be
--          a binary file.
--
-- ---------------------------------------------------------------------------
procedure send_email_text_attach_gt_32k
(
  p_sender      in varchar2
, p_recipients  in varchar2
, p_subject     in varchar2
, p_message     in varchar2
, p_dir         in varchar2
, p_filename    in varchar2
, p_smtp_host   in varchar2 default 'mr1.hsys.local'
, p_smtp_port   in number   default 25
)
is

  l_mail_conn   utl_smtp.connection;
  l_boundary    varchar2(50) := '----=*#abc1234321cba#*=';
  l_loc         bfile;
  l_amount      pls_integer;
  l_offset      pls_integer;
  l_buffer      raw(32767);
  l_buffer_size pls_integer := 32767;
  
begin

  l_mail_conn := utl_smtp.open_connection(p_smtp_host, p_smtp_port);
  utl_smtp.helo(l_mail_conn, p_smtp_host);
  utl_smtp.mail(l_mail_conn, p_sender);
  utl_smtp.rcpt(l_mail_conn, p_recipients);

  utl_smtp.open_data(l_mail_conn);
  
  utl_smtp.write_data(l_mail_conn, 'Date: ' || to_char(sysdate, 'dd-mon-yyyy hh24:mi:ss') || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'To: ' || p_recipients || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'From: ' || p_sender || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Subject: ' || p_subject || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Reply-To: ' || p_sender || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'MIME-Version: 1.0' || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Content-Type: multipart/mixed; boundary="' || l_boundary || '"' || utl_tcp.crlf || utl_tcp.crlf);
  
  utl_smtp.write_data(l_mail_conn, '--' || l_boundary || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Content-Type: text/plain; charset="iso-8859-1"' || utl_tcp.crlf || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, p_message);
  utl_smtp.write_data(l_mail_conn, utl_tcp.crlf || utl_tcp.crlf);
  
  utl_smtp.write_data(l_mail_conn, '--' || l_boundary || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Content-Type: ' || 'text/plain'  || '; name="' || p_filename || '"' || utl_tcp.crlf);
  --utl_smtp.write_data(l_mail_conn, 'Content-Transfer-Encoding: base64' || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn, 'Content-Disposition: attachment; filename="' || p_filename || '"' || utl_tcp.crlf || utl_tcp.crlf);
   
  l_loc := bfilename(p_dir, p_filename);
  
  if (dbms_lob.fileexists(l_loc) = 1) then
  
    l_amount := l_buffer_size;
    l_offset := 1;
    
    dbms_lob.open(l_loc, dbms_lob.lob_readonly);
    
    while (l_amount >= l_buffer_size) loop
    
      dbms_lob.read
      (
        file_loc => l_loc
      , amount   => l_amount
      , offset   => l_offset
      , buffer   => l_buffer
      );
      l_offset := l_offset + l_amount;
        
			--utl_smtp.write_raw_data(l_mail_conn, l_buffer);
			utl_smtp.write_data(l_mail_conn, utl_raw.cast_to_varchar2(l_buffer));
			            
    end loop;
    
    dbms_lob.close(l_loc);
    
  end if;
  
  utl_smtp.write_data(l_mail_conn, utl_tcp.crlf);
  
  utl_smtp.close_data(l_mail_conn);
  utl_smtp.quit(l_mail_conn);
  
end send_email_text_attach_gt_32k;

-- ---------------------------------------------------------------------------
--
-- procedure:  send_email_bin_attach_gt_32k
--
-- purpose: Sends an email with an attachment of any size. The attachment can be
--          a binary file or a text file.
--
-- ---------------------------------------------------------------------------
procedure send_email_bin_attach_gt_32k
(
  p_sender      in varchar2
, p_recipients  in varchar2
, p_subject     in varchar2
, p_message     in varchar2
, p_dir         in varchar2
, p_filename    in varchar2
, p_smtp_host   in varchar2 default 'mr1.hsys.local'
, p_smtp_port   in number   default 25
)
is
  l_src_loc  bfile := bfilename(p_dir, p_filename);
  l_buffer   raw(54);
  l_buff_amt integer := 54;
  l_pos      integer := 1;
  l_blob     blob := empty_blob;
  l_blob_len integer;
  l_size     integer;
 
  l_conn_handle        utl_smtp.connection;
  
  procedure send_header(p_name in varchar2, p_header in varchar2) as
  begin
    utl_smtp.write_data(l_conn_handle, p_name || ': ' || p_header || utl_tcp.crlf);
  end;
   
begin

  -- preparing the lob from file for attachment
  dbms_lob.open(l_src_loc, dbms_lob.lob_readonly);   --read the file
  dbms_lob.createtemporary(l_blob, true);            --create temporary lob to store the file.
  l_size := dbms_lob.getlength(l_src_loc);           --amount to store.
  dbms_lob.loadfromfile(l_blob, l_src_loc, l_size);  -- loading from file into temporary lob
  l_blob_len := dbms_lob.getlength(l_blob);
 
  -- UTL_SMTP related coding
  l_conn_handle := utl_smtp.open_connection(p_smtp_host);
  utl_smtp.helo(l_conn_handle, p_smtp_host);
  utl_smtp.mail(l_conn_handle, p_sender);
  utl_smtp.rcpt(l_conn_handle, p_recipients);
  utl_smtp.open_data(l_conn_handle);
  send_header('From', p_sender);
  send_header('To', p_recipients);
  send_header('Subject', p_subject);
  
  --MIME header.
  utl_smtp.write_data(l_conn_handle, 'MIME-Version: 1.0' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, 'Content-Type: multipart/mixed; ' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, ' boundary= "' || 'xyz.SECBOUND' || '"' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
   
  -- mail body
  utl_smtp.write_data(l_conn_handle, '--' || 'xyz.SECBOUND' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, 'Content-Type: text/plain;' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, ' charset=US-ASCII' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, p_message || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
   
  -- mail attachment
  utl_smtp.write_data(l_conn_handle, '--' || 'xyz.SECBOUND' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, 'Content-Type: application/octet-stream' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, 'Content-Disposition: attachment; ' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, ' filename="' || p_filename || '"' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, 'Content-Transfer-Encoding: base64' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
  
  -- Writing the blob in chunks
  while l_pos < l_blob_len loop
    dbms_lob.read(l_blob, l_buff_amt, l_pos, l_buffer);
    utl_smtp.write_raw_data(l_conn_handle, utl_encode.base64_encode(l_buffer));
    utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
    l_buffer := null;
    l_pos    := l_pos + l_buff_amt;
  end loop;
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf);
   
  -- Close Email
  utl_smtp.write_data(l_conn_handle, '--' || 'xyz.SECBOUND' || '--' || utl_tcp.crlf);
  utl_smtp.write_data(l_conn_handle, utl_tcp.crlf || '.' || utl_tcp.crlf);
   
  utl_smtp.close_data(l_conn_handle);
  utl_smtp.quit(l_conn_handle);
  dbms_lob.freetemporary(l_blob);
  dbms_lob.fileclose(l_src_loc);
     
end send_email_bin_attach_gt_32k;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_ext_tab_ddl
--
-- purpose: Outputs an external table definition based on passed in query.
--
-- ---------------------------------------------------------------------------
--
procedure gen_ext_tab_ddl
(
  p_query       in varchar2
, p_delim       in varchar2
, p_table_name  in varchar2
, p_dir         in varchar2
, p_files       in varchar2
, p_date_format in varchar2 := 'mm-dd-yyyy hh24:mi:ss'
)
is
  l_utl_handle utl_file.file_type;
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
  l_comma varchar2(10) := ' ';
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);
    
  l_utl_handle := utl_file.fopen(p_dir, p_table_name || '.sql', 'w');

  utl_file.put_line(l_utl_handle, 'create table ' || p_table_name);
  utl_file.put_line(l_utl_handle, '(');

  -- output columns and the datatypes   
  for i in 1 .. l_desc_tab.count loop
    if (l_desc_tab(i).col_type = 12) then
      utl_file.put_line
      (
        l_utl_handle
       , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' date '
      );
    elsif (l_desc_tab(i).col_type = 2) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' number' 
      );
    else
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' varchar2(' || to_char(l_desc_tab(i).col_max_len) || ')' 
      );
    end if;
    l_comma := ',';
  end loop;

  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'organization external');
  utl_file.put_line(l_utl_handle, '(');
  utl_file.put_line(l_utl_handle, '  type oracle_loader');
  utl_file.put_line(l_utl_handle, '  default directory ' || p_dir);
  utl_file.put_line(l_utl_handle, '  access parameters');
  utl_file.put_line(l_utl_handle, '  (');
  utl_file.put_line(l_utl_handle, '    records delimited by newline');
  utl_file.put_line(l_utl_handle, '    nologfile');
  utl_file.put_line(l_utl_handle, '    nodiscardfile');
  utl_file.put_line(l_utl_handle, '    fields terminated by ''' || p_delim || '''');
  utl_file.put_line(l_utl_handle, '    missing field values are null');
  utl_file.put_line(l_utl_handle, '    reject rows with all null');
  utl_file.put_line(l_utl_handle, '    fields');
  utl_file.put_line(l_utl_handle, '    (');
  
  l_comma := ' ';
  for i in 1 .. l_desc_tab.count loop
    if (l_desc_tab(i).col_type = 12) then
      utl_file.put_line
      (
        l_utl_handle
      , '    ' || l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' date "' || p_date_format || '"'
      );
    else
      utl_file.put_line
      (
        l_utl_handle
      , '    ' || l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) 
      );
    end if;
    l_comma := ',';
  end loop;
  
  utl_file.put_line(l_utl_handle, '    )');
  utl_file.put_line(l_utl_handle, '  )');
  utl_file.put_line(l_utl_handle, '  location (''' || p_files || ''')');
  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'reject limit unlimited');
  utl_file.put_line(l_utl_handle, 'parallel');
  utl_file.put_line(l_utl_handle, ';');

  utl_file.fclose(l_utl_handle);

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;  
    raise;
    
end gen_ext_tab_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_dp_ext_tab_ddl
--
-- purpose: Outputs 2 external data pump table definition based on passed in query.
--
-- ---------------------------------------------------------------------------
--
procedure gen_dp_ext_tab_ddl
(
  p_query       in varchar2
, p_table_name  in varchar2
, p_files       in varchar2
, p_src_dir_obj in varchar2
, p_dst_dir_obj in varchar2
)
is
  l_utl_handle utl_file.file_type;
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
  l_col_value varchar2(4000);
  l_retcode pls_integer;
  l_comma varchar2(10) := ' ';
begin

  -- Create external datapump table for source table unload
  l_utl_handle := utl_file.fopen(p_src_dir_obj, 'src_' || p_table_name || '.sql', 'w');
  
  utl_file.put_line(l_utl_handle, 'create table ' || p_table_name);
  utl_file.put_line(l_utl_handle, 'organization external');
  utl_file.put_line(l_utl_handle, '(');
  utl_file.put_line(l_utl_handle, '  type oracle_datapump');
  utl_file.put_line(l_utl_handle, '  default directory ' || upper(p_src_dir_obj));
  utl_file.put_line(l_utl_handle, '  access parameters (nologfile)');
  utl_file.put_line(l_utl_handle, '  location (''' || p_files || ''')');
  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'parallel');
  utl_file.put_line(l_utl_handle, 'as');
  utl_file.put_line(l_utl_handle, p_query);
  utl_file.put_line(l_utl_handle, ';');
  
  utl_file.fclose(l_utl_handle);
  
  -- Create external datapump table for destination table load
  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);
    
  l_utl_handle := utl_file.fopen(p_src_dir_obj, 'dst_' || p_table_name || '.sql', 'w');

  utl_file.put_line(l_utl_handle, 'create table ' || p_table_name);
  utl_file.put_line(l_utl_handle, '(');

  -- output columns and the datatypes   
  for i in 1 .. l_desc_tab.count loop
    if (l_desc_tab(i).col_type = 12) then
      utl_file.put_line
      (
        l_utl_handle
       , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' date '
      );
    elsif (l_desc_tab(i).col_type = 2) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' number' 
      );
    elsif (l_desc_tab(i).col_type = 1) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' varchar2(' || to_char(l_desc_tab(i).col_max_len) || ')' 
      );
    elsif (l_desc_tab(i).col_type = 96) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' char(' || to_char(l_desc_tab(i).col_max_len) || ')' 
      );
    elsif (l_desc_tab(i).col_type = 180) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' timestamp'
      );
    elsif (l_desc_tab(i).col_type = 113) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' blob'
      );
    elsif (l_desc_tab(i).col_type = 112) then
      utl_file.put_line
      (
        l_utl_handle
      , l_comma || ' ' || rpad(l_desc_tab(i).col_name, 30) || ' clob'
      );
    end if;
    l_comma := ',';
  end loop;

  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'organization external');
  utl_file.put_line(l_utl_handle, '(');
  utl_file.put_line(l_utl_handle, '  type oracle_datapump');
  utl_file.put_line(l_utl_handle, '  default directory ' || p_dst_dir_obj);
  utl_file.put_line(l_utl_handle, '  access parameters (nologfile)');
  utl_file.put_line(l_utl_handle, '  location (''' || p_files || ''')');
  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'reject limit unlimited');
  utl_file.put_line(l_utl_handle, 'parallel');
  utl_file.put_line(l_utl_handle, ';');

  utl_file.fclose(l_utl_handle);

exception

  when others then
  
    if ( utl_file.is_open(l_utl_handle) ) then
      utl_file.fclose(l_utl_handle);
    end if;  
    raise;

end gen_dp_ext_tab_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_sqlloader_ctl
--
-- purpose: Outputs a SQL*Loader control file.  You can set the date format
--   required by alter session set nls_date_format ...
--
-- ---------------------------------------------------------------------------
--
procedure gen_sqlloader_ctl
(
  p_query       in varchar2
, p_delim       in varchar2
, p_table_name  in varchar2
, p_dir         in varchar2
)
is
  l_cur pls_integer;
  l_desc_tab dbms_sql.desc_tab;
  l_num_cols pls_integer;
begin

  l_cur := dbms_sql.open_cursor;
  dbms_sql.parse(l_cur, p_query, dbms_sql.native);
  dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);
  
  output_controlfile(l_desc_tab, p_table_name, p_delim, p_dir);
  
  dbms_sql.close_cursor(l_cur);
    
end gen_sqlloader_ctl;

-- ---------------------------------------------------------------------------
--
-- function:  partition_table_by_rowid
--
-- purpose: Partitions a table by rowid, can be used to parallelize a query
--   against a table.  Based on query from Tom Kyte in Effective Oracle by Design.
--
-- ---------------------------------------------------------------------------
--
function partition_table_by_rowid
(
  p_owner          in varchar2
, p_table_name     in varchar2
, p_num_partitions in number
)
return partition_table_t pipelined
is

	cursor l_cur(p_own varchar2, p_tab varchar2, p_n number) is
	select
		grp
	, dbms_rowid.rowid_create(1, data_object_id, lo_fno,lo_block, 0)     min_rid
	, dbms_rowid.rowid_create(1, data_object_id, hi_fno,hi_block, 10000) max_rid
	from
		(
			select distinct
				grp
			, first_value(relative_fno)
				 over (partition by grp order by relative_fno, block_id rows between unbounded preceding and unbounded following) lo_fno
			, first_value(block_id)
				 over(partition by grp order by relative_fno, block_id rows between unbounded preceding and unbounded following)  lo_block
			, last_value(relative_fno)
				 over(partition by grp order by relative_fno, block_id rows between unbounded preceding and unbounded following)  hi_fno
			, last_value(block_id+blocks-1)
				 over(partition by grp order by relative_fno, block_id rows between unbounded preceding and unbounded following)  hi_block
			, sum(blocks) over (partition by grp) sum_blocks
			from
				(
					select
						relative_fno
					, block_id
					, blocks
					, trunc( (sum(blocks) over (order by relative_fno, block_id) - 0.01) / (sum(blocks) over () / p_n)) grp 
					from
						dba_extents 
					where segment_name = p_tab 
						and owner = p_own
				)
		)
		,
		(
			select
				data_object_id
			from
				all_objects
			where object_name = p_tab
				and owner = p_own
		)
	order by 1;  
	
begin

  for rec in l_cur(p_owner, p_table_name, p_num_partitions)
  loop
    pipe row(rec);
  end loop;

end partition_table_by_rowid;

-- ---------------------------------------------------------------------------
--
-- procedure: add_rownum2file
--
-- purpose: Adds a row number to a flat file.
--
-- ---------------------------------------------------------------------------
--
procedure add_rownum2file
(
  p_dir        in varchar2
, p_src_file   in varchar2
, p_dst_file   in varchar2
, p_delim      in varchar2
, p_col_name   in varchar2 := null
, p_col_header boolean := false
)
is

  l_in  utl_file.file_type;
  l_out utl_file.file_type;
  l_buffer varchar2(32767);
  l_rn pls_integer := 0;
  
begin

  l_in  := utl_file.fopen(p_dir, p_src_file, 'r', 32767);
  l_out := utl_file.fopen(p_dir, p_dst_file, 'w');
  
  if (p_col_header) then
    utl_file.get_line(l_in, l_buffer);
    utl_file.put_line(l_out, p_col_name || p_delim || l_buffer);
  end if;
  
  loop
  
    begin
      utl_file.get_line(l_in, l_buffer);
      
      l_rn := l_rn + 1;
      utl_file.put_line(l_out, l_rn || p_delim || l_buffer);
      
    exception
      when no_data_found then
        exit;
    end;
  
  end loop;
  
  utl_file.fclose(l_in);
  utl_file.fclose(l_out);
  
exception

  when others then
  
    if ( utl_file.is_open(l_in) ) then
      utl_file.fclose(l_in);
    end if;  
    
    if ( utl_file.is_open(l_out) ) then
      utl_file.fclose(l_out);
    end if;  
    
    raise;

end add_rownum2file;

-- ---------------------------------------------------------------------------
--
-- procedure:  put_clob
--
-- purpose: Writes an OS file to clob column in table.
--   The query (p_query parameter) should insert clob column setting it
--   equal to emptry_clob() returning the clob column into bind variable.
--
-- For example, assume a table called clob_table with a clob column called
-- clob_col:
--
-- p_query = insert into clob_table 
--           (non_clob_col, clob_col) 
--           values ('filename', empty_clob())
--           returning clob_col into :1
--
-- ---------------------------------------------------------------------------
--
procedure put_clob
(
  p_query    in varchar2
, p_dir      in varchar2
, p_filename in varchar2
)
is
  l_clob_loc clob;
  l_bfile_handle bfile := bfilename(p_dir, p_filename);
  l_warning number;
  l_dest_offset number := 1;
  l_src_offset number := 1;
  l_lang number := dbms_lob.default_lang_ctx;
begin
  
  execute immediate p_query returning into l_clob_loc;
  
  --dbms_lob.fileopen(l_bfile_handle);
  dbms_lob.open(l_bfile_handle);
  --dbms_lob.loadfromfile(l_clob_loc, l_bfile_handle, dbms_lob.getlength(l_bfile_handle));
  dbms_lob.loadclobfromfile
  (
    dest_lob     => l_clob_loc
  , src_bfile    => l_bfile_handle
  , amount       => dbms_lob.getlength(l_bfile_handle)
  , dest_offset  => l_dest_offset
  , src_offset   => l_src_offset
  , bfile_csid   => dbms_lob.default_csid
  , lang_context => l_lang
  , warning      => l_warning
  );
  dbms_lob.fileclose(l_bfile_handle);
  commit;

exception
  
  when others then
    
    if (dbms_lob.isopen(l_bfile_handle) = 1) then
      dbms_lob.fileclose(l_bfile_handle);
    end if;
    raise;
    
end put_clob;

-- ---------------------------------------------------------------------------
--
-- function:  call_webservice
--
-- purpose: Consumes a Webservice
--
-- The following should be ran by SYS to allow access control and permissions:
--
-- begin
--
-- dbms_network_acl_admin.drop_acl('webservice.xml');
--
-- dbms_network_acl_admin.create_acl
-- (
--   acl         => 'webservice.xml',
--   description => 'Allow webservice',
--   principal   => 'JMDBA',
--   is_grant    => TRUE,
--   privilege   => 'connect'
-- );
-- commit;
--
-- dbms_network_acl_admin.add_privilege
-- (
--   acl       => 'webservice.xml',
--   principal => 'JMDBA',
--   is_grant  => TRUE,
--   privilege => 'resolve'
-- );
-- commit;
--
-- dbms_network_acl_admin.assign_acl
-- (
--   acl  => 'webservice.xml',
--   host => '*'
-- );
-- commit;
--
-- end;
--
-- ---------------------------------------------------------------------------
--
function call_webservice
(
  p_url        in varchar2 := 'http://www.webservicex.net/currencyconvertor.asmx/ConversionRate'
, p_param_list in varchar2 := 'FromCurrency=EUR&ToCurrency=USD'
) 
return varchar2
is

  l_req         utl_http.req;
  l_resp        utl_http.resp;
  l_msg         varchar2(32767);
  l_entire_msg  varchar2(32767) := null;

begin

  -- Preparing the Request
  l_req  := utl_http.begin_request(url => p_url, method => 'POST');
  
  -- Set header attributes
  utl_http.set_header(l_req, 'Content-Type', 'application/x-www-form-urlencoded');
	utl_http.set_header(l_req, 'Content-Length', length(p_param_list));
	
	-- Set input parameters
	utl_http.write_text(l_req, p_param_list);

  -- Get response and obtain received value
  l_resp := utl_http.get_response(r => l_req);
  
  begin

     loop
       utl_http.read_text(r => l_resp, data => l_msg);
       l_entire_msg := l_entire_msg || l_msg;
     end loop;

  exception 

    when utl_http.end_of_body 
      then  null;

  end;
  
  utl_http.end_response(r => l_resp);

  return l_entire_msg;

end call_webservice;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_ext_tab_from_heap_tab
--
-- purpose: Generates an external table definition based on the heap table
--   passed in.
--
-- ---------------------------------------------------------------------------
procedure gen_ext_tab_from_heap_tab
(
  p_heap_owner     in varchar2
, p_heap_table     in varchar2
, p_ext_table      in varchar2
, p_dir_obj        in varchar2
, p_filename       in varchar2
, p_has_col_header in varchar2 := 'N'
, p_col_delim      in varchar2 := '|'
, p_row_delim      in varchar2 := 'newline'
, p_add_rec_num    in varchar2 := 'N'
)
is

  type char_array_t is table of all_tab_columns.column_name%type index by binary_integer;
  
  l_first_rec    boolean := true;
  l_col_def      varchar2(4000);
  l_tab_col      char_array_t;
  l_num          pls_integer := 1;
  l_sql_filename varchar2(500) := p_ext_table || '.sql';
  l_utl_handle utl_file.file_type;
  
begin

  l_utl_handle := utl_file.fopen(p_dir_obj, l_sql_filename, 'w');
  
  utl_file.put_line(l_utl_handle, 'create table ' || p_ext_table);
  utl_file.put_line(l_utl_handle, '(');
  
  for rec in 
  (
    select column_name
    from all_tab_columns
    where owner = upper(p_heap_owner)
      and table_name = upper(p_heap_table)
    order by column_id
  )
  loop 

    l_tab_col(l_num) := rec.column_name;
    l_num := l_num + 1;
    
    l_col_def := rpad(rec.column_name, 30, ' ') || ' varchar2(4000)';
    
    if (l_first_rec) then
      utl_file.put_line(l_utl_handle, '  ' || l_col_def);
    else
      utl_file.put_line(l_utl_handle, ', ' || l_col_def);
    end if;
    
    l_first_rec := false;
        
  end loop;
  
  if (upper(p_add_rec_num) = 'Y') then
    l_col_def := rpad('rec_num', 30, ' ') || ' number';
  end if;
  
  utl_file.put_line(l_utl_handle, ')');
  utl_file.put_line(l_utl_handle, 'organization external');
  utl_file.put_line(l_utl_handle, '(');
  utl_file.put_line(l_utl_handle, '  type oracle_loader');
  utl_file.put_line(l_utl_handle, '  default directory ' || upper(p_dir_obj));
  utl_file.put_line(l_utl_handle, '  access parameters');
  utl_file.put_line(l_utl_handle, '  (');
  utl_file.put_line(l_utl_handle, '    records delimited by ' || p_row_delim);
  if (p_has_col_header = 'Y') then
    utl_file.put_line(l_utl_handle, '    skip 1');
  end if;
  utl_file.put_line(l_utl_handle, '    nobadfile');
  utl_file.put_line(l_utl_handle, '    nodiscardfile');
  utl_file.put_line(l_utl_handle, '    nologfile');
  utl_file.put_line(l_utl_handle, '    fields terminated by ''' || p_col_delim || '''');
  utl_file.put_line(l_utl_handle, '    missing field values are null');
  utl_file.put_line(l_utl_handle, '    reject rows with all null fields');
  
  if (upper(p_add_rec_num) = 'Y') then
  
    utl_file.put_line(l_utl_handle, '    (');
    
    for i in 1 .. l_tab_col.count loop
         
     l_col_def := rpad(l_tab_col(i), 30, ' ');

      if (i = 1) then
        utl_file.put_line(l_utl_handle, '      ' || l_col_def);
      else
        utl_file.put_line(l_utl_handle, '    , ' || l_col_def);
      end if;
      
    end loop;
    
    l_col_def := rpad('rec_num', 30, ' ') || ' recnum';
    utl_file.put_line(l_utl_handle, '    , ' || l_col_def);
    utl_file.put_line(l_utl_handle, '    )');
    
  end if;
  
  utl_file.put_line(l_utl_handle, '  )');
	utl_file.put_line(l_utl_handle, '  location (''' || p_filename || ''')');
	utl_file.put_line(l_utl_handle, ')');
	utl_file.put_line(l_utl_handle, 'reject limit unlimited');
	utl_file.put_line(l_utl_handle, 'parallel');
  utl_file.put_line(l_utl_handle, ';');
  
  utl_file.fclose(l_utl_handle);
  
end gen_ext_tab_from_heap_tab;

end jmh_util_pkg;
/

show errors
