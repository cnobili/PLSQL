create or replace package body nobc_util 
as
--
-- ---------------------------------------------------------------------------
--
-- package:  nobc_util
--
-- purpose:  Misc general routines
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- ---------------------------------------------------------------------------

--
-- Private Module Variables
--

g_NEW_LINE constant varchar2(2) := '0A';

--
-- Private Methods
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
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- ---------------------------------------------------------------------------
--
function get_spid
--return v$process.spid%type
return varchar2
is
  --l_spid v$process.spid%type;
  l_spid varchar2(12);
begin

/*   select
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
  ; */

  return l_spid;

end get_spid;

-- ---------------------------------------------------------------------------
--
-- procedure:  writeln
--
-- purpose: Writes out a string.
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  l_delimiter varchar2(10) := ' ';
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
      , l_delimiter || ' ' || rpad(p_desc_tab(i).col_name, 30) || ' date "' || sys_context('userenv', 'nls_date_format') || '"'
      );
    else
      writeln
      (
        p_dir
      , l_utl_handle
      , l_delimiter || ' ' || rpad(p_desc_tab(i).col_name, 30)|| ' char(' || to_char(p_desc_tab(i).col_max_len) || ')' 
      );
    end if;
    l_delimiter := p_delim;
  end loop;

  writeln(p_dir, l_utl_handle, ')' || chr(10));

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

end output_controlfile;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_constraints_ddl
--
-- purpose: Generates DDL to drop, enable, disable table constraints.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_constraints_ddl
(
  p_table_name in user_tables.table_name%type := null
, p_constraint_type in user_constraints.constraint_type%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_table_name user_tables.table_name%type := upper(p_table_name);
  l_constraint_type user_constraints.constraint_type%type := upper(p_constraint_type);
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
    select table_name, constraint_type, constraint_name
    from user_constraints
    where table_name = decode(l_table_name, null, table_name, l_table_name)
      and constraint_type = decode(l_constraint_type, null, constraint_type, l_constraint_type)
    order by table_name, constraint_type, constraint_name
  ) 
  loop
    l_str := 'alter table ' || rec.table_name || ' ' || p_action || ' ' || rec.constraint_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

end gen_constraints_ddl;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_disable_tab_triggers
--
-- purpose: Generates DDL to enable or diable table triggers.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_table_name user_tables.table_name%type := upper(p_table_name);
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
    select table_name, trigger_name
    from user_triggers
    where table_name = decode(l_table_name, null, table_name, l_table_name)
    order by table_name, trigger_name
  ) 
  loop
    l_str := 'alter table ' || rec.table_name || ' ' || p_action || ' all triggers ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

end enable_disable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_disable_triggers
--
-- purpose: Generates DDL to enable or diable triggers.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
, p_action in varchar2
)
is
  l_table_name user_tables.table_name%type := upper(p_table_name);
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
    select table_name, trigger_name
    from user_triggers
    where table_name = decode(l_table_name, null, table_name, l_table_name)
    order by table_name, trigger_name
  ) 
  loop
    l_str := 'alter trigger ' || rec.trigger_name || ' ' || p_action || ' ;' ;
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

end enable_disable_triggers;

--
-- Public Methods
--

-- ---------------------------------------------------------------------------
--
-- function:  get_session_trace_filename
--
-- purpose: Gets the session's trace file.  You need to turn tracing on in 
--   your session first, i.e. alter session set sql_trace=true;
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- If p_spid is not passed in, it calls a private function to get the spid
-- from v$process, but the invoker will need direct select access on
-- v$process and v$session, select via a role will not work.
--
-- If you do not have select access on v$process and v$session granted
-- directly by sys, then pass in the spid when calling.
--
-- ---------------------------------------------------------------------------
--
function get_session_trace_filename (p_spid in varchar2 := null)
return varchar2
is
begin
  
  return sys_context('USERENV', 'INSTANCE_NAME') || '_ora_' || nvl(p_spid, get_spid) || '.trc' ;
  
end get_session_trace_filename;

-- ---------------------------------------------------------------------------
--
-- function:  display_file
--
-- purpose: Displays the contents of an OS file as a virtual table.  The file
--   must be in a directory that is referenced in by a Oracle directory object.
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
-- purpose: Generates a pivot rable of row numbers.
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- This could also be done via the following methods:
--   * Create an actual table and insert into it, but his requires a physical table.
--   * select level from dual connect by level <= :n
--   * select rownum from all_objects where rownum <= :n
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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory and filename are passed in, the output will go
--   to the screen, assuming you have set serveroutput on in your session.
--
--   If tablename is passed in it writes out a SQL*Loader controlfile.  You
--   can set the date format required by alter session set nls_date_format ...
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
  
end dump_resultset;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_drop_objects_ddl
--
-- purpose: Generates DDL to drop schema objects.
--   If p_object_name is null (i.e. not passed in when called), it generates
--   the SQL DDL to drop all objects for the passed in object_type.
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_drop_objects_ddl
(
  p_object_type in user_objects.object_type%type 
, p_object_name in user_objects.object_name%type := null
, p_dir in varchar2 := null
)
is
  l_object_type user_objects.object_type%type := upper(p_object_type);
  l_object_name user_objects.object_name%type := upper(p_object_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_str varchar2(500);
begin
  
  if (p_dir is not null) then
    l_filename := 'drop_' || l_object_type || '_ddl.sql' ;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
  end if;

  for rec in
  (
    select object_type, object_name
    from user_objects
    where object_type = l_object_type
      and object_name = decode(l_object_name, null, object_name, l_object_name)
    order by object_type, object_name
  ) 
  loop
    l_str := 'drop ' || rec.object_type || ' ' || rec.object_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_drop_constraints_ddl
(
  p_table_name in user_tables.table_name%type := null
, p_constraint_type in user_constraints.constraint_type%type := null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_table_name
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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_disable_constraints_ddl
(
  p_table_name in user_tables.table_name%type := null
, p_constraint_type in user_constraints.constraint_type%type := null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_table_name
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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  21 Apr 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_enable_constraints_ddl
(
  p_table_name in user_tables.table_name%type := null
, p_constraint_type in user_constraints.constraint_type%type :=null
, p_dir in varchar2 := null
)
is

begin

   gen_constraints_ddl
   (
     p_table_name
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

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_tab_triggers
 (
   p_table_name
 , p_dir
 , 'enable'
 );

end enable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  disable_tab_triggers
--
-- purpose: Generates DDL to disable table triggers.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_tab_triggers
 (
   p_table_name
 , p_dir
 , 'disable'
 );

end disable_tab_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  enable_triggers
--
-- purpose: Generates DDL to enable triggers.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_triggers
 (
   p_table_name
 , p_dir
 , 'enable'
 );

end enable_triggers;

-- ---------------------------------------------------------------------------
--
-- procedure:  disable_triggers
--
-- purpose: Generates DDL to disable triggers.

-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
begin

 enable_disable_triggers
 (
   p_table_name
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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  29 Apr 2008
-- author:  Craig Nobili
-- desc: original
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
  p_table_name in user_tables.table_name%type := null
, p_dir in varchar2 := null
)
is
  l_table_name user_tables.table_name%type := upper(p_table_name);
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
    select table_name
    from user_tables
    where table_name = decode(l_table_name, null, table_name, l_table_name)
    order by table_name
  ) 
  loop
    l_str := 'truncate table ' || rec.table_name || ' ;';
    writeln(p_dir, l_utl_handle, l_str);
  end loop;

  if (p_dir is not null) then
    utl_file.fclose(l_utl_handle);
  end if;

end truncate_tables;

-- -----------------------------------------------------------------------------
--
-- function: get_token
--
-- purpose: Returns a token in a delimited string.
--
-- -----------------------------------------------------------------------------
--
-- rev log
--
-- date: 02 May 2008
-- author: Craig Nobili
-- desc: original
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
-- -----------------------------------------------------------------------------
--
-- rev log
--
-- date:  02 May 2008
-- author:  Craig Nobili
-- org:  nslc pacific
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
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  02 May 2008
-- author:  Craig Nobili
-- desc: original
--
-- If p_type is null, it does all PL/SQL objects types. If p_name is null, it
-- does all objects within the type.
--
-- ---------------------------------------------------------------------------
--
procedure gen_plsql_code
(
  p_dir in varchar2
, p_type in user_source.type%type := null
, p_name in user_source.name%type := null  
)
is
  l_type user_source.type%type := upper(p_type);
  l_name user_source.name%type := upper(p_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);
  l_bytes number;
  l_line_len number;
  l_BUFFER_LIMIT constant number := 32767;
begin

  for proc in 
  (
    select distinct 
      type
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
    from user_source
    where type = decode(l_type, null, type, l_type)
      and name = decode(l_name, null, name, l_name)
    order by type, name
  )
  loop
    
    l_filename := proc.filename;
    l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
    dbms_output.put_line('Writing out ' || l_filename);
    
    utl_file.put(l_utl_handle, 'create or replace ');
    l_bytes := length('create or replace ');
    
    for line in
    (
      select text
      from user_source
      where type = proc.type
        and name = proc.name
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
    
end gen_plsql_code;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_views
--
-- purpose: Generates views
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  03 May 2008
-- author:  Craig Nobili
-- desc: original
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
  p_view_name in user_views.view_name%type := null  
, p_dir in varchar2 := null
)
is
  l_view_name user_views.view_name%type := upper(p_view_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);  
begin

  for vw in 
  (
    select view_name ,text
    from user_views
    where view_name = decode(l_view_name, null, view_name, l_view_name)
  )
  loop
  
    if (p_dir is not null) then
      l_filename := vw.view_name || '.vw' ;
      l_utl_handle := utl_file.fopen(p_dir, l_filename, 'w');
    end if;
    
    writeln(p_dir, l_utl_handle, 'create or replace view ' || vw.view_name);
    writeln(p_dir, l_utl_handle, 'as');
    writeln(p_dir, l_utl_handle, vw.text);
    writeln(p_dir, l_utl_handle, '/');
       
    if (p_dir is not null) then
      utl_file.fclose(l_utl_handle);
    end if;
      
  end loop;
  
end gen_views;

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_create_users
--
-- purpose: Generates users
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  04 May 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_username is null, it generates all users.
--
-- ---------------------------------------------------------------------------
--
/*
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
 
end gen_create_users;
*/

-- ---------------------------------------------------------------------------
--
-- procedure:  gen_indexes
--
-- purpose: Generates indexes.
--
-- ---------------------------------------------------------------------------
--
-- rev log
--
-- date:  25 May 2008
-- author:  Craig Nobili
-- desc: original
--
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- If p_table_name is null, it generates all indexes..
--
-- ---------------------------------------------------------------------------
--
procedure gen_indexes
(
  p_table_name in user_indexes.table_name%type := null  
, p_dir in varchar2 := null
)
is
  l_table_name user_indexes.table_name%type := upper(p_table_name);
  l_utl_handle utl_file.file_type;
  l_filename varchar2(255);  
  l_first_time boolean;
begin


  for t in
  (
    select distinct table_name
    from user_indexes
    where table_name = decode(l_table_name, null, table_name, l_table_name)
    order by table_name
  )
  loop
  
    if (p_dir is not null) then
      l_utl_handle := utl_file.fopen(p_dir, 'create_' || lower(t.table_name) || '_indexes.sql', 'w');
    end if;
  
    for tab in
    (
      select uniqueness, index_name, table_name, tablespace_name
      from user_indexes
      where table_name = t.table_name
      order by index_name
    )
    loop

      if (tab.uniqueness = 'UNIQUE') then
        writeln(p_dir, l_utl_handle, 'CREATE ' || tab.uniqueness || ' INDEX ' || tab.index_name);
      else
        writeln(p_dir, l_utl_handle, 'CREATE INDEX ' || tab.index_name);
      end if;
      writeln(p_dir, l_utl_handle, 'ON ' || tab.table_name);
      writeln(p_dir, l_utl_handle, '(');

      l_first_time := true;

      for idx in 
      (
        select column_name
        from user_ind_columns
        where table_name = tab.table_name
          and index_name = tab.index_name
        order by column_position
      )
      loop
        if (l_first_time) then
          writeln(p_dir, l_utl_handle, '  ' || idx.column_name);    
        else
          writeln(p_dir, l_utl_handle, ', ' || idx.column_name);    
        end if;

        l_first_time := false;
      end loop;

      writeln(p_dir, l_utl_handle, ')');
      writeln(p_dir, l_utl_handle, 'TABLESPACE ' || tab.tablespace_name);
      writeln(p_dir, l_utl_handle, '/');
      writeln(p_dir, l_utl_handle, '');

    end loop;

    if (p_dir is not null) then
      utl_file.fclose(l_utl_handle);
    end if;   
    
  end loop;
  
end gen_indexes;

end nobc_util;
/

show errors
