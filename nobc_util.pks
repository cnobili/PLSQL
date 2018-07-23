create or replace package nobc_util 
authid current_user
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
-- Types
--

type char_array_t is table of varchar2(32767);
type num_array_t is table of number;

type by_cols_rec is record
(
  row_num   number
, col_name  varchar2(30)
, col_value varchar2(4000)
);

type by_cols_t is table of by_cols_rec;

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
function get_session_trace_filename (p_spid in varchar2 := null) return varchar2;

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
) return char_array_t pipelined;

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
) return char_array_t pipelined;

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
);

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
) return by_cols_t pipelined;

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
function generate_rows(p_num in number) return num_array_t pipelined;

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
);

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
);

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
);

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
);

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
, p_constraint_type in user_constraints.constraint_type%type := null
, p_dir in varchar2 := null
);

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
);

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
);

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
);

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
);

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
);

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
return varchar2;

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
return varchar2;

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
);

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
);

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
--procedure gen_create_users
--(
--  p_username in dba_users.username%type := null  
--, p_dir in varchar2 := null
--);

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
);

end nobc_util;
/

show errors
