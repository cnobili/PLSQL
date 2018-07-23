create or replace package jmh_util_pkg
authid current_user
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

type dump_rec is record    
(
  dir        varchar2(30)
, filename   varchar2(256) 
, num_recs   number
, session_id number
, start_tm   date
, end_tm     date
);

type dump_tab is table of dump_rec; 

type partition_table_rec is record
(
  grp     number
, min_rid varchar2(2000)
, max_rid varchar2(2000)
);

type partition_table_t is table of partition_table_rec;

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
function get_spid return v$process.spid%type;

-- ---------------------------------------------------------------------------
--
-- function:  get_session_trace_filename
--
-- purpose: Gets the session's trace file.  You need to turn tracing on in 
--   your session first, i.e. alter session set sql_trace=true;
--
-- Must have select access on v$process (i.e. v_$process) and v$session (i.e. v_$session) 
-- granted directly by sys.
--
-- ---------------------------------------------------------------------------
--
function get_session_trace_filename return varchar2;

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
) return char_array_t pipelined;

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
procedure show_resultset_by_cols
(
  p_query in varchar2
);

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
function generate_rows(p_num in number) return num_array_t pipelined;

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
);

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
);

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
;

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
);

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
-- Note: If no directory is passed in, the output will go to the screen,
-- assuming you have set serveroutput on in your session.
--
-- ---------------------------------------------------------------------------
--
procedure gen_enable_constraints_ddl
(
  p_owner in all_tables.owner%type
, p_table_name in all_tables.table_name%type := null
, p_constraint_type in all_constraints.constraint_type%type := null
, p_dir in varchar2 := null
);

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
);

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
);

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
);

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
);

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
);

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
return varchar2;

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
return varchar2;

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
);

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
);

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
);

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
);

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
);

-- ---------------------------------------------------------------------------
--
-- procedure:  clob2file
--
-- purpose: Writes clob data passed in to file.
--
-- ---------------------------------------------------------------------------
--
procedure clob2file
(
  p_dir       in varchar2
, p_filename  in varchar2
, p_clob_data in out nocopy clob
);

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
);

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
);

-- -----------------------------------------------------------------------------
--
-- function: delim_str
--
-- purpose: Returns the string of characters delimited by specified deliminator,
--          defaults to using a comman if not specified.
--
-- -----------------------------------------------------------------------------
--
function delim_str
(
  p_str varchar2
, p_delim varchar2 := ','
)
return varchar2;

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
);

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
);

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
) return clob;

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
return partition_table_t pipelined;

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
);

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
);


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
return varchar2;

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
);

end jmh_util_pkg;
/

show errors
