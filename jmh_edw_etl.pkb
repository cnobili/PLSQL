CREATE OR REPLACE package body JMDBA.jmh_edw_etl
-- --------------------------------------------------------------------------
--
-- module name: jmh_edw_etl
--
-- description: Encapsulates Enterprise Data Warehouse ETL routines.
--
-- Uses the following packages:
--
-- jmh_log_pkg - logging
-- jmh_index_pkg - indexes
-- jmh_util_pkg  - General Utility routines
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:   10-12-2011
-- author: Craig Nobili
-- desc:   original
--
-- date:   01/17/2012
-- desc:   Nobili - changed parameter type from boolean to varchar2
--         in procedures ld_dim_patient, merge_dim_patient, merge_echo_provider,
--         and merge_star_provider.  You can't pass in boolean (non-SQL data types)
--         to the job scheduler when calling a procedure.
--
-- date:   2/10/2012
-- desc:   Nobili - Added ora_unload and ora_unload_compress java methods.
--
-- date:   2/29/2012
-- desc:   Nobili - Added copy_file, rename_file, remove_file, file_exists,
--         os_gzip_file, os_gunzip_file, send_vendor_files routines.
--
-- date:   5/17/2012
-- desc:   Nobili - Modified procedures that load echo provider data.
--
-- date:   7/27/2012
-- desc:   Nobili - In procedure send_vendor_files use jmh_util_pkg.dump_resultset
--         to write out flat file instead of ora_unload.  ora_unload uses a java
--         stored procedure (edw_java_proc/oraUnload) to write out the file and
--         there is bug with long string sizes (i.e. Oracle JVM memory issue).
--
-- date:   10/09/2012
-- desc:   Nobili - Added refresh_mview procedure
--
-- date:   10/23/2012
-- desc:   Nobili - Added procedure purge_appworx_archive and function count_lines
--
-- date:    10/25/2012
-- desc:    Nobili - Added procedures ins_appworx_log
--
-- date: 11/29/2012
-- desc: Nobili - Added procedure gen_dump_resultset_sql
--
-- date: 12/10/2012
-- desc: Nobili - Added function get_appworx_log
--
-- date: 01/08/2013
-- desc: Nobili - Added procedure scp_put_file
--
-- date: 02/06/2013
-- desc: Nobili - Added another run_os_command procedure.
--
-- date: 04/11/2013
-- desc: Nobili - Move Medventive specific routines into package in medvent schema.
--
-- date: 05/28/2013
-- desc: Nobili - Added run_etl_jobs procedure.
--
-- date: 06/05/2013
-- desc: Nobili - Added procedures sftp_put and sftp_get.
--
-- date: 06/26/2013
-- desc: Nobili - Added functions double_quote_str, obfuscate, unobfuscate
--
-- --------------------------------------------------------------------------
as

--
-- Private Globals
--
g_jre_exe varchar2(500);
g_java_classpath varchar2(500);

g_letters varchar2(26) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
g_map     varchar2(26) := 'SEIMQUYBFJNRVZCGKOAWDHLPTX';

-- ---------------------------------------------------------------------------
--
-- procedure: truncate_table
--
-- purpose: Truncates the table passed in.
--
-- ---------------------------------------------------------------------------
procedure truncate_table(p_table_name in varchar2)
is
begin

 execute immediate 'truncate table ' || p_table_name;

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in truncate_table, p_table_name = ' || p_table_name
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end truncate_table;

-- -----------------------------------------------------------------------------
--
-- function: get_token
--
-- purpose: Returns a token in a delimited string.
--
-- -----------------------------------------------------------------------------
function get_token
(
  p_str in varchar2
, p_delim in varchar2
, p_num in number
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
-- function: gen_uniq_id
--
-- purpose: Returns a hashed value for given input string. Used to generate
-- repeatable deterministic obfuscated global identifiers.
--
-- -----------------------------------------------------------------------------
--
function gen_uniq_id (p_input in varchar2) return varchar2
is
 l_hash_str varchar2(4000);
 l_clob_str clob := p_input;
begin

 l_hash_str := rawtohex( dbms_crypto.hash(l_clob_str, dbms_crypto.hash_sh1) );

 return l_hash_str;

end gen_uniq_id;

-- -----------------------------------------------------------------------------
--
-- function: str2ascii
--
-- purpose: Converts string to ascii numbers.
--
-- -----------------------------------------------------------------------------
--
function str2ascii (p_str in varchar2)
return varchar2
is
 l_new_str varchar2(255) := null;
 l_len pls_integer;
begin

 if (p_str is null) then return null; end if;

 l_len := length(p_str);
 for i in 1 .. l_len loop
   l_new_str := l_new_str || ascii(substr(p_str, i, 1));
 end loop;

 return(l_new_str);

end str2ascii;

-- ---------------------------------------------------------------------------
--
-- procedure: run_os_command
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
-- that allows PL/SQL to execute an OS command.
--
-- Note: In order to run owner needs java permissions which can be granted
-- by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure run_os_command(p_cmd in varchar2)
as language java
name 'edw_java_proc.execCommand(java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: run_os_command
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute an OS command.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure run_os_command(p_shell in varchar2, p_switch in varchar2, p_cmd in varchar2)
as language java
name 'edw_java_proc.execCommand(java.lang.String, java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: ora_unload
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure oraUnload.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure ora_unload
(
  p_db_url      in varchar2
, p_db_user     in varchar2
, p_db_pass     in varchar2
, p_output_file in varchar2
, p_sql_stmt    in varchar2
, p_delim       in varchar2
, p_append_flg  in varchar2
)
as language java
name 'edw_java_proc.oraUnload(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: ora_unload
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure oraUnload.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- Overloaded version that includes defaultDateFormat mask for controlling
-- format of date columns (if not converted to char in select statement).
--
-- ---------------------------------------------------------------------------
procedure ora_unload
(
  p_db_url      in varchar2
, p_db_user     in varchar2
, p_db_pass     in varchar2
, p_output_file in varchar2
, p_sql_stmt    in varchar2
, p_delim       in varchar2
, p_append_flg  in varchar2
, p_default_date_format in varchar2
)
as language java
name 'edw_java_proc.oraUnload(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: ora_unload_compressed
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure oraUnloadCompressed.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure ora_unload_compressed
(
  p_db_url      in varchar2
, p_db_user     in varchar2
, p_db_pass     in varchar2
, p_output_file in varchar2
, p_sql_stmt    in varchar2
, p_delim       in varchar2
)
as language java
name 'edw_java_proc.oraUnloadCompressed(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- function: file_exists
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to check for operating system files. Pass in the directory
--   and the file pattern to search for.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
function file_exists(p_dir in varchar2, p_regex in varchar2) return boolean
as language java
name 'edw_java_proc.fileExists(java.lang.String, java.lang.String) return boolean'
;

-- ---------------------------------------------------------------------------
--
-- function: count_lines
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to count number of lines in a file.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
function count_lines(p_file_path in varchar2) return number
as language java
name 'edw_java_proc.countLines(java.lang.String) return long'
;

-- ---------------------------------------------------------------------------
--
-- procedure: copy_file
--
-- purpose: Copies a file.
--
-- ---------------------------------------------------------------------------
procedure copy_file
(
  p_src_dir   in varchar2
, p_src_file  in varchar2
, p_dest_dir  in varchar2
, p_dest_file in varchar2
)
is
begin

  jmh_log_pkg.wlog('copy_file src = ' || p_src_file || ' dest = ' || p_dest_file , jmh_log_pkg.LOG_NORM);

  utl_file.fcopy
  (
    src_location  => p_src_dir
  , src_filename  => p_src_file
  , dest_location => p_dest_dir
  , dest_filename => p_dest_file
  );

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in copy_file'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end copy_file;

-- ---------------------------------------------------------------------------
--
-- procedure: rename_file
--
-- purpose: Renames a file.
--
-- ---------------------------------------------------------------------------
procedure rename_file
(
  p_src_dir   in varchar2
, p_src_file  in varchar2
, p_dest_dir  in varchar2
, p_dest_file in varchar2
)
is
begin

  jmh_log_pkg.wlog('rename_file src = ' || p_src_file || ' dest = ' || p_dest_file , jmh_log_pkg.LOG_NORM);

  utl_file.frename
  (
    src_location  => p_src_dir
  , src_filename  => p_src_file
  , dest_location => p_dest_dir
  , dest_filename => p_dest_file
  , overwrite     => true
  );

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in rename_file'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end rename_file;

-- ---------------------------------------------------------------------------
--
-- procedure: remove_file
--
-- purpose: Removes a file.
--
-- ---------------------------------------------------------------------------
procedure remove_file
(
  p_dir   in varchar2
, p_file  in varchar2
)
is
begin

  jmh_log_pkg.wlog('remove_file = ' || p_file, jmh_log_pkg.LOG_NORM);

  utl_file.fremove
  (
    location => p_dir
  , filename => p_file
  );

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in remove_file'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end remove_file;

-- ---------------------------------------------------------------------------
--
-- procedure: get_dir_list
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure getDirList.
--   Inserts the directory output into the global temporary table gtt_dir_list
--   so that the caller of this procedure can query the output.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure get_dir_list(p_dir in varchar2)
as language java
name 'edw_java_proc.getDirList(java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: compress_files_in_dir
--
-- purpose: Compress files in directory, p_dir can be an Oracle directory object
--   or the physical path to the actual directory on the OS.
--
-- ---------------------------------------------------------------------------
procedure compress_files_in_dir(p_dir in varchar2)
is
  l_dir_path varchar2(150);
  l_file_path varchar2(256);
begin

  -- check if directory object was passed in
  begin

    select directory_path
    into l_dir_path
    from all_directories
    where directory_name = upper(p_dir);

  exception
    when no_data_found then
      l_dir_path := p_dir;

  end;

  get_dir_list(l_dir_path);

  for rec in
  (
    select filename
    from gtt_dir_list
    where filename not like 'archive%'
  )
  loop

    l_file_path := l_dir_path || '/' || rec.filename;
    jmh_log_pkg.wlog('compress_files_in_dir filename = ' || l_file_path, jmh_log_pkg.LOG_NORM);
    os_gzip_file(l_file_path);

  end loop;

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in compress_files_in_dir'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end compress_files_in_dir;

-- ---------------------------------------------------------------------------
--
-- procedure: os_gzip_file
--
-- purpose: Wrapper around OS command gzip that compresses a file.
--
-- ---------------------------------------------------------------------------
procedure os_gzip_file
(
  p_src_file_path in varchar2
)
is
  l_os_cmd varchar2(100);
begin

  jmh_log_pkg.wlog('os_gzip_file = ' || p_src_file_path, jmh_log_pkg.LOG_NORM);

  l_os_cmd := '/bin/gzip ' || p_src_file_path;
  run_os_command(l_os_cmd);

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in os_gzip_file'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end os_gzip_file;

-- ---------------------------------------------------------------------------
--
-- procedure: os_gunzip_file
--
-- purpose: Wrapper around OS command gunzip that uncompresses a file.
--
-- ---------------------------------------------------------------------------
procedure os_gunzip_file
(
  p_gzip_file_path in varchar2
)
is
  l_os_cmd varchar2(100);
begin

  jmh_log_pkg.wlog('os_gunzip_file = ' || p_gzip_file_path, jmh_log_pkg.LOG_NORM);
  l_os_cmd := '/bin/gunzip ' || p_gzip_file_path;
  run_os_command(l_os_cmd);

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in os_gunzip_file'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end os_gunzip_file;

-- ---------------------------------------------------------------------------
--
-- procedure: unzip_file
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure unzipFile.
--   destDirPath needs to end in the directory separator, for example:
--
--   c:\\des_dir\\
--   /home/dest_dir/
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure unzip_file(p_zip_file_path in varchar2, p_dest_dir_path in varchar2)
as language java
name 'edw_java_proc.unzipFile(java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: zip_file
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure zipFile.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure zip_file(p_src_file_path in varchar2)
as language java
name 'edw_java_proc.zipFile(java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: zip_files_in_dir
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure zipFilesInDir.
--   The directory (p_dir) must end in the directory separator, for example:
--     /home/dest_dir/
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure zip_files_in_dir(p_dir in varchar2, p_zip_file_path in varchar2)
as language java
name 'edw_java_proc.zipFilesInDir(java.lang.String, java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: gunzip_file
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure gunzipFile.
--   destDirPath needs to end in the directory separator, for example:
--
--   c:\\des_dir\\
--   /home/dest_dir/
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure gunzip_file(p_gzip_file_path in varchar2)
as language java
name 'edw_java_proc.gunzipFile(java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- procedure: gzip_file
--
-- purpose: PL/SQL wrapper around java stored procedure edw_java_proc
--   that allows PL/SQL to execute java stored procedure gzipFile.
--
-- Note: In order to run owner needs java permissions which can be granted
--   by running dbms_java.grant_permission as SYS.
--
-- ---------------------------------------------------------------------------
procedure gzip_file(p_src_file_path in varchar2)
as language java
name 'edw_java_proc.gzipFile(java.lang.String)'
;

-- ---------------------------------------------------------------------------
--
-- function: run_job_on_fly
--
-- purpose: Creates a job on the fly and then runs it asynchronously.  After the job
--   is ran, it is dropped.  Allows one to run multiple processes in the same session.
--
-- Returns the job name so that the caller can wait on the job if necessary.
--
-- ---------------------------------------------------------------------------
function run_job_on_fly(p_plsql_block in varchar2, p_job_prefix in varchar2 := null)
return varchar2
is
  l_job_prefix varchar2(30);
  l_job_name varchar2(256);
begin

  -- Note: job prefix can't exceed 18 chars
  l_job_prefix := nvl
                 (
                   substr(p_job_prefix, 1, 18)
                 , substr(sys_context('userenv', 'session_user'), 1, 18)
                 );

  l_job_name := dbms_scheduler.generate_job_name(prefix =>  l_job_prefix);

  dbms_scheduler.create_job
  (
    job_name        => l_job_name
  , job_type        => 'plsql_block'
  , job_action      => p_plsql_block
  , start_date      => systimestamp
  , repeat_interval=>  null
  , end_date        => null
  , enabled         => false
  , comments        => 'Run ' || p_plsql_block
  , auto_drop       => true
  );

  -- Enabling the job will cause it to run immediately in another session
  dbms_scheduler.enable(name => l_job_name);
  --dbms_scheduler.run_job(job_name => l_job_name, use_current_session => FALSE);

  jmh_log_pkg.wlog('run_job_on_fly> submitted job = ' || l_job_name, jmh_log_pkg.LOG_NORM);

  return(l_job_name);

exception

 when others then
   rollback;
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in run_job_on_fly'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end run_job_on_fly;

-- ---------------------------------------------------------------------------
--
-- function: run_job_on_fly2
--
-- purpose: Creates a job on the fly and then runs it either in current session or asynchronously.
--   After the job is ran, it is dropped.  Allows one to run multiple processes in the same session.
--
-- Returns the job name so that the caller can wait on the job if necessary.
--
-- ---------------------------------------------------------------------------
function run_job_on_fly2
(
  p_plsql_block in varchar2
, p_job_prefix  in varchar2        := null
, p_use_current_session in boolean := false
)
return varchar2
is
  l_job_prefix varchar2(30);
  l_job_name   varchar2(256);
begin

  -- Note: job prefix can't exceed 18 chars
  l_job_prefix := nvl
                 (
                   substr(p_job_prefix, 1, 18)
                 , substr(sys_context('userenv', 'session_user'), 1, 18)
                 );

  l_job_name := dbms_scheduler.generate_job_name(prefix =>  l_job_prefix);

  dbms_scheduler.create_job
  (
    job_name        => l_job_name
  , job_type        => 'plsql_block'
  , job_action      => p_plsql_block
  , start_date      => systimestamp
  , repeat_interval=>  null
  , end_date        => null
  , enabled         => false
  , comments        => 'Run ' || p_plsql_block
  , auto_drop       => true
  );

  if (p_use_current_session) then
    dbms_scheduler.run_job(job_name => l_job_name, use_current_session => p_use_current_session);  
    dbms_scheduler.drop_job(job_name => l_job_name); -- need to drop the job or it remains in scheduler
  else
    -- Enabling the job will cause it to run immediately in another session
    -- and the job will get dropped
    dbms_scheduler.enable(name => l_job_name);
  end if;
  
  jmh_log_pkg.wlog('run_job_on_fly2> submitted job = ' || l_job_name, jmh_log_pkg.LOG_NORM);
  
  return(l_job_name);

exception

 when others then
   rollback;
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in run_job_on_fly2'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end run_job_on_fly2;

-- ---------------------------------------------------------------------------
--
-- procedure: dump_resultset_for_hpm
--
-- purpose: Dumps a result set to a file for the passed in query (p_query).
--
-- ---------------------------------------------------------------------------
procedure dump_resultset_for_hpm
(
  p_query in varchar2
, p_extension_name in varchar2
, p_filename in varchar2
, p_dir in varchar2 := 'JMH_DW_DIR'
, p_delim in varchar2 := '|'
, p_entity_code in varchar2 := '01'
, p_enterprise_id in varchar2 := '1'
, p_version in varchar2 := '3.0'
)
is
 l_utl_handle utl_file.file_type;
 l_line varchar2(32767);
 l_cur pls_integer;
 l_desc_tab dbms_sql.desc_tab;
 l_num_cols pls_integer;
 l_col_value varchar2(4000);
 l_retcode pls_integer;
 l_cnt pls_integer := 0;
begin

 jmh_log_pkg.wlog
 (
   p_log_msg => 'dump_resultset_for_hpm: Write records to ' || p_filename || ' for query = ' || p_query
 , p_log_level => jmh_log_pkg.LOG_NORM
 );

 l_cur := dbms_sql.open_cursor;
 dbms_sql.parse(l_cur, p_query, dbms_sql.native);
 dbms_sql.describe_columns(l_cur, l_num_cols, l_desc_tab);

 l_utl_handle := utl_file.fopen(p_dir, p_filename, 'w');

 --
 -- Write out header information for HPM
 --
 utl_file.put_line(l_utl_handle, g_REC_TYPE_ENTHDR || p_delim || p_enterprise_id || p_delim || p_version);
 utl_file.put_line(l_utl_handle, g_REC_TYPE_STAGEHDR || p_delim || p_extension_name);

 -- Define all columns as varchar2
 for i in 1 .. l_num_cols loop
   dbms_sql.define_column(l_cur, i, l_col_value, 4000);
 end loop;

 l_retcode := dbms_sql.execute(l_cur);

 while (dbms_sql.fetch_rows(l_cur) > 0) loop
   utl_file.put(l_utl_handle, g_REC_TYPE_STAGEDATA || p_delim || p_entity_code || p_delim);
   l_line := null;
   for i in 1 .. l_num_cols loop
     dbms_sql.column_value(l_cur, i, l_col_value);

     if (i = l_num_cols) then
       l_line := l_line || l_col_value;
     else
       l_line := l_line || l_col_value || p_delim;
     end if;

   end loop;
    utl_file.put_line(l_utl_handle, l_line);
    l_cnt := l_cnt + 1;

 end loop;

 dbms_sql.close_cursor(l_cur);
 utl_file.fclose(l_utl_handle);

 jmh_log_pkg.wlog
 (
   p_log_msg => 'End dumpresultset_for_hpm: ' || l_cnt || ' records written out.'
 , p_log_level => jmh_log_pkg.LOG_NORM
 );

exception

 when others then

 if ( utl_file.is_open(l_utl_handle) ) then
   utl_file.fclose(l_utl_handle);
 end if;

 jmh_log_pkg.wlog
 (
   p_log_msg => 'Error in dump_resultset_for_hpm'
 , p_log_level => jmh_log_pkg.LOG_MUST
 );
 raise;

end dump_resultset_for_hpm;

-- ---------------------------------------------------------------------------
--
-- procedure: send_vendor_files
--
-- purpose: Writes out vendor flat files for delivery by AppWorx.
--
-- ---------------------------------------------------------------------------
procedure send_vendor_files
(
  p_vendor_cd in varchar2 := null
, p_file_type in varchar2 := null
, p_frequency in varchar2 := null
)
is
  l_filename varchar2(100);
  l_dir_path varchar2(256);
  l_file_path varchar2(500);
  l_date_format varchar2(100);
  l_num_recs number;
begin

  jmh_log_pkg.wlog('Begin send_vendor_files' , jmh_log_pkg.LOG_NORM);

  update outbound_file set
    lock_flag = 'Y'
  where active_flag = 'Y'
    and vendor_cd = nvl(upper(p_vendor_cd), vendor_cd)
    and frequency = nvl(upper(p_frequency), frequency)
    and nvl(file_type, 'N/A') = coalesce(upper(p_file_type), file_type, 'N/A')
    and ready_flag = 'Y'
    and lock_flag = 'N'
  ;

  if (sql%rowcount = 0) then
    jmh_log_pkg.wlog('No files to send out.' , jmh_log_pkg.LOG_NORM);
    return;
  else
    commit;
  end if;

  for rec in
  (
    select
      outbound_file_id
    , query
    , count_query
    , dest_dir_obj
    , compress_flag
    , delimitor
    , default_date_format
    , filename_spec
    , extract_dt
    , thru_dt
    from
      outbound_file
    where active_flag = 'Y'
      and vendor_cd = nvl(upper(p_vendor_cd), vendor_cd)
      and frequency = nvl(upper(p_frequency), frequency)
      and nvl(file_type, 'N/A') = coalesce(upper(p_file_type), file_type, 'N/A')
      and ready_flag = 'Y'
    order by
      outbound_file_id
    --for update
  )
  loop

    update outbound_file set
      start_transfer_tm = sysdate
    , end_transfer_tm = null
    , status = 'RUNNING'
    where outbound_file_id = rec.outbound_file_id;
    commit;

    execute immediate rec.count_query into l_num_recs;

    l_filename := replace
                  (
                    rec.filename_spec
                  , 'YYYYMMDD'
                  , coalesce
                    (
                      to_char(rec.thru_dt, 'YYYYMMDD')
                    , to_char(rec.extract_dt, 'YYYYMMDD')
                    , to_char(sysdate, 'YYYYMMDD')
                    )
                  );

    select directory_path
    into l_dir_path
    from all_directories
    where directory_name = rec.dest_dir_obj;

    l_file_path := l_dir_path || '/' || l_filename;

    jmh_log_pkg.wlog('Writing out file = ' || l_filename || ' to ' || l_dir_path, jmh_log_pkg.LOG_NORM);

    l_date_format := 'alter session set nls_date_format=' || '''' || rec.default_date_format || '''';
    execute immediate l_date_format;
    jmh_util_pkg.dump_resultset
    (
      p_query    => rec.query
    , p_dir      => rec.dest_dir_obj
    , p_filename => l_filename
    , p_delim    => rec.delimitor
    );
    --ora_unload
    --(
    --  p_db_url      => null -- local connection used
    --, p_db_user     => null
    --, p_db_pass     => null
    --, p_output_file => l_file_path
    --, p_sql_stmt    => rec.query
    --, p_delim       => rec.delimitor
    --, p_append_flg  => 'F'
    --, p_default_date_format => rec.default_date_format
    --);

    if (rec.compress_flag = 'Y') then
      jmh_log_pkg.wlog('Compress file = ' || l_filename, jmh_log_pkg.LOG_NORM);
      --gzip_file(p_src_file_path => l_file_path);
      os_gzip_file(p_src_file_path => l_file_path);
    end if;

    -- remove the non-compressed file
    --remove_file(rec.dest_dir_obj, l_filename);

    update outbound_file set
      generated_filename = l_filename
    , end_transfer_tm = sysdate
    , recs_in_file = l_num_recs
    , status = 'COMPLETED'
    --, ready_flag = 'N'
    where outbound_file_id = rec.outbound_file_id;
    commit;

  end loop;

  update outbound_file set
    lock_flag = 'N'
  , ready_flag = 'N'
  where active_flag = 'Y'
    and vendor_cd = nvl(upper(p_vendor_cd), vendor_cd)
    and frequency = nvl(upper(p_frequency), frequency)
    and nvl(file_type, 'N/A') = coalesce(upper(p_file_type), file_type, 'N/A')
    and ready_flag = 'Y'
    and lock_flag = 'Y'
  ;
  commit;

  jmh_log_pkg.wlog('End send_vendor_files' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   rollback;
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in send_vendor_files'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );

   update outbound_file set
     lock_flag = 'N'
   , ready_flag = 'N'
   where active_flag = 'Y'
     and vendor_cd = nvl(upper(p_vendor_cd), vendor_cd)
     and frequency = nvl(upper(p_frequency), frequency)
     and nvl(file_type, 'N/A') = coalesce(upper(p_file_type), file_type, 'N/A')
     and ready_flag = 'Y'
     and lock_flag = 'Y'
   ;
   commit;

   raise; -- don't swallow the exception

end send_vendor_files;

-- ---------------------------------------------------------------------------
--
-- procedure: exec_load_datamart
--
-- purpose: executes os java program based on the JMDBA.JMH_APP_PARAMETERS table.
--
-- ---------------------------------------------------------------------------
procedure exec_load_datamart ( p_app_parm IN VARCHAR2 )
is
 l_os_cmd varchar2(2000);
 v_value varchar2(2000);

begin
    select value into v_value from jmdba.jmh_app_parameters where parameter = p_app_parm;

    jmh_log_pkg.wlog('Begin exec_load_datamart' , jmh_log_pkg.LOG_NORM);
    l_os_cmd := g_jre_exe || ' -classpath ' || g_java_classpath || ' ' || v_value;
    jmh_log_pkg.wlog('Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    run_os_command(l_os_cmd);
    jmh_log_pkg.wlog('Done Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    jmh_log_pkg.wlog('End exec_load_datamart' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in exec_load_datamart'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end exec_load_datamart;

-- ---------------------------------------------------------------------------
--
-- procedure: exec_os_java_prg
--
-- purpose: executes os java program
--
-- ---------------------------------------------------------------------------
procedure exec_os_java_prg
(
  p_dir_object_name in varchar2
, p_java_prg        in varchar2
, p_java_prg_args   in varchar2
)
is
 l_os_cmd varchar2(2000);
 l_value varchar2(2000);

begin
    select directory_path into l_value from all_directories where directory_name = upper(p_dir_object_name);

    jmh_log_pkg.wlog('Begin exec_os_java_prg' , jmh_log_pkg.LOG_NORM);
    
    l_os_cmd := g_jre_exe || ' -classpath ' || g_java_classpath || ' ' || p_java_prg || ' ' || l_value || '/' || p_java_prg_args ;
    
    jmh_log_pkg.wlog('Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    
    run_os_command(l_os_cmd);
    
    jmh_log_pkg.wlog('Done Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    jmh_log_pkg.wlog('End exec_os_java_prg' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in exec_os_java_prg'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end exec_os_java_prg;

-- ---------------------------------------------------------------------------
--
-- procedure: exec_os_java_prg
--
-- purpose: executes os java program
--
-- ---------------------------------------------------------------------------
procedure exec_os_java_prg
(
  p_java_prg        in varchar2
, p_java_prg_args   in varchar2
)
is
 l_os_cmd varchar2(2000);
begin

    jmh_log_pkg.wlog('Begin exec_os_java_prg' , jmh_log_pkg.LOG_NORM);
    
    l_os_cmd := g_jre_exe || ' -classpath ' || g_java_classpath || ' ' || p_java_prg || ' ' || p_java_prg_args ;
    
    jmh_log_pkg.wlog('Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    
    run_os_command(l_os_cmd);
    
    jmh_log_pkg.wlog('Done Executing OS program->' || l_os_cmd, jmh_log_pkg.LOG_NORM);
    jmh_log_pkg.wlog('End exec_os_java_prg' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in exec_os_java_prg'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end exec_os_java_prg;

-- ---------------------------------------------------------------------------
--
-- procedure: refresh_mview
--
-- purpose: Refreshes materialized view passed in as parameter.
--
-- ---------------------------------------------------------------------------
procedure refresh_mview(p_mview_name in varchar2)
is
begin

  jmh_log_pkg.wlog('Begin refresh_mview - refresh ' || p_mview_name, jmh_log_pkg.LOG_NORM);

  dbms_mview.refresh
  (
    list => p_mview_name    -- do not need a refresh group
  , method => 'C'           -- complete refresh
  , atomic_refresh => FALSE -- so that on a refresh it truncates instead of deletes?
  );
  commit;

  jmh_log_pkg.wlog('End refresh_mview - completed refresh of ' || p_mview_name, jmh_log_pkg.LOG_NORM);

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in refresh_mview'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end refresh_mview;

-- ---------------------------------------------------------------------------
--
-- procedure: purge_appworx_archive
--
-- purpose: Purges AppWorx archive directories and files.
--
-- ---------------------------------------------------------------------------
procedure purge_appworx_archive
is
begin

  -- Medventive files
  --purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/archive/JMH_claims_data', 21);
  --purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/archive/JMH_data', 21);
  --purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/archive/JMH_emrfiles', 21);
  purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/JMH_claims_data', 21);
  purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/JMH_data', 21);
  purge_appworx_archive('/opt/boe/hsysnas1-edi/Medventive/JMH_emrfiles', 21);
  
  -- PBGH files
  purge_appworx_archive('/opt/boe/hsysnas1-edi/PGBH/archive', 180);

end purge_appworx_archive;

-- ---------------------------------------------------------------------------
--
-- procedure: purge_appworx_archive
--
-- purpose: Purges AppWorx archive directories and files.
--
-- ---------------------------------------------------------------------------
procedure purge_appworx_archive (p_dir in varchar2, p_days_to_keep in number)
is
  l_fullpath varchar2(500);

begin

  get_dir_list(p_dir);

  for rec in
  (
    select *
    from gtt_dir_list
    where modified_date < sysdate - p_days_to_keep
  )
  loop

    l_fullpath := p_dir || '/' || rec.filename;
    run_os_command('/bin/rm -r ' || l_fullpath);

  end loop;

end purge_appworx_archive;

-- ---------------------------------------------------------------------------
--
-- procedure: ins_appworx_log
--
-- purpose: Inserts into the appworx_log table files that have been sent.
--
-- ---------------------------------------------------------------------------
procedure ins_appworx_log
is

  l_date_today varchar(10)  := to_char(sysdate - 1, 'MM-DD-YYYY');
  l_prefix_dir varchar2(500) := '/opt/boe/hsysnas1-edi/Medventive/archive';

begin

  --
  -- Medventive files
  --
  /*
  if ( file_exists(l_prefix_dir || '/JMH_claims_data', l_date_today) )
  then
    ins_appworx_log(l_prefix_dir || '/JMH_claims_data/' || l_date_today);
  end if;

  if ( file_exists(l_prefix_dir || '/JMH_data', l_date_today) )
  then
    ins_appworx_log(l_prefix_dir || '/JMH_data/' || l_date_today);
  end if;

  if ( file_exists(l_prefix_dir || '/JMH_emrfiles', l_date_today) )
  then
    ins_appworx_log(l_prefix_dir || '/JMH_emrfiles/' || l_date_today);
  end if;
  */
  --
  -- PBGH files
  --
  l_prefix_dir := '/opt/boe/hsysnas1-edi/PGBH/archive';
  l_date_today := to_char(sysdate, 'YYYYMMDD');
  
  get_dir_list(l_prefix_dir);
  
  for rec in (select * from gtt_dir_list)
  loop
  
    if ( l_date_today = substr(rec.filename, 1, 8) )
    then
      ins_appworx_log(l_prefix_dir || '/' || rec.filename, to_date(rec.filename, 'YYYYMMDDhh24mi'));
    end if;
    
  end loop;
  
end ins_appworx_log;

-- ---------------------------------------------------------------------------
--
-- procedure: ins_appworx_log
--
-- purpose: Inserts into the appworx_log table files that have been sent.
--
-- ---------------------------------------------------------------------------
procedure ins_appworx_log(p_dir in varchar2, p_transfer_date in date := null)
is
  l_recs number;
begin

  get_dir_list(p_dir);

  for rec in (select * from gtt_dir_list)
  loop

    l_recs := count_lines(p_dir || '/' || rec.filename);

    insert into appworx_log
    (
      directory
    , filename
    , file_size_bytes
    , num_of_recs
    , transfer_date
    )
    values
    (
      p_dir
    , rec.filename
    , rec.file_size_bytes
    , l_recs
    , nvl(p_transfer_date, rec.modified_date)
    );

    commit;

  end loop;

end ins_appworx_log;

-- ---------------------------------------------------------------------------
--
-- procedure: ins_appworx_log
--
-- purpose: Inserts into the appworx_log table files that have been sent.
--
-- ---------------------------------------------------------------------------
procedure ins_appworx_log
(
  p_dir             in varchar2
, p_filename        in varchar2
, p_file_size_bytes in number
, p_num_of_recs     in number
, p_transfer_date   in varchar2
)
is
begin

  insert into appworx_log
  (
    directory
  , filename
  , file_size_bytes
  , num_of_recs
  , transfer_date
  )
  values
  (
    p_dir
  , p_filename
  , p_file_size_bytes
  , p_num_of_recs
  , to_date(p_transfer_date, 'mm-dd-yyyy hh24:mi:ss')
  );
  
  commit;

end ins_appworx_log;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_dump_resultset_sql
--
-- purpose: Writes out the cfg and sql files for the java DumpResultSet program
--          by selecting from the dump_resultset_sql table.
--
-- ---------------------------------------------------------------------------
procedure gen_dump_resultset_sql(p_cfg_filename in varchar2, p_sql_filename in varchar2)
is

  l_clob clob;
  
begin

  select cfg_text
  into l_clob
  from dump_resultset_sql
  where cfg_filename = p_cfg_filename
  ;
  
  jmh_util_pkg.clob2file
  (
    p_dir       => 'JMH_JAVA_PRG'
  , p_filename  => p_cfg_filename
  , p_clob_data => l_clob
  );

  select sql_text
  into l_clob
  from dump_resultset_sql
  where cfg_filename = p_cfg_filename
  ;
  
  jmh_util_pkg.clob2file
  (
    p_dir       => 'JMH_JAVA_PRG'
  , p_filename  => p_sql_filename
  , p_clob_data => l_clob
  );
  
end gen_dump_resultset_sql;

-- ---------------------------------------------------------------------------
--
-- function: get_appworx_log
--
-- purpose: Returns ref cursor result set of records in appworx_log table
--   for number of days (p_num_days).
--
-- ---------------------------------------------------------------------------
function get_appworx_log(p_num_days in number) return sys_refcursor
is
  l_cur sys_refcursor;
begin

  open l_cur for
    select *
    from
      appworx_log
    where transfer_date >= sysdate - p_num_days
    order by
      transfer_date
    , directory
    ;

  return l_cur;
  
end get_appworx_log;  

-- ---------------------------------------------------------------------------
--
-- function: appworx_log_refcur_to_tbl
--
-- purpose: Returns result set of records in appworx_log table
--   as a piplelined table function.  Pass in get_appworx_log function as
--   input parameter.
--
-- Example usuage:
--
-- SELECT *
-- FROM
--   TABLE(jmh_edw_etl.appworx_log_refcur_to_tbl(jmh_edw_etl.get_appworx_log(7)))
--
-- ---------------------------------------------------------------------------
function appworx_log_refcur_to_tbl(p_refcur in sys_refcursor)
return appworx_log_out_t
pipelined
is

  out_rec appworx_log%rowtype;
  
begin

  loop
    fetch p_refcur into out_rec;
    exit when p_refcur%notfound;
    pipe row (out_rec);
  end loop;
  
  close p_refcur;
  return;
  
end appworx_log_refcur_to_tbl;

-- ---------------------------------------------------------------------------
--
-- procedure: scp_put_file
--
-- purpose: secure copies a file.
--
-- ---------------------------------------------------------------------------
procedure scp_put_file
(
  p_hostname in varchar2
, p_user     in varchar2
, p_pass     in varchar2
, p_src_file in varchar2
, p_dst_file in varchar2
)
is
  l_sshpass_prg varchar2(255);
  l_sshpass_cmd varchar2(500);
begin

  l_sshpass_prg := jmh_app_parameters_pkg.get_value('JMH_SSHPASS_PRG');
  l_sshpass_cmd := l_sshpass_prg || ' -p ' || p_pass || ' /usr/bin/scp ' || p_src_file || ' ' || p_user || '@' || p_hostname || ':' || p_dst_file;
  
  run_os_command(l_sshpass_cmd); 
  
end scp_put_file;

-- ---------------------------------------------------------------------------
--
-- procedure: scp_get_file
--
-- purpose: secure copies a file.
--
-- ---------------------------------------------------------------------------
procedure scp_get_file
(
  p_hostname in varchar2
, p_user     in varchar2
, p_pass     in varchar2
, p_src_file in varchar2
, p_dst_file in varchar2
)
is
  l_sshpass_prg varchar2(255);
  l_sshpass_cmd varchar2(500);
begin

  l_sshpass_prg := jmh_app_parameters_pkg.get_value('JMH_SSHPASS_PRG');
  l_sshpass_cmd := l_sshpass_prg || ' -p ' || p_pass || ' /usr/bin/scp ' || p_user || '@' || p_hostname || ':' || p_src_file || ' ' || p_dst_file;
  
  run_os_command(l_sshpass_cmd); 
  
end scp_get_file;

-- ---------------------------------------------------------------------------
--
-- procedure: sftp_put
--
-- purpose: Secure ftp put.
--
-- ---------------------------------------------------------------------------
procedure sftp_put
(
  p_hostname in varchar2
, p_user     in varchar2
, p_pass     in varchar2
, p_src_file in varchar2
, p_dst_file in varchar2
)
is
  l_args varchar2(2000) := p_hostname || ' ' || p_user || ' ' || p_pass || ' ' || p_src_file || ' ' || p_dst_file;
begin

  --
  -- Run java program
  --
  jmh_edw_etl.exec_os_java_prg
	(
    p_java_prg        => 'SftpPut'
	, p_java_prg_args   => l_args
  );

end sftp_put;

-- ---------------------------------------------------------------------------
--
-- procedure: sftp_get
--
-- purpose: Secure ftp get.
--
-- ---------------------------------------------------------------------------
procedure sftp_get
(
  p_hostname in varchar2
, p_user     in varchar2
, p_pass     in varchar2
, p_src_file in varchar2
, p_dst_file in varchar2
)
is
  l_args varchar2(2000) := p_hostname || ' ' || p_user || ' ' || p_pass || ' ' || p_src_file || ' ' || p_dst_file;
begin

  --
  -- Run java program
  --
  jmh_edw_etl.exec_os_java_prg
	(
    p_java_prg        => 'SftpGet'
	, p_java_prg_args   => l_args
  );

end sftp_get;

-- ---------------------------------------------------------------------------
--
-- procedure: exec_mailx
--
-- purpose: Sends an email using Linux mailx command.
--
-- ---------------------------------------------------------------------------
procedure exec_mailx
(
  p_recipients in varchar2
, p_subject    in varchar2
, p_message    in varchar2
)
is
  l_shell varchar2(30) := '/bin/bash';
  l_switch varchar2(2) := '-c';
  l_cmd varchar2(4000) := '/bin/echo "' || p_message || '" | /bin/mailx -s "' || p_subject || '" ' || replace(p_recipients, ',', ' ');
begin

  jmh_edw_etl.run_os_command(l_shell, l_switch, l_cmd);
  
end exec_mailx;

-- ---------------------------------------------------------------------------
--
-- procedure: run_etl_jobs
--
-- purpose: Runs all ETL jobs in the ETL_JOB table.  Submits each job to the scheduler.
--
-- ---------------------------------------------------------------------------
procedure run_etl_jobs
is

  l_use_current_session boolean := false;
  l_ret varchar2(100);  
begin

  jmh_log_pkg.wlog('Begin run_etl_jobs' , jmh_log_pkg.LOG_NORM);

  for rec in
  (
    select
      plsql_block
    , run_async_flg
    , step_num
    from
      etl_job
    where active_flg = 'Y'
    order by 
      step_num
  )
  loop
  
    if (rec.run_async_flg = 'N') then
      l_use_current_session := true;
    end if;
            
    l_ret := run_job_on_fly2
    (
      p_plsql_block         => rec.plsql_block
    , p_job_prefix          => 'ETL_' || rec.step_num || '_'
    , p_use_current_session => l_use_current_session
    );
    
    if (l_use_current_session = false) then
      jmh_log_pkg.wlog('Running job = ' || l_ret || ' asynchronously', jmh_log_pkg.LOG_NORM);
    else
      jmh_log_pkg.wlog('Ran job = ' || l_ret || ' in current session', jmh_log_pkg.LOG_NORM);
    end if;
  
  end loop;
  
  jmh_log_pkg.wlog('End run_etl_jobs' , jmh_log_pkg.LOG_NORM);

exception

  when others then
    jmh_log_pkg.wlog
    (
      p_log_msg => 'Error in run_etl_jobs'
    , p_log_level => jmh_log_pkg.LOG_MUST
    );
    raise; -- don't swallow the exception

end run_etl_jobs;

-- ---------------------------------------------------------------------------
--
-- procedure: fw_prg
--
-- purpose: File watcher program, invoked by scheduler.
--
-- ---------------------------------------------------------------------------
procedure fw_prg(p_payload in sys.scheduler_filewatcher_result)
is
begin

  insert into appworx_log
  (
    directory
  , filename
  , file_size_bytes
  , num_of_recs
  , transfer_date
  )
  values
  (
    p_payload.directory_path
  , p_payload.actual_file_name
  , p_payload.file_size
  , count_lines(p_payload.directory_path || '/' || p_payload.actual_file_name)
  , sysdate
  );
  commit;
 
end fw_prg;

-- ---------------------------------------------------------------------------
--
-- function: double_quote_str
--
-- purpose: Returns string with double quotes around it.
--
-- ---------------------------------------------------------------------------
function double_quote_str(p_str in varchar2) return varchar2
is
begin

  return ( dbms_assert.enquote_name(p_str) );
  
end double_quote_str;

-- ---------------------------------------------------------------------------
--
-- function: obfuscate
--
-- purpose: Returns string obfuscated.
--
-- ---------------------------------------------------------------------------
function obfuscate(p_str in varchar2) return varchar2
is
begin

  return ( translate(upper(p_str), g_letters, g_map) );
  
end obfuscate;

-- ---------------------------------------------------------------------------
--
-- function: unobfuscate
--
-- purpose: Returns string unobfuscated.
--
-- ---------------------------------------------------------------------------
function unobfuscate(p_str in varchar2) return varchar2
is
begin

  return ( translate(upper(p_str), g_map, g_letters) );

end unobfuscate;

-- ---------------------------------------------------------------------------
--
-- procedure: send_email_msg
--
-- purpose: Sends an email message.
--
-- Calls sendEmail in edw_java_proc class.
-- 
-- ---------------------------------------------------------------------------
procedure send_email_msg
(
  p_sender     in varchar2
, p_recipients in varchar2
, p_subject    in varchar2
, p_message    in clob
)
as language java
name 'edw_java_proc.sendEmail(java.lang.String, java.lang.String, java.lang.String, oracle.sql.CLOB)'
;


--
-- Initialization
--
begin

 g_jre_exe := jmh_app_parameters_pkg.get_value('JMH_JRE_EXE');
 g_java_classpath := jmh_app_parameters_pkg.get_value('JMH_JAVA_CLASSPATH');
 
end jmh_edw_etl;
/
