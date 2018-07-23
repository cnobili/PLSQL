CREATE OR REPLACE package jmh_edw_etl
-- --------------------------------------------------------------------------
--
-- module name: JMDBA.jmh_edw_etl
--
-- description: Encapsulates Enterprise Data Warehouse ETL routines.
--
-- Uses the following packages:
--
-- jmh_log_pkg   - Logging
-- jmh_index_pkg - Indexes
-- jmh_util_pkg  - General Utility routines
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date: 10-12-2011
-- author: Craig Nobili
-- desc: original
--
-- date: 01/17/2012
-- desc: Nobili - changed parameter type from boolean to varchar2
-- in procedures ld_dim_patient, merge_dim_patient, merge_echo_provider,
-- and merge_star_provider. You can't pass in boolean (non-SQL data types)
-- to the job scheduler when calling a procedure.
--
-- date: 2/10/2012
-- desc: Nobili - Added ora_unload and ora_unload_compress java methods.
--
-- date: 2/29/2012
-- desc: Nobili - Added copy_file, rename_file, remove_file, file_exists,
-- os_gzip_file, os_gunzip_file, send_vendor_files routines.
--
-- date: 10/09/2012
-- desc: Nobili - Added refresh_mview procedure
--
-- date: 10/23/2012
-- desc: Nobili - Added procedure purge_appworx_archive and function count_lines
--
-- date: 10/25/2012
-- desc: Nobili - Added procedures ins_appworx_log
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
-- Globals Constants
--

g_PRG_NAME constant varchar2(30) := 'JMDBA.JMH_EDW_ETL';

g_REC_TYPE_ENTHDR constant varchar2(6) := 'ENTHDR';
g_REC_TYPE_STAGEHDR  constant varchar2(8) := 'STAGEHDR';
g_REC_TYPE_STAGEDATA constant varchar2(9) := 'STAGEDATA';

g_TRUE  constant varchar2(1) := 'T';
g_FALSE constant varchar2(1) := 'F';

--
-- Types
--
type appworx_log_out_t is table of appworx_log%rowtype;


-- ---------------------------------------------------------------------------
--
-- procedure: truncate_table
--
-- purpose: Truncates the table passed in.
--
-- ---------------------------------------------------------------------------
procedure truncate_table(p_table_name in varchar2);

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
  p_str   in varchar2
, p_delim in varchar2
, p_num   in number
)
return varchar2;

-- -----------------------------------------------------------------------------
--
-- function: gen_uniq_id
--
-- purpose: Returns a hashed value for given input string.  Used to generate
--   repeatable deterministic obfuscated global identifiers.
--
-- -----------------------------------------------------------------------------
--
function gen_uniq_id (p_input in varchar2) return varchar2;

-- -----------------------------------------------------------------------------
--
-- function: str2ascii
--
-- purpose: Converts string to ascii numbers.
--
-- -----------------------------------------------------------------------------
--
function str2ascii (p_str in varchar2) return varchar2;

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
procedure run_os_command(p_cmd in varchar2);

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
procedure run_os_command(p_shell in varchar2, p_switch in varchar2, p_cmd in varchar2);

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
);

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
);

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
);

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
function file_exists(p_dir in varchar2, p_regex in varchar2) return boolean;

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
function count_lines(p_file_path in varchar2) return number;

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
);

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
);

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
);

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
procedure get_dir_list(p_dir in varchar2);

-- ---------------------------------------------------------------------------
--
-- procedure: compress_files_in_dir
--
-- purpose: Compress files in directory, p_dir can be an Oracle directory object
--   or the physical path to the actual directory on the OS.
--
-- ---------------------------------------------------------------------------
procedure compress_files_in_dir(p_dir in varchar2);

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
);

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
);

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
procedure unzip_file(p_zip_file_path in varchar2, p_dest_dir_path in varchar2);

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
procedure zip_file(p_src_file_path in varchar2);

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
procedure zip_files_in_dir(p_dir in varchar2, p_zip_file_path in varchar2);

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
procedure gunzip_file(p_gzip_file_path in varchar2);

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
procedure gzip_file(p_src_file_path in varchar2);

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
return varchar2;

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
return varchar2;

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
);

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
);

-- ---------------------------------------------------------------------------
--
-- procedure: exec_load_datamart
--
-- purpose: executes os java program based on the JMDBA.JMH_APP_PARAMETERS table.
--
-- ---------------------------------------------------------------------------
procedure exec_load_datamart (p_app_parm in varchar2);

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
);

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
);

-- ---------------------------------------------------------------------------
--
-- procedure: refresh_mview
--
-- purpose: Refreshes materialized view passed in as parameter.
--
-- ---------------------------------------------------------------------------
procedure refresh_mview(p_mview_name in varchar2);

-- ---------------------------------------------------------------------------
--
-- procedure: purge_appworx_archive
--
-- purpose: Purges AppWorx archive directories and files.
--
-- ---------------------------------------------------------------------------
procedure purge_appworx_archive;

-- ---------------------------------------------------------------------------
--
-- procedure: purge_appworx_archive
--
-- purpose: Purges AppWorx archive directories and files.
--
-- ---------------------------------------------------------------------------
procedure purge_appworx_archive (p_dir in varchar2, p_days_to_keep in number);

-- ---------------------------------------------------------------------------
--
-- procedure: ins_appworx_log
--
-- purpose: Inserts into the appworx_log table files that have been sent.
--
-- ---------------------------------------------------------------------------
procedure ins_appworx_log;

-- ---------------------------------------------------------------------------
--
-- procedure: ins_appworx_log
--
-- purpose: Inserts into the appworx_log table files that have been sent.
--
-- ---------------------------------------------------------------------------
procedure ins_appworx_log(p_dir in varchar2, p_transfer_date in date := null);

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
);

-- ---------------------------------------------------------------------------
--
-- procedure: gen_dump_resultset_sql
--
-- purpose: Writes out the cfg and sql files for the java DumpResultSet program
--          by selecting from the dump_resultset_sql table.
--
-- ---------------------------------------------------------------------------
procedure gen_dump_resultset_sql(p_cfg_filename in varchar2, p_sql_filename in varchar2);

-- ---------------------------------------------------------------------------
--
-- function: get_appworx_log
--
-- purpose: Returns ref cursor result set of records in appworx_log table
--   for number of days (p_num_days).
--
-- ---------------------------------------------------------------------------
function get_appworx_log(p_num_days in number) return sys_refcursor;

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
pipelined;

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
);

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
);

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
);

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
);

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
);

-- ---------------------------------------------------------------------------
--
-- procedure: run_etl_jobs
--
-- purpose: Runs all ETL jobs in the ETL_JOB table.  Submits each job to the scheduler.
--
-- ---------------------------------------------------------------------------
procedure run_etl_jobs;

-- ---------------------------------------------------------------------------
--
-- procedure: fw_prg
--
-- purpose: File watcher program, invoked by scheduler.
--
-- ---------------------------------------------------------------------------
procedure fw_prg(p_payload in sys.scheduler_filewatcher_result);

-- ---------------------------------------------------------------------------
--
-- function: double_quote_str
--
-- purpose: Returns string with double quotes around it.
--
-- ---------------------------------------------------------------------------
function double_quote_str(p_str in varchar2) return varchar2;

-- ---------------------------------------------------------------------------
--
-- function: obfuscate
--
-- purpose: Returns string obfuscated.
--
-- ---------------------------------------------------------------------------
function obfuscate(p_str in varchar2) return varchar2;

-- ---------------------------------------------------------------------------
--
-- function: unobfuscate
--
-- purpose: Returns string unobfuscated.
--
-- ---------------------------------------------------------------------------
function unobfuscate(p_str in varchar2) return varchar2;

-- ---------------------------------------------------------------------------
--
-- procedure: send_email_msg
--
-- purpose: Sends an email message.
-- 
-- ---------------------------------------------------------------------------
procedure send_email_msg
(
  p_sender     in varchar2
, p_recipients in varchar2
, p_subject    in varchar2
, p_message    in clob
);

end jmh_edw_etl;
/
