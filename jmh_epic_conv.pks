CREATE OR REPLACE package epic.jmh_epic_conv
-- --------------------------------------------------------------------------
--
-- module name: EPIC.jmh_epic_conv
--
-- description: Encapsulates routines to extract data for conversion into Epic.
--
-- Uses the following packages:
--   jmh_log_pkg   - logging
--   jmh_index_pkg - indexes
--   jmh_edw_etl   - ETL modules
--
-- --------------------------------------------------------------------------
--
-- rev log
--
-- date:    09-14-2012
-- author:  Craig Nobili
-- desc:    original
--
-- date:    12-20-2012
-- desc:    Added exclusion logic to eliminate bad Passport records from 
--          extract.
--
--
-- date:    01-08-2013
-- desc:    Added duplicate matching logic for Passport data.
--
-- date:    02-25-2013
-- desc:    Added procedure ld_if_bh_mpi which keeps a history of files
--          generated from BH (i.e. if_bh_mpi_ext).
--
-- date:    03-01-2013
-- desc:    Added procedure ld_if_pat_enc.
--
-- date:    03-05-2013
-- desc:    Added procedures put_file and get_file
--
-- date:    03-14-2013
-- desc:    Added procedure ld_if_pat_enc_orphan
--
-- date:    03-22-2013
-- desc:    Added procedure gen_passport_patient2
--
-- date:    04-17-2013
-- desc:    Added prefix and suffix to ld_stage_passport, ld_passport, ld_dim_passport
--
-- date:    04-29-2013
-- desc:    Added procedures ld_stage_passport_bld, ld_passport_bld
--
-- date:    08-13-2013
-- desc:    Added procedure ld_kbs_patient
--
-- date:    08-25-2013
-- desc:    Added procedure ld_dim_cl_patient
--
-- date:    08-28-2013
-- desc:    Added procedure ld_kbs_provider
--
-- date:    09-09-2013
-- desc:    Added procedure gen_passport_patient_no_mrn
--
-- date:    10-21-2013
-- desc:    Added procedures ld_stage_passport_inactive, ld_passport_inactive
--
-- date:    11-11-2013
-- desc:    Added procedure ld_mergelog
--
-- date:    01-06-2014
-- desc:    Added routines for BH
--
-- date:    01-15-2014
-- desc:    Data conversion validation extracts and routines.
--
-- date:    02-06-2014
-- desc:    Duplicate matching logic for Epic.
--
-- date:    02-27-2014
-- desc:    Multiple MRN issue
--
-- date:    03-05-2014
-- desc:    In-Paient go-live additions
--
-- date:    04-02-2014
-- desc:    Additions for Multiple MRN issue
--
-- date:    09-08-2014
-- desc:    DataArk MRN mapping
--
-- date:    09-10-2014
-- desc:    Add additional data elements to dim_cl_provder
--
-- --------------------------------------------------------------------------
as

--
-- Globals Constants
--

g_PRG_NAME    constant varchar2(30) := 'JMH_EPIC_CONV';

g_TRUE  constant varchar2(1) := 'T';
g_FALSE constant varchar2(1) := 'F';

--
-- Matching Weights
--
g_WT_LAST_NAME   pls_integer := 8;
g_WT_FIRST_NAME  pls_integer := 3;
g_WT_MIDDLE_NAME pls_integer := 1;
g_WT_GENDER      pls_integer := 1;
g_WT_DOB         pls_integer := 7;
g_WT_SSN         pls_integer := 12;

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
  p_query          in varchar2
, p_dir            in varchar2 := null
, p_filename       in varchar2 := null
, p_delim          in varchar2 := null
, p_tablename      in varchar2 := null
, p_add_header_rec in boolean := false
);

-- ---------------------------------------------------------------------------
--
-- procedure: truncate_table
--
-- purpose: Truncates the table passed in.
--
-- ---------------------------------------------------------------------------
procedure truncate_table(p_table_name in varchar2);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_mergelog
--
-- purpose: Loads the mergelog table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_mergelog;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport
--
-- purpose: Loads the stage_passport table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport_inactive
--
-- purpose: Loads the stage_passport_inactive table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport_inactive;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport_bld
--
-- purpose: Loads the stage_passport_bld table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport_bld;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_plus
--
-- purpose: Loads the stage_plus table using plus tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_plus;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport
--
-- purpose: Loads the passport table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_inactive
--
-- purpose: Loads the passport_inactive table using stage_passport_inactive, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_inactive(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_bld
--
-- purpose: Loads the passport_bld table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_bld(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_passport
--
-- purpose: Loads the dim_passport table from the passport table, excluding
--   bad records and joining in items from the stage_plus table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_personmatch
--
-- purpose: Loads the stage_personmatch table from passport, only selects
--          records where verified = 2
--
-- ---------------------------------------------------------------------------
procedure ld_stage_personmatch;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_passport_patient
--
-- purpose: Generate Patient master data file from Passport.
--
-- ---------------------------------------------------------------------------
procedure gen_passport_patient
(
  p_filename       in varchar2 := 'jmh_patient.dat'
, p_dir            in varchar2 := 'JMH_EPIC_CONV'  
, p_delim          in varchar2 := '|'
, p_add_header_rec in boolean  := false
);

-- ---------------------------------------------------------------------------
--
-- procedure: gen_passport_patient2
--
-- purpose: Generate Patient master data file from Passport.
--
-- ---------------------------------------------------------------------------
procedure gen_passport_patient2
(
  p_filename       in varchar2 := 'jmh_patient_mrns.dat'
, p_dir            in varchar2 := 'JMH_EPIC_CONV'  
, p_delim          in varchar2 := '|'
, p_add_header_rec in boolean  := false
);

-- ---------------------------------------------------------------------------
--
-- procedure: gen_passport_patient_no_mrn
--
-- purpose: Generate Patient master data file from Passport for records that
--   are missing an Epic MRN.
--
-- ---------------------------------------------------------------------------
procedure gen_passport_patient_no_mrn
(
  p_filename       in varchar2 := 'jmh_patient_no_epic_mrn.dat'
, p_dir            in varchar2 := 'JMH_EPIC_CONV'  
, p_delim          in varchar2 := '|'
, p_add_header_rec in boolean  := false
);

-- ---------------------------------------------------------------------------
--
-- procedure: gen_data_ark_mrn
--
-- purpose: Generate MRN mapping for DataArk
--
-- ---------------------------------------------------------------------------
procedure gen_data_ark_mrn
(
  p_filename       in varchar2 := 'jmh_data_ark_mrn.dat'
, p_dir            in varchar2 := 'JMH_EPIC_CONV'  
, p_delim          in varchar2 := '|'
, p_add_header_rec in boolean  := true
);

-- ---------------------------------------------------------------------------
--
-- procedure: gen_known_nonduplicate
--
-- purpose: Generates a flat of known non-duplicate HNE Ids from Passport.
--   file format is: HNEID1|HNEID2
--
-- ---------------------------------------------------------------------------
procedure gen_known_nonduplicate
(
  p_filename       in varchar2 := 'jmh_known_nonduplicate.dat'
, p_dir            in varchar2 := 'JMH_EPIC_CONV'  
, p_delim          in varchar2 := '|'
, p_add_header_rec in boolean  := false
); 

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dup_passport
--
-- purpose: Loads the dup_passport table with potential duplicates from the
--          vw_match_* views.
--
-- ---------------------------------------------------------------------------
procedure ld_dup_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: run_passport_to_epic
--
-- purpose: Runs all procs to generate Passport files and sftp to Epic MPI server.
--
-- To send email, the following has to be granted as sys because jmh_util_pkg
-- is run under invoker rights:
--
--  Create ACL
--  dbms_network_acl_admin.create_acl
--  (
--    acl         => 'utl_mail_epic.xml',
--    description => 'Allow mail to be send',
--    principal   => 'EPIC',
--    is_grant    => TRUE,
--    privilege   => 'connect'
--  );
--  commit;
--
--  -- Add Privilege
--
--  dbms_network_acl_admin.add_privilege
--  (
--    acl       => 'utl_mail_epic.xml',
--    principal => 'EPIC',
--    is_grant  => TRUE,
--    privilege => 'resolve'
--  );
--  commit;
--
-- Assign ACL
-- 
--  dbms_network_acl_admin.assign_acl
--  (
--    acl  => 'utl_mail_epic.xml',
--    host => 'mr1.hsys.local'
--  );
--  commit;
--
--end;
--
-- ---------------------------------------------------------------------------
procedure run_passport_to_epic(p_email_id in varchar2 := null);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_bh_mpi
--
-- purpose: Loads the if_bh_mpi table from if_bh_mpi_ext.
--
-- ---------------------------------------------------------------------------
procedure ld_if_bh_mpi;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_bh_mpi_cdc
--
-- purpose: Loads the bh_mpi_cdc table by only selecting changes (new and updated
--   records) from the last BH load.  Compares the two most recent BH files
--   by selecting on the 2 most recent load_dt's in the if_bh_mpi table.
--
-- ---------------------------------------------------------------------------
procedure ld_bh_mpi_cdc;

-- ---------------------------------------------------------------------------
--
-- procedure: gen_bh_delta
--
-- purpose: Generate BH delta file.
--
-- ---------------------------------------------------------------------------
procedure gen_bh_delta
(
  p_filename        in varchar2 := 'BHEPICDL_DELTA.TXT'
, p_dir             in varchar2 := 'JMH_EPIC_CONV'  
, p_delim           in varchar2 := '|'
, p_add_header_rec  in boolean  := false
);

-- ---------------------------------------------------------------------------
--
-- procedure: run_bh_to_epic
--
-- purpose: Runs all procs to generate BH delta file and sftp to Epic MPI server.
--
-- ---------------------------------------------------------------------------
procedure run_bh_to_epic;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_pat_enc
--
-- purpose: Loads the if_pat_enc table from if_pat_enc_ext.
--
-- ---------------------------------------------------------------------------
procedure ld_if_pat_enc;

-- ---------------------------------------------------------------------------
--
-- procedure: put_file
--
-- purpose: Inserts an OS file into blob column of conv_files table.
--
-- ---------------------------------------------------------------------------
procedure put_file
(
  p_dirname     in varchar2 := 'JMH_EPIC_CONV'
, p_filename    in varchar2
, p_description in varchar2
);

-- ---------------------------------------------------------------------------
--
-- procedure: get_file
--
-- purpose: Gets a file or all files from conv_files table.
--   If p_file_seq is null, gets all files in table.
--
-- ---------------------------------------------------------------------------
procedure get_file
(
  p_dirname  in varchar2 := 'JMH_EPIC_CONV'
, p_file_seq in number := null
);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_pat_enc_orphan
--
-- purpose: Loads the if_pat_enc_orphan table with encounters that don't
--   have a matching Passport master patient record.
--
-- ---------------------------------------------------------------------------
procedure ld_if_pat_enc_orphan;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_clarity_ser
--
-- purpose: Loads the CLARITY_SER table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_clarity_ser;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_ser_id
--
-- purpose: Loads the IDENTITY_SER_ID table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_ser_id;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_patient
--
-- purpose: Loads the PATIENT table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_patient;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_id
--
-- purpose: Loads the IDENTITY_ID table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_id;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_id_type
--
-- purpose: Loads the IDENTITY_ID_TYPE table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_id_type;

-- ---------------------------------------------------------------------------
--
-- procedure: beg_etl_email
--
-- purpose: Email beginning of MPI file generation process.
--
-- ---------------------------------------------------------------------------
procedure beg_etl_email;

-- ---------------------------------------------------------------------------
--
-- procedure: end_etl_email
--
-- purpose: Email end of MPI file generation process.
--
-- ---------------------------------------------------------------------------
procedure end_etl_email;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_patient
--
-- purpose: Loads the dim_kbs_paient table by selecting data from Chronicles
-- using java and kbsql.
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_patient(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_provider
--
-- purpose: Loads the dim_kbs_provider table by selecting data from Chronicles
-- using java and kbsql.
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_provider(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_plus
--
-- purpose: Loads the passport_plus table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_plus(p_load_stg in varchar2 := g_TRUE);

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_passport_plus
--
-- purpose: Loads the dim_passport_plus table from the passport_plus table, excluding
--   bad records and joining in items from the stage_plus table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_passport_plus;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_cl_patient
--
-- purpose: Loads the dim_cl_patient table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_cl_patient;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_cl_provider
--
-- purpose: Loads the dim_cl_provider table
--
-- ---------------------------------------------------------------------------
procedure ld_dim_cl_provider;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_pat_enc
--
-- purpose: Loads the kbs_pat_enc table
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_pat_enc;

end jmh_epic_conv;
/
