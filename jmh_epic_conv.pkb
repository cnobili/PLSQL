CREATE OR REPLACE package body EPIC.jmh_epic_conv
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
-- Private Globals
--
g_jre_exe varchar2(500);
g_java_classpath varchar2(500);

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

--
-- Public Methods
--

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

-- ---------------------------------------------------------------------------
--
-- procedure: ld_mergelog
--
-- purpose: Loads the mergelog table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_mergelog
is
begin

 jmh_log_pkg.wlog('Begin ld_mergelog' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('mergelog');
 jmh_index_pkg.make_index_unusable('mergelog');

 insert /*+ append parallel(e) */ into mergelog e
 (
   survivinghneid   
 , nonsurvivinghneid
 , status         
 , systemid       
 , activeobjid    
 , hneuserid      
 , createdate     
 , eventdttm      
 , transactiondttm
 , loaded_dt
 , loaded_by
 )
 select
   survivinghneid   
 , nonsurvivinghneid
 , status         
 , systemid       
 , activeobjid    
 , hneuserid      
 , createdate     
 , eventdttm      
 , transactiondttm
 , sysdate
 , g_PRG_NAME
 from
   hne_live.mergelog@passport
 ;

 commit;
 jmh_index_pkg.rebuild_index('mergelog');
 
 jmh_log_pkg.wlog('End ld_mergelog' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_mergelog'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_mergelog;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport
--
-- purpose: Loads the stage_passport table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport
is
begin

 jmh_log_pkg.wlog('Begin ld_stage_passport' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('stage_passport');
 jmh_index_pkg.make_index_unusable('stage_passport');

 insert /*+ append parallel(e) */ into stage_passport e
 (
   objid
 , id
 , id_type
 , asgnauthorityid
 , hne_id
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , createdate
 , active 
 , loaded_dt
 , loaded_by
 )
 with map as
 (
 select
   i.objid
 , i.id
 , i.type as id_type
 , i.asgnauthorityid
 , h.id as hne_id
 , i.active 
 from
   hne_live.hnemap@passport h
   inner join
   hne_live.idmap@passport i
   on h.objid = i.objid
 where 1 = 1
   and i.active = 1
   and h.pseudopersonind = 0
   and h.active = 1
   and
   (
		 (i.type = 'SSN')
		 or
		 (i.type = 'MRN' and i.asgnauthorityid in (294517187, 219953093, 1169281912, 1198649375, 4, 236357570))
   )
 )
 , mapp as
 (
 select
   h.objid
 , h.id
 , h.id_type
 , h.asgnauthorityid
 , h.hne_id
 , p.lastname   as last_name
 , p.firstname  as first_name
 , p.middlename as middle_name
 , p.prefix
 , p.suffix
 , p.createdate as createdate
 , h.active 
 from
   map h
   left outer join
   hne_live.personname@passport p
   on p.objid = h.objid
   and p.currentdata = 1
   and p.type = 'PatientName'
 )
 , mapd as
 (
 select
   m.*
 , p.thedate as dob
 from
   mapp m
   left outer join
   hne_live.persondateinfo@passport p
   on p.objid = m.objid
   and p.currentdata = 1
   and p.type = 'DOB'
 )
 , mapg as
 (
 select
   m.*
 , c.code as gender
 from
   mapd m
   left outer join
   hne_live.personsex@passport p
   on p.objid = m.objid
   and p.currentdata = 1
   left outer join
   hne_live.codemap@passport c
   on c.codeid = p.sexcodeid
 )
 , mapa as
 (
 select
   m.*
 , p.street           as address1
 , p.otherdesignation as address2
 , p.city
 , c.code   as state
 , p.zip
 from
   mapg m
   left outer join
   hne_live.address@passport p
   on p.objid = m.objid
   and p.currentdata = 1
   and p.type = 'Home'
   left outer join
   hne_live.codemap@passport c
   on c.codeid = p.statecdid
   and c.category = 'State'
 )
 select
   m.objid
 , m.id
 , m.id_type
 , m.asgnauthorityid
 , m.hne_id
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , null         as phone
 , null         as last_update_dt
 , null         as alias_name
 , null         as email_address
 , null         as phone_business
 , null         as phone_cell
 , null         as ethnicity
 , null         as primary_language
 , null         as race
 , null         as emerg_contact_name
 , null         as emerg_contact_phone
 , m.createdate 
 , m.active 
 , sysdate      as loaded_dt
 , g_PRG_NAME   as loaded_by
 from
   mapa m
 ;

 commit;
 jmh_index_pkg.rebuild_index('stage_passport');
 jmh_log_pkg.wlog('End ld_stage_passport' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_stage_passport'
    , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_stage_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport_inactive
--
-- purpose: Loads the stage_passport_inactive table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport_inactive
is
begin

 jmh_log_pkg.wlog('Begin ld_stage_passport_inactive' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('stage_passport_inactive');
 jmh_index_pkg.make_index_unusable('stage_passport_inactive');

 insert /*+ append parallel(e) */ into stage_passport_inactive e
 (
   objid
 , id
 , id_type
 , asgnauthorityid
 , hne_id
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , createdate
 , active 
 , loaded_dt
 , loaded_by
 )
 with map as
 (
 select
   i.objid
 , i.id
 , i.type as id_type
 , i.asgnauthorityid
 , h.id as hne_id
 , i.active 
 from
   hne_live.hnemap@passport h
   inner join
   hne_live.idmap@passport i
   on h.objid = i.objid
 where 1 = 1
   and i.active = 0
   and h.pseudopersonind = 0
   and h.active = 0
   and
   (
		 (i.type = 'SSN')
		 or
		 (i.type = 'MRN' and i.asgnauthorityid in (294517187, 219953093, 1169281912, 1198649375, 4, 236357570))
   )
 )
 , mapp as
 (
 select
   h.objid
 , h.id
 , h.id_type
 , h.asgnauthorityid
 , h.hne_id
 , p.lastname   as last_name
 , p.firstname  as first_name
 , p.middlename as middle_name
 , p.prefix
 , p.suffix
 , p.createdate as createdate
 , h.active 
 from
   map h
   left outer join
   hne_live.personname@passport p
   on p.objid = h.objid
   and p.currentdata = 0
   and p.type = 'PatientName'
 )
 , mapd as
 (
 select
   m.*
 , p.thedate as dob
 from
   mapp m
   left outer join
   hne_live.persondateinfo@passport p
   on p.objid = m.objid
   and p.currentdata = 0
   and p.type = 'DOB'
 )
 , mapg as
 (
 select
   m.*
 , c.code as gender
 from
   mapd m
   left outer join
   hne_live.personsex@passport p
   on p.objid = m.objid
   and p.currentdata = 0
   left outer join
   hne_live.codemap@passport c
   on c.codeid = p.sexcodeid
 )
 , mapa as
 (
 select
   m.*
 , p.street           as address1
 , p.otherdesignation as address2
 , p.city
 , c.code   as state
 , p.zip
 from
   mapg m
   left outer join
   hne_live.address@passport p
   on p.objid = m.objid
   and p.currentdata = 0
   and p.type = 'Home'
   left outer join
   hne_live.codemap@passport c
   on c.codeid = p.statecdid
   and c.category = 'State'
 )
 select
   m.objid
 , m.id
 , m.id_type
 , m.asgnauthorityid
 , m.hne_id
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , null         as phone
 , null         as last_update_dt
 , null         as alias_name
 , null         as email_address
 , null         as phone_business
 , null         as phone_cell
 , null         as ethnicity
 , null         as primary_language
 , null         as race
 , null         as emerg_contact_name
 , null         as emerg_contact_phone
 , m.createdate 
 , m.active 
 , sysdate      as loaded_dt
 , g_PRG_NAME   as loaded_by
 from
   mapa m
 ;

 commit;
 jmh_index_pkg.rebuild_index('stage_passport_inactive');
 jmh_log_pkg.wlog('End ld_stage_passport_inactive' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_stage_passport_inactive'
    , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_stage_passport_inactive;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_passport_bld
--
-- purpose: Loads the stage_passport_bld table using passport tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_passport_bld
is
begin

 jmh_log_pkg.wlog('Begin ld_stage_passport_bld' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('stage_passport_bld');
 jmh_index_pkg.make_index_unusable('stage_passport_bld');

 insert /*+ append parallel(e) */ into stage_passport_bld e
 (
   objid
 , id
 , id_type
 , asgnauthorityid
 , hne_id
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , createdate
 , loaded_dt
 , loaded_by
 )
 with map as
 (
 select
   i.objid
 , i.id
 , i.type as id_type
 , i.asgnauthorityid
 , h.id as hne_id
 from
   hne_build.hnemap@passport_build h
   inner join
   hne_build.idmap@passport_build i
   on h.objid = i.objid
 where h.active = 1
   and h.pseudopersonind = 0
   and i.active = 1
   and
   (
		 (i.type = 'SSN')
		 or
		 (i.type = 'MRN' and i.asgnauthorityid in (1035179444, 148742948, 959727789, 4, 5, 440136160))
   )
 )
 , mapp as
 (
 select
   h.objid
 , h.id
 , h.id_type
 , h.asgnauthorityid
 , h.hne_id
 , p.lastname   as last_name
 , p.firstname  as first_name
 , p.middlename as middle_name
 , p.prefix
 , p.suffix
 , p.createdate as createdate
 from
   map h
   left outer join
   hne_build.personname@passport_build p
   on p.objid = h.objid
   and p.currentdata = 1
   and p.type = 'PatientName'
 )
 , mapd as
 (
 select
   m.*
 , p.thedate as dob
 from
   mapp m
   left outer join
   hne_build.persondateinfo@passport_build p
   on p.objid = m.objid
   and p.currentdata = 1
   and p.type = 'DOB'
 )
 , mapg as
 (
 select
   m.*
 , c.code as gender
 from
   mapd m
   left outer join
   hne_build.personsex@passport_build p
   on p.objid = m.objid
   and p.currentdata = 1
   left outer join
   hne_build.codemap@passport_build c
   on c.codeid = p.sexcodeid
 )
 , mapa as
 (
 select
   m.*
 , p.street           as address1
 , p.otherdesignation as address2
 , p.city
 , c.code   as state
 , p.zip
 from
   mapg m
   left outer join
   hne_build.address@passport_build p
   on p.objid = m.objid
   and p.currentdata = 1
   and p.type = 'Home'
   left outer join
   hne_build.codemap@passport_build c
   on c.codeid = p.statecdid
   and c.category = 'State'
 )
 select
   m.objid
 , m.id
 , m.id_type
 , m.asgnauthorityid
 , m.hne_id
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , null         as phone
 , null         as last_update_dt
 , null         as alias_name
 , null         as email_address
 , null         as phone_business
 , null         as phone_cell
 , null         as ethnicity
 , null         as primary_language
 , null         as race
 , m.createdate 
 , sysdate      as loaded_dt
 , g_PRG_NAME   as loaded_by
 from
   mapa m
 ;

 commit;
 jmh_index_pkg.rebuild_index('stage_passport_bld');
 jmh_log_pkg.wlog('End ld_stage_passport_bld' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_stage_passport_bld'
    , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_stage_passport_bld;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_plus
--
-- purpose: Loads the stage_plus table using plus tables.
--
-- ---------------------------------------------------------------------------
procedure ld_stage_plus
is
begin

 jmh_log_pkg.wlog('Begin ld_stage_plus' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('stage_plus');
 jmh_index_pkg.make_index_unusable('stage_plus');

  insert /*+ append parallel(x) */ into stage_plus x 
  (
    source_system_cd
  , source_system_id
  , hne_id          
  , plus_mrn  
  , last_name       
  , first_name      
  , middle_name     
  , gender          
  , dob             
  , address1        
  , address2        
  , city            
  , state           
  , zip             
  , phone           
  , ssn             
  , last_update_dt  
  , alias_name      
  , email_address   
  , phone_business  
  , phone_cell      
  , ethnicity       
  , primary_language
  , race  
  , emerg_contact_name
  , emerg_contact_phone
  , loaded_dt       
  , loaded_by       
  )
  select
    plus.*
	, sysdate             as loaded_dt       
	, g_PRG_NAME          as loaded_by       
	from
	  vw_stage_plus plus
	;    
 
 commit;
 jmh_index_pkg.rebuild_index('stage_plus');
 jmh_log_pkg.wlog('End ld_stage_plus' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_stage_plus'
    , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_stage_plus;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport
--
-- purpose: Loads the passport table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport(p_load_stg in varchar2 := g_TRUE)
is
begin

 jmh_log_pkg.wlog('Begin ld_passport' , jmh_log_pkg.LOG_NORM);

 -- Load passport data first, then perform pivot on local database.
 -- Performing pivot on remote database causes sort and temp space to fill up there.
 if (p_load_stg = g_TRUE) then
   ld_stage_passport;
 end if;

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('passport');
 jmh_index_pkg.make_index_unusable('passport');

 insert /*+ append parallel(e) */ into passport e
 (
   source_system_cd
 , source_system_id
 , epic_mrn 
 , hne_id
 , star_mrn
 , meditech_mrn
 , plus_mrn
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , ssn
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , createdate
 , active 
 , loaded_dt
 , loaded_by
 )
 with mapm as
 (
 select
   'PASSPORT'     as source_system_cd
 , i.objid        as source_system_id
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name
 , null             as email_address
 , null             as phone_business
 , null             as phone_cell
 , null             as ethnicity
 , null             as primary_language
 , null             as race
 , null             as emerg_contact_name
 , null             as emerg_contact_phone
 , i.createdate
 , i.active 
 , max
   (
     case
     when i.id_type = 'SSN'
     then i.id
     else null
     end
   ) as ssn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 294517187
     then i.id
     else null
     end
   ) as star_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 219953093
     then i.id
     else null
     end
   ) as meditech_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1169281912
     then i.id
     else null
     end
   ) as plus_cust1_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1198649375
     then i.id
     else null
     end
   ) as plus_cust2_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 4
     then i.id
     else null
     end
    ) as plus_cust3_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 236357570
     then i.id
     else null
     end
    ) as epic_mrn
 from
   stage_passport i
 group by
   'PASSPORT'
 , i.objid   
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name 
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , i.createdate
 , i.active 
 )
 select
   m.source_system_cd
 , m.source_system_id
 , m.epic_mrn 
 , m.hne_id
 , m.star_mrn
 , m.meditech_mrn
 , coalesce(m.plus_cust1_mrn, m.plus_cust2_mrn, m.plus_cust3_mrn) as plus_mrn
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , m.phone
 , m.ssn
 , m.last_update_dt
 , m.alias_name
 , m.email_address
 , m.phone_business
 , m.phone_cell
 , m.ethnicity
 , m.primary_language
 , m.race
 , m.emerg_contact_name
 , m.emerg_contact_phone
 , m.createdate
 , m.active 
 , sysdate          as loaded_dt
 , g_PRG_NAME       as loaded_by
 from
   mapm m
 ;

 commit;
 jmh_index_pkg.rebuild_index('passport');
 jmh_log_pkg.wlog('End ld_passport' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_passport'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_inactive
--
-- purpose: Loads the passport_inactive table using stage_passport_inactive, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_inactive(p_load_stg in varchar2 := g_TRUE)
is
begin

 jmh_log_pkg.wlog('Begin ld_passport_inactive' , jmh_log_pkg.LOG_NORM);

 -- Load passport_inactive data first, then perform pivot on local database.
 -- Performing pivot on remote database causes sort and temp space to fill up there.
 if (p_load_stg = g_TRUE) then
   ld_stage_passport_inactive;
 end if;

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('passport_inactive');
 jmh_index_pkg.make_index_unusable('passport_inactive');

 insert /*+ append parallel(e) */ into passport_inactive e
 (
   source_system_cd
 , source_system_id
 , epic_mrn 
 , hne_id
 , star_mrn
 , meditech_mrn
 , plus_mrn
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , ssn
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , createdate
 , active 
 , loaded_dt
 , loaded_by
 )
 with mapm as
 (
 select 
   'PASSPORT'     as source_system_cd
 , i.objid        as source_system_id
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name
 , null             as email_address
 , null             as phone_business
 , null             as phone_cell
 , null             as ethnicity
 , null             as primary_language
 , null             as race
 , null             as emerg_contact_name
 , null             as emerg_contact_phone
 , i.createdate
 , i.active 
 , max
   (
     case
     when i.id_type = 'SSN'
     then i.id
     else null
     end
   ) as ssn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 294517187
     then i.id
     else null
     end
   ) as star_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 219953093
     then i.id
     else null
     end
   ) as meditech_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1169281912
     then i.id
     else null
     end
   ) as plus_cust1_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1198649375
     then i.id
     else null
     end
   ) as plus_cust2_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 4
     then i.id
     else null
     end
    ) as plus_cust3_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 236357570
     then i.id
     else null
     end
    ) as epic_mrn
 from
   stage_passport_inactive i
 group by
   'PASSPORT'
 , i.objid   
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name 
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , i.createdate
 , i.active 
 )
 , mapm2 as
 (
 select
   m.source_system_cd
 , m.source_system_id
 , m.epic_mrn 
 , m.hne_id
 , m.star_mrn
 , m.meditech_mrn
 , coalesce(m.plus_cust1_mrn, m.plus_cust2_mrn, m.plus_cust3_mrn) as plus_mrn
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , m.phone
 , m.ssn
 , m.last_update_dt
 , m.alias_name
 , m.email_address
 , m.phone_business
 , m.phone_cell
 , m.ethnicity
 , m.primary_language
 , m.race
 , m.emerg_contact_name
 , m.emerg_contact_phone
 , m.createdate
 , m.active 
 , sysdate          as loaded_dt
 , g_PRG_NAME       as loaded_by
 , row_number() over(partition by m.first_name, m.last_name, m.gender, m.dob, m.hne_id, m.star_mrn, m.meditech_mrn order by m.hne_id) as rn 
 from
   mapm m
 )
 select
   m.source_system_cd
 , m.source_system_id
 , m.epic_mrn 
 , m.hne_id
 , m.star_mrn
 , m.meditech_mrn
 , m.plus_mrn
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , m.phone
 , m.ssn
 , m.last_update_dt
 , m.alias_name
 , m.email_address
 , m.phone_business
 , m.phone_cell
 , m.ethnicity
 , m.primary_language
 , m.race
 , m.emerg_contact_name
 , m.emerg_contact_phone
 , m.createdate
 , m.active 
 , m.loaded_dt
 , m.loaded_by
 from
   mapm2 m
 where m.rn = 1
 ;

 commit;
 jmh_index_pkg.rebuild_index('passport_inactive');
 jmh_log_pkg.wlog('End ld_passport_inactive' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_passport_inactive'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_passport_inactive;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_bld
--
-- purpose: Loads the passport_bld table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_bld(p_load_stg in varchar2 := g_TRUE)
is
begin

 jmh_log_pkg.wlog('Begin ld_passport_bld' , jmh_log_pkg.LOG_NORM);

 -- Load passport data first, then perform pivot on local database.
 -- Performing pivot on remote database causes sort and temp space to fill up there.
 if (p_load_stg = g_TRUE) then
   ld_stage_passport_bld;
 end if;

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('passport_bld');
 jmh_index_pkg.make_index_unusable('passport_bld');

 insert /*+ append parallel(e) */ into passport_bld e
 (
   source_system_cd
 , source_system_id
 , hne_id
 , star_mrn
 , meditech_mrn
 , plus_mrn
 , epic_id
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , ssn
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , createdate
 , loaded_dt
 , loaded_by
 )
 with mapm as
 (
 select
   'PASSPORT'     as source_system_cd
 , i.objid        as source_system_id
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name
 , null             as email_address
 , null             as phone_business
 , null             as phone_cell
 , null             as ethnicity
 , null             as primary_language
 , null             as race
 , i.createdate
 , max
   (
     case
     when i.id_type = 'SSN'
     then i.id
     else null
     end
   ) as ssn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1035179444
     then i.id
     else null
     end
   ) as star_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 148742948
     then i.id
     else null
     end
   ) as meditech_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 959727789
     then i.id
     else null
     end
   ) as plus_cust1_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 4
     then i.id
     else null
     end
   ) as plus_cust2_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 5
     then i.id
     else null
     end
    ) as plus_cust3_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 440136160
     then i.id
     else null
     end
    ) as epic_id
 from
   stage_passport_bld i
 group by
   'PASSPORT'
 , i.objid   
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name 
 , null
 , null
 , null
 , null
 , null
 , null
 , i.createdate
 )
 select
   m.source_system_cd
 , m.source_system_id
 , m.hne_id
 , m.star_mrn
 , m.meditech_mrn
 , coalesce(m.plus_cust1_mrn, m.plus_cust2_mrn, m.plus_cust3_mrn) as plus_mrn
 , m.epic_id
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , m.phone
 , m.ssn
 , m.last_update_dt
 , m.alias_name
 , m.email_address
 , m.phone_business
 , m.phone_cell
 , m.ethnicity
 , m.primary_language
 , m.race
 , m.createdate
 , sysdate          as loaded_dt
 , g_PRG_NAME       as loaded_by
 from
   mapm m
 where 1 = 1
   and not
   (
     ( m.star_mrn is null and m.meditech_mrn is null and coalesce(m.plus_cust1_mrn, m.plus_cust2_mrn, m.plus_cust3_mrn) is null )
     or ( m.first_name || m.last_name is null ) 
     or ( regexp_like(m.first_name || m.last_name, '[[:digit:]]') ) 
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%DO NOT USE%' ) 
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%BLOOD BANK%' )
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%BLOODBANK%' )
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%REFUND%' ) 
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%UNKNOWN%' )   
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%CONFIDENTIAL%' )  
     or ( upper(m.first_name || m.middle_name || m.last_name) like '%TEST%' ) 
    -- or ( length(m.last_name) = 1 )
    --or ( m.createdate < to_date('09/01/2003', 'mm/dd/yyyy') )
   )     
 ;

 commit;
 jmh_index_pkg.rebuild_index('passport_bld');
 jmh_log_pkg.wlog('End ld_passport_bld' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_passport_bld'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_passport_bld;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_passport
--
-- purpose: Loads the dim_passport table from the passport table, excluding
--   bad records and joining in items from the stage_plus table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_passport
is
begin

 jmh_log_pkg.wlog('Begin ld_dim_passport' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('dim_passport');
 jmh_index_pkg.make_index_unusable('dim_passport');

 insert /*+ append parallel(e) */ into dim_passport e
 (
   source_system_cd
 , source_system_id
 , epic_mrn 
 , hne_id          
 , star_mrn        
 , meditech_mrn    
 , plus_mrn         
 , last_name        
 , first_name       
 , middle_name      
 , prefix
 , suffix
 , gender           
 , dob              
 , address1         
 , address2         
 , city             
 , state            
 , zip              
 , phone            
 , ssn              
 , last_update_dt   
 , alias_name       
 , email_address    
 , phone_business   
 , phone_cell       
 , ethnicity        
 , primary_language 
 , race             
 , emerg_contact_name
 , emerg_contact_phone
 , createdate       
 , loaded_dt        
 , loaded_by        
 )
 with plus as
 (
 select
   hne_id
 , phone          as plus_phone
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , row_number() over(partition by hne_id order by plus_mrn) as rn
 from
   epic.stage_plus
 )
 , pass as
 (
 select
   pa.source_system_cd
 , pa.source_system_id
 , pa.epic_mrn 
 , pa.hne_id
 , pa.star_mrn
 , pa.meditech_mrn
 , pa.plus_mrn
 , pa.last_name
 , pa.first_name
 , pa.middle_name
 , pa.prefix
 , pa.suffix
 , pa.gender
 , pa.dob
 , pa.address1
 , pa.address2
 , pa.city
 , pa.state
 , pa.zip
 , pa.phone
 , pa.ssn
 , pa.last_update_dt
 , pa.alias_name
 , pl.plus_phone
 , pl.email_address
 , pl.phone_business
 , pl.phone_cell
 , pl.ethnicity
 , pl.primary_language
 , pl.race
 , pl.emerg_contact_name
 , pl.emerg_contact_phone
 , pa.createdate
 , pa.active 
 , pa.loaded_dt
 , pa.loaded_by
 , pa.first_name || pa.last_name                                 as first_last
 , pa.first_name || '#' || pa.middle_name || '#' || pa.last_name as first_middle_last
 --, row_number() over(partition by pl.hne_id order by pl.rowid) as rn
 from
   epic.passport pa
   left outer join
   plus pl
   on pl.hne_id = pa.hne_id
   and pl.rn = 1
 )
 select
   p.source_system_cd
 , p.source_system_id
 , p.epic_mrn 
 , p.hne_id
 , p.star_mrn
 , p.meditech_mrn
 , p.plus_mrn
 , p.last_name
 , p.first_name
 , p.middle_name
 , p.prefix
 , p.suffix
 , p.gender
 , p.dob
 , p.address1
 , p.address2
 , p.city
 , p.state
 , p.zip
 , p.plus_phone
 , p.ssn
 , p.last_update_dt
 , p.alias_name
 , p.email_address
 , p.phone_business
 , p.phone_cell
 , p.ethnicity
 , p.primary_language
 , p.race
 , p.emerg_contact_name
 , p.emerg_contact_phone
 , p.createdate
 , p.loaded_dt
 , p.loaded_by
 from
  pass p
where 1 = 1
  --and active = 1
  and not
  (
    ( p.star_mrn is null and p.meditech_mrn is null and p.plus_mrn is null )
    or ( p.first_last is null ) 
    or ( regexp_like(p.first_last, '[[:digit:]]') ) 
    or ( upper(p.first_middle_last) like '%DO NOT USE%' ) 
    or ( upper(p.first_middle_last) like '%BLOOD BANK%' )
    or ( upper(p.first_middle_last) like '%BLOODBANK%' )
    or ( upper(p.first_middle_last) like '%REFUND%' ) 
    or ( upper(p.first_middle_last) like '%UNKNOWN%' )   
    or ( upper(p.first_middle_last) like '%CONFIDENTIAL%' )  
    or ( upper(p.last_name) = 'TEST' ) 
    --or ( length(p.last_name) = 1 )
    --or ( p.createdate < to_date('09/01/2003', 'mm/dd/yyyy') )
  )  
  ;

 commit;
 jmh_index_pkg.rebuild_index('dim_passport');
 jmh_log_pkg.wlog('End ld_dim_passport' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_dim_passport'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_dim_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_stage_personmatch
--
-- purpose: Loads the stage_personmatch table from passport, only selects
--          records where verified = 2
--
-- ---------------------------------------------------------------------------
procedure ld_stage_personmatch
is
begin

 jmh_log_pkg.wlog('Begin ld_stage_personmatch' , jmh_log_pkg.LOG_NORM);

 truncate_table('stage_personmatch');

 insert /*+ append parallel(e) */ into stage_personmatch e
 (
   personmatchid
 , hneid
 , matchhneid
 , totalscore
 , sufficientscore
 , action
 , verified
 , createdate
 , systemid
 , msgcontrolid
 )
 select *
 from hne_live.personmatch@passport
 where verified = 2
 ;
 commit;
 
 jmh_log_pkg.wlog('End ld_stage_personmatch' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_stage_personmatch'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_stage_personmatch;

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
)
is
begin

 jmh_log_pkg.wlog('Begin gen_passport_patient' , jmh_log_pkg.LOG_NORM);
 
 dump_resultset
 (
   p_query          => 'select * from vw_epic_conv_patient_file'
 , p_dir            => p_dir
 , p_filename       => p_filename
 , p_delim          => p_delim
 , p_tablename      => null
 , p_add_header_rec => p_add_header_rec
 );
 
 jmh_log_pkg.wlog('End gen_passport_patient' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_passport_patient'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_passport_patient;

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
)
is
begin

 jmh_log_pkg.wlog('Begin gen_passport_patient2' , jmh_log_pkg.LOG_NORM);
 
 dump_resultset
 (
   p_query          => 'select * from vw_epic_conv_patient_file2'
 , p_dir            => p_dir
 , p_filename       => p_filename
 , p_delim          => p_delim
 , p_tablename      => null
 , p_add_header_rec => p_add_header_rec
 );
 
 jmh_log_pkg.wlog('End gen_passport_patient2' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_passport_patient2'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_passport_patient2;

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
)
is
begin

 jmh_log_pkg.wlog('Begin gen_passport_patient_no_mrn' , jmh_log_pkg.LOG_NORM);
 
 dump_resultset
 (
   p_query          => 'select * from vw_epic_conv_patient_no_mrn'
 , p_dir            => p_dir
 , p_filename       => p_filename
 , p_delim          => p_delim
 , p_tablename      => null
 , p_add_header_rec => p_add_header_rec
 );
 
 jmh_log_pkg.wlog('End gen_passport_patient_no_mrn' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_passport_patient_no_mrn'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_passport_patient_no_mrn;

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
)
is
begin
 jmh_log_pkg.wlog('Begin gen_data_ark_mrn' , jmh_log_pkg.LOG_NORM);
 
 dump_resultset
 (
   p_query          => 'select * from vw_data_ark_mrn_map'
 , p_dir            => p_dir
 , p_filename       => p_filename
 , p_delim          => p_delim
 , p_tablename      => null
 , p_add_header_rec => p_add_header_rec
 );
 
 jmh_log_pkg.wlog('End gen_data_ark_mrn' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_data_ark_mrn'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_data_ark_mrn;

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
) 
is
begin

  jmh_log_pkg.wlog('Begin gen_known_nonduplicate' , jmh_log_pkg.LOG_NORM);
 
  dump_resultset
  (
    --p_query          => 'select hneid, matchhneid from hne_live.personmatch@passport where verified = 2 and action = ''Associate'''
    p_query          => 'select hneid, matchhneid from stage_personmatch'
  , p_dir            => p_dir
  , p_filename       => p_filename
  , p_delim          => p_delim
  , p_tablename      => null
  , p_add_header_rec => p_add_header_rec
  );
 
  jmh_log_pkg.wlog('End gen_known_nonduplicate' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_known_nonduplicate'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_known_nonduplicate;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dup_passport
--
-- purpose: Loads the dup_passport table with potential duplicates from the
--          vw_match_* views.
--
-- ---------------------------------------------------------------------------
procedure ld_dup_passport
is
begin

 jmh_log_pkg.wlog('Begin ld_dup_passport' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('dup_passport');
 jmh_index_pkg.make_index_unusable('dup_passport');

 insert /*+ append parallel(e) */ into dup_passport e
 select
   p.*
 from
   vw_match_last_dob_sex p
 ;
 commit;
 
 insert /*+ append parallel(e) */ into dup_passport e
 select
   p.*
 from
   vw_match_ssn_dob p
 where not exists
 (
   select null from dup_passport d
   where d.pat_seq = p.pat_seq
 )
 ;
 commit; 

 insert /*+ append parallel(e) */ into dup_passport e
 select
   p.*
 from
   vw_match_ssn_last p
 where not exists
 (
   select null from dup_passport d
   where d.pat_seq = p.pat_seq
 )
 ;
 commit; 
 
 jmh_index_pkg.rebuild_index('dup_passport');
 jmh_log_pkg.wlog('End ld_dup_passport' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_dup_passport'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_dup_passport;

-- ---------------------------------------------------------------------------
--
-- procedure: run_passport_to_epic
--
-- purpose: Runs all procs to generate Passport files and sftp to Epic MPI server.
--
-- ---------------------------------------------------------------------------
procedure run_passport_to_epic(p_email_id in varchar2 := null)
is

  l_hostname varchar2(20) := 'jmhmpiprod';
  l_user     varchar2(20) := 'epicadm';
  l_pass     varchar2(20);
  l_src_file varchar2(200);
  l_dst_file varchar2(200);
  
begin

  beg_etl_email;
  
  l_pass := jmh_app_parameters_pkg.get_value('JMH_EPICADM_PWD');
  
  if (p_email_id is not null) then
    jmdba.jmh_edw_etl.exec_mailx
    (
      p_recipients => p_email_id
    , p_subject    => 'Launched run_passport_to_epic'
    , p_message    => 'Started run_passport_to_epic, another email will be sent when job is done'
    );  
  end if;

  jmh_log_pkg.wlog('Begin run_passport_to_epic' , jmh_log_pkg.LOG_NORM);

  jmh_epic_conv.ld_mergelog;
  jmh_epic_conv.ld_stage_plus;
  jmh_epic_conv.ld_passport;
  jmh_epic_conv.ld_dim_passport;
  jmh_epic_conv.ld_stage_personmatch;
  jmh_epic_conv.gen_passport_patient;
  --jmh_epic_conv.gen_passport_patient2;
  jmh_epic_conv.gen_known_nonduplicate;
  
  l_src_file := '/jmh_dw/epic_conv/jmh_patient.dat';
  l_dst_file := '/epic/conv_file/jmh_patient.dat';
  jmh_log_pkg.wlog('Send file ' || l_src_file || ' to Epic MPI server ' || l_hostname, jmh_log_pkg.LOG_NORM);
  
  jmh_edw_etl.scp_put_file
	(
	  p_hostname => l_hostname
	, p_user     => l_user
	, p_pass     => l_pass
	, p_src_file => l_src_file
	, p_dst_file => l_dst_file
  );

  l_src_file := '/jmh_dw/epic_conv/jmh_known_nonduplicate.dat';
  l_dst_file := '/epic/conv_file/jmh_known_nonduplicate.dat';
  jmh_log_pkg.wlog('Send file ' || l_src_file || ' to Epic MPI server ' || l_hostname, jmh_log_pkg.LOG_NORM);
  
  jmh_edw_etl.scp_put_file
	(
	  p_hostname => l_hostname
	, p_user     => l_user
	, p_pass     => l_pass
	, p_src_file => l_src_file
	, p_dst_file => l_dst_file
  );
  /*
  l_src_file := '/jmh_dw/epic_conv/jmh_patient_mrns.dat';
  l_dst_file := '/epic/conv_file/jmh_patient_mrns.dat';
  jmh_log_pkg.wlog('Send file ' || l_src_file || ' to Epic MPI server ' || l_hostname, jmh_log_pkg.LOG_NORM);
  
  jmh_edw_etl.scp_put_file
	(
	  p_hostname => l_hostname
	, p_user     => l_user
	, p_pass     => l_pass
	, p_src_file => l_src_file
	, p_dst_file => l_dst_file
  );
  */  
  jmh_log_pkg.wlog('End run_passport_to_epic' , jmh_log_pkg.LOG_NORM);
  
  if (p_email_id is not null) then
    jmdba.jmh_edw_etl.exec_mailx
    (
      p_recipients => p_email_id
    , p_subject    => 'Finished run_passport_to_epic'
    , p_message    => 'run_passport_to_epic successfully completed'
    );  
  end if;
  
  end_etl_email;

exception

 when others then

   if (p_email_id is not null) then
     jmdba.jmh_edw_etl.exec_mailx
     (
       p_recipients => p_email_id
     , p_subject    => 'Error run_passport_to_epic'
     , p_message    => 'Error running run_passport_to_epic' || sqlerrm
     );  
   end if;

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in run_passport_to_epic'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end run_passport_to_epic;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_bh_mpi
--
-- purpose: Loads the if_bh_mpi table from if_bh_mpi_ext.
--
-- ---------------------------------------------------------------------------
procedure ld_if_bh_mpi
is
begin

  jmh_log_pkg.wlog('Begin ld_if_bh_mpi' , jmh_log_pkg.LOG_NORM);
  
  --
  -- Do not truncate table if_bh_mpi, we are keeping prior loads (i.e. load_dt column)
  --
  
  insert /*+ append parallel(e) */ into if_bh_mpi e
  select
    sysdate           as load_dt
  , mrn
	, last_name
	, first_name
	, middle_name
	, gender
	, dob
	, address1
	, address2
	, city           
	, state          
	, zip            
	, phone          
	, ssn            
	, last_update_dt
  , alias_name        
  from
    if_bh_mpi_ext
  ;  
  commit; 

  jmh_log_pkg.wlog('End ld_if_bh_mpi' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_if_bh_mpi'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_if_bh_mpi;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_bh_mpi_cdc
--
-- purpose: Loads the bh_mpi_cdc table by only selecting changes (new and updated
--   records) from the last BH load.  Compares the two most recent BH files
--   by selecting on the 2 most recent load_dt's in the if_bh_mpi table.
--
-- ---------------------------------------------------------------------------
procedure ld_bh_mpi_cdc
is
begin

  jmh_log_pkg.wlog('Begin ld_bh_mpi_cdc' , jmh_log_pkg.LOG_NORM);

  truncate_table('bh_mpi_cdc');
  
  -- 
  -- New Records
  --
  insert into epic.bh_mpi_cdc
  select
    c.*
  , 'I'  as chg_type  
  from
    epic.if_bh_mpi c
  where c.load_dt = (select load_dt1 from epic.vw_bh_mpi_top2_load_dt)
    and c.mrn is not null
    and not exists
    (
      select null
      from epic.if_bh_mpi p
      where p.load_dt = (select load_dt2 from epic.vw_bh_mpi_top2_load_dt)
        and p.mrn = c.mrn
    ) 
  ;
  --
  -- Updates
  --
  insert into epic.bh_mpi_cdc
  select
    c.*
  , 'U' as chg_type  
  from
    epic.if_bh_mpi c
    inner join
    epic.if_bh_mpi p
    on p.mrn = c.mrn
  where c.load_dt = (select load_dt1 from epic.vw_bh_mpi_top2_load_dt)
    and p.load_dt = (select load_dt2 from epic.vw_bh_mpi_top2_load_dt)
    and not
    (
      nvl(c.last_name, '?') = nvl(p.last_name, '?')
      and nvl(c.first_name, '?') = nvl(p.first_name, '?')
      and nvl(c.middle_name, '?') = nvl(p.middle_name, '?')
      and nvl(c.gender, '?') = nvl(p.gender, '?')
      and nvl(c.dob, c.load_dt) = nvl(p.dob, c.load_dt)
      and nvl(c.address1, '?') = nvl(p.address1, '?')
      and nvl(c.address2, '?') = nvl(p.address2, '?')
      and nvl(c.city, '?') = nvl(p.city, '?')
      and nvl(c.state, '?') = nvl(p.state, '?')
      and nvl(c.zip, '?') = nvl(p.zip, '?')
      and nvl(c.phone, '?') = nvl(p.phone, '?')
      and nvl(c.ssn, '?') = nvl(p.ssn, '?')
      and nvl(c.last_update_dt, c.load_dt) = nvl(p.last_update_dt, c.load_dt)
      and nvl(c.alias_name, '?') = nvl(p.alias_name, '?')
    )
  ;
  commit;

  jmh_log_pkg.wlog('End ld_bh_mpi_cdc' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_bh_mpi_cdc'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_bh_mpi_cdc;

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
)
is
begin

  jmh_log_pkg.wlog('Begin gen_bh_delta' , jmh_log_pkg.LOG_NORM);
  
  dump_resultset
  (
    p_query          => 'select * from vw_bh_mpi_cdc'
  , p_dir            => p_dir
  , p_filename       => p_filename
  , p_delim          => p_delim
  , p_tablename      => null
  , p_add_header_rec => p_add_header_rec
  );
  
  jmh_log_pkg.wlog('End gen_bh_delta' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in gen_bh_delta'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end gen_bh_delta;

-- ---------------------------------------------------------------------------
--
-- procedure: run_bh_to_epic
--
-- purpose: Runs all procs to generate BH delta file and sftp to Epic MPI server.
--
-- ---------------------------------------------------------------------------
procedure run_bh_to_epic
is

  l_hostname varchar2(20)  := 'jmhmpiprod';
  l_user     varchar2(20)  := 'epicadm';
  l_pass     varchar2(20);
  l_src_file varchar2(200) := '/jmh_dw/epic_conv/BHEPICDL_DELTA.TXT';
  l_dst_file varchar2(200) := '/epic/conv_file/BHEPICDL_DELTA.TXT';
  
begin

  jmh_log_pkg.wlog('Begin run_bh_to_epic' , jmh_log_pkg.LOG_NORM);
  
  l_pass := jmh_app_parameters_pkg.get_value('JMH_EPICADM_PWD');  

  ld_if_bh_mpi;
  ld_bh_mpi_cdc;
  gen_bh_delta;

  jmh_log_pkg.wlog('Send file ' || l_src_file || ' to Epic MPI server ' || l_hostname, jmh_log_pkg.LOG_NORM);
  
  jmh_edw_etl.scp_put_file
	(
	  p_hostname => l_hostname
	, p_user     => l_user
	, p_pass     => l_pass
	, p_src_file => l_src_file
	, p_dst_file => l_dst_file
  );
  
  jmh_log_pkg.wlog('End run_bh_to_epic' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in run_bh_to_epic'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end run_bh_to_epic;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_pat_enc
--
-- purpose: Loads the if_pat_enc table from if_pat_enc_ext.
--
-- ---------------------------------------------------------------------------
procedure ld_if_pat_enc
is
begin

  jmh_log_pkg.wlog('Begin ld_if_pat_enc' , jmh_log_pkg.LOG_NORM);
  truncate_table('if_pat_enc');
  
  insert /*+ append parallel(e) */ into if_pat_enc e
  select
    source_system_cd
  , hne_id    
  , mrn
	, last_name
	, first_name
	, middle_name
	, gender
	, dob
	, address1
	, address2
	, city           
	, state          
	, zip            
	, phone          
	, ssn            
	, last_update_dt
  , alias_name        
  , enc_num
  , admit_dt
  , filename  
  from
    if_pat_enc_ext
  ;  
  commit; 

  jmh_log_pkg.wlog('End ld_if_pat_enc' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_if_pat_enc'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_if_pat_enc;

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
)
is
  l_query           varchar2(500);
  l_file_seq        number;
begin

  --
  -- Insert non-blob columns and return
  -- primary key used (sequence number)
  --
  insert into conv_files
  (
    dirname
  , filename
  , description
  )
  values
  (
    p_dirname
  , p_filename
  , p_description
  )
  returning file_seq into l_file_seq
  ;
  commit;

  --
  -- Build update query
  --
  l_query := 'update conv_files ' ||
             '  set file_blob = empty_blob() ' ||
             'where file_seq = ' || l_file_seq ||
             ' returning file_blob into :1'
  ;
  --
  -- Insert file into blob column in table
  --
  jmh_util_pkg.put_blob
  (
    p_query    => l_query
  , p_dir      => p_dirname
  , p_filename => p_filename
  );

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in put_file'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end put_file;

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
)
is
  l_filename varchar2(200);
  l_query    varchar2(500);
begin

  for rec in
  (
    select
      file_seq
    , filename
    from
      epic.conv_files
    where decode(p_file_seq, null, file_seq, p_file_seq) = file_seq
  )
  loop

    l_query :=  'select file_blob from conv_files where file_seq = ' || rec.file_seq;
    l_filename := rec.filename || '.' || to_char(sysdate, 'YYYYMMDDhh24miss');

    --
    -- Get file from blob column in table
    --
    jmh_util_pkg.get_blob
    (
      p_query    => l_query
    , p_dir      => p_dirname
    , p_filename => l_filename
    );

  end loop;

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in get_file'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end get_file;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_if_pat_enc_orphan
--
-- purpose: Loads the if_pat_enc_orphan table with encounters that don't
--   have a matching Passport master patient record.
--
-- ---------------------------------------------------------------------------
procedure ld_if_pat_enc_orphan
is
begin

  jmh_log_pkg.wlog('Begin ld_if_pat_enc_orphan' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('if_pat_enc_orphan');
  
  --
  -- Load Behavorial Health Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan e
  with bh as
  (
    select *
    from
    epic.if_bh_mpi b
    where b.load_dt = 
    (
      select max(load_dt) from epic.if_bh_mpi
    )
  )
  select
    e.*
  , 'NON-MATCH MRN'            as match_criteria  
  , null                       as passport_hne_id
  , null                       as passport_mrn
  from
    epic.if_pat_enc e
    left outer join
    bh b
    on substr(b.mrn, 2) = e.mrn -- take off the leading V from the BH MRN
  where e.source_system_cd = 'BH'
    and b.mrn is null
  ;  
  commit;

  --
  -- Load Meditech Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan e
  select
    e.*
  , 'NON-MATCH MRN'            as match_criteria  
  , null                       as passport_hne_id
  , null                       as passport_mrn
  from
    epic.if_pat_enc e
    left outer join
    epic.dim_passport p
    on p.meditech_mrn = e.mrn 
  where e.source_system_cd = 'MEDITECH'
    and p.meditech_mrn is null
  ;  
  commit;
    
  --
  -- Load Plus Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan i
  with enc as
  (
  select
  	e.*
  , p.hne_id                   as passport_hne_id
  , p.plus_mrn                 as passport_mrn
  from
  	epic.if_pat_enc e
  	left outer join
  	epic.dim_passport p
  	on p.plus_mrn = e.mrn
  	or p.hne_id = e.hne_id
  where e.source_system_cd = 'PLUS'
  )
  select
    e.*
  , case
    when e.passport_hne_id is null and e.passport_mrn is null
    then
      'NON-MATCH HNE_ID and MRN'
    when e.passport_hne_id <> e.hne_id and e.passport_mrn is not null
    then
      'NON-MATCH HNE_ID'
    when e.passport_hne_id is not null and e.passport_mrn <> e.mrn
    then
      'NON-MATCH MRN'
    end                        as match_criteria
  from 
    enc e
  where
    (e.passport_hne_id is null and e.passport_mrn is null)
    or
    (e.passport_hne_id <> e.hne_id and e.passport_mrn is not null)
    or
    (e.passport_hne_id is not null and e.passport_mrn <> e.mrn)
  ;
  commit;
  
  --
  -- Load WC Star Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan i
  with enc as
  (
  select
  	e.*
  , p.hne_id                   as passport_hne_id
  , p.star_mrn                 as passport_mrn
  from
  	epic.if_pat_enc e
  	left outer join
  	epic.dim_passport p
  	on p.star_mrn = e.mrn
  	or p.hne_id = e.hne_id
  where e.source_system_cd = 'STAR'
    and e.filename like '%wc_enc%'
  )
  select
    e.*
  , case
    when e.passport_hne_id is null and e.passport_mrn is null
    then
      'NON-MATCH HNE_ID and MRN'
    when e.passport_hne_id <> e.hne_id and e.passport_mrn is not null
    then
      'NON-MATCH HNE_ID'
    when e.passport_hne_id is not null and e.passport_mrn <> e.mrn
    then
      'NON-MATCH MRN'
    end                        as match_criteria
  from 
    enc e
  where
    (e.passport_hne_id is null and e.passport_mrn is null)
    or
    (e.passport_hne_id <> e.hne_id and e.passport_mrn is not null)
    or
    (e.passport_hne_id is not null and e.passport_mrn <> e.mrn)
  ;
	commit;
	
  --
  -- Load WC Star Allergy Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan i
  with enc as
  (
  select
  	e.*
  , p.hne_id                   as passport_hne_id
  , p.star_mrn                 as passport_mrn
  from
  	epic.if_pat_enc e
  	left outer join
  	epic.dim_passport p
  	on p.star_mrn = substr(e.mrn, 2)
  	or p.hne_id = e.hne_id
  where e.source_system_cd = 'STAR'
    and e.filename like '%wc_allergy_backload%'
  )
  select
    e.*
  , case
    when e.passport_hne_id is null and e.passport_mrn is null
    then
      'NON-MATCH HNE_ID and MRN'
    when e.passport_hne_id <> e.hne_id and e.passport_mrn is not null
    then
      'NON-MATCH HNE_ID'
    when e.passport_hne_id is not null and e.passport_mrn <> e.mrn
    then
      'NON-MATCH MRN'
    end                        as match_criteria
  from 
    enc e
  where
    (e.passport_hne_id is null and e.passport_mrn is null)
    or
    (e.passport_hne_id <> e.hne_id and e.passport_mrn is not null)
    or
    (e.passport_hne_id is not null and e.passport_mrn <> e.mrn)
  ;
  commit;

  --
  -- Load WC StarRad Encounters
  --
  insert /*+ append */ into if_pat_enc_orphan i
  with enc as
  (
  select
  	e.*
  , p.hne_id                   as passport_hne_id
  , p.star_mrn                 as passport_mrn
  from
  	epic.if_pat_enc e
  	left outer join
  	epic.dim_passport p
  	on p.star_mrn = e.mrn
  	or p.hne_id = e.hne_id
  where e.source_system_cd = 'STAR'
    and e.filename like '%wc_rad%'
  )
  select
    e.*
  , case
    when e.passport_hne_id is null and e.passport_mrn is null
    then
      'NON-MATCH HNE_ID and MRN'
    when e.passport_hne_id <> e.hne_id and e.passport_mrn is not null
    then
      'NON-MATCH HNE_ID'
    when e.passport_hne_id is not null and e.passport_mrn <> e.mrn
    then
      'NON-MATCH MRN'
    end                        as match_criteria
  from 
    enc e
  where
    (e.passport_hne_id is null and e.passport_mrn is null)
    or
    (e.passport_hne_id <> e.hne_id and e.passport_mrn is not null)
    or
    (e.passport_hne_id is not null and e.passport_mrn <> e.mrn)
  ;
	commit;
  
  jmh_log_pkg.wlog('End ld_if_pat_enc_orphan' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_if_pat_enc_orphan'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_if_pat_enc_orphan;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_clarity_ser
--
-- purpose: Loads the CLARITY_SER table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_clarity_ser
is
begin

  jmh_log_pkg.wlog('Begin ld_clarity_ser' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('clarity_ser');
  
  insert into clarity_ser
  (
    prov_id
  , prov_name
  , prov_type
  , epic_prov_id
  , upin
  , ssn
  , external_name
  , active_status
  , dea_number
  , sex
  , birth_date
  )
  select
    prov_id
  , prov_name
  , prov_type
  , epic_prov_id
  , upin
  , ssn
  , external_name
  , active_status
  , dea_number
  , sex
  , birth_date
  from
    clarity_ser@dg4msql_poc
  ;
  commit;

  jmh_log_pkg.wlog('End ld_clarity_ser' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_clarity_ser'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_clarity_ser;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_ser_id
--
-- purpose: Loads the IDENTITY_SER_ID table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_ser_id
is
begin

  jmh_log_pkg.wlog('Begin ld_identity_ser_id' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('identity_ser_id');
  
  insert into identity_ser_id
  (
    prov_id
  , line
  , identity_id
  , identity_type_id
  )
  select
    prov_id
  , line
  , identity_id
  , identity_type_id
  from
    identity_ser_id@dg4msql_poc
  ;
  
  commit;

  jmh_log_pkg.wlog('End ld_identity_ser_id' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_identity_ser_id'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_identity_ser_id;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_patient
--
-- purpose: Loads the PATIENT table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_patient
is
begin

  jmh_log_pkg.wlog('Begin ld_patient' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('patient');
  
  insert into patient
  (
    pat_id
  , pat_name
  , pat_first_name 
  , pat_middle_name
  , pat_last_name  
  , sex            
  , birth_date     
  , ssn            
  , epic_pat_id    
  , pat_mrn_id     
  , medicare_num   
  , medicaid_num   
  , email_address  
  , pat_status     
  )
  select
    pat_id
  , pat_name
  , pat_first_name 
  , pat_middle_name
  , pat_last_name  
  , sex            
  , birth_date     
  , ssn            
  , epic_pat_id    
  , pat_mrn_id     
  , medicare_num   
  , medicaid_num   
  , email_address  
  , pat_status     
  from
    patient@dg4msql_poc
  ;
  commit;

  jmh_log_pkg.wlog('End ld_patient' , jmh_log_pkg.LOG_NORM);

exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_patient'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_patient;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_id
--
-- purpose: Loads the IDENTITY_ID table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_id
is
begin

  jmh_log_pkg.wlog('Begin ld_identity_id' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('identity_id');
  
  insert into identity_id
  (
    pat_id
  , line
  , identity_id
  , identity_type_id
  )
  select
    pat_id
  , line
  , identity_id
  , identity_type_id
  from
    identity_id@dg4msql_poc
  ;
  
  commit;

  jmh_log_pkg.wlog('End ld_identity_id' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_identity_id'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_identity_id;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_identity_id_type
--
-- purpose: Loads the IDENTITY_ID_TYPE table from the Clarity database.
--
-- ---------------------------------------------------------------------------
procedure ld_identity_id_type
is
begin

  jmh_log_pkg.wlog('Begin ld_identity_id_type' , jmh_log_pkg.LOG_NORM);
  
  truncate_table('identity_id_type');
  
  insert into identity_id_type
  (
    id_type
  , id_type_name
  )
  select
    id_type
  , id_type_name
  from
    identity_id_type@dg4msql_poc
  ;
  
  commit;

  jmh_log_pkg.wlog('End ld_identity_id_type' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_identity_id_type'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_identity_id_type;

-- ---------------------------------------------------------------------------
--
-- procedure: beg_etl_email
--
-- purpose: Email beginning of MPI file generation process.
--
-- ---------------------------------------------------------------------------
procedure beg_etl_email
is

  l_recipients varchar2(4000);
  l_subject    varchar2(100) := 'Begin Passport-Plus MPI File Generation for Epic';
  l_msg        varchar2(4000);
  
begin

  l_recipients := jmh_app_parameters_pkg.get_value
                  (
                    p_parameter => 'JMH_EPIC_MPI_EMAILS'
                  , p_default_value => 'craig.nobili@johnmuirhealth.com'
                  );

  l_msg := 'Started Passport MPI file generation for Epic at ' || to_char(sysdate, 'mm-dd-yyyy hh24:mi:ss');
  l_msg := l_msg || chr(10) || chr(10) || 'Regards,' || chr(10) || 'Craig Nobili';
  
  jmdba.jmh_edw_etl.exec_mailx
	(
	  p_recipients => l_recipients
	, p_subject    => l_subject
	, p_message    => l_msg
  );  
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in beg_etl_email'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
  
end beg_etl_email;

-- ---------------------------------------------------------------------------
--
-- procedure: end_etl_email
--
-- purpose: Email end of MPI file generation process.
--
-- ---------------------------------------------------------------------------
procedure end_etl_email
is

  l_recipients varchar2(4000);
  -- mailx end subject with newline and put mimetype of html on separate line
  l_subject    varchar2(100) := 'End Passport-Plus MPI File Generation for Epic' || chr(10) || 'Content-type: text/html';
  l_msg        varchar2(32767);
  l_mpi_cnt    pls_integer;
  l_nondup_cnt pls_integer;
  
begin

  l_recipients := jmh_app_parameters_pkg.get_value
                  (
                    p_parameter => 'JMH_EPIC_MPI_EMAILS'
                  , p_default_value => 'craig.nobili@johnmuirhealth.com'
                  );
  select count(*)
  into l_mpi_cnt
  from vw_epic_conv_patient_file
  ;
  
  select count(*)
  into l_nondup_cnt
  from stage_personmatch
  ;
  
  -- Build email message
  l_msg := '<html>' || chr(10);
  l_msg := l_msg || '<body>' || chr(10);
  l_msg := l_msg || '<br>' || chr(10);
  l_msg := l_msg || '<h4>Ended Passport MPI file generation for Epic at ' || to_char(sysdate, 'mm-dd-yyyy hh24:mi:ss') || '</h4>' || chr(10);
  l_msg := l_msg || '<h4>The files below are on server jmhmpiprod in the /epic/conv_file directory' || '</h4>' || chr(10);
  l_msg := l_msg || '<br>' || chr(10);
  l_msg := l_msg || '<table border="1">' || chr(10);
  l_msg := l_msg || '<tr>' || chr(10);
  l_msg := l_msg || '<th>filename</th>' || chr(10);
  l_msg := l_msg || '<th>num_of_records</th>' || chr(10);
  l_msg := l_msg || '</tr>' || chr(10);
  
  l_msg := l_msg || '<tr>' || chr(10);
  l_msg := l_msg || '<td>' || 'jmh_patient.dat' || '</td>' || chr(10);
  l_msg := l_msg || '<td>' || l_mpi_cnt || '</td>' || chr(10);
  l_msg := l_msg || '</tr>' || chr(10);
  
  l_msg := l_msg || '<tr>' || chr(10);
	l_msg := l_msg || '<td>' || 'jmh_known_nonduplicate.dat' || '</td>' || chr(10);
	l_msg := l_msg || '<td>' || l_nondup_cnt || '</td>' || chr(10);
	l_msg := l_msg || '</tr>' || chr(10);
     
  l_msg := l_msg || '</table>' || chr(10);
  l_msg := l_msg || '<br> <br>' || chr(10); 
  l_msg := l_msg || '<h4>Regards,</h4>' || chr(10);
  l_msg := l_msg || '<h4>Craig Nobili</h4>' || chr(10);
  l_msg := l_msg || '</body>' || chr(10);
  l_msg := l_msg || '</html>';
    
  jmdba.jmh_edw_etl.exec_mailx
	(
	  p_recipients => l_recipients
	, p_subject    => l_subject
	, p_message    => l_msg
  );  
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in end_etl_email'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
  
end end_etl_email;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_patient
--
-- purpose: Loads the dim_kbs_paient table by selecting data from Chronicles
-- using java and kbsql.
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_patient(p_load_stg in varchar2 := g_TRUE)
is
begin

  jmh_log_pkg.wlog('Begin ld_kbs_patient' , jmh_log_pkg.LOG_NORM);
  
  if (p_load_stg = g_TRUE) then
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_PATIENT_PRG');
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_IDENTITY_ID_PRG');
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_IDENTITY_ID_TYPE_PRG');
  end if;
  
  -- truncate table before making indexes unusable as truncate will make them valid
  truncate_table('dim_kbs_patient');
  jmh_index_pkg.make_index_unusable('dim_kbs_patient');
  
  insert /*+ append parallel(e) */ into dim_kbs_patient e
  (
    pat_id
  , epic_mrn
  , epic_epi 
  , hne_id
  , star_mrn
  , meditech_mrn
  , plus_mrn
  , bh_mrn 
  , first_name
  , middle_name
  , last_name
  , gender
  , dob
  , ssn
  , loaded_dt
  , loaded_by
  )
  with id as
  (
  select
    i.*
  , row_number() over(partition by i.patient_id, i.identity_type_id order by i.line) as rn
  from
    epic.kbs_identity_id_ext i
  )
  , id2 as
  (
  select 
    i.*
  from
    id i
  where i.rn = 1
  )
  , mpi as
  (
  select
    p.pat_id                            as pat_id
  , p.pat_mrn_id                        as epic_mrn   
  , p.pat_first_name                    as first_name
  , p.pat_middle_name                   as middle_name
  , p.pat_last_name                     as last_name
  , p.sex                               as gender
  , to_date(p.birth_date, 'mm/dd/yyyy') as dob
  , p.social_security_num               as ssn
  , max
    (
      case
      when t.id_type_name = 'ENTERPRISE ID NUMBER'
      then i.ident_id
      else null
      end
    ) as epic_epi
  , max
    (
      case
      when t.id_type_name = 'PASSPORT ENTERPRISE MRN'
      then i.ident_id
      else null
      end
    ) as hne_id
  , max
    (
      case
      when t.id_type_name = 'WC STAR MRN'
      then i.ident_id
      else null
      end
    ) as star_mrn
  , max
    (
      case
      when t.id_type_name = 'MEDITECH MRN'
      then i.ident_id
      else null
      end
    ) as meditech_mrn
  , max
    (
      case
      when t.id_type_name = 'PLUS MRN'
      then i.ident_id
      else null
      end
    ) as plus_mrn
   , max
    (
      case
      when t.id_type_name = 'BH STAR MRN'
      then i.ident_id
      else null
      end
    ) as bh_mrn
   from
    kbs_patient_ext p
    --inner join
    left outer join
    id2 i
    on i.patient_id = p.pat_id
    --inner join
    left outer join
    kbs_identity_id_type_ext t
    on t.id_type = i.identity_type_id
  --where t.id_type_name in
  --  (
  --    'ENTERPRISE ID NUMBER'
  --  , 'PASSPORT ENTERPRISE MRN'
  --  , 'WC STAR MRN'
  --  , 'MEDITECH MRN'
  --  , 'PLUS MRN'
  --  , 'BH STAR MRN'
  --  )
  group by
    p.pat_id                           
  , p.pat_mrn_id                       
  , p.pat_first_name                   
  , p.pat_middle_name                  
  , p.pat_last_name                    
  , p.sex                              
  , to_date(p.birth_date, 'mm/dd/yyyy')
  , p.social_security_num
  )
  select
    m.pat_id
  , m.epic_mrn
  , m.epic_epi 
  , m.hne_id
  , m.star_mrn
  , m.meditech_mrn
  , m.plus_mrn
  , m.bh_mrn 
  , m.first_name
  , m.middle_name
  , m.last_name
  , m.gender
  , m.dob
  , m.ssn
  , sysdate          as loaded_dt
  , g_PRG_NAME       as loaded_by
  from
    mpi m
  ;
  commit;

  jmh_index_pkg.rebuild_index('dim_kbs_patient');
  
  jmh_log_pkg.wlog('End ld_kbs_patient' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_kbs_patient'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_kbs_patient;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_provider
--
-- purpose: Loads the dim_kbs_provider table by selecting data from Chronicles
-- using java and kbsql.
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_provider(p_load_stg in varchar2 := g_TRUE)
is
begin

  jmh_log_pkg.wlog('Begin ld_kbs_provider' , jmh_log_pkg.LOG_NORM);
  
  if (p_load_stg = g_TRUE) then
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_CLARITY_SER_PRG');
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_IDENTITY_SER_ID_PRG');
    jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_IDENTITY_ID_TYPE_PRG');
  end if;
  
  -- truncate table before making indexes unusable as truncate will make them valid
  truncate_table('dim_kbs_provider');
  jmh_index_pkg.make_index_unusable('dim_kbs_provider');
  
  insert /*+ append parallel(e) */ into dim_kbs_provider e
  (
    prov_id 
  , prov_name
  , prov_type
  , prov_abbr
  , user_id  
  , epic_prov_id
  , upin        
  , ssn         
  , emp_status  
  , active_status
  , email        
  , dea_number   
  , sex          
  , dob   
  , medicare_prov_id
  , medicard_prov_id
  , npi     
  , clinician_title
  , referral_srce_type
  , referral_source_type
  , doctors_degree      
  , staff_resource      
  , star_prov_id
  , meditech_prov_id
  , plus_prov_id    
  , bh_prov_id      
  , facets_prov_id  
  , hlab_prov_id
  , loaded_dt           
  , loaded_by           
  )
  with mpi as
  (
  select
    p.prov_id
  , p.prov_name
  , p.prov_type
  , p.prov_abbr
  , p.user_id
  , p.epic_prov_id
  , p.upin
  , p.ssn
  , p.emp_status
  , p.active_status
  , p.email        
  , p.dea_number   
  , p.sex          
  , to_date(p.birth_date, 'mm/dd/yyyy') as dob
  , p.medicare_prov_id
  , p.medicard_prov_id
  , p.npi    
  , p.clinician_title 
  , p.referral_srce_type
  , p.referral_source_type
  , p.doctors_degree      
  , p.staff_resource      
  , max
    (
      case
      when t.id_type_name = 'JMH STAR PROVIDER ID'
      then i.ident_id
      else null
      end
    ) as star_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH MEDITECH PROVIDER ID'
      then i.ident_id
      else null
      end
    ) as meditech_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH PLUS PROVIDER ID'
      then i.ident_id
      else null
      end
    ) as plus_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH BH PROVIDER ID'
      then i.ident_id
      else null
      end
    ) as bh_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH FACETS PROVIDER ID'
      then i.ident_id
      else null
      end
    ) as facets_prov_id
  , max
    (
      case
      when t.id_type_name = 'HLAB PROVIDER'
      then i.ident_id
      else null
      end
    ) as hlab_prov_id
  from
    kbs_clarity_ser_ext p
    left outer join
    kbs_identity_ser_id_ext i
    on i.provider_id = p.prov_id
    left outer join
    kbs_identity_id_type_ext t
    on t.id_type = i.identity_type_id
  group by
    p.prov_id
  , p.prov_name
  , p.prov_type
  , p.prov_abbr
  , p.user_id
  , p.epic_prov_id
  , p.upin
  , p.ssn
  , p.emp_status
  , p.active_status
  , p.email        
  , p.dea_number   
  , p.sex          
  , to_date(p.birth_date, 'mm/dd/yyyy')
  , p.medicare_prov_id
  , p.medicard_prov_id
  , p.npi    
  , p.clinician_title 
  , p.referral_srce_type
  , p.referral_source_type
  , p.doctors_degree      
  , p.staff_resource      
  )
  select
    m.prov_id 
  , m.prov_name
  , m.prov_type
  , m.prov_abbr
  , m.user_id  
  , m.epic_prov_id
  , m.upin        
  , m.ssn         
  , m.emp_status  
  , m.active_status
  , m.email        
  , m.dea_number   
  , m.sex          
  , m.dob
  , m.medicare_prov_id
  , m.medicard_prov_id
  , m.npi     
  , m.clinician_title 
  , m.referral_srce_type
  , m.referral_source_type
  , m.doctors_degree      
  , m.staff_resource      
  , m.star_prov_id
  , m.meditech_prov_id
  , m.plus_prov_id    
  , m.bh_prov_id      
  , m.facets_prov_id   
  , m.hlab_prov_id
  , sysdate                  as loaded_dt
  , g_PRG_NAME               as loaded_by
  from
    mpi m
  ;
  commit;

  jmh_index_pkg.rebuild_index('dim_kbs_provider');
  
  jmh_log_pkg.wlog('End ld_kbs_provider' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_kbs_provider'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_kbs_provider;


-- ---------------------------------------------------------------------------
--
-- procedure: ld_passport_plus
--
-- purpose: Loads the passport_plus table using stage_passport, pivoting
--          the id and id_type to get ssn, mrns, and hne id.
--
-- ---------------------------------------------------------------------------
procedure ld_passport_plus(p_load_stg in varchar2 := g_TRUE)
is
begin

 jmh_log_pkg.wlog('Begin ld_passport_plus' , jmh_log_pkg.LOG_NORM);

 -- Load passport data first, then perform pivot on local database.
 -- Performing pivot on remote database causes sort and temp space to fill up there.
 if (p_load_stg = g_TRUE) then
   ld_stage_passport;
 end if;

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('passport_plus');
 jmh_index_pkg.make_index_unusable('passport_plus');

 insert /*+ append parallel(e) */ into passport_plus e
 (
   source_system_cd
 , source_system_id
 , hne_id
 , star_mrn
 , meditech_mrn
 , plus_cust1_mrn
 , plus_cust2_mrn
 , plus_cust3_mrn
 , last_name
 , first_name
 , middle_name
 , prefix
 , suffix
 , gender
 , dob
 , address1
 , address2
 , city
 , state
 , zip
 , phone
 , ssn
 , last_update_dt
 , alias_name
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , createdate
 , loaded_dt
 , loaded_by
 )
 with mapm as
 (
 select
   'PASSPORT'     as source_system_cd
 , i.objid        as source_system_id
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name
 , null             as email_address
 , null             as phone_business
 , null             as phone_cell
 , null             as ethnicity
 , null             as primary_language
 , null             as race
 , null             as emerg_contact_name
 , null             as emerg_contact_phone
 , i.createdate
 , max
   (
     case
     when i.id_type = 'SSN'
     then i.id
     else null
     end
   ) as ssn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 294517187
     then i.id
     else null
     end
   ) as star_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 219953093
     then i.id
     else null
     end
   ) as meditech_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1169281912
     then i.id
     else null
     end
   ) as plus_cust1_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 1198649375
     then i.id
     else null
     end
   ) as plus_cust2_mrn
 , max
   (
     case
     when i.id_type = 'MRN' and i.asgnauthorityid = 4
     then i.id
     else null
     end
    ) as plus_cust3_mrn
 from
   stage_passport i
 group by
   'PASSPORT'
 , i.objid   
 , i.hne_id
 , i.last_name
 , i.first_name
 , i.middle_name
 , i.prefix
 , i.suffix
 , i.gender
 , i.dob
 , i.address1
 , i.address2
 , i.city
 , i.state
 , i.zip
 , i.phone
 , i.last_update_dt
 , i.alias_name 
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , null
 , i.createdate
 )
 select
   m.source_system_cd
 , m.source_system_id
 , m.hne_id
 , m.star_mrn
 , m.meditech_mrn
 , m.plus_cust1_mrn
 , m.plus_cust2_mrn
 , m.plus_cust3_mrn
-- , coalesce(m.plus_cust1_mrn, m.plus_cust2_mrn, m.plus_cust3_mrn) as plus_mrn
 , m.last_name
 , m.first_name
 , m.middle_name
 , m.prefix
 , m.suffix
 , m.gender
 , m.dob
 , m.address1
 , m.address2
 , m.city
 , m.state
 , m.zip
 , m.phone
 , m.ssn
 , m.last_update_dt
 , m.alias_name
 , m.email_address
 , m.phone_business
 , m.phone_cell
 , m.ethnicity
 , m.primary_language
 , m.race
 , m.emerg_contact_name
 , m.emerg_contact_phone
 , m.createdate
 , sysdate          as loaded_dt
 , g_PRG_NAME       as loaded_by
 from
   mapm m
 ;

 commit;
 jmh_index_pkg.rebuild_index('passport_plus');
 jmh_log_pkg.wlog('End ld_passport_plus' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_passport_plus'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_passport_plus;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_passport_plus
--
-- purpose: Loads the dim_passport_plus table from the passport_plus table, excluding
--   bad records and joining in items from the stage_plus table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_passport_plus
is
begin

 jmh_log_pkg.wlog('Begin ld_dim_passport_plus' , jmh_log_pkg.LOG_NORM);

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('dim_passport_plus');
 jmh_index_pkg.make_index_unusable('dim_passport_plus');

 insert /*+ append parallel(e) */ into dim_passport_plus e
 (
   source_system_cd
 , source_system_id
 , hne_id          
 , star_mrn        
 , meditech_mrn  
 , plus_cust1_mrn
 , plus_cust2_mrn
 , plus_cust3_mrn
 , plus_mrn         
 , last_name        
 , first_name       
 , middle_name      
 , prefix
 , suffix
 , gender           
 , dob              
 , address1         
 , address2         
 , city             
 , state            
 , zip              
 , phone            
 , ssn              
 , last_update_dt   
 , alias_name       
 , email_address    
 , phone_business   
 , phone_cell       
 , ethnicity        
 , primary_language 
 , race             
 , emerg_contact_name
 , emerg_contact_phone
 , createdate       
 , loaded_dt        
 , loaded_by        
 )
 with plus as
 (
 select
   hne_id
 , phone          as plus_phone
 , email_address
 , phone_business
 , phone_cell
 , ethnicity
 , primary_language
 , race
 , emerg_contact_name
 , emerg_contact_phone
 , row_number() over(partition by hne_id order by plus_mrn) as rn
 from
   epic.stage_plus
 )
 , pass as
 (
 select
   pa.source_system_cd
 , pa.source_system_id
 , pa.hne_id
 , pa.star_mrn
 , pa.meditech_mrn
 , pa.plus_cust1_mrn
 , pa.plus_cust2_mrn
 , pa.plus_cust3_mrn
 , coalesce(pa.plus_cust1_mrn, pa.plus_cust2_mrn, pa.plus_cust3_mrn) as plus_mrn
 , pa.last_name
 , pa.first_name
 , pa.middle_name
 , pa.prefix
 , pa.suffix
 , pa.gender
 , pa.dob
 , pa.address1
 , pa.address2
 , pa.city
 , pa.state
 , pa.zip
 , pa.phone
 , pa.ssn
 , pa.last_update_dt
 , pa.alias_name
 , pl.plus_phone
 , pl.email_address
 , pl.phone_business
 , pl.phone_cell
 , pl.ethnicity
 , pl.primary_language
 , pl.race
 , pl.emerg_contact_name
 , pl.emerg_contact_phone
 , pa.createdate
 , pa.loaded_dt
 , pa.loaded_by
 , pa.first_name || pa.last_name                   as first_last
 , pa.first_name || pa.middle_name || pa.last_name as first_middle_last
 --, row_number() over(partition by pl.hne_id order by pl.rowid) as rn
 from
   epic.passport_plus pa
   left outer join
   plus pl
   on pl.hne_id = pa.hne_id
   and pl.rn = 1
 )
 select
   p.source_system_cd
 , p.source_system_id
 , p.hne_id
 , p.star_mrn
 , p.meditech_mrn
 , p.plus_cust1_mrn
 , p.plus_cust2_mrn
 , p.plus_cust3_mrn
 , p.plus_mrn
 , p.last_name
 , p.first_name
 , p.middle_name
 , p.prefix
 , p.suffix
 , p.gender
 , p.dob
 , p.address1
 , p.address2
 , p.city
 , p.state
 , p.zip
 , p.plus_phone
 , p.ssn
 , p.last_update_dt
 , p.alias_name
 , p.email_address
 , p.phone_business
 , p.phone_cell
 , p.ethnicity
 , p.primary_language
 , p.race
 , p.emerg_contact_name
 , p.emerg_contact_phone
 , p.createdate
 , p.loaded_dt
 , p.loaded_by
 from
  pass p
where 1 = 1
  and not
  (
    ( p.star_mrn is null and p.meditech_mrn is null and p.plus_mrn is null )
    or ( p.first_last is null ) 
    or ( regexp_like(p.first_last, '[[:digit:]]') ) 
    or ( upper(p.first_middle_last) like '%DO NOT USE%' ) 
    or ( upper(p.first_middle_last) like '%BLOOD BANK%' )
    or ( upper(p.first_middle_last) like '%BLOODBANK%' )
    or ( upper(p.first_middle_last) like '%REFUND%' ) 
    or ( upper(p.first_middle_last) like '%UNKNOWN%' )   
    or ( upper(p.first_middle_last) like '%CONFIDENTIAL%' )  
    or ( upper(p.first_middle_last) like '%TEST%' ) 
    --or ( length(p.last_name) = 1 )
    --or ( p.createdate < to_date('09/01/2003', 'mm/dd/yyyy') )
  )  
  ;

 commit;
 jmh_index_pkg.rebuild_index('dim_passport_plus');
 jmh_log_pkg.wlog('End ld_dim_passport_plus' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_dim_passport_plus'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_dim_passport_plus;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_cl_patient
--
-- purpose: Loads the dim_cl_patient table.
--
-- ---------------------------------------------------------------------------
procedure ld_dim_cl_patient
is
begin

 jmh_log_pkg.wlog('Begin ld_dim_cl_patient' , jmh_log_pkg.LOG_NORM);

 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_PATIENT_PRG');
 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_PATIENT_2_PRG');
 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_PATIENT_3_PRG');
 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_IDENTITY_ID_PRG');
 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_IDENTITY_ID_TYPE_PRG');

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('dim_cl_patient');
 jmh_index_pkg.make_index_unusable('dim_cl_patient');

 insert /*+ append parallel(e) */ into dim_cl_patient e
 (
   pat_id
 , pat_first_name
 , pat_middle_name
 , pat_last_name  
 , pat_name       
 , sex            
 , birth_date     
 , ssn            
 , pat_mrn_id     
 , jmh_epic_mrn   
 , epic_epi       
 , hne_id         
 , star_mrn       
 , meditech_mrn   
 , plus_mrn       
 , bh_mrn         
 , add_line1      
 , add_line2      
 , city           
 , state          
 , zip            
 , home_phone     
 , work_phone     
 , email_address  
 , pat_status      
 , loaded_dt
 , loaded_by
 )
 with id as
 (
 select
   i.*
 , row_number() over(partition by i.pat_id, i.identity_type_id order by i.line) as rn
 from
   epic.cl_identity_id_ext i
 )
 , id2 as
 (
 select 
   i.*
 from
   id i
 where i.rn = 1
 )
 , pat as
 (
 select
   p.pat_id
 , p.pat_first_name
 , p.pat_middle_name
 , p.pat_last_name
 , p.pat_nAme
 , p.sex
 , p.birth_date
 , p.ssn
 , p.pat_mrn_id
 , p.add_line_1
 , p.add_line_2
 , p.city
 , p.state_c
 , p.zip
 , p.home_phone
 , p.work_phone
 , p.email_address
 , p.pat_status
 , max
   (
     case
     when t.id_type_name = 'JMH EPIC MRN'
     then i.identity_id
     else null
     end
   ) as jmh_epic_mrn
 , max
   (
     case
     when t.id_type_name = 'ENTERPRISE ID NUMBER'
     then i.identity_id
     else null
     end
   ) as epic_epi
 , max
   (
     case
     when t.id_type_name = 'PASSPORT ENTERPRISE MRN'
     then i.identity_id
     else null
     end
   ) as hne_id
 , max
   (
     case
     when t.id_type_name = 'WC STAR MRN'
     then i.identity_id
     else null
     end
   ) as star_mrn
 , max
   (
     case
     when t.id_type_name = 'MEDITECH MRN'
     then i.identity_id
     else null
     end
   ) as meditech_mrn
 , max
   (
     case
     when t.id_type_name = 'PLUS MRN'
     then i.identity_id
     else null
     end
   ) as plus_mrn
 , max
   (
     case
     when t.id_type_name = 'BH STAR MRN'
     then i.identity_id
     else null
     end
   ) as bh_mrn
 from
   epic.cl_patient_ext p
   --inner join
   left outer join
   id2 i
   on i.pat_id = p.pat_id
   --inner join
   left outer join
   epic.cl_identity_id_type_ext t
   on t.id_type = i.identity_type_id
 --where t.id_type_name in
 --  (
 --    'JMH EPIC MRN'
 --  , 'ENTERPRISE ID NUMBER'
 --  , 'PASSPORT ENTERPRISE MRN'
 --  , 'WC STAR MRN'
 --  , 'MEDITECH MRN'
 --  , 'PLUS MRN'
 --  , 'BH STAR MRN'
 --  )
 group by
   p.pat_id
 , p.pat_first_name
 , p.pat_middle_name
 , p.pat_last_nAme
 , p.pat_nAme
 , p.sex
 , p.birth_date
 , p.ssn
 , p.pat_mrn_id
 , p.add_line_1
 , p.add_line_2
 , p.city
 , p.state_c
 , p.zip
 , p.home_phone
 , p.work_phone
 , p.email_address
 , p.pat_status
 )
 select
   pat_id
 , pat_first_name
 , pat_middle_name
 , pat_last_name  
 , pat_name       
 , sex            
 , to_date(substr(birth_date, 1, 10), 'YYYY-MM-DD') as birth_date
 , ssn            
 , pat_mrn_id     
 , jmh_epic_mrn   
 , epic_epi       
 , hne_id         
 , star_mrn       
 , meditech_mrn   
 , plus_mrn       
 , bh_mrn         
 , add_line_1                        as add_line1
 , add_line_2                        as add_line2  
 , city           
 , state_c                           as state          
 , zip            
 , home_phone     
 , work_phone     
 , email_address  
 , pat_status      
 , sysdate                           as loaded_dt
 , g_PRG_NAME                        as loaded_by
 from
   pat p
 ;

 commit;
 jmh_index_pkg.rebuild_index('dim_cl_patient');
 jmh_log_pkg.wlog('End ld_dim_cl_patient' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_dim_cl_patient'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception
   
end ld_dim_cl_patient;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_dim_cl_provider
--
-- purpose: Loads the dim_cl_provider table
--
-- ---------------------------------------------------------------------------
procedure ld_dim_cl_provider
is
begin

  jmh_log_pkg.wlog('Begin ld_dim_cl_provider' , jmh_log_pkg.LOG_NORM);

  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_CLARITY_SER_PRG');
  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_CLARITY_SER_2_PRG');
  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_IDENTITY_SER_ID_PRG');
  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_IDENTITY_ID_TYPE_PRG');
  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_CLARITY_SER_ADDR_PRG');
  jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_CL_ZC_STATE_PRG');
  
  -- truncate table before making indexes unusable as truncate will make them valid
  truncate_table('dim_cl_provider');
  jmh_index_pkg.make_index_unusable('dim_cl_provider');
  
  insert /*+ append parallel(e) */ into dim_cl_provider e
  (
    prov_id 
  , prov_name
  , prov_type
  , prov_abbr
  , user_id  
  , epic_prov_id
  , upin        
  , ssn         
  , emp_status  
  , active_status
  , email        
  , dea_number   
  , sex          
  , dob   
  , clinician_title
  , referral_srce_type
  , referral_source_type
  , doctors_degree
  , staff_resource  
  , medicare_prov_id
  , medicaid_prov_id
  , npi             
  , star_prov_id
  , meditech_prov_id
  , plus_prov_id    
  , bh_prov_id      
  , facets_prov_id  
  , hlab_prov_id
  , provider_id
  , addr_line_1
  , addr_line_2
  , city
  , state_cd
  , zip
  , phone
  , fax
  , loaded_dt           
  , loaded_by           
  )
  with mpi as
  (
  select
    p.prov_id
  , p.prov_name
  , p.prov_type
  , p.prov_abbr
  , p.user_id
  , p.epic_prov_id
  , p.upin
  , p.ssn
  , p.emp_status
  , p.active_status
  , p.email        
  , p.dea_number   
  , p.sex          
  , p.birth_date       as dob
  , p.clinician_title
  , p.referral_srce_type
  , p.referral_source_type
  , p.doctors_degree  
  , p.staff_resource  
  , p.medicare_prov_id
  , p.medicaid_prov_id
  , p2.npi    
  , a.addr_line_1
  , a.addr_line_2
  , a.city
  , s.abbr           as state_cd
  , a.zip
  , a.phone
  , a.fax
  , max
    (
      case
      when t.id_type_name = 'JMH STAR PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as star_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH MEDITECH PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as meditech_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH PLUS PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as plus_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH BH PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as bh_prov_id
  , max
    (
      case
      when t.id_type_name = 'JMH FACETS PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as facets_prov_id
  , max
    (
      case
      when t.id_type_name = 'HLAB PROVIDER'
      then i.identity_id
      else null
      end
    ) as hlab_prov_id
  , max
    (
      case
      when t.id_type_name = 'PROVIDER ID'
      then i.identity_id
      else null
      end
    ) as provider_id    
  from
    cl_clarity_ser_ext p
    inner join
    cl_clarity_ser_2_ext p2
    on p2.prov_id = p.prov_id
    inner join
    cl_clarity_ser_addr_ext a
    on a.prov_id = p.prov_id
    inner join
    cl_zc_state_ext s
    on s.state_c = a.state_c
    left outer join
    cl_identity_ser_id_ext i
    on i.prov_id = p.prov_id
    left outer join
    cl_identity_id_type_ext t
    on t.id_type = i.identity_type_id
  group by
    p.prov_id
  , p.prov_name
  , p.prov_type
  , p.prov_abbr
  , p.user_id
  , p.epic_prov_id
  , p.upin
  , p.ssn
  , p.emp_status
  , p.active_status
  , p.email        
  , p.dea_number   
  , p.sex          
  , p.birth_date
  , p.clinician_title
  , p.referral_srce_type
  , p.referral_source_type
  , p.doctors_degree  
  , p.staff_resource  
  , p.medicare_prov_id
  , p.medicaid_prov_id
  , p2.npi     
  , a.addr_line_1
  , a.addr_line_2
  , a.city
  , s.abbr         
  , a.zip
  , a.phone
  , a.fax
  )
  select
    m.prov_id 
  , m.prov_name
  , m.prov_type
  , m.prov_abbr
  , m.user_id  
  , m.epic_prov_id
  , m.upin        
  , m.ssn         
  , m.emp_status  
  , m.active_status
  , m.email        
  , m.dea_number   
  , m.sex          
  , m.dob
  , m.clinician_title
  , m.referral_srce_type
  , m.referral_source_type
  , m.doctors_degree  
  , m.staff_resource  
  , m.medicare_prov_id
  , m.medicaid_prov_id
  , m.npi             
  , m.star_prov_id
  , m.meditech_prov_id
  , m.plus_prov_id    
  , m.bh_prov_id      
  , m.facets_prov_id   
  , m.hlab_prov_id
  , m.provider_id  
  , m.addr_line_1
  , m.addr_line_2
  , m.city
  , m.state_cd
  , m.zip
  , m.phone
  , m.fax
  , sysdate                  as loaded_dt
  , g_PRG_NAME               as loaded_by
  from
    mpi m
  ;
  commit;

  jmh_index_pkg.rebuild_index('dim_cl_provider');
  
  jmh_log_pkg.wlog('End ld_dim_cl_provider' , jmh_log_pkg.LOG_NORM);
  
exception

 when others then

   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_dim_cl_provider'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_dim_cl_provider;

-- ---------------------------------------------------------------------------
--
-- procedure: ld_kbs_pat_enc
--
-- purpose: Loads the kbs_pat_enc table
--
-- ---------------------------------------------------------------------------
procedure ld_kbs_pat_enc
is
begin

 jmh_log_pkg.wlog('Begin ld_kbs_pat_enc' , jmh_log_pkg.LOG_NORM);

 jmdba.jmh_edw_etl.exec_load_datamart('JMH_EPIC_KBS_PAT_ENC_PRG');

 -- truncate table before making indexes unusable as truncate will make them valid
 truncate_table('kbs_pat_enc');
 jmh_index_pkg.make_index_unusable('kbs_pat_enc');
 
  insert into kbs_pat_enc
  (
    pat_id
  , pat_enc_csn_id
  , contact_date
  , enc_type_c
  , appt_status_c
  , yyyymm
  , is_enc_closed
  , pat_mrn_id
  , loaded_dt
  , loaded_by
  )
  select
    k.*
  , sysdate                        as loaded_dt
  , 'JMH_EPIC_CONV.ld_kbs_pat_enc' as loaded_by
  from
    kbs_pat_enc_ext k
  ;

 commit;
 jmh_index_pkg.rebuild_index('kbs_pat_enc');
 jmh_log_pkg.wlog('End ld_kbs_pat_enc' , jmh_log_pkg.LOG_NORM);

exception

 when others then
   jmh_log_pkg.wlog
   (
     p_log_msg => 'Error in ld_kbs_pat_enc'
   , p_log_level => jmh_log_pkg.LOG_MUST
   );
   raise; -- don't swallow the exception

end ld_kbs_pat_enc;


--
-- Initialization
--
begin

 g_jre_exe := jmh_app_parameters_pkg.get_value('JMH_JRE_EXE');
 g_java_classpath := jmh_app_parameters_pkg.get_value('JMH_JAVA_CLASSPATH');

end jmh_epic_conv;
/
