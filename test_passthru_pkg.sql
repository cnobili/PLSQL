set serveroutput on
set time on
set timing on


begin
    /*
    passthru_pkg.get_phs_data
    (
      q'#
      SELECT DISTINCT
				appt_id, -- link PSH..appt.appt_id = HSM..casemain.appt_id
				resunit.facility_abbr AS facility,
					convert(varchar,entered_datetime,120) as case_added_dt --will be used for case_added_dt in HSM Case_data file
			FROM 
				appt WITH (NOLOCK)
				inner join resunit WITH (NOLOCK) ON appt.resunit_id = resunit.resunit_id
			WHERE resunit.facility_abbr IN ('JMMC-C','JMMC-WC')
      #'
    , 3
    , 'JMH_COMPASS_DATA'
    , 'data.dat'
    );
        
    passthru_pkg.get_hsm_data
    (
      q'#
      SELECT 
				psmresunit.facility_name AS facility,
				casemain.casemain_id AS surgicalcase_num,
				casepro.actual_pro_id AS actual_proc_cd,
				REPLACE(REPLACE(casepro.actual_proname,char(13),''),char(10),'') AS actual_proc_name, -- replace NULL characters and carriage returns
				infectionclass.name AS wound_class,
				casepro.primpract_res_name AS proc_surg,
				convert(varchar,casepro.casepro_start_datetime,120) AS proc_start_tm,
				convert(varchar,casepro.casepro_stop_datetime,120) AS proc_end_tm,
				casepro.rank AS proc_seq,
				case casepro.is_primary when 1 then 'Y' else 'N' end AS prim_ind,
				psmproname.abbr AS alternate_proc_abbr,
				psmproname.name AS alternate_proc_name
			
			FROM
				casemain WITH (NOLOCK)
			
			LEFT JOIN casepro WITH (NOLOCK)
				ON casemain.casemain_id = casepro.casemain_id
			
			LEFT JOIN caseintraop WITH (NOLOCK)
				ON casemain.casemain_id = caseintraop.casemain_id
			
			LEFT JOIN psmresunit WITH (NOLOCK)
			 	ON psmresunit.resunit_id = casemain.resunit_id
			
			LEFT JOIN infectionclass WITH (NOLOCK)
				ON caseintraop.infectionclass_id = infectionclass.infectionclass_id
			
			-- Join to get the standard library of procedure abbreviations and names
			LEFT JOIN psmpro WITH (NOLOCK)
				ON psmpro.pro_id = casepro.actual_pro_id
			
			LEFT JOIN psmproname WITH (NOLOCK)
				ON psmproname.proname_id = psmpro.proname_id
			
			WHERE psmresunit.facility_abbr IN ('JMMC-C','JMMC-WC')
  AND psmresunit.resunit_name IN ('SURGERY WALNUT CREEK','SURGERY CONCORD')
      #'
    , 12
    , 'JMH_COMPASS_DATA'
    , 'data.dat'
    );
    */
    
    passthru_pkg.get_hemm_data
    (
      q'#
      SELECT DISTINCT
			  VEND.VEND_CODE AS vendr_cd
			  ,VEND.NAME AS vendr_nm
			  ,ADDR.ADDR1 AS address1
			  ,ADDR.ADDR2 AS address2
			  ,ADDR.ADDR3 AS address3
			  ,ADDR.CITY AS city
			  ,ADDR.STATE AS province
			  ,ADDR.POST_CODE AS postalcode
			  --,VEND_LOC.AVAIL_ACCT_NO AS account_no
			  ,VEND_LOC.CUST_NO AS account_no
			  --,' ' AS account_no -- May need to us this instead if there are vendors with multiple customer numbers causing duplicates
			  ,ADDR.PHONE AS vndr_phone
			  ,ADDR.FAX AS vndr_fax
			  --,' ' AS facility_code -- place holder for fixed facility code
			  ,CASE WHEN CORP.ACCT_NO = 01 THEN '01'
			   WHEN CORP.ACCT_NO = 02 THEN '02'
			   ELSE '00' END  AS facility_code
			  --,'JOHN MUIR HEALTH SYSTEM ' AS facility_name -- place holder for fixed facility name
			  ,CASE WHEN CORP.ACCT_NO = 01 THEN 'JM-WALNUT CREEK'
			   WHEN CORP.ACCT_NO = 02 THEN 'JM-CONCORD'
			   ELSE 'JOHN MUIR MEDICAL CENTER' END AS facility_name
			
			FROM
			  VEND WITH (NOLOCK)
			  
			
			LEFT JOIN VEND_ADDR WITH (NOLOCK) ON VEND_ADDR.VEND_ID = VEND.VEND_ID AND VEND_ADDR.VEND_IDB=VEND.VEND_IDB
			LEFT JOIN ADDR WITH (NOLOCK) ON ADDR.ADDR_ID = VEND_ADDR.ADDR_ID AND ADDR.ADDR_IDB=VEND_ADDR.VEND_IDB
			LEFT JOIN VEND_LOC WITH (NOLOCK) ON VEND_LOC.VEND_ID = VEND.VEND_ID AND VEND_LOC.VEND_IDB=VEND.VEND_IDB
			
			--Join to PO table to bring in facility name and code. If no PO record will be fixed to default facility
			LEFT JOIN PO WITH (NOLOCK) ON PO.VEND_ID=VEND.VEND_ID AND PO.VEND_IDB=VEND.VEND_IDB
			LEFT JOIN CORP WITH (NOLOCK) ON PO.CORP_ID = CORP.CORP_ID AND CORP.CORP_IDB=PO.PO_IDB
			
			--Where VEND_LOC.CUST_NO != ' '
			
--WHERE VEND.VEND_CODE='8615'
      #'
    , 13
    , 'JMH_COMPASS_DATA'
    , 'data.dat'
    );
    
end;
/
