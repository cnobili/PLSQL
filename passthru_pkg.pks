create or replace package passthru_pkg
as

--
-- Types
--

type char_array_t is table of varchar2(32767);

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
);

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
);

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
);
  
end passthru_pkg;
/
