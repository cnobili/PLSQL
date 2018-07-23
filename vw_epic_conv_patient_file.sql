create or replace view vw_epic_conv_patient_file
as
select
  p.hne_id
, p.last_name
, p.first_name
, p.middle_name
, p.prefix
, p.suffix
, p.gender
, to_char(p.dob, 'mm-dd-yyyy') as dob
, p.address1
, p.address2
, p.city
, p.state
, p.zip
, p.phone
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
, p.star_mrn
, p.meditech_mrn
, p.plus_mrn
from
  epic.dim_passport p
;
