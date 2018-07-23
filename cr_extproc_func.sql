create or replace function extproc_toupper(p_str in varchar2)
return varchar2
as language C
library extlib
name "str_to_upper"
parameters (p_str string)
;
/
