update prod_dna_core.inditg_integration.itg_dailysales
set rtrname = TRIM(REGEXP_REPLACE(rtrname, '\\\\', ''))
where  rtrname like '%\\%';
