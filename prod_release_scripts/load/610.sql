
UPDATE META_RAW.PARAMETERS 
SET parameter_value = 'asp'
where parameter_group_id in (1717, 1718, 1719, 1720, 1721, 1722 )
and parameter_name = 'container';

UPDATE META_RAW.PARAMETERS 
SET parameter_value = 'SELECT acct_hier_shrt_desc as "acct_hier_shrt_desc", amt_obj_crncy as "amt_obj_crncy", caln_yr_mo as "caln_yr_mo", co_cd as "co_cd", ctry_key as "ctry_key", cust_num as "cust_num", fisc_yr as "fisc_yr", matl_num as "matl_num", obj_crncy_co_obj as "obj_crncy_co_obj", qty as "qty", sls_org as "sls_org", prft_ctr as "prft_ctr", crt_dttm as "crt_dttm", updt_dttm as "updt_dttm" FROM aspedw_integration.edw_copa_trans_fact WHERE ( (acct_hier_shrt_desc = ''NTS'') AND to_date(crt_dttm) =TO_CHAR(DATEADD(DAY, -1, CURRENT_DATE), ''YYYY-MM-DD''))"' 
where parameter_id in (19874);
