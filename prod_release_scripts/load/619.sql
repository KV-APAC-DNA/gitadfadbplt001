Update META_RAW.PARAMETERS set Parameter_Value=replace(PARAMETER_VALUE,'_TEMP','')
where parameter_group_id in 
('976','977') and PARAMETER_NAME='target_schema_table';

Update META_RAW.PARAMETERS set Parameter_Value=replace(PARAMETER_VALUE,'_temp','')
where parameter_group_id in 
(
'1703','1704','1705','1706','1707','1708','1709','1710','1711',
'1712','1713','1714','1715','1716','1717','1718','1719','1720',
'1721','1722','1723','1740','2211','2212'
) and PARAMETER_NAME='target_schema_table';


Update META_RAW.PARAMETERS set Parameter_Value=concat('sql_server/MDS_Reverse_Sync/',replace(parameter_group_name,'_group','/'))
where parameter_group_id in 
(
'976','977',
'1703','1704','1705','1706','1707','1708','1709','1710','1711',
'1712','1713','1714','1715','1716','1717','1718','1719','1720',
'1721','1722','1723','1740','2211','2212'
) and PARAMETER_NAME='landing_file_path';


UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE = 
'SELECT acct_hier_shrt_desc as "acct_hier_shrt_desc", amt_obj_crncy as "amt_obj_crncy", caln_yr_mo as "caln_yr_mo", co_cd as "co_cd", ctry_key as "ctry_key", cust_num as "cust_num", fisc_yr as "fisc_yr", matl_num as "matl_num", obj_crncy_co_obj as "obj_crncy_co_obj", qty as "qty", sls_org as "sls_org", prft_ctr as "prft_ctr", crt_dttm as "crt_dttm", updt_dttm as "updt_dttm" 
FROM aspedw_integration.edw_copa_trans_fact 
WHERE ( (acct_hier_shrt_desc = ''NTS'') AND to_date(crt_dttm) =TO_CHAR(DATEADD(DAY, -1, CURRENT_DATE), ''YYYY-MM-DD''))'
WHERE PARAMETER_ID = 19874;


INSERT INTO META_RAW.PROCESS VALUES (2231,2231,'edw_customer_dim',2231,1,1,FALSE,TRUE,326,9,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2232,2232,'edw_product_attr_dim',2232,1,1,FALSE,TRUE,326,9,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2233,2233,'edw_rpt_regional_sellout_offtake',2233,1,1,FALSE,TRUE,326,9,1,NULL,'','','','','','','');

----PARAMETERS


INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27265,2231,'edw_customer_dim_group','container','asp',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27266,2231,'edw_customer_dim_group','landing_file_path','sql_server/MDS_Reverse_Sync/edw_customer_dim/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27267,2231,'edw_customer_dim_group','landing_file_name','edw_customer_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27268,2231,'edw_customer_dim_group','target_schema_table','dbo.rs_in_edw_customer_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27269,2231,'edw_customer_dim_group','source_schema_table','indedw_integration.edw_customer_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27270,2231,'edw_customer_dim_group','source_query','select CUSTOMER_CODE as "customer_code", REGION_CODE as "region_code", REGION_NAME as "region_name", ZONE_CODE as "zone_code", ZONE_NAME as "zone_name", ZONE_CLASSIFICATION as "zone_classification", TERRITORY_CODE as "territory_code", TERRITORY_NAME as "territory_name", TERRITORY_CLASSIFICATION as "territory_classification", STATE_CODE as "state_code", STATE_NAME as "state_name", TOWN_NAME as "town_name", TOWN_CLASSIFICATION as "town_classification", CITY as "city", TYPE_CODE as "type_code", TYPE_NAME as "type_name", CUSTOMER_NAME as "customer_name", CUSTOMER_ADDRESS1 as "customer_address1", CUSTOMER_ADDRESS2 as "customer_address2", CUSTOMER_ADDRESS3 as "customer_address3", ACTIVE_FLAG as "active_flag", ACTIVE_START_DATE as "active_start_date", WHOLESALERCODE as "wholesalercode", SUPER_STOCKIEST as "super_stockiest", DIRECT_ACCOUNT_FLAG as "direct_account_flag", ABI_CODE as "abi_code", ABI_NAME as "abi_name", RDS_SIZE as "rds_size", CRT_DTTM as "crt_dttm", UPDT_DTTM as "updt_dttm", NUM_OF_RETAILERS as "num_of_retailers", CUSTOMER_TYPE as "customer_type", PSNONPS as "psnonps", SUPPLIEDBY as "suppliedby", CFA as "cfa",CFA_NAME as "cfa_name", TOWN_CODE as "town_code" FROM indedw_integration.edw_customer_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27271,2231,'edw_customer_dim_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27272,2231,'edw_customer_dim_group','decide_source','dna_core',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27273,2232,'edw_product_attr_dim_group','container','asp',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27274,2232,'edw_product_attr_dim_group','landing_file_path','sql_server/MDS_Reverse_Sync/edw_product_attr_dim/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27275,2232,'edw_product_attr_dim_group','landing_file_name','edw_product_attr_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27276,2232,'edw_product_attr_dim_group','target_schema_table','dbo.RS_rg_edw_product_attr_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27277,2232,'edw_product_attr_dim_group','source_schema_table','aspedw_integration.edw_product_attr_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27278,2232,'edw_product_attr_dim_group','source_query','select AW_REMOTE_KEY as "aw_remote_key",AWREFS_PROD_REMOTEKEY as "awrefs_prod_remotekey", AWREFS_BUSS_UNIT as "awrefs_buss_unit", SAP_MATL_NUM as "sap_matl_num", CNTRY as "cntry", EAN as "ean", PROD_HIER_L1 as "prod_hier_l1", PROD_HIER_L2 as "prod_hier_l2", PROD_HIER_L3 as "prod_hier_l3", PROD_HIER_L4 as "prod_hier_l4", PROD_HIER_L5 as "prod_hier_l5", PROD_HIER_L6 as "prod_hier_l6", PROD_HIER_L7 as "prod_hier_l7", PROD_HIER_L8 as "prod_hier_l8", PROD_HIER_L9 as "prod_hier_l9", CRT_DTTM as "crt_dttm", UPDT_DTTM as "updt_dttm", LCL_PROD_NM as "lcl_prod_nm" FROM aspedw_integration.edw_product_attr_dim',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27279,2232,'edw_product_attr_dim_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27280,2232,'edw_product_attr_dim_group','decide_source','dna_core',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27281,2233,'edw_rpt_regional_sellout_offtake_group','container','asp',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27282,2233,'edw_rpt_regional_sellout_offtake_group','landing_file_path','sql_server/MDS_Reverse_Sync/edw_rpt_regional_sellout_offtake/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27283,2233,'edw_rpt_regional_sellout_offtake_group','landing_file_name','edw_rpt_regional_sellout_offtake',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27284,2233,'edw_rpt_regional_sellout_offtake_group','target_schema_table','dbo.RS_rg_edw_rpt_regional_sellout_offtake',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27285,2233,'edw_rpt_regional_sellout_offtake_group','source_schema_table','aspedw_integration.edw_rpt_regional_sellout_offtake',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27286,2233,'edw_rpt_regional_sellout_offtake_group','source_query','SELECT CAL_DATE as "cal_date", COUNTRY_CODE as "country_code", COUNTRY_NAME as "country_name", DISTRIBUTOR_CODE as "distributor_code", DISTRIBUTOR_NAME as "distributor_name", GLOBAL_PRODUCT_BRAND as "global_product_brand", EAN as "ean", SKU_CODE as "sku_code", SKU_DESCRIPTION as "sku_description" FROM ( SELECT CAL_DATE, COUNTRY_CODE, COUNTRY_NAME, DISTRIBUTOR_CODE, DISTRIBUTOR_NAME, GLOBAL_PRODUCT_BRAND, EAN, SKU_CODE, SKU_DESCRIPTION, ROW_NUMBER() OVER(PARTITION BY EAN,COUNTRY_NAME ORDER BY CAL_DATE DESC) RNK FROM aspedw_integration.edw_rpt_regional_sellout_offtake where EAN <> ''NA'' and COUNTRY_CODE = ''CN'' )A WHERE RNK= 1',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27287,2233,'edw_rpt_regional_sellout_offtake_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27288,2233,'edw_rpt_regional_sellout_offtake_group','decide_source','dna_core',FALSE,TRUE);

UPDATE META_RAW.PROCESS 
SET SNOWFLAKE_STAGE = 'JPNSDL_RAW.PROD_LOAD_STAGE_ADLS'
WHERE PROCESS_ID = '1647'
and USECASE_ID = '321';
