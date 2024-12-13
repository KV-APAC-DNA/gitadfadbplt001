update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'sql_server/MDS_Reverse_Sync/edw_rpt_regional_sellout_offtake_Mothercode/'
WHERE PARAMETER_ID = 27008;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'edw_rpt_regional_sellout_offtake_Mothercode'
WHERE PARAMETER_ID = 27009;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'dbo.RS_rg_edw_rpt_regional_sellout_offtake_Mothercode'
WHERE PARAMETER_ID = 27010;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'aspedw_integration.edw_rpt_regional_sellout_offtake' 
WHERE PARAMETER_ID = 27011;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'SELECT CAL_DATE as "cal_date", COUNTRY_CODE as "country_code", COUNTRY_NAME as "country_name", DISTRIBUTOR_CODE as "distributor_code", DISTRIBUTOR_NAME as "distributor_name", GLOBAL_PRODUCT_BRAND as "global_product_brand", EAN as "ean", SKU_CODE as "sku_code", SKU_DESCRIPTION as "sku_description" FROM ( SELECT CAL_DATE, COUNTRY_CODE, COUNTRY_NAME, DISTRIBUTOR_CODE, DISTRIBUTOR_NAME, GLOBAL_PRODUCT_BRAND, EAN, SKU_CODE, SKU_DESCRIPTION, ROW_NUMBER() OVER(PARTITION BY EAN,COUNTRY_NAME ORDER BY CAL_DATE DESC) RNK FROM aspedw_integration.edw_rpt_regional_sellout_offtake where EAN <> ''NA'' )A WHERE RNK= 1'
WHERE PARAMETER_ID = 27012;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'Y'
WHERE PARAMETER_ID = 27013;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'dna_core'
WHERE PARAMETER_ID = 27014;
