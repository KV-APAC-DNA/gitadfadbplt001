delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAITG_INTEGRATION__ITG_HK_RE_MSL_LIST;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAITG_INTEGRATION__ITG_HK_RE_MSL_LIST (

with itg_re_msl_input_definition as (
    select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_RE_MSL_INPUT_DEFINITION
),
edw_calendar_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_CALENDAR_DIM
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.V_EDW_VW_CAL_RETAIL_EXCELLENCE_DIM
),
wks_hk_base_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HK_BASE_RETAIL_EXCELLENCE
),

MSL as 
(
    SELECT DISTINCT CAL.FISC_YR AS YEAR,
       MARKET,
       CAL.JJ_MNTH_ID,
       MSL_DEF.CUSTOMER,
       MSL_DEF.STORE_GRADE,
       UPPER(MSL_DEF.RETAIL_ENVIRONMENT) AS RETAIL_ENVIRONMENT,
       LTRIM(MSL_DEF.SKU_UNIQUE_IDENTIFIER,'0') AS sku_unique_identifier
    FROM ITG_RE_MSL_INPUT_DEFINITION MSL_DEF
    LEFT JOIN (SELECT DISTINCT FISC_YR,
                    SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
    WHERE UPPER(market) = 'HONG KONG'
    AND   active_status_code = 'Y'
),

REG_SO as 
(SELECT DISTINCT data_source,
                SOLDTO_CODE as sold_to_code,
                distributor_code,
                distributor_name,
                store_code,
                store_name,
                ltrim(ean,'0') as ean,
                sku_description,
                sku_code,
                retail_environment,
                store_grade,
                channel AS CHANNEL_DESC,
                store_type,
                CNTRY_CD,
                CNTRY_NM as country
                FROM WKS_HK_BASE_RETAIL_EXCELLENCE)
				
    SELECT DISTINCT MSL.YEAR :: numeric(18,0) as fisc_yr,
       MSL.JJ_MNTH_ID :: numeric(18,0) as fisc_per,
       REG_SO.COUNTRY,
       REG_SO.DATA_SOURCE,
       REG_SO.CHANNEL_DESC,
       REG_SO.RETAIL_ENVIRONMENT,
       REG_SO.DISTRIBUTOR_CODE,	
       REG_SO.DISTRIBUTOR_NAME,
       REG_SO.SOLD_TO_CODE,	
       REG_SO.STORE_CODE,
       REG_SO.STORE_NAME,
	   REG_SO.STORE_GRADE,
	   REG_SO.STORE_TYPE,
       LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') AS EAN,
	   REG_SO.SKU_CODE,	
	   REG_SO.SKU_DESCRIPTION,
       SYSDATE() AS CRTD_DTTM :: timestamp without time zone as crtd_dttm
    FROM MSL
    LEFT JOIN REG_SO ON  UPPER (LTRIM(MSL.sku_unique_identifier,'0')) = UPPER (LTRIM(REG_SO.EAN,'0'))
				     AND UPPER (MSL.retail_environment) = UPPER (REG_SO.RETAIL_ENVIRONMENT)
                     --AND UPPER (MSL.store_grade) = UPPER (REG_SO.store_grade)
    WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths
                         FROM edw_vw_cal_Retail_excellence_Dim)
    AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)
    AND   STORE_CODE IS NOT NULL
    AND   DISTRIBUTOR_CODE IS NOT NULL
);
