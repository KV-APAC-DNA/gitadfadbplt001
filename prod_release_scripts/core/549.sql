delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMITG_INTEGRATION__ITG_VN_RE_MSL_LIST;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMITG_INTEGRATION__ITG_VN_RE_MSL_LIST (

with itg_re_msl_input_definition as (
    select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_RE_MSL_INPUT_DEFINITION
),
edw_calendar_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.edw_calendar_dim
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.v_edw_vw_cal_Retail_excellence_dim
),
wks_vn_base_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VN_BASE_RETAIL_EXCELLENCE
),
edw_product_key_attributes as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_PRODUCT_KEY_ATTRIBUTES
),
itg_vn_product_mapping as (
    select * from PROD_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_PRODUCT_MAPPING
),

MSL as 
(
    SELECT DISTINCT 
       CAL.FISC_YR AS YEAR,
       MARKET,
       CAL.JJ_MNTH_ID,
       MSL_DEF.RETAIL_ENVIRONMENT,
       MSL_DEF.CHANNEL,
       LTRIM(MSL_DEF.SKU_UNIQUE_IDENTIFIER,'0') AS sku_unique_identifier
    FROM ITG_RE_MSL_INPUT_DEFINITION MSL_DEF
    LEFT JOIN (SELECT DISTINCT FISC_YR,
                    SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
    WHERE UPPER(market) = 'VIETNAM'
    --AND   active_status_code = 'Y'
)

    SELECT DISTINCT MSL.YEAR :: numeric(18,0) as fisc_yr,
       msl.jj_mnth_id :: numeric(18,0) as fisc_per,
	   offtake.data_src,
       offtake.soldto_code,
       offtake.distributor_code,
       offtake.distributor_name,
       offtake.store_code,
       offtake.store_name,
       offtake.store_type,
       LTRIM(msl.sku_unique_identifier,0) AS EAN,
       UPPER(msl.retail_environment) AS retail_environment,
       UPPER(msl.channel) AS channel,
       UPPER(offtake.channel) AS Sell_Out_Channel,
	   offtake.sku_code as sku_code,
	   offtake.sku_description as sku_description,
       msl.market,
       prodhier.prod_hier_l1,
       prodhier.prod_hier_l2,
       prodhier.prod_hier_l3,
       prodhier.prod_hier_l4,
       SYSDATE() AS CRTD_DTTM
       FROM MSL
  LEFT JOIN (SELECT matl_num,
                    ean_code,
                    prod_hier_l1,
                    prod_hier_l2,
                    prod_hier_l3,
                    prod_hier_l4
             FROM (SELECT LTRIM(matl_num,'0') AS matl_num,
                          ean_upc AS ean_code,
                          gcph_franchise AS prod_hier_l1,
                          gcph_needstate AS prod_hier_l2,
                          gcph_category AS prod_hier_l3,
                          gcph_subcategory AS prod_hier_l4,
                          ROW_NUMBER() OVER (PARTITION BY ean_upc ORDER BY lst_nts DESC) AS rn
                   FROM edw_product_key_attributes
                   WHERE ctry_nm = 'Vietnam'
                   AND   matl_type_cd = 'FERT'
                   AND   (lst_nts IS NOT NULL)) a
               JOIN itg_vn_product_mapping c ON a.ean_code = c.barcode
             WHERE rn = 1) prodhier ON LTRIM (msl.sku_unique_identifier,'0') = LTRIM (prodhier.ean_code)
  LEFT JOIN (SELECT Data_Src,
                    CNTRY_CD,
                    CNTRY_NM,
                    soldto_code,
                    Distributor_code,
                    Region,
                    ZONE,
                    Distributor_Name,
                    Channel,
                    store_code,
                    store_name,
                    store_type,
                    ean,
					sku_description,
					sku_code,
                    RETAIL_ENVIRONMENT AS retail_env
             FROM WKS_VN_BASE_RETAIL_EXCELLENCE) offtake
         ON UPPER (offtake.retail_env) = UPPER (msl.Retail_Environment)
        AND LTRIM (offtake.EAN, '0') = LTRIM (msl.sku_unique_identifier, '0')
WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths
                         FROM edw_vw_cal_Retail_excellence_Dim)
AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)
AND   STORE_CODE IS NOT NULL
AND   DISTRIBUTOR_CODE IS NOT NULL
);

-------------------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_actuals;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_actuals (

with wks_vn_base_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VN_BASE_RETAIL_EXCELLENCE
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.v_edw_vw_cal_Retail_excellence_dim
),
wks_vn_regional_sellout_act_lm as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_lm
),
wks_vn_regional_sellout_act_l3m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l3m
),
wks_vn_regional_sellout_act_l6m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l6m
),
wks_vn_regional_sellout_act_l12m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l12m
)

    SELECT RE_BASE_DIM.CNTRY_CD,
       RE_BASE_DIM.CNTRY_NM,
       SUBSTRING(BASE_DIM.MONTH,1,4) AS YEAR,
       BASE_DIM.MONTH AS MNTH_ID,
	   RE_BASE_DIM.SOLDTO_CODE,	
       RE_BASE_DIM.DISTRIBUTOR_CODE,
	   RE_BASE_DIM.DISTRIBUTOR_NAME,
       RE_BASE_DIM.STORE_CODE,
	   RE_BASE_DIM.STORE_NAME,
       RE_BASE_DIM.STORE_TYPE,
       RE_BASE_DIM.EAN,	
       RE_BASE_DIM.SKU_CODE,
	   RE_BASE_DIM.SKU_DESCRIPTION,
       RE_BASE_DIM.DATA_SRC,
       RE_BASE_DIM.REGION,	
       RE_BASE_DIM.ZONE,	
	   RE_BASE_DIM.LIST_PRICE,
       RE_BASE_DIM.SAP_PARENT_CUSTOMER_KEY,
       RE_BASE_DIM.SAP_PARENT_CUSTOMER_DESCRIPTION,		
       RE_BASE_DIM.SAP_CUSTOMER_CHANNEL_KEY,
       RE_BASE_DIM.SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       RE_BASE_DIM.SAP_CUSTOMER_SUB_CHANNEL_KEY,		
       RE_BASE_DIM.SAP_SUB_CHANNEL_DESCRIPTION,
       RE_BASE_DIM.SAP_GO_TO_MDL_KEY,	
       RE_BASE_DIM.SAP_GO_TO_MDL_DESCRIPTION,	
       RE_BASE_DIM.SAP_BANNER_KEY,	
       RE_BASE_DIM.SAP_BANNER_DESCRIPTION,	
       RE_BASE_DIM.SAP_BANNER_FORMAT_KEY,	
       RE_BASE_DIM.SAP_BANNER_FORMAT_DESCRIPTION,	
       RE_BASE_DIM.RETAIL_ENVIRONMENT,	
	   RE_BASE_DIM.CHANNEL,	
       RE_BASE_DIM.CUSTOMER_SEGMENT_KEY,
       RE_BASE_DIM.CUSTOMER_SEGMENT_DESCRIPTION,	
       RE_BASE_DIM.GLOBAL_PRODUCT_FRANCHISE,	
       RE_BASE_DIM.GLOBAL_PRODUCT_BRAND,	
       RE_BASE_DIM.GLOBAL_PRODUCT_SUB_BRAND,	
       RE_BASE_DIM.GLOBAL_PRODUCT_VARIANT,	
       RE_BASE_DIM.GLOBAL_PRODUCT_SEGMENT,	
       RE_BASE_DIM.GLOBAL_PRODUCT_SUBSEGMENT,	
       RE_BASE_DIM.GLOBAL_PRODUCT_CATEGORY,	
       RE_BASE_DIM.GLOBAL_PRODUCT_SUBCATEGORY,	
       RE_BASE_DIM.GLOBAL_PUT_UP_DESCRIPTION,	
       RE_BASE_DIM.PKA_PRODUCT_KEY,	
       RE_BASE_DIM.PKA_PRODUCT_KEY_DESCRIPTION,	
       CM.SO_SLS_QTY AS CM_SALES_QTY,	
       CM.SO_SLS_VALUE AS CM_SALES,	
       CM.SO_AVG_QTY AS CM_AVG_SALES_QTY,	
       CM.SALES_VALUE_LIST_PRICE AS CM_SALES_VALUE_LIST_PRICE,	
       LM_SALES AS LM_SALES,
       LM_SALES_QTY AS LM_SALES_QTY,
       LM_AVG_SALES_QTY AS LM_AVG_SALES_QTY,
       LM_SALES_LP AS LM_SALES_LP,
       L3M_SALES AS P3M_SALES,
       L3M_SALES_QTY AS P3M_QTY,
       L3M_AVG_SALES_QTY AS P3M_AVG_QTY,
       L3M_SALES_LP AS P3M_SALES_LP,
       F3M_SALES AS F3M_SALES,
       F3M_SALES_QTY AS F3M_QTY,
       F3M_AVG_SALES_QTY AS F3M_AVG_QTY,
       L6M_SALES AS P6M_SALES,
       L6M_SALES_QTY AS P6M_QTY,
       L6M_AVG_SALES_QTY AS P6M_AVG_QTY,
       L6M_SALES_LP AS P6M_SALES_LP,
       L12M_SALES AS P12M_SALES,
       L12M_SALES_QTY AS P12M_QTY,
       L12M_AVG_SALES_QTY AS P12M_AVG_QTY,
       L12M_SALES_LP AS P12M_SALES_LP,
       CASE
         WHEN LM_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS LM_SALES_FLAG,
       CASE
         WHEN P3M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P3M_SALES_FLAG,
       CASE
         WHEN P6M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P6M_SALES_FLAG,
       CASE
         WHEN P12M_SALES > 0 THEN 'Y'
         ELSE 'N'
       END AS P12M_SALES_FLAG,
	   SYSDATE() AS CRTD_DTTM
FROM (SELECT DISTINCT CNTRY_CD,
             sellout_dim_key,
             data_src,
             MONTH
      FROM (SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_VN_REGIONAL_SELLOUT_ACT_LM	
            WHERE LM_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_VN_REGIONAL_SELLOUT_ACT_L3M	
            WHERE L3M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_VN_REGIONAL_SELLOUT_ACT_L6M	
            WHERE L6M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_VN_REGIONAL_SELLOUT_ACT_L12M
            WHERE L12M_SALES IS NOT NULL)) BASE_DIM
  LEFT JOIN (SELECT DISTINCT CNTRY_CD,
                    cntry_nm,
                    data_src,
                    sellout_dim_key,
                    DISTRIBUTOR_CODE,
                    DISTRIBUTOR_NAME,
                    SOLDTO_CODE,
                    store_code,
                    EAN,
					SKU_DESCRIPTION,
					SKU_CODE,
                    STORE_NAME,
                    RETAIL_ENVIRONMENT,
					store_type,
					Region,
					zone,
					list_price,
                    CHANNEL,
                    SAP_PARENT_CUSTOMER_KEY,
                    SAP_PARENT_CUSTOMER_DESCRIPTION,
                    SAP_CUSTOMER_CHANNEL_KEY,
                    SAP_CUSTOMER_CHANNEL_DESCRIPTION,
                    SAP_CUSTOMER_SUB_CHANNEL_KEY,
                    SAP_SUB_CHANNEL_DESCRIPTION,
                    SAP_GO_TO_MDL_KEY,
                    SAP_GO_TO_MDL_DESCRIPTION,
                    SAP_BANNER_KEY,
                    SAP_BANNER_DESCRIPTION,
                    SAP_BANNER_FORMAT_KEY,
                    SAP_BANNER_FORMAT_DESCRIPTION,
                    CUSTOMER_SEGMENT_KEY,
                    CUSTOMER_SEGMENT_DESCRIPTION,
                    GLOBAL_PRODUCT_FRANCHISE,
                    GLOBAL_PRODUCT_BRAND,
                    GLOBAL_PRODUCT_SUB_BRAND,
                    GLOBAL_PRODUCT_VARIANT,
                    GLOBAL_PRODUCT_SEGMENT,
                    GLOBAL_PRODUCT_SUBSEGMENT,
                    GLOBAL_PRODUCT_CATEGORY,
                    GLOBAL_PRODUCT_SUBCATEGORY,
                    GLOBAL_PUT_UP_DESCRIPTION,
                    PKA_PRODUCT_KEY,
                    PKA_PRODUCT_KEY_DESCRIPTION
             FROM WKS_VN_BASE_RETAIL_EXCELLENCE) RE_BASE_DIM	
         ON RE_BASE_DIM.CNTRY_CD = BASE_DIM.CNTRY_CD	
        AND RE_BASE_DIM.SELLOUT_DIM_KEY = BASE_DIM.SELLOUT_DIM_KEY
        AND RE_BASE_DIM.DATA_SRC = BASE_DIM.DATA_SRC	
  LEFT OUTER JOIN (SELECT DISTINCT CNTRY_CD,
                          sellout_dim_key,
                          data_src,
                          mnth_id,
                          SO_SLS_QTY,
                          SO_SLS_VALUE,
                          SO_AVG_QTY,
                          SALES_VALUE_LIST_PRICE
                   FROM WKS_VN_BASE_RETAIL_EXCELLENCE) CM	
               ON BASE_DIM.CNTRY_CD = CM.CNTRY_CD	
              AND BASE_DIM.MONTH = CM.MNTH_ID	
              AND BASE_DIM.SELLOUT_DIM_KEY = CM.SELLOUT_DIM_KEY	
              AND BASE_DIM.DATA_SRC = CM.DATA_SRC
  LEFT OUTER JOIN
--Last Month
WKS_VN_REGIONAL_SELLOUT_ACT_LM LM
               ON BASE_DIM.CNTRY_CD = LM.CNTRY_CD	
              AND BASE_DIM.MONTH = LM.MONTH		
              AND BASE_DIM.SELLOUT_DIM_KEY = LM.SELLOUT_DIM_KEY	
              AND BASE_DIM.DATA_SRC = LM.DATA_SRC
  LEFT OUTER JOIN
--L3M
WKS_VN_REGIONAL_SELLOUT_ACT_L3M L3M
               ON BASE_DIM.CNTRY_CD = L3M.CNTRY_CD	
              AND BASE_DIM.MONTH = L3M.MONTH	
              AND BASE_DIM.SELLOUT_DIM_KEY = L3M.SELLOUT_DIM_KEY
              AND BASE_DIM.DATA_SRC = L3M.DATA_SRC
  LEFT OUTER JOIN
--L6M
WKS_VN_REGIONAL_SELLOUT_ACT_L6M L6M
               ON BASE_DIM.CNTRY_CD = L6M.CNTRY_CD	
              AND BASE_DIM.MONTH = L6M.MONTH	
              AND BASE_DIM.SELLOUT_DIM_KEY = L6M.SELLOUT_DIM_KEY
              AND BASE_DIM.DATA_SRC = L6M.DATA_SRC	
  LEFT OUTER JOIN
--L12M
WKS_VN_REGIONAL_SELLOUT_ACT_L12M L12M
               ON BASE_DIM.CNTRY_CD = L12M.CNTRY_CD		
              AND BASE_DIM.MONTH = L12M.MONTH	
              AND BASE_DIM.SELLOUT_DIM_KEY = L12M.SELLOUT_DIM_KEY	
              AND BASE_DIM.DATA_SRC = L12M.DATA_SRC
WHERE BASE_DIM.MONTH >= (SELECT last_16mnths
                        FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC 
AND   BASE_DIM.MONTH <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC
);
