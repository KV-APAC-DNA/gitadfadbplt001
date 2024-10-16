delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACTUALS;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACTUALS (    
with wks_tw_base_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE
),
wks_tw_re_act_lm as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACT_LM
),
wks_tw_re_act_l3m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACT_L3M
),
wks_tw_re_act_l6m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACT_L6M
),
wks_tw_re_act_l12m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_ACT_L12M
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.v_edw_vw_cal_Retail_excellence_dim
)

SELECT RE_BASE_DIM.CNTRY_CD,
       RE_BASE_DIM.CNTRY_NM,
       RE_BASE_DIM.data_src,
       SUBSTRING(BASE_DIM.MONTH,1,4) AS YEAR,
       BASE_DIM.MONTH AS MNTH_ID,
       RE_BASE_DIM.SOLDTO_CODE,
       RE_BASE_DIM.DISTRIBUTOR_CODE,
       RE_BASE_DIM.DISTRIBUTOR_NAME,
       RE_BASE_DIM.STORE_CODE,
       RE_BASE_DIM.EAN,
       RE_BASE_DIM.STORE_NAME,
       RE_BASE_DIM.RETAIL_ENVIRONMENT,
	   RE_BASE_DIM.store_type,
       RE_BASE_DIM.store_grade,
       RE_BASE_DIM.list_price,
       RE_BASE_DIM.msl_product_desc,
       RE_BASE_DIM.sku_code,
	   RE_BASE_DIM.Region,
       RE_BASE_DIM.zone_or_area,
       CHANNEL_NAME,
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
       PKA_PRODUCT_KEY_DESCRIPTION,
       CM.so_sls_qty AS CM_SALES_QTY,
       CM.so_sls_value AS CM_SALES,
       CM.so_avg_qty AS CM_AVG_SALES_QTY,
       CM.SALES_VALUE_LIST_PRICE AS SALES_VALUE_LIST_PRICE,
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
       SYSDATE() AS CRT_DTTM
FROM (SELECT DISTINCT CNTRY_CD,
             sellout_dim_key,
             data_src,
             MONTH
      FROM (SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_LM
            WHERE LM_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L3M
            WHERE L3M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L6M
            WHERE L6M_SALES IS NOT NULL
            UNION ALL
            SELECT CNTRY_CD,
                   sellout_dim_key,
                   data_src,
                   MONTH
            FROM WKS_TW_RE_ACT_L12M
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
                    STORE_NAME,
                    RETAIL_ENVIRONMENT,
					store_type,
                    store_grade,
                    list_price,
                    msl_product_desc,
                    sku_code,
					Region,
                    zone_or_area,
                    CHANNEL_NAME,
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
             FROM WKS_TW_BASE_RE) RE_BASE_DIM
         ON RE_BASE_DIM.cntry_cd = BASE_DIM.cntry_cd
        AND RE_BASE_DIM.sellout_dim_key = BASE_DIM.sellout_dim_key
        AND RE_BASE_DIM.DATA_SRC = BASE_DIM.DATA_SRC
  LEFT OUTER JOIN (SELECT DISTINCT CNTRY_CD,
                          sellout_dim_key,
                          data_src,
                          mnth_id,
                          so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          SALES_VALUE_LIST_PRICE
                   FROM WKS_TW_BASE_RE) CM
               ON BASE_DIM.CNTRY_CD = CM.CNTRY_CD
              AND BASE_DIM.Month = CM.mnth_id
              AND BASE_DIM.sellout_dim_key = CM.sellout_dim_key
              AND BASE_DIM.DATA_SRC = CM.DATA_SRC
  LEFT OUTER JOIN
--Last Month
WKS_TW_RE_ACT_LM LM
               ON BASE_DIM.CNTRY_CD = LM.CNTRY_CD
              AND BASE_DIM.month = LM.MONTH
              AND BASE_DIM.sellout_dim_key = LM.sellout_dim_key
              AND BASE_DIM.DATA_SRC = LM.DATA_SRC
  LEFT OUTER JOIN
--L3M
WKS_TW_RE_ACT_L3M L3M
               ON BASE_DIM.CNTRY_CD = L3M.CNTRY_CD
              AND BASE_DIM.month = L3M.MONTH
              AND BASE_DIM.sellout_dim_key = L3M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L3M.DATA_SRC
  LEFT OUTER JOIN
--L6M
WKS_TW_RE_ACT_L6M L6M
               ON BASE_DIM.CNTRY_CD = L6M.CNTRY_CD
              AND BASE_DIM.month = L6M.MONTH
              AND BASE_DIM.sellout_dim_key = L6M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L6M.DATA_SRC
  LEFT OUTER JOIN
--L12M
WKS_TW_RE_ACT_L12M L12M
               ON BASE_DIM.CNTRY_CD = L12M.CNTRY_CD
              AND BASE_DIM.month = L12M.MONTH
              AND BASE_DIM.sellout_dim_key = L12M.sellout_dim_key
              AND BASE_DIM.DATA_SRC = L12M.DATA_SRC
where BASE_DIM.month >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
  and BASE_DIM.month <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric 
);
