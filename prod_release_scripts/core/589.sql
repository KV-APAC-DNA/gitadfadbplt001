delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_actuals;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_actuals ( 
with wks_tw_base_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE
),
wks_tw_re_act_lm as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_lm
),
wks_tw_re_act_l3m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l3m
),
wks_tw_re_act_l6m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l6m
),
wks_tw_re_act_l12m as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l12m
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.V_EDW_VW_CAL_RETAIL_EXCELLENCE_DIM
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


-----------------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_msl_list;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_msl_list (   
with edw_calendar_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.edw_calendar_dim
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.aspedw_integration.v_edw_vw_cal_Retail_excellence_dim
),
itg_re_msl_input_definition as (
    select * from PROD_DNA_CORE.aspitg_integration.itg_re_msl_input_definition
),
wks_tw_base_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE
),
edw_vw_pop6_products as (
    select * from PROD_DNA_CORE.aspedw_integration.edw_vw_pop6_products
)

SELECT DISTINCT jj_year,
       jj_mnth_id,
       soldto_code,
       data_src,
       distributor_code,
       distributor_name,
       store_code,
       store_name,
       store_type,
       ean,
       store_grade,
       Sell_Out_Channel,
       retail_environment,
       market,
       cntry_cd,
       prod_hier_l1,
       prod_hier_l2,
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       prod_hier_l6,
       prod_hier_l7,
       prod_hier_l8,
       prod_hier_l9,
       sku_code,
       --MAX(msl_product_desc) OVER (PARTITION BY LTRIM(ean,0) ORDER BY LENGTH(msl_product_desc) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msl_product_desc,
       msl_product_desc,
       region,
       zone_or_area
FROM (SELECT DISTINCT SUBSTRING(base.jj_mnth_id,1,4) AS jj_year,
             base.jj_mnth_id,
             noo.soldto_code,
             noo.data_src,
             noo.distributor_code,
             noo.distributor_name,
             noo.distributor_name_new,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             LTRIM(base.sku_unique_identifier,0) AS ean,
             base.store_grade,
             noo.channel AS Sell_Out_Channel,
             UPPER(base.retail_environment) AS retail_environment,
             base.market,
             base.cntry_cd,
             epd.prod_hier_l1,
             epd.prod_hier_l2,
             epd.prod_hier_l3,
             epd.prod_hier_l4,
             epd.prod_hier_l5,
             prod_hier_l6,
             prod_hier_l7,
             prod_hier_l8,
             epd.prod_hier_l9,
             noo.sku_code,
             noo.msl_product_desc,
             noo.region,
             noo.zone_or_area,
             base.customer_name
      FROM (SELECT DISTINCT *,
                   CASE
                     WHEN msl.market = 'Taiwan' THEN 'TW'
                   END cntry_cd,
                   CASE
                     WHEN customer LIKE '%Carrefour%' THEN 'Carrefour'
                     WHEN customer LIKE '%PX%' THEN 'PX'
                     WHEN customer LIKE '%Watson%' THEN 'Watsons'
                     ELSE customer
                   END AS customer_name
            FROM itg_re_msl_input_definition msl
              LEFT JOIN (SELECT DISTINCT SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM
                         WHERE jj_mnth_id >= (SELECT last_17mnths FROM edw_vw_cal_Retail_excellence_Dim)
                         AND   jj_mnth_id <= (SELECT last_2mnths FROM edw_vw_cal_Retail_excellence_Dim)) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM') <= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= cal.jj_mnth_id
            WHERE UPPER(msl.market) = 'TAIWAN'
            --in ('Taiwan')	 and msl.retail_environment='MT'
            ) base
        LEFT JOIN (SELECT DISTINCT data_src,
                          distributor_code,
                          distributor_name,
                          CASE
                            WHEN distributor_name LIKE '%Carrefour%' THEN 'Carrefour'
                            WHEN distributor_name LIKE '%PX%' THEN 'PX'
                            WHEN distributor_name LIKE '%Watsons%' THEN 'Watsons'
                            ELSE distributor_name
                          END AS distributor_name_new,
                          store_code,
                          store_type,
                          region,
                          zone_or_area,
                          RETAIL_ENVIRONMENT,
                          store_grade,
                          CHANNEL_NAME AS channel,
                          CNTRY_CD,
                          soldto_code,
                          store_name,
                          msl_product_desc,
                          sku_code,
                          EAN
                   FROM WKS_TW_BASE_RE
                   --WHERE CNTRY_CD in  ('TW') and data_src='POS' and RETAIL_ENVIRONMENT='MT'
                   ) NOO
               ON UPPER (base.channel) = UPPER (noo.channel)
              AND UPPER(base.customer_name) = UPPER(noo.distributor_name_new)
              AND UPPER(base.Retail_Environment) = UPPER(noo.Retail_Environment)
              AND UPPER(base.cntry_cd) = UPPER(noo.CNTRY_CD)
              AND TRIM (base.sku_unique_identifier) = TRIM (noo.EAN)
        LEFT JOIN (SELECT DISTINCT prd.country_l1 AS prod_hier_l1,
                          prd.regional_franchise_l2 AS prod_hier_l2,
                          prd.franchise_l3 AS prod_hier_l3,
                          prd.brand_l4 AS prod_hier_l4,
                          prd.sub_category_l5 AS prod_hier_l5,
                          prd.platform_l6 AS prod_hier_l6,
                          prd.variance_l7 AS prod_hier_l7,
                          prd.pack_size_l8 AS prod_hier_l8,
                          NULL AS prod_hier_l9,
                          prd.barcode
                   FROM edw_vw_pop6_products prd
                   WHERE cntry_cd = 'TW') epd ON UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.barcode))
      -- and row_no=1
    )
    WHERE STORE_CODE IS NOT NULL
    AND   DISTRIBUTOR_CODE IS NOT NULL
);
