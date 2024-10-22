delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE (

with v_edw_vn_rpt_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE
)

    SELECT FISC_YR::numeric(18,0) AS fisc_yr,
       CAST(FISC_PER AS numeric(18,0) ) AS FISC_PER,
       "cluster"::VARCHAR(100) AS "cluster",
       MARKET,
       MD5(nvl (SELL_OUT_CHANNEL,'soc') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (REGION,'reg') ||
	   nvl (ZONE_NAME,'zn') ||nvl (CITY,'cty') ||nvl (PROD_HIER_L1,'ph1') ||nvl (PROD_HIER_L2,'ph2') || 
	   nvl (PROD_HIER_L3,'ph3') ||nvl (PROD_HIER_L4,'ph4') || nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||
	   nvl (GLOBAL_PRODUCT_BRAND,'gpb') || nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') || 
	   nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') || 
	   nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')) AS FLAG_AGG_DIM_KEY,
       DATA_SRC,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       REGION,
       ZONE_NAME,
       CITY,
       RETAIL_ENVIRONMENT,
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       NULL AS PROD_HIER_L5,
       NULL AS PROD_HIER_L6,
       NULL AS PROD_HIER_L7,
       NULL AS PROD_HIER_L8,
       NULL AS PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       STORE_CODE,
       PRODUCT_CODE,
       SUM(SALES_VALUE) AS SALES_VALUE,
       SUM(SALES_QTY) AS SALES_QTY,
       AVG(SALES_QTY) AS AVG_SALES_QTY,
       SUM(LM_SALES) AS LM_SALES,
       SUM(LM_SALES_QTY) AS LM_SALES_QTY,
       AVG(LM_SALES_QTY) AS LM_AVG_SALES_QTY,
       SUM(P3M_SALES) AS P3M_SALES,
       SUM(P3M_QTY) AS P3M_QTY,
       AVG(P3M_QTY) AS P3M_AVG_QTY,
       SUM(P6M_SALES) AS P6M_SALES,
       SUM(P6M_QTY) AS P6M_QTY,
       AVG(P6M_QTY) AS P6M_AVG_QTY,
       SUM(P12M_SALES) AS P12M_SALES,
       SUM(P12M_QTY) AS P12M_QTY,
       AVG(P12M_QTY) AS P12M_AVG_QTY,
       SUM(F3M_SALES) AS F3M_SALES,
       SUM(F3M_QTY) AS F3M_QTY,
       AVG(F3M_QTY) AS F3M_AVG_QTY,
       MAX(LIST_PRICE) AS LIST_PRICE,
       CASE
         WHEN SUM(
           CASE
             WHEN LM_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS LM_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P3M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P3M_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P6M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P6M_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P12M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P12M_SALES_FLAG,
       CASE
         WHEN MDP_FLAG = 'Y' THEN 1
         ELSE 0
       END AS MDP_FLAG,
       MAX(size_of_price_lm) AS size_of_price_lm,
       MAX(size_of_price_p3m) AS size_of_price_p3m,
       MAX(size_of_price_p6m) AS size_of_price_p6m,
       MAX(size_of_price_p12m) AS size_of_price_p12m,
       SUM(SALES_VALUE_LIST_PRICE) AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
       MAX(size_of_price_lm_lp) AS size_of_price_lm_lp,
       MAX(size_of_price_p3m_lp) AS size_of_price_p3m_lp,
       MAX(size_of_price_p6m_lp) AS size_of_price_p6m_lp,
       MAX(size_of_price_p12m_lp) AS size_of_price_p12m_lp,
       --MAX(TARGET_COMPLAINCE) OVER (PARTITION BY FISC_PER, GLOBAL_PRODUCT_BRAND, MDP_FLAG) AS TARGET_COMPLAINCE,
       TARGET_COMPLAINCE::numeric(38,6) AS target_complaince,
       SYSDATE() AS CRT_DTTM
    FROM v_edw_vn_rpt_retail_excellence FLAGS

    GROUP BY FLAGS.FISC_YR,
         FLAGS.FISC_PER,
         FLAGS."cluster",
         FLAGS.MARKET,
         MD5(nvl (SELL_OUT_CHANNEL,'soc') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (REGION,'reg') ||
		 nvl (ZONE_NAME,'zn') ||nvl (CITY,'cty') ||nvl (PROD_HIER_L1,'ph1') ||nvl (PROD_HIER_L2,'ph2') || 
		 nvl (PROD_HIER_L3,'ph3') ||nvl (PROD_HIER_L4,'ph4') || nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||
		 nvl (GLOBAL_PRODUCT_BRAND,'gpb') || nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') || 
		 nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') || nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')),
         FLAGS.DATA_SRC,
         FLAGS.DISTRIBUTOR_CODE,
         FLAGS.DISTRIBUTOR_NAME,
         FLAGS.SELL_OUT_CHANNEL,
         FLAGS.REGION,
         FLAGS.ZONE_NAME,
         FLAGS.CITY,
         FLAGS.RETAIL_ENVIRONMENT,
         FLAGS.PROD_HIER_L1,
         FLAGS.PROD_HIER_L2,
         FLAGS.PROD_HIER_L3,
         FLAGS.PROD_HIER_L4,
         FLAGS.GLOBAL_PRODUCT_FRANCHISE,
         FLAGS.GLOBAL_PRODUCT_BRAND,
         FLAGS.GLOBAL_PRODUCT_SUB_BRAND,
         FLAGS.GLOBAL_PRODUCT_SEGMENT,
         FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,
         FLAGS.GLOBAL_PRODUCT_CATEGORY,
         FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,
         (CASE WHEN MDP_FLAG = 'Y' THEN 1 ELSE 0 END),
         TARGET_COMPLAINCE,
         STORE_CODE,
         PRODUCT_CODE
);