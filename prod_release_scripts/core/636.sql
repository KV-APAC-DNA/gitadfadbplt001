delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_details;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_details (
with jp_edw_rpt_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.jpnedw_integration__edw_rpt_retail_excellence
),

itg_query_parameters as (
    select * from PROD_DNA_CORE.aspitg_integration.itg_query_parameters
)

SELECT FISC_YR,
       CAST(FISC_PER AS INTEGER) AS FISC_PER,
       "CLUSTER",
       MARKET,
       CHANNEL_NAME,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       STORE_TYPE,
       PRIORITIZATION_SEGMENTATION,
       STORE_CATEGORY,
       STORE_CODE,
       STORE_NAME,
       STORE_GRADE,
       STORE_SIZE,
       REGION,
       ZONE_NAME,
       CITY,
       RTRLATITUDE,
       RTRLONGITUDE,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       RETAIL_ENVIRONMENT,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       CUSTOMER_NAME,
       CUSTOMER_CODE,
       PRODUCT_CODE,
       PRODUCT_NAME,
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       PROD_HIER_L5,
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
       MAPPED_SKU_CD,
       SAP_PROD_SGMT_CD,
       SAP_PROD_SGMT_DESC,
       SAP_BASE_PROD_DESC,
       SAP_MEGA_BRND_DESC,
       SAP_BRND_DESC,
       SAP_VRNT_DESC,
       SAP_PUT_UP_DESC,
       SAP_GRP_FRNCHSE_CD,
       SAP_GRP_FRNCHSE_DESC,
       SAP_FRNCHSE_CD,
       SAP_FRNCHSE_DESC,
       SAP_PROD_FRNCHSE_CD,
       SAP_PROD_FRNCHSE_DESC,
       SAP_PROD_MJR_CD,
       SAP_PROD_MJR_DESC,
       SAP_PROD_MNR_CD,
       SAP_PROD_MNR_DESC,
       SAP_PROD_HIER_CD,
       SAP_PROD_HIER_DESC,
       PKA_FRANCHISE_DESC,
       PKA_BRAND_DESC,
       PKA_SUB_BRAND_DESC,
       PKA_VARIANT_DESC,
       PKA_SUB_VARIANT_DESC,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       EAN,
       SKU_CODE,
       SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       SALES_VALUE,
       SALES_QTY,
       AVG_SALES_QTY,
       LM_SALES,
       LM_SALES_QTY,
       LM_AVG_SALES_QTY,
       P3M_SALES,
       P3M_QTY,
       P3M_AVG_QTY,
       P6M_SALES,
       P6M_QTY,
       P6M_AVG_QTY,
       P12M_SALES,
       P12M_QTY,
       P12M_AVG_QTY,
       F3M_SALES,
       F3M_QTY,
       F3M_AVG_QTY,
       LM_SALES_FLAG,
       P3M_SALES_FLAG,
       P6M_SALES_FLAG,
       P12M_SALES_FLAG,
       MDP_FLAG,
       TARGET_COMPLAINCE,
       LIST_PRICE,
       SIZE_OF_PRICE_LM,
       SIZE_OF_PRICE_P3M,
       SIZE_OF_PRICE_P6M,
       SIZE_OF_PRICE_P12M,
       LM_SALES_FLAG_COUNT,
       P3M_SALES_FLAG_COUNT,
       P6M_SALES_FLAG_COUNT,
       P12M_SALES_FLAG_COUNT,
       MDP_FLAG_COUNT,
       DATA_SRC,
       SALES_VALUE_LIST_PRICE,
       LM_SALES_LP,
       P3M_SALES_LP,
       P6M_SALES_LP,
       P12M_SALES_LP,
       SIZE_OF_PRICE_LM_LP,
       SIZE_OF_PRICE_P3M_LP,
       SIZE_OF_PRICE_P6M_LP,
       SIZE_OF_PRICE_P12M_LP,
       SOLDTO_CODE,
       current_timestamp()::date AS CRT_DTTM
FROM jp_edw_rpt_retail_excellence
WHERE market = 'Japan' and upper(RETAIL_ENVIRONMENT) not in (select distinct parameter_value from itg_query_parameters where parameter_name='EXCLUDE_RE_RETAIL_ENV' and country_code='JP')
AND   FISC_PER >= (SELECT REPLACE(SUBSTRING(add_months (TO_DATE(MAX(fisc_per)::varchar,'YYYYMM'),-1),1,7),'-','')::INTEGER 
                   FROM jp_edw_rpt_retail_excellence)
AND   FISC_PER <= (SELECT MAX(fisc_per) FROM jp_edw_rpt_retail_excellence)
);

------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_summary_base;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_summary_base (

with jp_edw_rpt_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.jpnedw_integration__edw_rpt_retail_excellence
)

Select FISC_YR,
       CAST(FISC_PER AS numeric(18,0) ) AS FISC_PER,		--// INTEGER
       CLUSTER,
       MARKET,
              MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')
            ||nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
           nvl(PROD_HIER_L3,'ph3')||nvl(PROD_HIER_L4,'ph4')||nvl(PROD_HIER_L5,'ph5')||
           nvl(GLOBAL_PRODUCT_FRANCHISE,'gpf')||nvl(GLOBAL_PRODUCT_BRAND,'gpb')||
           nvl(GLOBAL_PRODUCT_SUB_BRAND,'gpsb')||nvl(GLOBAL_PRODUCT_SEGMENT,'gps')||
           nvl(GLOBAL_PRODUCT_SUBSEGMENT,'gpss')||nvl(GLOBAL_PRODUCT_CATEGORY,'gpc')||
           nvl(GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')) AS FLAG_AGG_DIM_KEY,
       data_src,
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
       PROD_HIER_L5,
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
       store_code,
       product_code,
       SUM(SALES_VALUE)AS SALES_VALUE,
       SUM(SALES_QTY)AS SALES_QTY,
       TRUNC(AVG(SALES_QTY)) AS AVG_SALES_QTY,		--// AVG
       SUM(LM_SALES)AS LM_SALES,
       SUM(LM_SALES_QTY)AS LM_SALES_QTY,
       TRUNC(AVG(LM_SALES_QTY))AS LM_AVG_SALES_QTY,		--// AVG
       SUM(P3M_SALES)AS P3M_SALES,
       SUM(P3M_QTY)AS P3M_QTY,
       TRUNC(AVG(P3M_QTY))AS P3M_AVG_QTY ,		--// AVG
       SUM(P6M_SALES)AS P6M_SALES,
       SUM(P6M_QTY)AS P6M_QTY,
       TRUNC(AVG(P6M_QTY))AS P6M_AVG_QTY,		--// AVG
       SUM(P12M_SALES)AS P12M_SALES,
       SUM(P12M_QTY)AS P12M_QTY,
       TRUNC(AVG(P12M_QTY))AS P12M_AVG_QTY,		--// AVG
       SUM(F3M_SALES)AS F3M_SALES,
       SUM(F3M_QTY)AS F3M_QTY,
       TRUNC(AVG(F3M_QTY))AS F3M_AVG_QTY,		--// AVG
	    MAX(LIST_PRICE) AS LIST_PRICE,
       case when SUM (case when LM_SALES_FLAG = 'Y' then 1 else 0 end ) >0 then 1 else 0 end as LM_SALES_FLAG,
       case when SUM(case when P3M_SALES_FLAG = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  P3M_SALES_FLAG,
       case when SUM(case when P6M_SALES_FLAG = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  P6M_SALES_FLAG,
       case when SUM(case when P12M_SALES_FLAG= 'Y' then 1 else 0 end ) >0 then 1 else 0 end as P12M_SALES_FLAG,
       case when MDP_FLAG ='Y' then 1 else 0 end as MDP_FLAG,
       max(size_of_price_lm)  AS   size_of_price_lm,
       max(size_of_price_p3m) As   size_of_price_p3m,
       max(size_of_price_p6m) AS   size_of_price_p6m,
       max(size_of_price_p12m) AS  size_of_price_p12m,
        sum(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
       max(size_of_price_lm_lp)  AS   size_of_price_lm_lp,
       max(size_of_price_p3m_lp) As   size_of_price_p3m_lp,
       max(size_of_price_p6m_lp) AS   size_of_price_p6m_lp,
       max(size_of_price_p12m_lp) AS  size_of_price_p12m_lp,
       TARGET_COMPLAINCE
 FROM jp_edw_rpt_retail_excellence FLAGS		--//  FROM JP_EDW.EDW_RPT_RETAIL_EXCELLENCE FLAGS
 GROUP BY FLAGS.FISC_YR,		--//  GROUP BY FLAGS.FISC_YR,
       FLAGS.FISC_PER,		--//        FLAGS.FISC_PER,
       FLAGS.CLUSTER,		--//        FLAGS."CLUSTER",
       FLAGS.MARKET,		--//        FLAGS.MARKET,
       MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')
            ||nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
           nvl(PROD_HIER_L3,'ph3')||nvl(PROD_HIER_L4,'ph4')||nvl(PROD_HIER_L5,'ph5')||
           nvl(GLOBAL_PRODUCT_FRANCHISE,'gpf')||nvl(GLOBAL_PRODUCT_BRAND,'gpb')||
           nvl(GLOBAL_PRODUCT_SUB_BRAND,'gpsb')||nvl(GLOBAL_PRODUCT_SEGMENT,'gps')||
           nvl(GLOBAL_PRODUCT_SUBSEGMENT,'gpss')||nvl(GLOBAL_PRODUCT_CATEGORY,'gpc')||
           nvl(GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')),
       FLAGS.DATA_SRC,		--//        FLAGS.data_src,
       FLAGS.DISTRIBUTOR_CODE,		--//        FLAGS.DISTRIBUTOR_CODE,
       FLAGS.DISTRIBUTOR_NAME,		--//        FLAGS.DISTRIBUTOR_NAME,
       FLAGS.SELL_OUT_CHANNEL,		--//        FLAGS.SELL_OUT_CHANNEL,
       FLAGS.REGION,		--//        FLAGS.REGION,
       FLAGS.ZONE_NAME,		--//        FLAGS.ZONE_NAME,
       FLAGS.CITY,		--//        FLAGS.CITY,
       FLAGS.RETAIL_ENVIRONMENT,		--//        FLAGS.RETAIL_ENVIRONMENT,
       FLAGS.PROD_HIER_L1,		--//        FLAGS.PROD_HIER_L1,
       FLAGS.PROD_HIER_L2,		--//        FLAGS.PROD_HIER_L2,
       FLAGS.PROD_HIER_L3,		--//        FLAGS.PROD_HIER_L3,
       FLAGS.PROD_HIER_L4,		--//        FLAGS.PROD_HIER_L4,
       FLAGS.PROD_HIER_L5,		--//        FLAGS.PROD_HIER_L5,
       FLAGS.GLOBAL_PRODUCT_FRANCHISE,		--//        FLAGS.GLOBAL_PRODUCT_FRANCHISE,
       FLAGS.GLOBAL_PRODUCT_BRAND,		--//        FLAGS.GLOBAL_PRODUCT_BRAND,
       FLAGS.GLOBAL_PRODUCT_SUB_BRAND,		--//        FLAGS.GLOBAL_PRODUCT_SUB_BRAND,
       FLAGS.GLOBAL_PRODUCT_SEGMENT,		--//        FLAGS.GLOBAL_PRODUCT_SEGMENT,
       FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,		--//        FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,
       FLAGS.GLOBAL_PRODUCT_CATEGORY,		--//        FLAGS.GLOBAL_PRODUCT_CATEGORY,
       FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,		--//        FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,
       (case when MDP_FLAG ='Y' then 1 else 0 end) ,
       TARGET_COMPLAINCE ,
       store_code,
       product_code

);

--------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_summary;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_summary (
with jp_edw_rpt_retail_excellence_summary_base as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1541.aspedw_integration__edw_jp_rpt_retail_excellence_summary_base
),

itg_query_parameters as (
    select * from PROD_DNA_CORE.aspitg_integration.itg_query_parameters
)

SELECT FISC_YR,
       FISC_PER,
       "CLUSTER",
       MARKET,
       data_src,
       FLAG_AGG_DIM_KEY,
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
       PROD_HIER_L5,
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       CASE WHEN LM_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS LM_SALES_FLAG,
       CASE WHEN P3M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END   AS P3M_SALES_FLAG,
       CASE WHEN P6M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS P6M_SALES_FLAG,
       CASE WHEN P12M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS P12M_SALES_FLAG,
       CASE WHEN MDP_FLAG = 1 THEN 'Y' ELSE 'N' END  AS MDP_FLAG,
       SUM(TARGET_COMPLAINCE) AS TARGET_COMPLAINCE,
	   SUM(SALES_VALUE)AS SALES_VALUE,
       SUM(SALES_QTY)AS SALES_QTY,
       TRUNC(AVG(SALES_QTY)) AS AVG_SALES_QTY,		--// AVG
       SUM(LM_SALES)AS LM_SALES,
       SUM(LM_SALES_QTY)AS LM_SALES_QTY,
       TRUNC(AVG(LM_SALES_QTY))AS LM_AVG_SALES_QTY,		--// AVG
       SUM(P3M_SALES)AS P3M_SALES,
       SUM(P3M_QTY)AS P3M_QTY,
       TRUNC(AVG(P3M_QTY))AS P3M_AVG_QTY ,		--// AVG
       SUM(P6M_SALES)AS P6M_SALES,
       SUM(P6M_QTY)AS P6M_QTY,
       TRUNC(AVG(P6M_QTY))AS P6M_AVG_QTY,		--// AVG
       SUM(P12M_SALES)AS P12M_SALES,
       SUM(P12M_QTY)AS P12M_QTY,
       TRUNC(AVG(P12M_QTY))AS P12M_AVG_QTY,		--// AVG
       SUM(F3M_SALES)AS F3M_SALES,
       SUM(F3M_QTY)AS F3M_QTY,
       TRUNC(AVG(F3M_QTY))AS F3M_AVG_QTY,		--// AVG
        SUM(size_of_price_lm)  AS size_of_price_lm,
        SUM(size_of_price_p3m) As size_of_price_p3m,
        SUM(size_of_price_p6m) AS size_of_price_p6m,
        SUM(size_of_price_p12m) AS  size_of_price_p12m ,
       count(LM_SALES_FLAG) LM_SALES_FLAG_COUNT,
        count(P3M_SALES_FLAG)P3M_SALES_FLAG_COUNT,
        count(P6M_SALES_FLAG)P6M_SALES_FLAG_COUNT,
        count(P12M_SALES_FLAG) P12M_SALES_FLAG_COUNT ,
        count(MDP_FLAG) MDP_FLAG_COUNT,
        MAX(LIST_PRICE) AS LIST_PRICE,
        SUM(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
         SUM(size_of_price_lm_lp)  AS size_of_price_lm_lp,
        SUM(size_of_price_p3m_lp) As size_of_price_p3m_lp,
        SUM(size_of_price_p6m_lp) AS size_of_price_p6m_lp,
        SUM(size_of_price_p12m_lp) AS  size_of_price_p12m_lp,
        current_timestamp()::date  as crt_dttm,
        null as cm_actual_stores,
        null as cm_universe_stores,
        null as cm_numeric_distribution,
        null as lm_actual_stores,
        null as lm_universe_stores,
        null as lm_numeric_distribution,
        null as l3m_actual_stores,
        null as l3m_universe_stores,
        null as l3m_numeric_distribution,
        null as l6m_actual_stores,
        null as l6m_universe_stores,
        null as l6m_numeric_distribution,
        null as l12m_actual_stores,
        null as l12m_universe_stores,
        null as l12m_numeric_distribution
 FROM jp_edw_rpt_retail_excellence_summary_base
 WHERE
 ---FISC_PER > TO_CHAR(ADD_MONTHS(SYSDATE,-17),'YYYYMM') and
 --FISC_PER <= (select replace(substring(add_months(to_date(max(fisc_per),'YYYYMM'),-2),1,7),'-','')::integer
 upper(RETAIL_ENVIRONMENT) not in (select distinct parameter_value from itg_query_parameters where parameter_name='EXCLUDE_RE_RETAIL_ENV' and country_code='JP')
 and FISC_PER >
---TO_CHAR(ADD_MONTHS((SELECT to_date(MAX(fisc_per),'YYYYMM') FROM jp_edw_rpt_retail_excellence_summary_base),-15),'YYYYMM')
TO_CHAR(ADD_MONTHS((SELECT to_date(MAX(fisc_per)::varchar,'YYYYMM') FROM  jp_edw_rpt_retail_excellence_summary_base),-15),'YYYYMM')
AND FISC_PER <= (select max(fisc_per) FROM jp_edw_rpt_retail_excellence_summary_base)
GROUP BY FISC_YR,
       FISC_PER,
       "CLUSTER",
       MARKET,
       FLAG_AGG_DIM_KEY,
       data_src,
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
       PROD_HIER_L5,
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       LM_SALES_FLAG,
      P3M_SALES_FLAG,
      P6M_SALES_FLAG,
      P12M_SALES_FLAG,
      MDP_FLAG
);
