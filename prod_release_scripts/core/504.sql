delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_TW_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_TW_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE (

with edw_rpt_tw_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAEDW_INTEGRATION__EDW_RPT_TW_RE
)

Select FISC_YR, 
       CAST(FISC_PER AS numeric(18,0) ) AS FISC_PER,		--// INTEGER
       "cluster",
       MARKET,
       MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')||
		   nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
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
       TARGET_COMPLAINCE --MAX(TARGET_COMPLAINCE) OVER (PARTITION BY FISC_PER, GLOBAL_PRODUCT_BRAND,mdp_flag) AS TARGET_COMPLAINCE  
 FROM edw_rpt_tw_re FLAGS		
 GROUP BY FLAGS.FISC_YR,		--//  GROUP BY FLAGS.FISC_YR,
       FLAGS.FISC_PER,		--//        FLAGS.FISC_PER,
       FLAGS."cluster",		--//        FLAGS."CLUSTER",
       FLAGS.MARKET,		--//        FLAGS.MARKET,
       MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')||
           nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
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


-------------------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_TW_RPT_RETAIL_EXCELLENCE_SUMMARY;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_TW_RPT_RETAIL_EXCELLENCE_SUMMARY (

with tw_edw_rpt_retail_excellence_summary_base as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_TW_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE
),

itg_query_parameters as (
    select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_QUERY_PARAMETERS
)

SELECT FISC_YR,
       FISC_PER,
       "cluster",
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
       sum(TARGET_COMPLAINCE) as TARGET_COMPLAINCE,--MAX(TARGET_COMPLAINCE) OVER (PARTITION BY FISC_PER, GLOBAL_PRODUCT_BRAND,mdp_flag) AS TARGET_COMPLAINCE,
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
        SUM(size_of_price_p12m_lp) AS  size_of_price_p12m_lp ,
        current_timestamp()::date as CRT_DTTM,
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
 FROM tw_edw_rpt_retail_excellence_summary_base		
WHERE FISC_PER > TO_CHAR(ADD_MONTHS((SELECT TO_DATE(MAX(fisc_per)::varchar,'YYYYMM')
                                    FROM tw_edw_rpt_retail_excellence_summary_base),-15),'YYYYMM')		
AND   FISC_PER <= (SELECT MAX(fisc_per)
                   FROM tw_edw_rpt_retail_excellence_summary_base)		
--AND   UPPER(RETAIL_ENVIRONMENT) NOT IN (SELECT DISTINCT parameter_value
                                 --FROM itg_query_parameters		--//                                  FROM rg_itg.itg_query_parameters
                                 --WHERE parameter_name = 'EXCLUDE_RE_RETAIL_ENV'
                                 --AND   country_code = 'TW')
GROUP BY FISC_YR,
       FISC_PER,
       "cluster",
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
