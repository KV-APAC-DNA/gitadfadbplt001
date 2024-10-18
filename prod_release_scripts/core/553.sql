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

------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY (

with vn_edw_rpt_retail_excellence_summary_base as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE
)

    select
        fisc_yr,
        fisc_per :: numeric(18,0) as fisc_per,
        "cluster",
        market,
        data_src,
        flag_agg_dim_key,
        distributor_code,
        distributor_name,
        sell_out_channel,
        region,
        zone_name,
        city,
        retail_environment,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        global_product_franchise,
        global_product_brand,
        global_product_sub_brand,
        global_product_segment,
        global_product_subsegment,
        global_product_category,
        global_product_subcategory,
        case when lm_sales_flag = 1 then 'Y' else 'N' end as lm_sales_flag,
        case when p3m_sales_flag = 1 then 'Y' else 'N' end as p3m_sales_flag,
        case when p6m_sales_flag = 1 then 'Y' else 'N' end as p6m_sales_flag,
        case when p12m_sales_flag = 1 then 'Y' else 'N' end as p12m_sales_flag,
        case when mdp_flag = 1 then 'Y' else 'N' end as mdp_flag,
        --MAX(target_complaince) OVER (PARTITION BY fisc_per, global_product_brand, mdp_flag) AS target_complaince,
        sum(target_complaince) as target_complaince,
        SUM(sales_value) as sales_value,
        SUM(sales_qty) as sales_qty,
        AVG(sales_qty) as avg_sales_qty,		--// AVG
        SUM(lm_sales) as lm_sales,
        SUM(lm_sales_qty) as lm_sales_qty,
        AVG(lm_sales_qty) as lm_avg_sales_qty,		--// AVG
        SUM(p3m_sales) as p3m_sales,
        SUM(p3m_qty) as p3m_qty,
        AVG(p3m_qty) as p3m_avg_qty,		--// AVG
        SUM(p6m_sales) as p6m_sales,
        SUM(p6m_qty) as p6m_qty,
        AVG(p6m_qty) as p6m_avg_qty,		--// AVG
        SUM(p12m_sales) as p12m_sales,
        SUM(p12m_qty) as p12m_qty,
        AVG(p12m_qty) as p12m_avg_qty,		--// AVG
        SUM(f3m_sales) as f3m_sales,
        SUM(f3m_qty) as f3m_qty,
        AVG(f3m_qty) as f3m_avg_qty,		--// AVG
        SUM(size_of_price_lm) as size_of_price_lm,
        SUM(size_of_price_p3m) as size_of_price_p3m,
        SUM(size_of_price_p6m) as size_of_price_p6m,
        SUM(size_of_price_p12m) as size_of_price_p12m,
        COUNT(lm_sales_flag) as lm_sales_flag_count,
        COUNT(p3m_sales_flag) as p3m_sales_flag_count,
        COUNT(p6m_sales_flag) as p6m_sales_flag_count,
        COUNT(p12m_sales_flag) as p12m_sales_flag_count,
        COUNT(mdp_flag) as mdp_flag_count,
        MAX(list_price) as list_price,
        SUM(sales_value_list_price) as sales_value_list_price,
        SUM(lm_sales_lp) as lm_sales_lp,
        SUM(p3m_sales_lp) as p3m_sales_lp,
        SUM(p6m_sales_lp) as p6m_sales_lp,
        SUM(p12m_sales_lp) as p12m_sales_lp,
        SUM(size_of_price_lm_lp) as size_of_price_lm_lp,
        SUM(size_of_price_p3m_lp) as size_of_price_p3m_lp,
        SUM(size_of_price_p6m_lp) as size_of_price_p6m_lp,
        SUM(size_of_price_p12m_lp) as size_of_price_p12m_lp,
        SYSDATE() as crt_dttm,
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
    from vn_edw_rpt_retail_excellence_summary_base
    where
        fisc_per
        > TO_CHAR(ADD_MONTHS((
            select TO_DATE(TO_CHAR(MAX(fisc_per)), 'YYYYMM')
            from vn_edw_rpt_retail_excellence_summary_base
        ), -15), 'YYYYMM')	
        and fisc_per <= (
            select MAX(fisc_per)
            from vn_edw_rpt_retail_excellence_summary_base
        )
    group by
        fisc_yr,
        fisc_per,
        "cluster",
        market,
        flag_agg_dim_key,
        data_src,
        distributor_code,
        distributor_name,
        sell_out_channel,
        region,
        zone_name,
        city,
        retail_environment,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        global_product_franchise,
        global_product_brand,
        global_product_sub_brand,
        global_product_segment,
        global_product_subsegment,
        global_product_category,
        global_product_subcategory,
        lm_sales_flag,
        p3m_sales_flag,
        p6m_sales_flag,
        p12m_sales_flag,
        mdp_flag
);

------------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_DETAILS;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_DETAILS (

with vn_edw_rpt_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE
)

SELECT FISC_YR,
       FISC_PER :: numeric(18,0) as FISC_PER,
       "cluster",
       MARKET,
       CHANNEL_NAME,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       STORE_TYPE,
       'Not Available' AS PRIORITIZATION_SEGMENTATION,
       'Not Available' AS STORE_CATEGORY,
       STORE_CODE,
       STORE_NAME,
       STORE_GRADE,
       'Not Available' AS STORE_SIZE,
       REGION,
       --POST_CODE,
       ZONE_NAME,
       CITY,
       'Not Available' AS RTRLATITUDE,
       'Not Available' AS RTRLONGITUDE,
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
	   SOLD_TO_CODE,
       SYSDATE() as crt_dttm	
    FROM vn_edw_rpt_retail_excellence		
    WHERE FISC_PER >= (SELECT REPLACE(SUBSTRING(add_months (TO_DATE(TO_CHAR(MAX(FISC_PER)),'YYYYMM'),-1),1,7),'-','')::INTEGER
                    FROM vn_edw_rpt_retail_excellence)		
    AND   FISC_PER <= (SELECT MAX(FISC_PER) FROM vn_edw_rpt_retail_excellence)
);
