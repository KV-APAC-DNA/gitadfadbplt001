delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths (

with wks_vn_base_retail_excellence as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VN_BASE_RETAIL_EXCELLENCE
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_COREaspedw_integration.v_edw_vw_cal_Retail_excellence_dim
),
edw_vw_os_time_dim as (
    select * from PROD_DNA_COREsgpedw_integration.edw_vw_os_time_dim
)
    SELECT ALL_MONTHS.CNTRY_CD,
       ALL_MONTHS.SELLOUT_DIM_KEY,
       ALL_MONTHS.data_src,
       ALL_MONTHS.MONTH,
       BASE.SO_SLS_QTY,
       BASE.SO_SLS_VALUE,
       BASE.SO_AVG_QTY,
	   BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             SELLOUT_DIM_KEY,
             data_src,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   SELLOUT_DIM_KEY,
                   data_src
            FROM WKS_VN_BASE_RETAIL_EXCELLENCE) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM EDW_VW_OS_TIME_DIM
			WHERE MNTH_ID >= (SELECT last_27mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
                 AND  MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  LEFT JOIN WKS_VN_BASE_RETAIL_EXCELLENCE base
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD
        AND ALL_MONTHS.SELLOUT_DIM_KEY = BASE.SELLOUT_DIM_KEY
        AND ALL_MONTHS.data_src = BASE.data_src
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID
);

-------------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_lm;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_lm (

with wks_vn_regional_sellout_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths
)

    select SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       SO.DATA_SRC,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_LP
    FROM WKS_VN_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         SO.DATA_SRC,
         MONTH
);

---------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l3m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l3m (

with wks_vn_regional_sellout_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths
)

    SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       SO.DATA_SRC,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_AVG_SALES_QTY
    FROM WKS_VN_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         SO.DATA_SRC,
         MONTH
);

-----------------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l6m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l6m (

with wks_vn_regional_sellout_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths
)
    SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       SO.DATA_SRC,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
    FROM WKS_VN_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         SO.DATA_SRC,
         MONTH
);

------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l12m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_act_l12m (

with wks_vn_regional_sellout_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.vnmwks_integration__wks_vn_regional_sellout_allmonths
)

    SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       SO.DATA_SRC,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_AVG_SALES_QTY,		--// AVG
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_LP
    FROM WKS_VN_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         SO.DATA_SRC,
         MONTH
);
