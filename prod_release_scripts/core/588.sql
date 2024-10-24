
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths (   
with edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.V_EDW_VW_CAL_RETAIL_EXCELLENCE_DIM
),
wks_tw_base_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE
),
edw_vw_os_time_dim as (
    select * from PROD_DNA_CORE.sgpedw_integration.edw_vw_os_time_dim
)

SELECT all_months.cntry_cd,
       all_months.sellout_dim_key,
       all_months.data_src,
       all_months.month,
       base.SO_SLS_QTY::NUMERIC(38,6) AS SO_SLS_QTY,
       base.SO_SLS_VALUE::NUMERIC(38,6) AS SO_SLS_VALUE,
       base.SO_AVG_QTY::NUMERIC(38,6) AS SO_AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             data_src,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key,
                   data_src
            FROM WKS_TW_BASE_RE) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM EDW_VW_OS_TIME_DIM		--//             FROM os_edw.edw_vw_os_time_dim
			WHERE MNTH_ID >= (SELECT last_28mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
AND   MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  LEFT JOIN WKS_TW_BASE_RE base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.MONTH = base.MNTH_ID
);

---------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_lm;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_lm (
with wks_tw_re_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths
)

SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       SO.data_src,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES,
       CAST(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS DECIMAL(10,2)) AS LM_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) LM_SALES_LP
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( 'b526ba0ea750a03d6ea7818ca70a60f2')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         SO.data_src,
         MONTH
);

---------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l3m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l3m (
with wks_tw_re_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths
) 

SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       SO.data_src,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_AVG_SALES_QTY
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( '105d8348b3394ef5bd2ff7986c950b91')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         SO.data_src,
         MONTH
);
		 
------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l6m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l6m (
with wks_tw_re_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths
)			 
SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       SO.data_src,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( '105d8348b3394ef5bd2ff7986c950b91')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         SO.data_src,
         MONTH
);

-----------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l12m;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_act_l12m (
with wks_tw_re_allmonths as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ntawks_integration__wks_tw_re_allmonths
)

SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       SO.data_src,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_LP
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( '105d8348b3394ef5bd2ff7986c950b91')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         SO.data_src,
         MONTH
);
