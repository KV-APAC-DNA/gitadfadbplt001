delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_RETAIL_EXCELLENCE_DETAILS where market in ('Hong Kong','Vietnam');

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_RETAIL_EXCELLENCE_DETAILS (

with 
edw_hk_rpt_retail_excellence_details as
(
  select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_HK_RPT_RETAIL_EXCELLENCE_DETAILS
),
edw_vn_rpt_retail_excellence_details as
(
  select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_DETAILS
)

    SELECT * FROM edw_hk_rpt_retail_excellence_details UNION ALL
    SELECT * FROM edw_vn_rpt_retail_excellence_details
);
