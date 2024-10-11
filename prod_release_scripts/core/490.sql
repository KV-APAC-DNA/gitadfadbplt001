delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_RETAIL_EXCELLENCE_SUMMARY where market in ('Hong Kong','Vietnam');

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_RETAIL_EXCELLENCE_SUMMARY (
with edw_hk_rpt_retail_excellence_summary as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_HK_RPT_RETAIL_EXCELLENCE_SUMMARY
),
edw_vn_rpt_retail_excellence_summary as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_VN_RPT_RETAIL_EXCELLENCE_SUMMARY
)

SELECT * FROM edw_hk_rpt_retail_excellence_summary UNION
SELECT * FROM edw_vn_rpt_retail_excellence_summary
);
