create or replace  table PROD_DNA_CORE.dbt_cloud_pr_5458_1598.hcpedw_integration__edw_hcp360_dwnld_kpi_rpt
         as
    (select * from PROD_DNA_CORE.HCPEDW_INTEGRATION.vw_edw_hcp360_hcpmaster_dim_golden_record);
 