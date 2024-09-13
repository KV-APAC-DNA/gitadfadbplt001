DROP TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_invoice_fact_snapshot;

CREATE TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_invoice_fact_snapshot CLONE PROD_DNA_CORE.pcfedw_integration.edw_invoice_fact_snapshot;

ALTER TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_invoice_fact_snapshot 
ADD COLUMN gros_trd_sls2 number(38,7);
ALTER TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_invoice_fact_snapshot 
ADD COLUMN CNFRM_QTY_PC number(38,7);
ALTER TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_invoice_fact_snapshot 
ADD COLUMN ord_qty_pc number(38,7);

-------------------------
drop table PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_sls_evolution;

CREATE TABLE PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_sls_evolution CLONE
PROD_DNA_CORE.pcfedw_integratioN.edw_sls_evolution;

DELETE FROM PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_sls_evolution 
WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM PROD_DNA_CORE.dbt_cloud_pr_5458_593.pcfedw_integration__edw_sls_evolution);
