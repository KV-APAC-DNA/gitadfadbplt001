delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_SALES_REPORTING where SNAP_SHOT_DT>='2024-09-01';
delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_SLS_EVOLUTION where SNAPSHOT_DATE='2024-09-10';
delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_INVOICE_FACT_SNAPSHOT where SNAPSHOT_DATE='2024-09-10';
delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_GTS_VISIBILITY where SNAPSHOT_DATE='2024-09-10';
delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_SLS_EVOLUTION_HISTORY where SNAPSHOT_DATE='2024-09-10';