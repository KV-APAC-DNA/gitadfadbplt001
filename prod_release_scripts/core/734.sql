CREATE OR REPLACE TABLE prod_dna_core.pcfedw_integration.edw_sls_evolution_20241211 
clone prod_dna_core.pcfedw_integration.edw_sls_evolution;
delete from prod_dna_core.pcfedw_integration.edw_sls_evolution WHERE SNAPSHOT_DATE = '2024-12-11';
