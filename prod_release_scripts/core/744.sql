-- INSERT NON DUPLICATE DEC 11 RECORDS
INSERT INTO prod_dna_core.pcfedw_integration.edw_sls_evolution
(select distinct * from pcfedw_integration.edw_sls_evolution_remove_20241211_duplicates where snapshot_date= '2024-12-11');
