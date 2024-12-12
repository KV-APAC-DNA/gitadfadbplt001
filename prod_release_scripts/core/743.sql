-- CREATE TABLE STRUCTURE SIMILAR TO SLS_EVOL
CREATE OR REPLACE TABLE prod_dna_core.pcfedw_integration.edw_sls_evolution_remove_20241211_duplicates LIKE prod_dna_core.pcfedw_integration.edw_sls_evolution;

-- INSERT NON DUPLICATE DEC 11 RECORDS
INSERT INTO prod_dna_core.pcfedw_integration.edw_sls_evolution_remove_20241211_duplicates 
(select distinct * from pcfedw_integration.edw_sls_evolution where snapshot_date= '2024-12-11');

-- CREATE SLS EVOL BACKUP
CREATE OR REPLACE TABLE prod_dna_core.pcfedw_integration.edw_sls_evolution_20241212
clone prod_dna_core.pcfedw_integration.edw_sls_evolution;

-- DELETE DEC 11 DATA
DELETE FROM prod_dna_core.pcfedw_integration.edw_sls_evolution WHERE snapshot_date= '2024-12-11';

-- INSERT NON DUPLICATE DEC 11 RECORDS
INSERT INTO prod_dna_core.pcfedw_integration.edw_sls_evolution_remove_20241211_duplicates 
(select * from pcfedw_integration.edw_sls_evolution_remove_20241211_duplicates where snapshot_date= '2024-12-11');
