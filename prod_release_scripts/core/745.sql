DELETE FROM prod_dna_core.pcfedw_integration.edw_sls_evolution where snapshot_date= '2024-12-11' AND futr_gts>0;

INSERT INTO prod_dna_core.pcfedw_integration.edw_sls_evolution 
(select * from pcfedw_integration.edw_sls_evolution_20241211 where snapshot_date= '2024-12-11' AND futr_gts>0);
