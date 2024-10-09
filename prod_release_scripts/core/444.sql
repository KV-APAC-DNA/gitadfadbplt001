ALTER TABLE prod_dna_core.aspedw_integration.edw_ecc_standard_cost ADD COLUMN TIMESTAMPS_1 timestamp_ntz(9);
UPDATE prod_dna_core.aspedw_integration.edw_ecc_standard_cost SET TIMESTAMPS_1=try_to_timestamp(left(timestamps::varchar,8), 'yyyymmdd');--587173
ALTER TABLE prod_dna_core.aspedw_integration.edw_ecc_standard_cost DROP COLUMN TIMESTAMPS;
ALTER TABLE prod_dna_core.aspedw_integration.edw_ecc_standard_cost RENAME COLUMN TIMESTAMPS_1 TO TIMESTAMPS;
