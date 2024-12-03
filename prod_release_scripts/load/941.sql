update prod_dna_load.meta_raw.process
set SNOWFLAKE_STAGE='HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS'
WHERE parameter_group_id in ('3046');
