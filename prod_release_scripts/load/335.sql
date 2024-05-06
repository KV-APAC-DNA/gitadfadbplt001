update meta_raw.parameters
set PARAMETER_VALUE = 'All J_J Items WE'
where PARAMETER_GROUP_ID = 323 and PARAMETER_NAME = 'val_file_name';

update meta_raw.process
set SNOWFLAKE_STAGE = 'PHLSDL_RAW.PROD_LOAD_STAGE_ADLS'
WHERE SNOWFLAKE_STAGE = 'PHLSDL_RAW.DEV_LOAD_STAGE_ADLS';
