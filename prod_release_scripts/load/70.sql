Create or replace FILE FORMAT META_RAW.PROD_CORE_CSV_FILEFORMAT
  type = 'csv' 
  field_delimiter = '\001' 
  skip_header = 1 
  compression=gzip 
  NULL_IF=('\\N')
  empty_field_as_null = false
  ;

create or replace stage META_RAW.DEV_CORE_STAGE_S3
 storage_integration = itx_arm_snowflake_external_stage_storage_integration 
 url = 's3://itx-arm-snowflake-external-stage/PROD_DNA_LOAD/' 
 file_format = META_RAW.PROD_CORE_CSV_FILEFORMAT;
 
update META_RAW.HISTORICAL_OBJ_METADATA
set STAGE_SCHEMA = 'META_RAW'
WHERE TARGET_DB = 'DEV_DNA_LOAD';
