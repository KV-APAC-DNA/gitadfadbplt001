Create or replace FILE FORMAT SNAPASPSDL_RAW.DEV_CORE_CSV_FILEFORMAT
  type = 'csv' 
  field_delimiter = '\001' 
  skip_header = 1 
  compression=gzip 
  NULL_IF=('\\N')
  empty_field_as_null = false
  ;
Create or replace FILE FORMAT UTILITY_RAW.DEV_CORE_CSV_FILEFORMAT
  type = 'csv' 
  field_delimiter = '\001' 
  skip_header = 1 
  compression=gzip 
  NULL_IF=('\\N')
  empty_field_as_null = false
  ;
  
create or replace stage SNAPASPSDL_RAW.DEV_CORE_STAGE_S3
 storage_integration = itx_arm_snowflake_external_stage_storage_integration 
 url = 's3://itx-arm-snowflake-external-stage/DEV_DNA_LOAD/' 
 file_format = SNAPASPSDL_RAW.DEV_CORE_CSV_FILEFORMAT;
 
create or replace stage UTILITY_RAW.DEV_CORE_STAGE_S3
 storage_integration = itx_arm_snowflake_external_stage_storage_integration 
 url = 's3://itx-arm-snowflake-external-stage/DEV_DNA_CORE/' 
 file_format = UTILITY_RAW.DEV_CORE_CSV_FILEFORMAT;