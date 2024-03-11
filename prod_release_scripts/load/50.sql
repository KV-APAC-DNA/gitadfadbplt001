update META_RAW.PARAMETERS 
set PARAMETER_VALUE = 'last' 
where PARAMETER_ID in (466,447);

ALTER TABLE meta_raw.s3_to_adls
ADD COLUMN delete_source_file VARCHAR(10) DEFAULT 'N';
