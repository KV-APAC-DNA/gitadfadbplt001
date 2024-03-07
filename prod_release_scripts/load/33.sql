Update META_RAW.S3_TO_ADLS
set s3_file = 'Hainan'
where id = 3;

Update META_RAW.S3_TO_ADLS
set s3_file = 'CNSC'
where id = 5;

update META_RAW.parameters set parameter_value='asp' where parameter_id=2228;

UPDATE META_RAW.PROCESS
SET IS_ACTIVE = FALSE
WHERE PROCESS_ID = 24;
