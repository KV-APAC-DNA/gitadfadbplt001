update meta_raw.s3_to_adls
set S3_FILE='Dufry'
WHERE ID = 4;

update META_RAW.HISTORICAL_OBJ_METADATA 
set isactive = TRUE;
