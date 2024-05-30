delete from meta_raw.process where process_id = 573;

delete from meta_raw.PARAMETERS where PARAMETER_GROUP_ID = 573;
update meta_raw.process
set sequence_id = 1
where process_id = 371;

CREATE or replace FILE FORMAT PROD_DNA_LOAD.IDNSDL_RAW.CSV_FORMAT_PIPE
FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
FIELD_DELIMITER = '|';


INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (181,20,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/grocery/','Monthly Sales Report, Supplier filter only_J_J','pac','ap_perenso/transaction/metcash/grocery',True,'Y');
