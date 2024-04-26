delete from meta_raw.s3_to_adls where id in(191,179,180,200,201,229);
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (191,24,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account','AcctGrp','pac','ap_perenso/master/account/AcctGrp',TRUE,'N');

