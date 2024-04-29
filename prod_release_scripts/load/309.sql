delete FROM meta_raw.S3_TO_ADLS whERE id in (134,135,191);


INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (134,24,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/overandabove/','OverandAboveState','pac','ap_perenso/transaction/overandabove',True,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (135,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/overandabove/','OverandAbove','pac','ap_perenso/transaction/overandabove/OverandAbove',True,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (191,24,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account/AcctGrp','AcctGrp','pac','ap_perenso/master/account/',TRUE,'Y');
