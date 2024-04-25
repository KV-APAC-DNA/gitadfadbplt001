delete from meta_raw.s3_to_adls
where id in (135,182,191,196,207);


INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (135,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/overandabove/','OverandAbove.','pac','ap_perenso/transaction/overandabove/OverandAbove',True,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (182,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master/product','ProdGrp.','pac','ap_perenso/master/product/ProdGrp',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (191,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account','AcctGrp.','pac','ap_perenso/master/account/AcctGrp',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (196,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account','Account.','pac','ap_perenso/master/account/Account',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (207,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/ranging/','Ranging.','pac','ap_perenso/transaction/ranging/Ranging',TRUE,'Y');
