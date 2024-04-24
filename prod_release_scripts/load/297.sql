delete from meta_raw.s3_to_adls
where id in(214,240);

INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (214,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/Survey/','SurveyResult','pac','ap_perenso/transaction/survey',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (240,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/Survey/','TodoOption','pac','ap_perenso/transaction/survey',TRUE,'Y');
