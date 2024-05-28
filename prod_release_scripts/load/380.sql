use schema meta_raw;

INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (226,15,'VN','itx-arm-conapdna-vietnam-prod-kdp','raw-data-lake/MT/MT_source/Sellin/Transaction/','DKSH_Sell_In','vnm','MT/transaction/Sellin/Transaction/DKSH',TRUE,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (227,15,'VN','itx-arm-conapdna-vietnam-prod-kdp','raw-data-lake/MT/MT_source/Sellin/Transaction/','COOP_Sell_In','vnm','MT/transaction/Sellin/Transaction/Coop_Sell_in',TRUE,'Y');
