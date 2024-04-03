USE SCHEMA META_RAW;

update parameters set parameter_value='otc_sellout/transaction' where parameter_name='folder_path' and parameter_group_id=225;
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (177,15,'VN','itx-arm-conapdna-vietnam-prod-kdp','raw-data-lake/MT/MT_source/Sellin/Target/','Target_Sell_In','vnm','MT/transaction/Sellin/Target',TRUE,'Y');
