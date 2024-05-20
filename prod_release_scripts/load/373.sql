delete from meta_raw.s3_to_adls
where group_id = 2;

use schema meta_raw;
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (8,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/AMAZON/','Amazon','sgp','scan360/amazon',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (9,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/NTUC/','NTUC','sgp','scan360/ntuc',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (10,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/SCOMMERCE/','Scommerce','sgp','scan360/scommerce',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (11,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/REDMART/','Redmart','sgp','scan360/redmart',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (12,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/MARKETPLACE/','Marketplace','sgp','scan360/marketplace',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (13,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/DFI/','DFI','sgp','scan360/dfi',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (14,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/WATSONS/','SG_SCAN_DATA_WATSONS','sgp','scan360/watsons',True,'Y');
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (15,2,'SG','itx-arm-conapdna-singapore-prod-kdp','Raw_Data_Lake/SCAN360/GUARDIAN/','Guardian','sgp','scan360/guardian',True,'Y');
