use database PROD_DNA_LOAD;
CREATE OR REPLACE TABLE meta_raw.s3_to_adls (
    id             INTEGER,
    group_id       INTEGER,
    country        STRING,
    s3_bucket      STRING,
    s3_path        STRING,
    s3_file        STRING,
    adls_container STRING,
    adls_path      STRING,
    Isactive       BOOLEAN
);

INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (1,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/','Shilla','asp','prd/transactional',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (2,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/','DFS','asp','prd/transactional',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (3,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/DFS/','Hainan_Vendor_Sales_Report_for_Asian','asp','prd/transactional/DFS',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (4,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/Dufry/','Dufry Hainan','asp','prd/transactional/Dufry',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (5,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/CNSC/','CNSC_DR.CILABO_SALES_REPORT','asp','prd/transactional/CNSC',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (6,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/Lagardere/','LSTR','asp','prd/transactional/Lagardere',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (7,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/','CDFG','asp','prd/transactional',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (8,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/amazon/','Amazon','sgp','prd/scan360/amazon',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (9,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/ntuc/','NTUC','sgp','prd/scan360/ntuc',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (10,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/scommerce/','Scommerce','sgp','prd/scan360/scommerce',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (11,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/redmart/','Redmart','sgp','prd/scan360/redmart',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (12,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/marketplace/','Marketplace','sgp','prd/scan360/marketplace',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (13,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/dfi/','DFI','sgp','prd/scan360/dfi',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (14,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/watsons/','SG_SCAN_DATA_WATSONS','sgp','prd/scan360/watsons',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (15,2,'SG','itx-arm-conapdna-singapore-prod','Certified_Data_Lake/scan360/guardian/','Guardian','sgp','prd/scan360/guardian',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (16,1,'RG','itx-arm-conapdna-aspac-prod','raw-data-lake/travel_retail/transaction_files/','SalesStock','asp','prd/transactional',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (17,3,'SG','itx-arm-conapdna-metadata-prod','ap_sg_metadata/master/sellout/','ciw_map','sgp','prd/master/sellout',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (18,3,'SG','itx-arm-conapdna-metadata-prod','ap_sg_metadata/master/sellout/','SG_Zuellig_Customer_Mapping','sgp','prd/master/sellout',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (19,3,'SG','itx-arm-conapdna-metadata-prod','ap_sg_metadata/master/sellout/','SG_Zuellig_Product_Mapping','sgp','prd/master/sellout',True);
INSERT INTO META_RAW.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (20,3,'SG','itx-arm-conapdna-metadata-prod','ap_sg_metadata/transaction/sellout/','SG_Zuellig_Sell_Out','sgp','prd/transaction/sellout',True);
