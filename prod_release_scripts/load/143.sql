USE SCHEMA META_RAW;

DELETE FROM s3_to_adls WHERE ID IN (117,118,119,120,121,122,123,124);

INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (117,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','A1_LAO','tha','LCM/master/Laos_Customer_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (118,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','I_LAO','tha','LCM/transaction/Laos_Inventory_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (119,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','S_LAO','tha','LCM/transaction/Laos_Sellout_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (120,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','SO_LAO','tha','LCM/transaction/Laos_Sales_Order_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (121,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','RouteHdr_LAO_','tha','LCM/master/Laos_Route_Hdr_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (122,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','RouteDtl_LAO_','tha','LCM/master/Laos_Route_Dtl_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (123,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','Schedule','tha','LCM/master/Laos_Schedule_Data/',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (124,9,'TH','itx-arm-conapdna-thailand-prod-kdp','GT_Intervention/LCM/','Visit_','tha','LCM/transaction/Laos_Visit_Data/',True);
