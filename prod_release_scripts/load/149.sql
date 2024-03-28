USE SCHEMA META_RAW;

DELETE FROM s3_to_adls WHERE ID IN (100,101,102,103,104,105,115);

INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (100,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Complaint','tha','SFMC/transaction/TH_Action_Complaint',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (101,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Bounce','tha','SFMC/transaction/TH_Action_Bounce',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (102,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Click','tha','SFMC/transaction/TH_Action_Click',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (103,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Open','tha','SFMC/transaction/TH_Action_Open',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (104,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Sent','tha','SFMC/transaction/TH_Action_Sent',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (105,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_Action_Unsubscribe','tha','SFMC/transaction/TH_Action_Unsubscribe',True);
INSERT INTO s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive) VALUES (115,10,'TH','itx-arm-conapdna-thailand-prod-kdp','raw-data-lake/SFMC/','TH_CRM_Children','tha','SFMC/master/TH_CRM_Children',True);

UPDATE s3_to_adls SET DELETE_SOURCE_FILE='Y' WHERE ID IN (100,101,102,103,104,105,115);
