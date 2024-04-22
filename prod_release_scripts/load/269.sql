delete from meta_raw.s3_to_adls
where id in (191,196,182,207);

INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (182,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master/product/ProdGrp','ProdGrp','pac','ap_perenso/master/product/',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (191,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account','AcctGrp','pac','ap_perenso/master/account/',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (196,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/master_files/account','Account','pac','ap_perenso/master/account/',TRUE,'Y');
INSERT INTO meta_raw.s3_to_adls(id,group_id,country,s3_bucket,s3_path,s3_file,adls_container,adls_path,Isactive,DELETE_SOURCE_FILE) VALUES (207,17,'PCF','itx-arm-conapdna-pacific-prod-kdp','ap_perenso/transaction_files/ranging/','Ranging','pac','ap_perenso/transaction/ranging',TRUE,'Y');
create or replace TABLE PCFSDL_RAW.SDL_PERENSO_PRODUCT (
    PROD_KEY NUMBER(10,0),
    PROD_ID VARCHAR(50),
    PROD_DESC VARCHAR(100),
    PROD_EAN VARCHAR(255),
    ACTIVE VARCHAR(10),
    RUN_ID NUMBER(14,0),
    CREATE_DT TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9)))
);


update META_RAW.HISTORICAL_OBJ_METADATA
set isactive = FALSE;

update META_RAW.HISTORICAL_OBJ_METADATA
set isactive = TRUE
WHERE MARKET = 'Pacific' and id != 1194;
