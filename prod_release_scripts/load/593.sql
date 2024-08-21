create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_PRODUCTS_TEST (
	STATUS NUMBER(18,0),
	PRODUCTDB_ID VARCHAR(255),
	BARCODE VARCHAR(150),
	SKU VARCHAR(150),
	UNIT_PRICE NUMBER(18,2),
	DISPLAY_ORDER NUMBER(18,0),
	LAUNCH_DATE VARCHAR(20),
	LARGEST_UOM_QUANTITY NUMBER(18,0),
	MIDDLE_UOM_QUANTITY NUMBER(18,0),
	SMALLEST_UOM_QUANTITY NUMBER(18,0),
	COMPANY VARCHAR(200),
	SKU_ENGLISH VARCHAR(200),
	SKU_CODE VARCHAR(200),
	PS_CATEGORY VARCHAR(200),
	PS_SEGMENT VARCHAR(200),
	PS_CATEGORY_SEGMENT VARCHAR(200),
	COUNTRY_L1 VARCHAR(200),
	REGIONAL_FRANCHISE_L2 VARCHAR(200),
	FRANCHISE_L3 VARCHAR(200),
	BRAND_L4 VARCHAR(200),
	SUB_CATEGORY_L5 VARCHAR(200),
	PLATFORM_L6 VARCHAR(200),
	VARIANCE_L7 VARCHAR(200),
	PACK_SIZE_L8 VARCHAR(200),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	HASHKEY VARCHAR(200)
);

create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_USERS_TEST (
	STATUS NUMBER(18,0),
	USERDB_ID VARCHAR(255),
	USERNAME VARCHAR(50),
	FIRST_NAME VARCHAR(50),
	LAST_NAME VARCHAR(50),
	TEAM VARCHAR(50),
	SUPERIOR_NAME VARCHAR(50),
	AUTHORISATION_GROUP VARCHAR(50),
	EMAIL_ADDRESS VARCHAR(50),
	LONGITUDE NUMBER(18,5),
	LATITUDE NUMBER(18,5),
	BUSINESS_UNITS_ID VARCHAR(200),
	BUSINESS_UNIT_NAME VARCHAR(200),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	HASHKEY VARCHAR(200)
);

create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_POPS_TEST (
	STATUS NUMBER(18,0),
	POPDB_ID VARCHAR(255),
	POP_CODE VARCHAR(50),
	POP_NAME VARCHAR(100),
	ADDRESS VARCHAR(1000),
	LONGITUDE NUMBER(18,5),
	LATITUDE NUMBER(18,5),
	BUSINESS_UNITS_ID VARCHAR(255),
	BUSINESS_UNIT_NAME VARCHAR(255),
	COUNTRY VARCHAR(200),
	CHANNEL VARCHAR(200),
	RETAIL_ENVIRONMENT_PS VARCHAR(200),
	SALES_GROUP_NAME VARCHAR(200),
	CUSTOMER VARCHAR(200),
	SALES_GROUP_CODE VARCHAR(200),
	CUSTOMER_GRADE VARCHAR(200),
	EXTERNAL_STORE_CODE VARCHAR(200),
	TERRITORY_OR_REGION VARCHAR(200),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	HASHKEY VARCHAR(200)
);


insert into meta_raw.process (PROCESS_ID,PARAMETER_GROUP_ID,PROCESS_NAME,PIPELINE_TYPE_ID,GROUP_ID,SEQUENCE_ID,IS_INCREMENTAL,IS_ACTIVE,USECASE_ID,SOURCE_ID,PHASE_ID,SCRIPT_GROUP_ID,SOURCE_CDC_COLUMN,SOURCE_CDC_DATATYPE,SNOWFLAKE_CDC_COLUMN,SNOWFLAKE_STAGE,SNOWFLAKE_FILE_FORMAT,PHASE_TYPE,DEPENDS_ON)
values 
(2192,2192,'pop6_th_products_test',2192,1,2,'false','true',154,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','',''),
(2193,2193,'pop6_th_users_test',2193,1,2,'false','true',154,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','',''),
(2194,2194,'pop6_th_pops_test',2194,1,2,'false','true',154,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26839,2192,'pop6_th_products_test','file_spec','products','FALSE','TRUE'),
(26840,2192,'pop6_th_products_test','val_file_name','products','FALSE','TRUE'),
(26841,2192,'pop6_th_products_test','val_file_extn','csv','FALSE','TRUE'),
(26842,2192,'pop6_th_products_test','load_method','sp','FALSE','TRUE'),
(26843,2192,'pop6_th_products_test','sp_name','THASDL_RAW.sdl_pop6_th_products_preprocessing','FALSE','TRUE'),
(26844,2192,'pop6_th_products_test','sheet_index','0','FALSE','TRUE'),
(26845,2192,'pop6_th_products_test','folder_path','pop6/master/masterdata_test/','FALSE','TRUE'),
(26846,2192,'pop6_th_products_test','target_table','sdl_pop6_th_products_test','FALSE','TRUE'),
(26847,2192,'pop6_th_products_test','container','tha','FALSE','TRUE'),
(26848,2192,'pop6_th_products_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26849,2192,'pop6_th_products_test','validation','1-1-0','FALSE','TRUE'),
(26850,2192,'pop6_th_products_test','index','pre','FALSE','TRUE'),
(26851,2192,'pop6_th_products_test','source_extn','csv','FALSE','TRUE'),
(26852,2192,'pop6_th_products_test','file_header_row_num','0','FALSE','TRUE'),
(26853,2192,'pop6_th_products_test','is_truncate','Y','FALSE','TRUE'),
(26854,2192,'pop6_th_products_test','business_mail_trigger','Y','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26871,2194,'pop6_th_pops_test','file_spec','pops','FALSE','TRUE'),
(26872,2194,'pop6_th_pops_test','val_file_name','pops','FALSE','TRUE'),
(26873,2194,'pop6_th_pops_test','val_file_extn','csv','FALSE','TRUE'),
(26874,2194,'pop6_th_pops_test','load_method','sp','FALSE','TRUE'),
(26875,2194,'pop6_th_pops_test','sp_name','THASDL_RAW.sdl_pop6_th_pops_preprocessing','FALSE','TRUE'),
(26876,2194,'pop6_th_pops_test','sheet_index','0','FALSE','TRUE'),
(26877,2194,'pop6_th_pops_test','folder_path','pop6/master/masterdata_test/','FALSE','TRUE'),
(26878,2194,'pop6_th_pops_test','target_table','sdl_pop6_th_pops_test','FALSE','TRUE'),
(26879,2194,'pop6_th_pops_test','container','tha','FALSE','TRUE'),
(26880,2194,'pop6_th_pops_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26881,2194,'pop6_th_pops_test','validation','1-1-0','FALSE','TRUE'),
(26882,2194,'pop6_th_pops_test','index','pre','FALSE','TRUE'),
(26883,2194,'pop6_th_pops_test','source_extn','csv','FALSE','TRUE'),
(26884,2194,'pop6_th_pops_test','file_header_row_num','0','FALSE','TRUE'),
(26885,2194,'pop6_th_pops_test','is_truncate','Y','FALSE','TRUE'),
(26886,2194,'pop6_th_pops_test','business_mail_trigger','Y','FALSE','TRUE');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26855,2193,'pop6_th_users_test','file_spec','users','FALSE','TRUE'),
(26856,2193,'pop6_th_users_test','val_file_name','users','FALSE','TRUE'),
(26857,2193,'pop6_th_users_test','val_file_extn','csv','FALSE','TRUE'),
(26858,2193,'pop6_th_users_test','load_method','sp','FALSE','TRUE'),
(26859,2193,'pop6_th_users_test','sp_name','THASDL_RAW.sdl_pop6_th_users_preprocessing','FALSE','TRUE'),
(26860,2193,'pop6_th_users_test','sheet_index','0','FALSE','TRUE'),
(26861,2193,'pop6_th_users_test','folder_path','pop6/master/masterdata_test/','FALSE','TRUE'),
(26862,2193,'pop6_th_users_test','target_table','sdl_pop6_th_users_test','FALSE','TRUE'),
(26863,2193,'pop6_th_users_test','container','tha','FALSE','TRUE'),
(26864,2193,'pop6_th_users_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26865,2193,'pop6_th_users_test','validation','1-1-0','FALSE','TRUE'),
(26866,2193,'pop6_th_users_test','index','pre','FALSE','TRUE'),
(26867,2193,'pop6_th_users_test','source_extn','csv','FALSE','TRUE'),
(26868,2193,'pop6_th_users_test','file_header_row_num','0','FALSE','TRUE'),
(26869,2193,'pop6_th_users_test','is_truncate','Y','FALSE','TRUE'),
(26870,2193,'pop6_th_users_test','business_mail_trigger','Y','FALSE','TRUE');
