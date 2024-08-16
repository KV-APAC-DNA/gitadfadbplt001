insert into meta_raw.process (PROCESS_ID,PARAMETER_GROUP_ID,PROCESS_NAME,PIPELINE_TYPE_ID,GROUP_ID,SEQUENCE_ID,IS_INCREMENTAL,IS_ACTIVE,USECASE_ID,SOURCE_ID,PHASE_ID,SCRIPT_GROUP_ID,SOURCE_CDC_COLUMN,SOURCE_CDC_DATATYPE,SNOWFLAKE_CDC_COLUMN,SNOWFLAKE_STAGE,SNOWFLAKE_FILE_FORMAT,PHASE_TYPE,DEPENDS_ON)
values 
(2189,2189,'pop6_th_trans_general_audits_test',2189,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','',''),
(2190,2190,'pop6_th_trans_sku_audits_test',2190,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','',''),
(2191,2191,'pop6_th_trans_executed_visits_test',2191,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(26785,2189,'pop6_th_trans_general_audits_test','val_file_header','Visit_ID,Audit_Form_ID,Audit_Form,Section_ID,Section,Subsection_ID,Subsection,Field_ID,Field_Code,Field_Label,Field_Type,Dependent_On_Field_ID,Response','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26786,2189,'pop6_th_trans_general_audits_test','file_spec','general_audits','FALSE','TRUE'),
(26787,2189,'pop6_th_trans_general_audits_test','val_file_name','general_audits','FALSE','TRUE'),
(26788,2189,'pop6_th_trans_general_audits_test','val_file_extn','csv','FALSE','TRUE'),
(26789,2189,'pop6_th_trans_general_audits_test','load_method','sp','FALSE','TRUE'),
(26790,2189,'pop6_th_trans_general_audits_test','sp_name','THASDL_RAW.Pop6_th_trans_PREPROCESSING','FALSE','TRUE'),
(26791,2189,'pop6_th_trans_general_audits_test','sheet_index','0','FALSE','TRUE'),
(26792,2189,'pop6_th_trans_general_audits_test','folder_path','pop6/transaction/auditdata_test/','FALSE','TRUE'),
(26793,2189,'pop6_th_trans_general_audits_test','target_table','sdl_pop6_th_general_audits_test','FALSE','TRUE'),
(26794,2189,'pop6_th_trans_general_audits_test','container','tha','FALSE','TRUE'),
(26795,2189,'pop6_th_trans_general_audits_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26796,2189,'pop6_th_trans_general_audits_test','validation','1-1-1','FALSE','TRUE'),
(26797,2189,'pop6_th_trans_general_audits_test','index','pre','FALSE','TRUE'),
(26798,2189,'pop6_th_trans_general_audits_test','source_extn','csv','FALSE','TRUE'),
(26799,2189,'pop6_th_trans_general_audits_test','file_header_row_num','0','FALSE','TRUE'),
(26800,2189,'pop6_th_trans_general_audits_test','is_truncate','Y','FALSE','TRUE'),
(26801,2189,'pop6_th_trans_general_audits_test','trigger_mail','Y','FALSE','TRUE'),
(26802,2189,'pop6_th_trans_general_audits_test','business_mail_trigger','Y','FALSE','TRUE');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(26803,2190,'pop6_th_trans_sku_audits_test','val_file_header','Visit_ID,Audit_Form_ID,Audit_Form,Section_ID,Section,Field_ID,Field_Code,Field_Label,Field_Type,Dependent_On_Field_ID,SKU_ID,SKU,Response','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26804,2190,'pop6_th_trans_sku_audits_test','file_spec','sku_audits','FALSE','TRUE'),
(26805,2190,'pop6_th_trans_sku_audits_test','val_file_name','sku_audits','FALSE','TRUE'),
(26806,2190,'pop6_th_trans_sku_audits_test','val_file_extn','csv','FALSE','TRUE'),
(26807,2190,'pop6_th_trans_sku_audits_test','load_method','sp','FALSE','TRUE'),
(26808,2190,'pop6_th_trans_sku_audits_test','sp_name','THASDL_RAW.Pop6_th_trans_PREPROCESSING','FALSE','TRUE'),
(26809,2190,'pop6_th_trans_sku_audits_test','sheet_index','0','FALSE','TRUE'),
(26810,2190,'pop6_th_trans_sku_audits_test','folder_path','pop6/transaction/auditdata_test/','FALSE','TRUE'),
(26811,2190,'pop6_th_trans_sku_audits_test','target_table','sdl_pop6_th_sku_audits_test','FALSE','TRUE'),
(26812,2190,'pop6_th_trans_sku_audits_test','container','tha','FALSE','TRUE'),
(26813,2190,'pop6_th_trans_sku_audits_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26814,2190,'pop6_th_trans_sku_audits_test','validation','1-1-1','FALSE','TRUE'),
(26815,2190,'pop6_th_trans_sku_audits_test','index','pre','FALSE','TRUE'),
(26816,2190,'pop6_th_trans_sku_audits_test','source_extn','csv','FALSE','TRUE'),
(26817,2190,'pop6_th_trans_sku_audits_test','file_header_row_num','0','FALSE','TRUE'),
(26818,2190,'pop6_th_trans_sku_audits_test','is_truncate','Y','FALSE','TRUE'),
(26819,2190,'pop6_th_trans_sku_audits_test','trigger_mail','Y','FALSE','TRUE'),
(26820,2190,'pop6_th_trans_sku_audits_test','business_mail_trigger','Y','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(26821,2191,'pop6_th_trans_executed_visits_test','val_file_header','Visit_ID,Visit_Date,Check-In_DateTime,Check-Out_DateTime,POPDB_ID,POP_Code,POP_Name,Address,Check-In_Longitude,Check-In_Latitude,Check-Out_Longitude,Check-Out_Latitude,Check-In_Photo,Check-Out_Photo,Username,User_Full_Name,Superior_Username,Superior_Name,Planned_Visit,Cancelled_Visit,Cancellation_Reason,Cancellation_Note','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26822,2191,'pop6_th_trans_executed_visits_test','file_spec','executed_visits','FALSE','TRUE'),
(26823,2191,'pop6_th_trans_executed_visits_test','val_file_name','executed_visits','FALSE','TRUE'),
(26824,2191,'pop6_th_trans_executed_visits_test','val_file_extn','csv','FALSE','TRUE'),
(26825,2191,'pop6_th_trans_executed_visits_test','load_method','sp','FALSE','TRUE'),
(26826,2191,'pop6_th_trans_executed_visits_test','sp_name','THASDL_RAW.Pop6_th_trans_PREPROCESSING','FALSE','TRUE'),
(26827,2191,'pop6_th_trans_executed_visits_test','sheet_index','0','FALSE','TRUE'),
(26828,2191,'pop6_th_trans_executed_visits_test','folder_path','pop6/transaction/visitdata_test/','FALSE','TRUE'),
(26829,2191,'pop6_th_trans_executed_visits_test','target_table','sdl_pop6_th_executed_visits_test','FALSE','TRUE'),
(26830,2191,'pop6_th_trans_executed_visits_test','container','tha','FALSE','TRUE'),
(26831,2191,'pop6_th_trans_executed_visits_test','target_schema','THASDL_RAW','FALSE','TRUE'),
(26832,2191,'pop6_th_trans_executed_visits_test','validation','1-1-1','FALSE','TRUE'),
(26833,2191,'pop6_th_trans_executed_visits_test','index','pre','FALSE','TRUE'),
(26834,2191,'pop6_th_trans_executed_visits_test','source_extn','csv','FALSE','TRUE'),
(26835,2191,'pop6_th_trans_executed_visits_test','file_header_row_num','0','FALSE','TRUE'),
(26836,2191,'pop6_th_trans_executed_visits_test','is_truncate','Y','FALSE','TRUE'),
(26837,2191,'pop6_th_trans_executed_visits_test','trigger_mail','Y','FALSE','TRUE'),
(26838,2191,'pop6_th_trans_executed_visits_test','business_mail_trigger','Y','FALSE','TRUE');

create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_GENERAL_AUDITS_TEST (
	VISIT_ID VARCHAR(255),
	AUDIT_FORM_ID VARCHAR(255),
	AUDIT_FORM VARCHAR(255),
	SECTION_ID VARCHAR(255),
	SECTION VARCHAR(255),
	SUBSECTION_ID VARCHAR(255),
	SUBSECTION VARCHAR(255),
	FIELD_ID VARCHAR(255),
	FIELD_CODE VARCHAR(255),
	FIELD_LABEL VARCHAR(255),
	FIELD_TYPE VARCHAR(50),
	DEPENDENT_ON_FIELD_ID VARCHAR(255),
	RESPONSE VARCHAR(65535),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_SKU_AUDITS_TEST (
	VISIT_ID VARCHAR(255),
	AUDIT_FORM_ID VARCHAR(255),
	AUDIT_FORM VARCHAR(255),
	SECTION_ID VARCHAR(255),
	SECTION VARCHAR(255),
	FIELD_ID VARCHAR(255),
	FIELD_CODE VARCHAR(255),
	FIELD_LABEL VARCHAR(255),
	FIELD_TYPE VARCHAR(50),
	DEPENDENT_ON_FIELD_ID VARCHAR(255),
	SKU_ID VARCHAR(255),
	SKU VARCHAR(255),
	RESPONSE VARCHAR(65535),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_POP6_TH_EXECUTED_VISITS_TEST (
	VISIT_ID VARCHAR(255),
	VISIT_DATE DATE,
	CHECK_IN_DATETIME TIMESTAMP_NTZ(9),
	CHECK_OUT_DATETIME TIMESTAMP_NTZ(9),
	POPDB_ID VARCHAR(255),
	POP_CODE VARCHAR(50),
	POP_NAME VARCHAR(100),
	ADDRESS VARCHAR(150),
	CHECK_IN_LONGITUDE NUMBER(18,5),
	CHECK_IN_LATITUDE NUMBER(18,5),
	CHECK_OUT_LONGITUDE NUMBER(18,5),
	CHECK_OUT_LATITUDE NUMBER(18,5),
	CHECK_IN_PHOTO VARCHAR(650),
	CHECK_OUT_PHOTO VARCHAR(650),
	USERNAME VARCHAR(50),
	USER_FULL_NAME VARCHAR(50),
	SUPERIOR_USERNAME VARCHAR(50),
	SUPERIOR_NAME VARCHAR(50),
	PLANNED_VISIT NUMBER(18,0),
	CANCELLED_VISIT NUMBER(18,0),
	CANCELLATION_REASON VARCHAR(255),
	CANCELLATION_NOTE VARCHAR(255),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);
