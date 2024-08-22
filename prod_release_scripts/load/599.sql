insert into meta_raw.process (PROCESS_ID,PARAMETER_GROUP_ID,PROCESS_NAME,PIPELINE_TYPE_ID,GROUP_ID,SEQUENCE_ID,IS_INCREMENTAL,IS_ACTIVE,USECASE_ID,SOURCE_ID,PHASE_ID,SCRIPT_GROUP_ID,SOURCE_CDC_COLUMN,SOURCE_CDC_DATATYPE,SNOWFLAKE_CDC_COLUMN,SNOWFLAKE_STAGE,SNOWFLAKE_FILE_FORMAT,PHASE_TYPE,DEPENDS_ON)
values 
(2190,2190,'pop6_th_trans_sku_audits_test',2190,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

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
