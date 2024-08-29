insert into meta_raw.process (PROCESS_ID,PARAMETER_GROUP_ID,PROCESS_NAME,PIPELINE_TYPE_ID,GROUP_ID,SEQUENCE_ID,IS_INCREMENTAL,IS_ACTIVE,USECASE_ID,SOURCE_ID,PHASE_ID,SCRIPT_GROUP_ID,SOURCE_CDC_COLUMN,SOURCE_CDC_DATATYPE,SNOWFLAKE_CDC_COLUMN,SNOWFLAKE_STAGE,SNOWFLAKE_FILE_FORMAT,PHASE_TYPE,DEPENDS_ON)
values 
(3000,3000,'pop6_th_rir_data_test_lists',3000,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(30000,3000,'pop6_th_rir_data_test_lists','val_file_header','Visit_ID,Photo,Related_Attribute,SKU_id,SKU,Layer,Total_Layer,Facing_of_this_layer','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(30001,3000,'pop6_th_rir_data_test_lists','file_spec','rir_data','FALSE','TRUE'),
(30002,3000,'pop6_th_rir_data_test_lists','val_file_name','rir_data','FALSE','TRUE'),
(30003,3000,'pop6_th_rir_data_test_lists','val_file_extn','csv','FALSE','TRUE'),
(30004,3000,'pop6_th_rir_data_test_lists','load_method','sp','FALSE','TRUE'),
(30005,3000,'pop6_th_rir_data_test_lists','sp_name','THASDL_RAW.Pop6_th_trans_PREPROCESSING','FALSE','TRUE'),
(30006,3000,'pop6_th_rir_data_test_lists','sheet_index','0','FALSE','TRUE'),
(30007,3000,'pop6_th_rir_data_test_lists','folder_path','pop6/transaction/rirdata_test/','FALSE','TRUE'),
(30008,3000,'pop6_th_rir_data_test_lists','target_table','sdl_pop6_th_rir_data_test','FALSE','TRUE'),
(30009,3000,'pop6_th_rir_data_test_lists','container','tha','FALSE','TRUE'),
(30010,3000,'pop6_th_rir_data_test_lists','target_schema','THASDL_RAW','FALSE','TRUE'),
(30011,3000,'pop6_th_rir_data_test_lists','validation','1-1-1','FALSE','TRUE'),
(30012,3000,'pop6_th_rir_data_test_lists','index','pre','FALSE','TRUE'),
(30013,3000,'pop6_th_rir_data_test_lists','source_extn','csv','FALSE','TRUE'),
(30014,3000,'pop6_th_rir_data_test_lists','file_header_row_num','0','FALSE','TRUE'),
(30015,3000,'pop6_th_rir_data_test_lists','is_truncate','Y','FALSE','TRUE'),
(30016,3000,'pop6_th_rir_data_test_lists','trigger_mail','Y','FALSE','TRUE'),
(30017,3000,'pop6_th_rir_data_test_lists','business_mail_trigger','Y','FALSE','TRUE');
