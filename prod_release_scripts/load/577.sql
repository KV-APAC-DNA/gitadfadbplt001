
insert into meta_raw.process (PROCESS_ID,PARAMETER_GROUP_ID,PROCESS_NAME,PIPELINE_TYPE_ID,GROUP_ID,SEQUENCE_ID,IS_INCREMENTAL,IS_ACTIVE,USECASE_ID,SOURCE_ID,PHASE_ID,SCRIPT_GROUP_ID,SOURCE_CDC_COLUMN,SOURCE_CDC_DATATYPE,SNOWFLAKE_CDC_COLUMN,SNOWFLAKE_STAGE,SNOWFLAKE_FILE_FORMAT,PHASE_TYPE,DEPENDS_ON)
values 
(2186,2186,'pop6_th_trans_rir_data_test_sftp',2186,1,1,'false','true',319,4,1,null,'','','','','','',''),
(2187,2187,'pop6_th_rir_data_test_lists',2187,1,2,'false','true',319,1,1,null,'','','','THASDL_RAW.PROD_LOAD_STAGE_ADLS','','','');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(26760,2186,'pop6_th_trans_rir_data_sftp','file_spec','rir_data','FALSE','TRUE'),
(26761,2186,'pop6_th_trans_rir_data_sftp','folder_path','pop6/transaction/rirdata_test/','FALSE','TRUE'),
(26762,2186,'pop6_th_trans_rir_data_sftp','container','tha','FALSE','TRUE'),
(26763,2186,'pop6_th_trans_rir_data_sftp','source_extn','zip','FALSE','TRUE'),
(26764,2186,'pop6_th_trans_rir_data_sftp','isUnzipNeeded','Y','FALSE','TRUE'),
(26765,2186,'pop6_th_trans_rir_data_sftp','ftpName','popcompany','FALSE','TRUE'),
(26766,2186,'pop6_th_trans_rir_data_sftp','ftpDirectory','/JnJ-KR-SB/data/rirdata/','FALSE','TRUE');

insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values 
(26767,2187,'pop6_th_rirdata','val_file_header','Visit_ID,Photo,Related_Attribute,SKU_id,SKU,Layer,Total_Layer,Facing_of_this_layer','FALSE','TRUE');


insert into meta_raw.parameters (parameter_id , parameter_group_id ,parameter_group_name , parameter_name, parameter_value , is_sensitive, is_active )
values
(26768,2187,'pop6_th_rirdata','file_spec','rir_data','FALSE','TRUE'),
(26769,2187,'pop6_th_rirdata','val_file_name','rir_data','FALSE','TRUE'),
(26770,2187,'pop6_th_rirdata','val_file_extn','csv','FALSE','TRUE'),
(26771,2187,'pop6_th_rirdata','load_method','sp','FALSE','TRUE'),
(26772,2187,'pop6_th_rirdata','sp_name','THASDL_RAW.Pop6_th_trans_PREPROCESSING','FALSE','TRUE'),
(26773,2187,'pop6_th_rirdata','sheet_index','0','FALSE','TRUE'),
(26774,2187,'pop6_th_rirdata','folder_path','pop6/transaction/rirdata_test/','FALSE','TRUE'),
(26775,2187,'pop6_th_rirdata','target_table','sdl_pop6_th_rir_data_test','FALSE','TRUE'),
(26776,2187,'pop6_th_rirdata','container','tha','FALSE','TRUE'),
(26777,2187,'pop6_th_rirdata','target_schema','THASDL_RAW','FALSE','TRUE'),
(26778,2187,'pop6_th_rirdata','validation','1-1-1','FALSE','TRUE'),
(26779,2187,'pop6_th_rirdata','index','pre','FALSE','TRUE'),
(26780,2187,'pop6_th_rirdata','source_extn','csv','FALSE','TRUE'),
(26781,2187,'pop6_th_rirdata','file_header_row_num','0','FALSE','TRUE'),
(26782,2187,'pop6_th_rirdata','is_truncate','Y','FALSE','TRUE'),
(26783,2187,'pop6_th_rirdata','trigger_mail','Y','FALSE','TRUE'),
(26784,2187,'pop6_th_rirdata','business_mail_trigger','Y','FALSE','TRUE');
