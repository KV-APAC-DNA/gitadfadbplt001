
INSERT INTO meta_raw.USECASE (USECASE_ID, USECASE_NAME,CATEGORY,USECASE_DESCRIPTION,IS_ACTIVE,SEQUENCE_ID) VALUES (503,'PH_Pos_Rka_Rose_Pharma_Ingestion','PH_Pos_Rka_Rose_Pharma_Load','PH_Pos_Rka_Rose_Pharma_Ingestion','TRUE',1);


INSERT INTO meta_raw.PROCESS VALUES (2190,2190,'PH_Pos_Rka_Rose_Pharma_Load',2190,1,1,FALSE,TRUE,503,1,1,NULL,'','','','PHLSDL_RAW.DEV_LOAD_STAGE_ADLS','','','');
INSERT INTO meta_raw.PROCESS VALUES (2214,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_Load',2214,1,1,FALSE,TRUE,521,1,1,NULL,'','','','PHLSDL_RAW.DEV_LOAD_STAGE_ADLS','','','');


INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26797,2190,'PH_Pos_Rka_Rose_Pharma_group','trigger_mail','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26798,2190,'PH_Pos_Rka_Rose_Pharma_group','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26799,2190,'PH_Pos_Rka_Rose_Pharma_group','val_file_header','Branch|"Code"|"Branch Name"|"Month"|"SKU"|"SKU Description"|"Qty"',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26800,2190,'PH_Pos_Rka_Rose_Pharma_group','file_spec','RSPHARM_OTC',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26801,2190,'PH_Pos_Rka_Rose_Pharma_group','val_file_name','RSPHARM_OTC',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26802,2190,'PH_Pos_Rka_Rose_Pharma_group','val_file_extn','xlsx',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26803,2190,'PH_Pos_Rka_Rose_Pharma_group','load_method','sp',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26804,2190,'PH_Pos_Rka_Rose_Pharma_group','sp_name','PHLSDL_RAW.PH_POS_RKS_ROSE_PHARMA_PREPROCESSING',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26805,2190,'PH_Pos_Rka_Rose_Pharma_group','sheet_index','1',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26806,2190,'PH_Pos_Rka_Rose_Pharma_group','folder_path','rosepharma_pos/transaction/',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26807,2190,'PH_Pos_Rka_Rose_Pharma_group','target_table','SDL_POS_RKS_ROSE_PHARMA',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26808,2190,'PH_Pos_Rka_Rose_Pharma_group','container','phl',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26809,2190,'PH_Pos_Rka_Rose_Pharma_group','target_schema','phlsdl_raw',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26810,2190,'PH_Pos_Rka_Rose_Pharma_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26811,2190,'PH_Pos_Rka_Rose_Pharma_group','index','full',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26812,2190,'PH_Pos_Rka_Rose_Pharma_group','source_extn','xlsx',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26813,2190,'PH_Pos_Rka_Rose_Pharma_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26814,2190,'PH_Pos_Rka_Rose_Pharma_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (26815,2190,'PH_Pos_Rka_Rose_Pharma_group','startRange','A2',FALSE,TRUE);



INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27054,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','trigger_mail','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27055,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27056,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','val_file_header','Branch_Code|Branch_Name|Month|SKU|SKU_Description|Qty',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27057,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','file_spec','RSPHARM',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27058,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','val_file_name','RSPHARM',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27059,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','val_file_extn','xlsx',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27060,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','load_method','sp',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27061,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','sp_name','PHLSDL_RAW.PH_POS_RKS_ROSE_PHARMA_CONSUMER_PREPROCESSING',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27062,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','sheet_index','1',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27063,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','folder_path','rosepharma_pos_consumer/transaction/',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27064,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','target_table','SDL_POS_RKS_ROSE_PHARMA_CONSUMER',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27065,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','container','phl',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27066,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','target_schema','phlsdl_raw',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27067,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27068,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','index','full',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27069,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','source_extn','xlsx',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27070,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27071,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO meta_raw.parameters (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27072,2214,'PH_Pos_Rka_Rose_Pharma_Consumer_group','startRange','A2',FALSE,TRUE);