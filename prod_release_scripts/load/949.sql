INSERT INTO META_RAW.PROCESS VALUES (3041,3041,'IN_HCP360_MDS_Data_Refresh_SDL_MDS_IN_HCP_SELFCARE',3041,1,1,FALSE,TRUE,507,1,1,NULL,'','','','HCPSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO META_RAW.PROCESS VALUES (3042,3042,'IN_HCP360_MDS_Data_Refresh_SDL_MDS_IN_HCP_SpecialtyCare',3042,1,1,FALSE,TRUE,507,1,1,NULL,'','','','HCPSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28691,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','container','hcp',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28692,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','decide_source','sql_server_mds',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28693,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','landing_file_name','SDL_MDS_IN_HCP_SpecialtyCare',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28694,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','landing_file_path','sql_server/MDS/MDS_Adhoc_IN_HCP_Targets',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28695,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','ms_query','SELECT * FROM MDS.MDM.IN_HCP_SpecialtyCare',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28696,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','target_schema','HCPSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28697,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','target_table','SDL_MDS_IN_HCP_SpecialtyCare',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28698,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28699,3041,'SDL_MDS_IN_HCP_SpecialtyCare_group','map_names','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28700,3042,'SDL_MDS_IN_HCP_SELFCARE_group','container','hcp',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28701,3042,'SDL_MDS_IN_HCP_SELFCARE_group','decide_source','sql_server_mds',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28702,3042,'SDL_MDS_IN_HCP_SELFCARE_group','landing_file_name','SDL_MDS_IN_HCP_SELFCARE',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28703,3042,'SDL_MDS_IN_HCP_SELFCARE_group','landing_file_path','sql_server/MDS/SDL_MDS_IN_HCP_SELFCARE',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28704,3042,'SDL_MDS_IN_HCP_SELFCARE_group','ms_query','SELECT * FROM MDS.MDM.SDL_MDS_IN_HCP_SELFCARE',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28705,3042,'SDL_MDS_IN_HCP_SELFCARE_group','target_schema','HCPSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28706,3042,'SDL_MDS_IN_HCP_SELFCARE_group','target_table','SDL_MDS_IN_HCP_SELFCARE',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28707,3042,'SDL_MDS_IN_HCP_SELFCARE_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (28708,3042,'SDL_MDS_IN_HCP_SELFCARE_group','map_names','Y',FALSE,TRUE);