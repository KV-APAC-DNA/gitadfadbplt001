--MDS MY GT Reverse Sync Market


INSERT INTO META_RAW.PROCESS VALUES (2211,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC',2211,1,1,FALSE,TRUE,43,9,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2212,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC',2212,1,1,FALSE,TRUE,43,9,1,NULL,'','','','','','','');


INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27015,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','container','mys',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27016,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','landing_file_path','sql_server/MDS_Reverse_Sync',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27017,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','landing_file_name','SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27018,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','target_schema_table','dbo.MY_GT_Sales_Staging_temp_mds',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27019,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','source_schema_table','MYSEDW_ACCESS.SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27020,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','source_query','SELECT DISTRIBUTOR_ID, SALES_ORDER_NUMBER, SALES_ORDER_DATE, TYPE, CUSTOMER_CODE, DISTRIBUTOR_WH_ID, SAP_MATERIAL_ID, PRODUCT_CODE, PRODUCT_EAN_CODE, PRODUCT_DESCRIPTION, GROSS_ITEM_PRICE, QUANTITY, UOM, QUANTITY_IN_PIECES, QUANTITY_AFTER_CONVERSION, SUB_TOTAL_1, DISCOUNT, SUB_TOTAL_2, BOTTOM_LINE_DISCOUNT, TOTAL_AMT_AFTER_TAX, TOTAL_AMT_BEFORE_TAX, SALES_EMPLOYEE, CRT_DTTM AS insert_dt, ''N'' AS processedtomds FROM MYSEDW_ACCESS.SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27021,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','truncate_and_load','N',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27022,2211,'SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','decide_source','dna_core',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27023,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','container','mys',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27024,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','landing_file_path','sql_server/MDS_Reverse_Sync',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27025,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','landing_file_name','SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27026,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','target_schema_table','dbo.MY_GT_Sales_Staging_temp_mds',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27027,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','source_schema_table','MYSEDW_ACCESS.SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27028,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','source_query','SELECT DISTRIBUTOR_ID, SALES_ORDER_NUMBER, SALES_ORDER_DATE, TYPE, CUSTOMER_CODE, DISTRIBUTOR_WH_ID, SAP_MATERIAL_ID, PRODUCT_CODE, PRODUCT_EAN_CODE, PRODUCT_DESCRIPTION, GROSS_ITEM_PRICE, QUANTITY, UOM, QUANTITY_IN_PIECES, QUANTITY_AFTER_CONVERSION, SUB_TOTAL_1, DISCOUNT, SUB_TOTAL_2, BOTTOM_LINE_DISCOUNT, TOTAL_AMT_AFTER_TAX, TOTAL_AMT_BEFORE_TAX, SALES_EMPLOYEE, CRT_DTTM AS insert_dt, ''N'' AS processedtomds FROM MYSEDW_ACCESS.SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27029,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','truncate_and_load','N',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (27030,2212,'SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC_GROUP','decide_source','dna_core',FALSE,TRUE);

