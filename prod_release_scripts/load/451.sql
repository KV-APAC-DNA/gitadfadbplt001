DELETE FROM META_RAW.HISTORICAL_OBJ_METADATA where id=5012;
INSERT INTO META_RAW.HISTORICAL_OBJ_METADATA (ID,SOURCE_SCHEMA,SOURCE_TABLE, TARGET_DB,TARGET_SCHEMA,TARGET_TABLE,STAGE_SCHEMA,ISACTIVE,MARKET,TYPE_OF_LOAD,PRIORITY) VALUES (5012,'AU_ITG','ITG_IRI_SCAN_SALES','PROD_DNA_CORE','DBT_CLOUD_PR_5458_502','PCFITG_INTEGRATION__ITG_IRI_SCAN_SALES_TEMP','UTILITY_RAW',TRUE,'PACIFIC','FULL_REFRESH',1);