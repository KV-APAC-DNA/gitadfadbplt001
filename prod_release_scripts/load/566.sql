INSERT INTO META_RAW.HISTORICAL_OBJ_METADATA 
(ID,SOURCE_SCHEMA,SOURCE_TABLE, TARGET_DB,TARGET_SCHEMA,TARGET_TABLE,STAGE_SCHEMA,ISACTIVE,MARKET,TYPE_OF_LOAD,PRIORITY) 
VALUES (5037,'AU_EDW','EDW_DEMAND_FORECAST_SNAPSHOT','PROD_DNA_CORE','PCFEDW_INTEGRATION','EDW_DEMAND_FORECAST_SNAPSHOT_SYNC','UTILITY_RAW',TRUE,'EDW_DEMAND_FORECAST','FULL_REFRESH',7);