update meta_raw.PROCESS
set SNOWFLAKE_STAGE = 'INDSDL_RAW.PROD_LOAD_STAGE_ADLS'
where process_id in (1127
,1040
,1041
,1042
,1043
,1044
,1045
,799
,806
,1051
,1052
,1053
,1054);