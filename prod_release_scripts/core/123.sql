insert into prod_dna_core.aspwks_integration.sap_transactional_processed_files(source_table_name,source_view_name,target_table_name,act_file_name,inserted_on,is_deleted) 
values
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240428_175525.csv',current_timestamp()::timestamp_ntz(9),'F');
