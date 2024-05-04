alter table prod_dna_core.pcfitg_integration.itg_weekly_forecast
add column file_name varchar(255);
-------------------------------------------------------
update prod_dna_core.pcfitg_integration.itg_weekly_forecast
set file_name='No file name in Legacy System';
------------------------------------------------------
insert into prod_dna_core.aspwks_integration.sap_transactional_processed_files(source_table_name,source_view_name,target_table_name,act_file_name,inserted_on,is_deleted) 
values
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240421_201134.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240414_174307.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240407_174256.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240331_191042.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240324_203429.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240317_173805.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240310_184600.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240303_175246.csv',current_timestamp()::timestamp_ntz(9),'F'),
('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast','SAP_BW_Weekly_Forecast_20240225_174234.csv',current_timestamp()::timestamp_ntz(9),'F');
