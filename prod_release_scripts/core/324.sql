create table PROD_DNA_CORE.pcfedw_integration.edw_demand_forecast_snapshot_temp_20240826 clone PROD_DNA_CORE.pcfedw_integration.edw_demand_forecast_snapshot_temp;
 
DELETE FROM PROD_DNA_CORE.pcfedw_integration.edw_demand_forecast_snapshot_temp WHERE snap_shot_dt = '2024-08-26 00:00:00.000';
