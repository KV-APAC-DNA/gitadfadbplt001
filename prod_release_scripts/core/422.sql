delete from pcfedw_integration.edw_demand_forecast_snapshot_temp
where snap_shot_dt ='2024-09-25 00:00:00.000';
  
delete from pcfedw_integration.edw_demand_forecast_snapshot_temp
where snap_shot_dt ='2024-09-26 00:00:00.000';

delete from dbt_cloud_pr_5458_1396.pcfedw_integration__edw_demand_forecast_snapshot_temp;

insert into dbt_cloud_pr_5458_1396.pcfedw_integration__edw_demand_forecast_snapshot_temp
select * from prod_dna_core.pcfedw_integration.edw_demand_forecast_snapshot_temp;
