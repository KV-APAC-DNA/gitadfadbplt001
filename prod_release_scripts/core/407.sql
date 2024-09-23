

delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_DEMAND_FORECAST_SNAPSHOT_temp
where pac_subsource_type='SAPBW_ACTUAL'
and to_char(snap_shot_dt, 'YYYYMM') in (202403,202404,202405);
