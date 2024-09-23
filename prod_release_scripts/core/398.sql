ALTER TABLE PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_DEMAND_FORECAST_SNAPSHOT_temp1 CLUSTER BY (snap_shot_dt, PAC_SUBSOURCE_TYPE,jj_period);

delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_DEMAND_FORECAST_SNAPSHOT_temp1
WHERE PAC_SUBSOURCE_TYPE ='SAPBW_ACTUAL'
and to_char(snap_shot_dt, 'YYYYMM') = 202403;