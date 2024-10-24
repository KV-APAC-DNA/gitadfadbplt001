
delete from PROD_DNA_CORE.PCFEDW_INTEGRATION.EDW_DEMAND_FORECAST_SNAPSHOT_temp
where pac_subsource_type='SAPBW_ACTUAL'
and jj_year in (2019,2020)
and to_char(snap_shot_dt, 'YYYYMM') in (202404,202405,202406,202407,202408,202409);
