INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.tt01_henpin_riyu
select distinct * from JPDCLEDW_INTEGRATION.tt01_henpin_riyu_20241212;

commit;