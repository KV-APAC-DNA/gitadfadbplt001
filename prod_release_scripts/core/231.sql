delete from ntaitg_integration.itg_ims where dstr_cd='100681' and ctry_cd='HK' and ims_txn_dt like '2024-07%' and crt_dttm='2024-07-30 17:21:18.433';
create or replace table ntaitg_integration.itg_ims_temp clone ntaitg_integration.itg_ims;
