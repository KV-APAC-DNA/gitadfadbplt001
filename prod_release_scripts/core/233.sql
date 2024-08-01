delete from ntaitg_integration.itg_ims where crt_dttm='2024-07-31 17:58:38.705' and dstr_cd='110256' and ims_txn_dt like '2024-07%' group by 1,2;
create or replace ntaitg_integration.itg_ims_temp clone ntaitg_integration.itg_ims;
