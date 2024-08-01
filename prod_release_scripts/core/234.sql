delete from prod_dna_core.ntaitg_integration.itg_ims where crt_dttm='2024-07-31 17:58:38.705' and dstr_cd='110256' and ims_txn_dt like '2024-07%';
create or replace prod_dna_core.ntaitg_integration.itg_ims_temp clone prod_dna_core.ntaitg_integration.itg_ims;
