delete from ntaitg_integration.itg_ims where prod_cd like '79611705/796331' and ims_txn_dt='2024-07-06' and dstr_cd='110256' and cust_cd='378087';
update ntaitg_integration.itg_ims set prod_cd='79611705/79633161' where prod_cd='79611705/796331';
