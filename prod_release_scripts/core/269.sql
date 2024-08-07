delete from ntaitg_integration.itg_pos where pos_dt like '2024-07-23%'   and ean_num in ('8801108002710','8801008002995') and str_cd in ('DAB8','B048');
delete from ntaedw_integration.edw_pos_fact where pos_dt like '2024-07-23%' and  sls_grp = 'Olive Young' AND   crncy_cd = 'KRW'and ean_num in ('8801108002710','8801008002995') and str_cd in ('DAB8','B048');
