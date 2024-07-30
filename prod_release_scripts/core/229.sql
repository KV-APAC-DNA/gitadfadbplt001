Delete from prod_dna_core.ntaitg_integration.itg_pos where crncy_cd='KRW' and src_sys_cd='Homeplus' and 
pos_dt in('2023-12-22','2023-12-23','2023-12-24','2023-12-25','2023-12-26','2023-10-21','2023-10-20','2023-10-22') and dist_cd='004';
delete from prod_dna_core.ntaitg_integration.itg_pos where crncy_cd='KRW' and src_sys_cd='Homeplus' and pos_dt between '2024-01-01' and '2024-01-28' and dist_cd='004';
