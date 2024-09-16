delete from prod_dna_core.ntaitg_integration.itg_pos
where src_sys_cd in ('Poya 寶雅') and pos_dt in ('2024-08-19','2024-08-18','2024-08-17');
delete from prod_dna_core.ntaitg_integration.itg_pos_temp
where src_sys_cd in ('Poya 寶雅') and pos_dt in ('2024-08-19','2024-08-18','2024-08-17');
