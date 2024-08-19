delete from prod_dna_core.ntaitg_integration.itg_pos
where  src_sys_cd in ('Poya 寶雅') 
and pos_dt = '2024-08-16';
delete from prod_dna_core.ntaitg_integration.itg_pos_temp
where  src_sys_cd in ('Poya 寶雅') 
and pos_dt = '2024-08-16';
