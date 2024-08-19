delete from prod_dna_core.ntaitg_integration.itg_pos
where  src_sys_cd in ('EC') 
and (left(pos_dt,7) = '2024-07');
delete from prod_dna_core.ntaitg_integration.itg_pos_temp
where  src_sys_cd in ('EC') 
and (left(pos_dt,7) = '2024-07');
