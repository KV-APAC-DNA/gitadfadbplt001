update prod_dna_core.ntaitg_integration.itg_pos
set sls_qty = (sls_qty/2)::int
where  src_sys_cd in ('EC') 
and (pos_dt between '2024-06-01' and '2024-06-30');
