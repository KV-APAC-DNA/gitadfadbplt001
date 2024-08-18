truncate table prod_dna_core.ntawks_integration.wks_sdl_hk_pos_scorecard_mannings;

delete from prod_dna_core.ntaitg_integration.itg_pos
where  src_sys_cd in ('Mannings') and (pos_dt between '2024-07-07' and '2024-07-13');
