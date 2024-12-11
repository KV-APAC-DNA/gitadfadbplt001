delete from jpnitg_integration.da_so_planet_accum where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
 
  delete from PROD_DNA_CORE.JPNEDW_INTEGRATION.DW_SO_SELL_OUT_DLY where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
  delete from PROD_DNA_CORE.JPNEDW_INTEGRATION.DM_INTEGRATION_DLY where so_id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
  
 
   delete from jpnwks_integration.wk_so_planet_revise where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
  delete from jpnwks_integration.wk_so_planet_no_dup_temp where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
  delete from jpnwks_integration.wk_so_planet_no_dup where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
   truncate table jpnwks_integration.wk_so_planet_today;
 
    delete from jpnwks_integration.wk_so_planet_cleansed where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
   delete from jpnwks_integration.wk_so_planet_modified where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
 
    delete from jpnitg_integration.DW_SO_PLANET_ERR where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet);
    delete from jpnitg_integration.DW_SO_PLANET_ERR_CD_2 where jcp_rec_seq in(select jcp_rec_Seq from jpnitg_integration.DW_SO_PLANET_ERR where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet));
delete from jpnwks_integration.consistency_error_2 where jcp_rec_seq in(select jcp_rec_Seq from jpnitg_integration.DW_SO_PLANET_ERR where id in(select id from prod_dna_load.jpnsdl_raw.edi_sell_out_planet))
