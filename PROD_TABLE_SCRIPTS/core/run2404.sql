delete from core_integration.dbtjobs_test_cdc_metadata where tempid='14';
delete from core_integration.dbtjobs_test_cdc_metadata where tempid='15';
insert into core_integration.dbtjobs_test_cdc_metadata values('my_sellin','14','(\'sdl_my_customer_dim\', \'sdl_my_accruals\', \'sdl_my_afgr\', \'sdl_my_trgts\', \'sdl_my_le_trgt\', \'sdl_my_ciw_map\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('my_sellout','15','(\'sdl_my_dstrbtr_doc_type\', \'sdl_my_dstrbtrr_dim\', \'sdl_my_in_transit\')','0');
