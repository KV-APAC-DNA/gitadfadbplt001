update PROD_DNA_CORE.CORE_INTEGRATION.DBTTEST_TABLES_METADATA
set query = 'select * from vnmwks_integration.TRATBL_sdl_vn_gt_topdoor_target__duplicate_test
union all
select * from vnmwks_integration.TRATBL_sdl_vn_gt_topdoor_target__null_test
union all
select * from vnmwks_integration.TRATBL_sdl_vn_gt_topdoor_target__format_test_to_cycle
union all
select * from vnmwks_integration.TRATBL_sdl_vn_gt_topdoor_target__format_test_from_cycle
;'
where model = 'sdl_vn_gt_topdoor_target' 
;
