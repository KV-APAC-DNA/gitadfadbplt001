insert into prod_dna_core.CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_shilla','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_shilla__null_test union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_shilla__product_lookup_test
union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_shilla__channel_lookup_test;');


delete from prod_dna_core.thaitg_integration.itg_la_gt_visit where filename in ('Visit_20240325232501_20240403043811.txt','Visit_20240324232501_20240403043610.txt','Visit_20240321232501_20240403042837.txt','Visit_20240320232501_20240403042621.txt','Visit_20240315232501_20240403041434.txt','Visit_20240314232501_20240403041217.txt','Visit_20240313232501_20240403040938.txt');
