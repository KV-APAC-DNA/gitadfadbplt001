insert into core_integration.dbtjobs_test_cdc_metadata values('in_rt_sales','91','(\'sdl_rrl_retailervalueclass\', \'sdl_rrl_retailercategory\' , \'sdl_rrl_routemaster\', \'sdl_rrl_usermaster\', \'sdl_rrl_townmaster\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('in_sdlitg_gtsales','98','(\'sdl_in_retailer\', \'sdl_in_retailer_route\', \'sdl_in_salesman\', \'sdl_in_salesman_route\',  \'sdl_in_route\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('in_mds_refresh','93','(\'sdl_mds_in_sap_distribution_channel\', \'sdl_mds_in_gl_account_master\', \'sdl_mds_in_ps_weights\', \'sdl_mds_in_product_hierarchy\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('in_etl_xdm_tlrsr_nielsen_load','100','(\'sdl_in_rretailergeoextension\'. \'sdl_in_rrsrdistributor\', \'sdl_in_rrsrheader\', \'sdl_in_rtlsalesman\', \'sdl_in_rtldistributor\', \'sdl_in_rtlheader\')','0');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_retailer','select * from indwks_integration.TRATBL_sdl_in_retailer__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_retailer__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rretailergeoextension','select * from indwks_integration.TRATBL_sdl_in_rretailergeoextension__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rretailergeoextension__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_in_sap_distribution_channel','select * from indwks_integration.TRATBL_sdl_mds_in_sap_distribution_channel__null_test
union all
select * from indwks_integration.TRATBL_sdl_mds_in_sap_distribution_channel__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_in_gl_account_master','select * from indwks_integration.TRATBL_sdl_mds_in_gl_account_master__null_test
union all
select * from indwks_integration.TRATBL_sdl_mds_in_gl_account_master__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_in_ps_weights','select * from indwks_integration.TRATBL_sdl_mds_in_ps_weights__null_test
union all
select * from indwks_integration.TRATBL_sdl_mds_in_ps_weights__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_in_product_hierarchy','select * from indwks_integration.TRATBL_sdl_mds_in_product_hierarchy__null_test
union all
select * from indwks_integration.TRATBL_sdl_mds_in_product_hierarchy__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rrl_retailervalueclass','select * from indwks_integration.TRATBL_sdl_rrl_retailervalueclass__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rrl_retailercategory','select * from indwks_integration.TRATBL_sdl_rrl_retailercategory__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rrl_routemaster','select * from indwks_integration.TRATBL_sdl_rrl_routemaster__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rrl_usermaster','select * from indwks_integration.TRATBL_sdl_rrl_usermaster__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rrl_townmaster','select * from indwks_integration.TRATBL_sdl_rrl_townmaster__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_retailer_route','select * from indwks_integration.TRATBL_sdl_in_retailer_route__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_retailer_route__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rrsrdistributor','select * from indwks_integration.TRATBL_sdl_in_rrsrdistributor__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rrsrdistributor__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rrsrheader','select * from indwks_integration.TRATBL_sdl_in_rrsrheader__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rrsrheader__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rtlsalesman','select * from indwks_integration.TRATBL_sdl_in_rtlsalesman__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rtlsalesman__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rtldistributor','select * from indwks_integration.TRATBL_sdl_in_rtldistributor__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rtldistributor__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_rtlheader','select * from indwks_integration.TRATBL_sdl_in_rtlheader__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_rtlheader__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_salesman','select * from indwks_integration.TRATBL_sdl_in_salesman__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_salesman__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_salesman_route','select * from indwks_integration.TRATBL_sdl_in_salesman_route__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_salesman_route__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_in_route','select * from indwks_integration.TRATBL_sdl_in_route__null_test
union all
select * from indwks_integration.TRATBL_sdl_in_route__duplicate_test;');
