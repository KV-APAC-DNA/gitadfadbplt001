insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_schedule','select * from thawks_integration.TRATBL_sdl_th_gt_schedule__null_test 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_schedule__duplicate_test 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_schedule__test_format 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_schedule__test_date_format_odd_eve_leap;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_mt_7_11','select * from thawks_integration.TRATBL_sdl_th_mt_7_11__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_mt_7_11__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_mt_tops','select * from thawks_integration.TRATBL_sdl_th_mt_tops__null_test 
union all
select * from thawks_integration.TRATBL_sdl_th_mt_tops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_visit','select * from thawks_integration.TRATBL_sdl_th_gt_visit__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_gt_visit__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_th_gt_visit__test_format
union all
select * from thawks_integration.TRATBL_sdl_th_gt_visit__test_date_format_odd_eve_leap;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_msl_distribution','select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__null_test 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__duplicate_test 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__test_format 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__test_date_format_odd_eve_leap 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__test_format_flag 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__test_format_null_flag 
union all
select * from thawks_integration.TRATBL_sdl_th_gt_msl_distribution__test_multiple_column;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_gt_productive_call_target','select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__test_format
union all
select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_consumerreach_cvs','select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_cvs__null_test
union all
select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_cvs__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_customer','select * from thawks_integration.TRATBL_sdl_la_gt_customer__null_test
union all
select * from thawks_integration.TRATBL_sdl_la_gt_customer__test_file
union all
select * from thawks_integration.TRATBL_sdl_la_gt_customer__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_dms_customer_dim','select * from thawks_integration.TRATBL_sdl_th_dms_customer_dim__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_dms_chana_customer','select * from thawks_integration.TRATBL_sdl_th_dms_chana_customer_dim__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_dms_chana_customer_dim__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_th_dms_chana_customer_dim__test_file;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_gt_scope','select * from thawks_integration.TRATBL_sdl_mds_th_gt_scope__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_gt_scope__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_lcm_distributor_target_sales_re','select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales_re__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales_re__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_district','select * from thawks_integration.TRATBL_sdl_mds_th_ref_district__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_ref_district__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_dms_inventory_fact','select * from thawks_integration.TRATBL_sdl_th_dms_inventory_fact__test_date_format
;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_rolling_forecast','select * from thawks_integration.TRATBL_sdl_mds_th_rolling_forecast__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_dms_customer_dim','select * from thawks_integration.TRATBL_sdl_th_dms_customer_dim__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_mt_branch_master','select * from thawks_integration.TRATBL_sdl_mds_th_mt_branch_master__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ps_targets','select * from thawks_integration.TRATBL_sdl_mds_th_ps_targets__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_ps_targets__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_mer_cop','select * from thawks_integration.TRATBL_sdl_jnj_mer_cop__null_test
union all
select * from thawks_integration.TRATBL_sdl_jnj_mer_cop__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_mer_share_of_shelf','select * from thawks_integration.TRATBL_sdl_jnj_mer_share_of_shelf__null_test
union all
select * from thawks_integration.TRATBL_sdl_jnj_mer_share_of_shelf__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_jnj_mer_share_of_shelf__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_osa_oos_report','select * from thawks_integration.TRATBL_sdl_jnj_osa_oos_report__null_test
union all
select * from thawks_integration.TRATBL_sdl_jnj_osa_oos_report__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_npi','select * from thawks_integration.TRATBL_sdl_mds_th_npi__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_npi__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_product_group','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_product_group__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_htc_sellout','select * from thawks_integration.TRATBL_sdl_mds_th_htc_sellout__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_click_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_click_data__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_click_data__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_click_data__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_complaint_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_complaint_data__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_complaint_data__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_complaint_data__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_sent_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_sent_data__null_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_sent_data__duplicate_test
union all
select * from thawks_integration.TRATBL_sdl_th_sfmc_sent_data__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_sub_district','select * from thawks_integration.TRATBL_sdl_mds_th_ref_sub_district__null_test
union all
select * from thawks_integration.TRATBL_sdl_mds_th_ref_sub_district__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_msl','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_msl__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_target_sales','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_target_sales__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_distributor_target_sales__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_list','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_list__null_test union all 
select * from thawks_integration.TRATBL_sdl_mds_th_distributor_list__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_region','select * from thawks_integration.TRATBL_sdl_mds_th_ref_region__null_test union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_region__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_product_master','select * from thawks_integration.TRATBL_sdl_mds_th_product_master__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_product_master__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_product_master__duplicate_test2
;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_market_share_distribution','select * from thawks_integration.TRATBL_sdl_mds_th_market_share_distribution__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_market_share_distribution__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_gt_kpi_target','select * from thawks_integration.TRATBL_sdl_mds_th_gt_kpi_target__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_gt_kpi_target__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_target_sales_re','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_target_sales_re__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_distributor_target_sales_re__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ps_weights','select * from thawks_integration.TRATBL_sdl_mds_th_ps_weights__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ps_weights__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_htc_inventory','select * from thawks_integration.TRATBL_sdl_mds_th_htc_inventory__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_lcm_distributor_target_sales','select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales__date_plan_format_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_product_group','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_product_group__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_npi','select * from thawks_integration.TRATBL_sdl_mds_th_npi__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_npi__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_district','select * from thawks_integration.TRATBL_sdl_mds_th_ref_district__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_district__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_sub_district','select * from thawks_integration.TRATBL_sdl_mds_th_ref_sub_district__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_sub_district__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_rolling_forecast','select * from thawks_integration.TRATBL_sdl_mds_th_rolling_forecast__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_gt_scope','select * from thawks_integration.TRATBL_sdl_mds_th_gt_scope__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_gt_scope__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ps_ttgt','select * from thawks_integration.TRATBL_sdl_mds_th_ps_targets__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ps_targets__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_htc_sellout','select * from thawks_integration.TRATBL_sdl_mds_th_htc_sellout__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_lcm_distributor_target_sales_re','select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales_re__null_test
union all 
select * from thawks_integration.TRATBL_sdl_mds_lcm_distributor_target_sales_re__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_children_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_children_data__null_test
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_children_data__duplicate_test
 union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_children_data__duplicate_test ;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_consumer_master','select * from thawks_integration.TRATBL_sdl_th_sfmc_consumer_master__null_test
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_consumer_master__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_consumer_master_additional','select * from thawks_integration.TRATBL_sdl_th_sfmc_consumer_master_additional__null_test
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_consumer_master_additional__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_consumer_master_additional__duplicate_test ;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_open_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_open_data__null_test
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_open_data__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_open_data__duplicate_test ;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_unsubscribe_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_unsubscribe_data__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_unsubscribe_data__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_unsubscribe_data__duplicate_test ;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_consumerreach_711','select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_711__null_test
union all 
select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_711__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_jnj_consumerreach_sfm','select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_sfm__null_test
union all 
select * from thawks_integration.TRATBL_sdl_jnj_consumerreach_sfm__test_date_format_odd_eve;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_distributor_cn_reason','select * from thawks_integration.TRATBL_sdl_mds_th_distributor_cn_reason__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_distributor_cn_reason__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_distributor_customer_group','select * from thawks_integration.TRATBL_sdl_mds_th_ref_distributor_customer_group__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_distributor_customer_group__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_distributor_item_unit','select * from thawks_integration.TRATBL_sdl_mds_th_ref_distributor_item_unit__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_distributor_item_unit__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_ref_city','select * from thawks_integration.TRATBL_sdl_mds_th_ref_city__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_ref_city__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_cop','select * from thawks_integration.TRATBL_sdl_mds_th_cop__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_cop__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_gt_productive_call_target','select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_gt_productive_call_target__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_one_jnj_data','select * from thawks_integration.TRATBL_sdl_mds_th_one_jnj_data__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mds_th_one_jnj_data__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_th_htc_customer','select * from thawks_integration.TRATBL_sdl_mds_th_htc_customer__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_visit','select * from thawks_integration.TRATBL_sdl_la_gt_visit__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__null_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__date_plan_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__date_visi_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__visit_end_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__time_visi_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__visit_time_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_visit__id_sale_format_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_sfmc_bounce_data','select * from thawks_integration.TRATBL_sdl_th_sfmc_bounce_data__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_bounce_data__null_test union all 
select * from thawks_integration.TRATBL_sdl_th_sfmc_bounce_data__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_mt_price_data','select * from thawks_integration.TRATBL_sdl_th_mt_price_data__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_th_mt_price_data__null_test union all 
select * from thawks_integration.TRATBL_sdl_th_mt_price_data__format_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_schedule','select * from thawks_integration.TRATBL_sdl_la_gt_schedule__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_schedule__null_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_schedule__schedule_date_format_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_schedule__employee_id_format_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mym_gt_sales_report_fact','select * from thawks_integration.TRATBL_sdl_mym_gt_sales_report_fact__null_test 
union all 
select * from thawks_integration.TRATBL_sdl_mym_gt_sales_report_fact__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_mym_gt_sales_report_fact__test_format;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_inventory_fact','select * from thawks_integration.TRATBL_sdl_la_gt_inventory_fact__test_format_recdate 
union all 
select * from thawks_integration.TRATBL_sdl_la_gt_inventory_fact__test_format_expirydate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_sellout_fact','select * from thawks_integration.TRATBL_sdl_la_gt_sellout_fact__duplicate_test 
union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sellout_fact__test_format;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_route_detail','
select * from thawks_integration.TRATBL_sdl_la_gt_route_detail__null_test 
union all
select * from thawks_integration.TRATBL_sdl_la_gt_route_detail__duplicate_test 
union all
select * from thawks_integration.TRATBL_sdl_la_gt_route_detail__test_format_created_date
union all
select * from thawks_integration.TRATBL_sdl_la_gt_route_detail__test_format_sale_unit;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_cbd_gt_sales_report_fact','select * from thawks_integration.TRATBL_sdl_cbd_gt_sales_report_fact__null_test union all 
select * from thawks_integration.TRATBL_sdl_cbd_gt_sales_report_fact__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_cbd_gt_sales_report_fact__test_format union all 
select * from thawks_integration.TRATBL_sdl_cbd_gt_sales_report_fact__test_format_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_route','select * from thawks_integration.TRATBL_sdl_th_gt_route__null_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route__test_format union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route__test_date_format_odd_eve_leap;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_route_detail','select * from thawks_integration.TRATBL_sdl_th_gt_route_detail__null_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route_detail__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route_detail__test_format union all 
select * from thawks_integration.TRATBL_sdl_th_gt_route_detail__test_date_format_odd_eve_leap;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_th_gt_sales_order','select * from thawks_integration.TRATBL_sdl_th_gt_sales_order__null_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_sales_order__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_th_gt_sales_order__test_format union all 
select * from thawks_integration.TRATBL_sdl_th_gt_sales_order__test_date_format_odd_eve_leap;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_sales_order_fact','select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__null_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format_2 union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format_3 union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format_4 union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format_5 union all 
select * from thawks_integration.TRATBL_sdl_la_gt_sales_order_fact__test_format_6;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_la_gt_route_header','select * from thawks_integration.TRATBL_sdl_la_gt_route_header__null_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_route_header__duplicate_test union all 
select * from thawks_integration.TRATBL_sdl_la_gt_route_header__test_format union all 
select * from thawks_integration.TRATBL_sdl_la_gt_route_header__test_format_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_cbd_gt_inventory_report_fact','select * from thawks_integration.TRATBL_sdl_cbd_gt_inventory_report_fact__null_test union all 
select * from thawks_integration.TRATBL_sdl_cbd_gt_inventory_report_fact__duplicate_test;');
