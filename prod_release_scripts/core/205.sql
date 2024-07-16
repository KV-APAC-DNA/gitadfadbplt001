delete from PROD_DNA_CORE.CORE_INTEGRATION.DBTTEST_TABLES_METADATA where model='sdl_kr_dads_naver_search_channel';

insert into core_integration.dbtjobs_test_cdc_metadata values('pop6_master_load','101','(\'sdl_pop6_jp_exclusion\', \'sdl_pop6_jp_promotion_plans\', \'sdl_pop6_jp_promotions\', \'sdl_pop6_jp_tasks\', \'sdl_pop6_jp_sku_audits\', \'sdl_pop6_jp_executed_visits\', \'sdl_pop6_jp_product_attribute_audits\', \'sdl_pop6_jp_general_audits\', \'sdl_pop6_jp_displays\', \'sdl_pop6_jp_pop_lists\', \'sdl_pop6_jp_product_lists_allocation\', \'sdl_pop6_jp_pops\', \'sdl_pop6_jp_products\', \'sdl_pop6_hk_products\', \'sdl_pop6_hk_pops\', \'sdl_pop6_hk_users\', \'sdl_pop6_kr_products\', \'sdl_pop6_kr_product_lists_pops\', \'sdl_pop6_kr_pops\', \'sdl_pop6_kr_pop_lists\', \'sdl_pop6_kr_product_lists_allocation\', \'sdl_pop6_kr_product_lists_products\', \'sdl_pop6_kr_users\', \'sdl_pop6_tw_products\', \'sdl_pop6_tw_product_lists_pops\', \'sdl_pop6_tw_pops\', \'sdl_pop6_tw_displays\', \'sdl_pop6_tw_executed_visits\', \'sdl_pop6_tw_general_audits\', \'sdl_pop6_tw_pop_lists\', \'sdl_pop6_tw_product_lists_allocation\', \'sdl_pop6_tw_product_lists_products\', \'sdl_pop6_tw_promotions\', \'sdl_pop6_tw_tasks\', \'sdl_pop6_tw_users\', \'sdl_pop6_kr_display_plans\', \'sdl_pop6_kr_displays\', \'sdl_pop6_kr_exclusion\', \'sdl_pop6_kr_executed_visits\', \'sdl_pop6_kr_general_audits\', \'sdl_pop6_kr_product_attribute_audits\', \'sdl_pop6_kr_promotion_plans\', \'sdl_pop6_kr_promotions\', \'sdl_pop6_kr_sku_audits\', \'sdl_pop6_kr_tasks\', \'sdl_pop6_hk_executed_visits\', \'sdl_pop6_hk_sku_audits\', \'sdl_pop6_hk_product_attribute_audits\', \'sdl_pop6_hk_general_audits\', \'sdl_pop6_hk_displays\', \'sdl_pop6_hk_promotion_plans\', \'sdl_pop6_hk_promotions\', \'sdl_pop6_hk_tasks\', \'sdl_pop6_hk_exclusion\', \'sdl_pop6_tw_sku_audits\', \'sdl_pop6_tw_product_attribute_audits\', \'sdl_pop6_tw_promotion_plans\', \'sdl_pop6_tw_exclusion\', \'sdl_pop6_sg_product_attribute_audits\', \'sdl_pop6_sg_sku_audits\', \'sdl_pop6_sg_tasks\', \'sdl_pop6_sg_general_audits\', \'sdl_pop6_sg_displays\', \'sdl_pop6_sg_promotions\', \'sdl_pop6_sg_promotion_plans\', \'sdl_pop6_sg_executed_visits\', \'sdl_pop6_th_product_lists_pops\', \'sdl_pop6_th_pops\', \'sdl_pop6_th_displays\', \'sdl_pop6_th_executed_visits\', \'sdl_pop6_th_general_audits\', \'sdl_pop6_th_product_attribute_audits\', \'sdl_pop6_th_promotion_plans\', \'sdl_pop6_th_promotions\', \'sdl_pop6_th_rir_data\', \'sdl_pop6_th_sku_audits\', \'sdl_pop6_th_product_lists_allocation\', \'sdl_pop6_th_product_lists_products\', \'sdl_pop6_th_tasks\', \'sdl_pop6_th_users\', \'sdl_pop6_th_products\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('gt_sellout_daiso_price_master','109','(\'sdl_mds_kr_retailer_price_master\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('gt_sellout_lotte_logistics_yang_ju','110','(\'sdl_kr_lotte_logistics_yang_ju_gt_sellout\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('gt_sellout_otc','111','(\'sdl_mds_kr_sub_customer_master\', \'sdl_kr_coupang_customer_brand_trend\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('gt_sellout_nacf','112','(\'sdl_kr_nacf_gt_sellout\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('kr_gt_sellout','113','(\'sdl_mds_kr_sales_store_master\', \'sdl_mds_kr_retailer_price_master\', \'sdl_kr_bo_young_jong_hap_logistics_gt_sellout\', \'sdl_kr_da_in_sang_sa_gt_sellout\', \'sdl_kr_dongbu_lsd_gt_sellout\', \'sdl_kr_hyundai_gt_sellout\', \'sdl_kr_du_bae_ro_yu_tong_gt_sellout\', \'sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout\', \'sdl_kr_ju_hj_life_gt_sellout\', \'sdl_kr_jungseok_gt_sellout\',  \'sdl_kr_lotte_ak_gt_sellout\', \'sdl_kr_sfmc_naver_data_additional\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('j_kr_tp_tracker_data_ingestion','114','(\'sdl_kr_ecom_naver_sellout_temp\', \'sdl_mds_kr_cost_of_goods\', \'sdl_mds_kr_tp_target\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('tw_ims_distributor_standard_customer','115','(\'sdl_tw_ims_dstr_std_customer_107479_customer\', \'sdl_tw_ims_dstr_std_customer_107485_customer\', \'sdl_tw_ims_dstr_std_customer_107501_customer\', \'sdl_tw_ims_dstr_std_customer_107507_customer\', \'sdl_tw_ims_dstr_std_customer_107510_customer\', \'sdl_tw_ims_dstr_std_customer_116047_customer\', \'sdl_tw_ims_dstr_std_customer_120812_customer\', \'sdl_tw_ims_dstr_std_customer_122296_customer\', \'sdl_tw_ims_dstr_std_customer_123291_customer\', \'sdl_tw_ims_dstr_std_customer_131953_customer\', \'sdl_tw_ims_dstr_std_customer_132349_customer\', \'sdl_tw_ims_dstr_std_customer_132508_customer\', \'sdl_tw_ims_dstr_std_customer_135307_customer\', \'sdl_tw_ims_dstr_std_customer_135561_customer\', \'sdl_tw_ims_dstr_std_customer_107482_customer\', \'sdl_tw_ims_dstr_std_customer_107483_customer\', \'sdl_tw_ims_dstr_std_customer_132222_customer\', \'sdl_tw_ims_dstr_std_customer_136454_customer\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('tw_strategic_customer_hierarchy','116','(\'sdl_tw_strategic_cust_hier\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('na_ims_inv_mds_sdl_itg_load','117','(\'sdl_mds_tw_ims_dstr_customer_mapping\', \'sdl_mds_tw_ims_dstr_prod_price_map\', \'sdl_mds_hk_wingkeung_gt_msl_items\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('tw_ims_distributor_standard_sell_out','118','(\'sdl_tw_ims_dstr_std_sel_out_107482_sellout\', \'sdl_tw_ims_dstr_std_sel_out_107483_sellout\', \'sdl_tw_ims_dstr_std_sel_out_107485_sellout\', \'sdl_tw_ims_dstr_std_sel_out_107501_sellout\', \'sdl_tw_ims_dstr_std_sel_out_107510_sellout\', \'sdl_tw_ims_dstr_std_sel_out_116047_sellout\', \'sdl_tw_ims_dstr_std_sel_out_122296_sellout\', \'sdl_tw_ims_dstr_std_sel_out_123291_sellout\', \'sdl_tw_ims_dstr_std_sel_out_131953_sellout\', \'sdl_tw_ims_dstr_std_sel_out_132222_sellout\', \'sdl_tw_ims_dstr_std_sel_out_132349_sellout\', \'sdl_tw_ims_dstr_std_sel_out_132508_sellout\', \'sdl_tw_ims_dstr_std_sel_out_134478_sellout\', \'sdl_tw_ims_dstr_std_sel_out_135307_sellout\', \'sdl_tw_ims_dstr_std_sel_out_135561_sellout\', \'sdl_tw_ims_dstr_std_sel_out_136454_sellout\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('tw_ims_distributor_standard_stock','119','(\'sdl_tw_ims_dstr_std_stock_107479_stock\', \'sdl_tw_ims_dstr_std_stock_107482_stock\', \'sdl_tw_ims_dstr_std_stock_107483_stock\', \'sdl_tw_ims_dstr_std_stock_107485_stock\', \'sdl_tw_ims_dstr_std_stock_107501_stock\', \'sdl_tw_ims_dstr_std_stock_107507_stock\', \'sdl_tw_ims_dstr_std_stock_107510_stock\', \'sdl_tw_ims_dstr_std_stock_116047_stock\', \'sdl_tw_ims_dstr_std_stock_120812_stock\', \'sdl_tw_ims_dstr_std_stock_122296_stock\', \'sdl_tw_ims_dstr_std_stock_123291_stock\', \'sdl_tw_ims_dstr_std_stock_131953_stock\', \'sdl_tw_ims_dstr_std_stock_132222_stock\', \'sdl_tw_ims_dstr_std_stock_132349_stock\', \'sdl_tw_ims_dstr_std_stock_132508_stock\', \'sdl_tw_ims_dstr_std_stock_134478_pxstock\', \'sdl_tw_ims_dstr_std_stock_134478_pxStore\', \'sdl_tw_ims_dstr_std_stock_134478_stock\', \'sdl_tw_ims_dstr_std_stock_135307_stock\', \'sdl_tw_ims_dstr_std_stock_135561_stock\', \'sdl_tw_ims_dstr_std_stock_136454_stock\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('gt_sellout_nh','120','(\'sdl_kr_nh_gt_sellout\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('j_kr_nts_target_load','121','(\'sdl_mds_kr_target\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('j_tw_sell_in_forecast_transaction_data_ingestion','122','(\'sdl_tw_bp_forecast\', \'sdl_tw_bu_forecast_sku\', \'sdl_tw_bu_forecast_prod_hier\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('my_perfect_store','108','(\'sdl_my_perfectstore_sku_mst\', \'sdl_my_perfectstore_osa\', \'sdl_my_perfectstore_outlet_mst\', \'sdl_my_perfectstore_sos\', \'sdl_my_perfectstore_oos\', \'sdl_my_perfectstore_promocomp\', \'sdl_mds_my_ps_msl\', \'sdl_mds_my_ps_targets\', \'sdl_mds_my_ps_weights\')','0');

insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_kr_sfmc_naver_data_additional','select * from ntawks_integration.TRATBL_sdl_kr_sfmc_naver_data_additional__duplicate_test union all  select * from ntawks_integration.TRATBL_sdl_kr_sfmc_naver_data_additional__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_exclusion','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_exclusion__lookup_test_1
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_exclusion__lookup_test_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_promotion_plans','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_promotion_plans__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_promotion_plans__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_promotions','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_promotions__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_promotions__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_tasks','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_tasks__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_tasks__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_sku_audits','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_sku_audits__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_sku_audits__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_executed_visits','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_executed_visits__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_executed_visits__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_product_attribute_audits','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_product_attribute_audits__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_product_attribute_audits__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_general_audits','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_general_audits__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_general_audits__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_displays','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_displays__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_displays__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_pop_lists','union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_pop_lists__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_pop_lists__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_product_lists_allocation','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_product_lists_allocation__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_product_lists_allocation__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_pops','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_pops__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_jp_products','select * from jpnwks_integration.TRATBL_sdl_pop6_jp_products__null_test
union all
select * from jpnwks_integration.TRATBL_sdl_pop6_jp_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_products','select * from ntawks_integration.TRATBL_sdl_pop6_hk_products__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_pops','select * from ntawks_integration.TRATBL_sdl_pop6_hk_pops__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_users','select * from ntawks_integration.TRATBL_sdl_pop6_hk_users__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_users__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_products','select * from ntawks_integration.TRATBL_sdl_pop6_kr_products__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_product_lists_pops','select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_pops__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_pops','select * from ntawks_integration.TRATBL_sdl_pop6_kr_pops__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_pop_lists','select * from ntawks_integration.TRATBL_sdl_pop6_kr_pop_lists__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_pop_lists__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_product_lists_allocation','select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_allocation__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_allocation__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_product_lists_products','select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_products__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_lists_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_users','select * from ntawks_integration.TRATBL_sdl_pop6_kr_users__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_users__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_products','select * from ntawks_integration.TRATBL_sdl_pop6_tw_products__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_product_lists_pops','select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_pops__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_pops','select * from ntawks_integration.TRATBL_sdl_pop6_tw_pops__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_displays','select * from ntawks_integration.TRATBL_sdl_pop6_tw_displays__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_displays__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_executed_visits','select * from ntawks_integration.TRATBL_sdl_pop6_tw_executed_visits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_executed_visits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_general_audits','select * from ntawks_integration.TRATBL_sdl_pop6_tw_general_audits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_general_audits__duplicate_test;
');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_pop_lists','select * from ntawks_integration.TRATBL_sdl_pop6_tw_pop_lists__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_pop_lists__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_product_lists_allocation','select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_allocation__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_allocation__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_product_lists_products','select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_products__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_lists_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_promotions','select * from ntawks_integration.TRATBL_sdl_pop6_tw_promotions__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_promotions__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_tasks','select * from ntawks_integration.TRATBL_sdl_pop6_tw_tasks__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_tasks__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_users','select * from ntawks_integration.TRATBL_sdl_pop6_tw_users__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_users__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_display_plans','select * from ntawks_integration.TRATBL_sdl_pop6_kr_display_plans__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_display_plans__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_displays','select * from ntawks_integration.TRATBL_sdl_pop6_kr_displays__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_displays__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_exclusion','select * from ntawks_integration.TRATBL_sdl_pop6_kr_exclusion__lookup_test_1
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_exclusion__lookup_test_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_executed_visits','select * from ntawks_integration.TRATBL_sdl_pop6_kr_executed_visits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_executed_visits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_general_audits','select * from ntawks_integration.TRATBL_sdl_pop6_kr_general_audits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_general_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_product_attribute_audits','select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_attribute_audits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_product_attribute_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_promotion_plans','select * from ntawks_integration.TRATBL_sdl_pop6_kr_promotion_plans__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_promotion_plans__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_promotions','select * from ntawks_integration.TRATBL_sdl_pop6_kr_promotions__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_promotions__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_sku_audits','select * from ntawks_integration.TRATBL_sdl_pop6_kr_sku_audits__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_sku_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_kr_tasks','select * from ntawks_integration.TRATBL_sdl_pop6_kr_tasks__null_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_kr_tasks__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_executed_visits','select * from ntawks_integration.TRATBL_sdl_pop6_hk_executed_visits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_executed_visits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_sku_audits','select * from ntawks_integration.TRATBL_sdl_pop6_hk_sku_audits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_sku_audits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_product_attribute_audits','select * from ntawks_integration.TRATBL_sdl_pop6_hk_product_attribute_audits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_product_attribute_audits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_general_audits','select * from ntawks_integration.TRATBL_sdl_pop6_hk_general_audits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_general_audits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_displays','select * from ntawks_integration.TRATBL_sdl_pop6_hk_displays__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_displays__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_promotion_plans','select * from ntawks_integration.TRATBL_sdl_pop6_hk_promotion_plans__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_promotion_plans__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_promotions','select * from ntawks_integration.TRATBL_sdl_pop6_hk_promotions__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_promotions__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_tasks','select * from ntawks_integration.TRATBL_sdl_pop6_hk_tasks__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_tasks__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_hk_exclusion','select * from ntawks_integration.TRATBL_sdl_pop6_hk_exclusion__lookup_test_1
union all
select * from ntawks_integration.TRATBL_sdl_pop6_hk_exclusion__lookup_test_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_sku_audits','select * from ntawks_integration.TRATBL_sdl_pop6_tw_sku_audits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_sku_audits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_product_attribute_audits','select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_attribute_audits__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_product_attribute_audits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_promotion_plans','select * from ntawks_integration.TRATBL_sdl_pop6_tw_promotion_plans__duplicate_test
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_promotion_plans__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_tw_exclusion','select * from ntawks_integration.TRATBL_sdl_pop6_tw_exclusion__lookup_test_1
union all
select * from ntawks_integration.TRATBL_sdl_pop6_tw_exclusion__lookup_test_2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_product_attribute_audits','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_product_attribute_audits__null_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_product_attribute_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_sku_audits','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_sku_audits__null_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_sku_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_tasks','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_tasks__null_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_tasks__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_general_audits','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_general_audits__null_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_general_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_displays','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_displays__duplicate_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_displays__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_promotions','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_promotions__duplicate_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_promotions__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_promotion_plans','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_promotion_plans__duplicate_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_promotion_plans__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_sg_executed_visits','select * from sgpwks_integration.TRATBL_sdl_pop6_sg_executed_visits__duplicate_test
union all
select * from sgpwks_integration.TRATBL_sdl_pop6_sg_executed_visits__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_product_lists_pops','select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_pops__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_pops','select * from thawks_integration.TRATBL_sdl_pop6_th_pops__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_pops__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_displays','select * from thawks_integration.TRATBL_sdl_pop6_th_displays__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_displays__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_executed_visits','select * from thawks_integration.TRATBL_sdl_pop6_th_executed_visits__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_executed_visits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_general_audits','select * from thawks_integration.TRATBL_sdl_pop6_th_general_audits__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_general_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_product_attribute_audits','select * from thawks_integration.TRATBL_sdl_pop6_th_product_attribute_audits__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_product_attribute_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_promotion_plans','select * from thawks_integration.TRATBL_sdl_pop6_th_promotion_plans__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_promotion_plans__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_promotions','select * from thawks_integration.TRATBL_sdl_pop6_th_promotions__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_promotions__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_rir_data','select * from thawks_integration.TRATBL_sdl_pop6_th_rir_data__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_rir_data__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_sku_audits','select * from thawks_integration.TRATBL_sdl_pop6_th_sku_audits__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_sku_audits__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_product_lists_allocation','select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_allocation__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_allocation__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_product_lists_products','select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_products__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_product_lists_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_tasks','select * from thawks_integration.TRATBL_sdl_pop6_th_tasks__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_tasks__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_users','select * from thawks_integration.TRATBL_sdl_pop6_th_users__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_users__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_pop6_th_products','select * from thawks_integration.TRATBL_sdl_pop6_th_products__null_test
union all
select * from thawks_integration.TRATBL_sdl_pop6_th_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_sku_mst','select * from myswks_integration.TRATBL_sdl_my_perfectstore_sku_mst__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_osa','select * from myswks_integration.TRATBL_sdl_my_perfectstore_osa__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_outlet_mst','select * from myswks_integration.TRATBL_sdl_my_perfectstore_outlet_mst__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_sos','select * from myswks_integration.TRATBL_sdl_my_perfectstore_sos__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_oos','select * from myswks_integration.TRATBL_sdl_my_perfectstore_oos__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_my_perfectstore_promocomp','select * from myswks_integration.TRATBL_sdl_my_perfectstore_promocomp__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_my_ps_msl','select * from myswks_integration.TRATBL_sdl_mds_my_ps_msl__null_test
union all
select * from myswks_integration.TRATBL_sdl_mds_my_ps_msl__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_my_ps_targets','select * from myswks_integration.TRATBL_sdl_mds_my_ps_targets__null_test
union all
select * from myswks_integration.TRATBL_sdl_mds_my_ps_targets__test_duplicate;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_my_ps_weights','select * from myswks_integration.TRATBL_sdl_mds_my_ps_weights__null_test
union all
select * from myswks_integration.TRATBL_sdl_mds_my_ps_weights__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_kr_dads_naver_search_channel','select * from ntawks_integration.TRATBL_sdl_kr_dads_naver_search_channel__null_test union all select * from ntawks_integration.TRATBL_sdl_kr_dads_naver_search_channel__format_test;');
