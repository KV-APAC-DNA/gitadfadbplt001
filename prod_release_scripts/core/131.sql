insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_cpg_calls','select * from phlwks_integration.TRATBL_sdl_ph_cpg_calls__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_ecommerce_offtake_acommerce','select * from phlwks_integration.TRATBL_sdl_ph_ecommerce_offtake_acommerce__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ps_weights','select * from phlwks_integration.TRATBL_sdl_mds_ph_ps_weights__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_ps_weights__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_msl_hdr','select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_hdr__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_hdr__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_hdr__format_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_hdr__format_test2;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_msl_dtls','select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_dtls__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_msl_dtls__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ps_targets','select * from phlwks_integration.TRATBL_sdl_mds_ph_ps_targets__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_ps_targets__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_distributor_supervisors','select * from phlwks_integration.TRATBL_sdl_mds_ph_distributor_supervisors__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_distributor_supervisors__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_repbrand','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_repbrand__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_repbrand__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_bounce_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_bounce_data__format_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_bounce_data__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_bounce_data__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_dms_sellout_sales_fact','select * from phlwks_integration.TRATBL_sdl_ph_dms_sellout_sales_fact__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_dms_sellout_sales_fact__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_dms_sellout_stock_fact','select * from phlwks_integration.TRATBL_sdl_ph_dms_sellout_stock_fact__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_dms_sellout_stock_fact__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveyanswers','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyanswers__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyanswers__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveycustomers','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveycustomers__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveycustomers__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveynotes','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveynotes__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveynotes__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_distributors','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_distributors__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_distributors__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_npi_peg_item','select * from phlwks_integration.TRATBL_sdl_mds_ph_npi_peg_item__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_npi_peg_item__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_npi_sales_groupings','select * from phlwks_integration.TRATBL_sdl_mds_ph_npi_sales_groupings__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_npi_sales_groupings__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_targets_by_account_and_skus','select * from phlwks_integration.TRATBL_sdl_mds_ph_targets_by_account_and_skus__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_targets_by_account_and_skus__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_distributor_product','select * from phlwks_integration.TRATBL_sdl_mds_ph_distributor_product__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_distributor_product__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_repfranchise','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_repfranchise__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_repfranchise__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_lav_customer','select * from phlwks_integration.TRATBL_sdl_mds_ph_lav_customer__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_lav_customer__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_pos_pricelist','select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_pricelist__null_test union all 
select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_pricelist__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_gt_customer','select * from phlwks_integration.TRATBL_sdl_mds_ph_gt_customer__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_gt_customer__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_pos_primary_sold_to','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_pos_primary_sold_to__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_pos_primary_sold_to__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_pos_product','select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_product__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_product__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_parent_customer','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_parent_customer__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_parent_customer__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_targets_by_national_and_skus','select * from phlwks_integration.TRATBL_sdl_mds_ph_targets_by_national_and_skus__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_targets_by_national_and_skus__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_retailer_soldto_map','select * from phlwks_integration.TRATBL_sdl_mds_ph_retailer_soldto_map__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_retailer_soldto_map__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_non_ise_weights','select * from phlwks_integration.TRATBL_sdl_mds_ph_non_ise_weights__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_mds_ph_non_ise_weights__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveyisehdr','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyisehdr__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyisehdr__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveyisequestion','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyisequestion__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveyisequestion__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveychoices','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveychoices__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveychoices__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_acctexec','select * from phlwks_integration.TRATBL_sdl_ph_tbl_acctexec__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_acctexec__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_surveycpg','select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveycpg__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_surveycpg__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_tbl_isebranchmaster','select * from phlwks_integration.TRATBL_sdl_ph_tbl_isebranchmaster__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_tbl_isebranchmaster__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_clobotics_task_raw_data','select * from phlwks_integration.TRATBL_sdl_ph_clobotics_task_raw_data__null_test union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_task_raw_data__format_test1 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_task_raw_data__format_test2 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_task_raw_data__format_test3;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_clobotics_survey_data','select * from phlwks_integration.TRATBL_sdl_ph_clobotics_survey_data__null_test union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_survey_data__format_test1 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_survey_data__format_test2 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_survey_data__format_test3;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_clobotics_store_raw_data','select * from phlwks_integration.TRATBL_sdl_ph_clobotics_store_raw_data__null_test union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_store_raw_data__format_test1 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_store_raw_data__format_test2 union all select * from phlwks_integration.TRATBL_sdl_ph_clobotics_store_raw_data__format_test3;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_bp_trgt','select * from phlwks_integration.TRATBL_sdl_ph_bp_trgt__duplicate_test union all select * from phlwks_integration.TRATBL_sdl_ph_bp_trgt__null_test union all select * from phlwks_integration.TRATBL_sdl_ph_bp_trgt__format_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_click_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_null__ff union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_duplicate__ff union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_lookup__ff');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_complaint_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_complaint_data__test_null__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_complaint_data__test_duplicate__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_complaint_data__test_lookup__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_complaint_data_format_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_sent_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_sent_data__test_null__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_sent_data__test_duplicate__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_sent_data__test_lookup__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_sent_data_format_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_unsubscribe_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_unsubscribe_data__test_null__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_unsubscribe_data__test_duplicate__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_unsubscribe_data__test_lookup__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_unsubscribe_data_format_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_children_data','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_children_data__test_null__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_children_data__test_duplicate__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_children_data__test_lookup__ff');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_consumer_master','select * from phlwks_integration.TRATBL_sdl_ph_sfmc_consumer_master__test_null__ff
union all
select * from phlwks_integration.TRATBL_sdl_ph_sfmc_consumer_master__test_duplicate__ff');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_lav_product','select * from phlwks_integration.TRATBL_sdl_mds_ph_lav_product__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_lav_product__duplicate_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_pos_customers','select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_customers__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_pos_customers__duplicate_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ph_ref_rka_master','select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_rka_master__null_test
union all
select * from phlwks_integration.TRATBL_sdl_mds_ph_ref_rka_master__duplicate_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_as_watsons_inventory','select * from phlwks_integration.TRATBL_sdl_ph_as_watsons_inventory__test_null__ff 
union all
select * from phlwks_integration.TRATBL_sdl_ph_as_watsons_inventory__test_duplicate__ff 
union all
select * from phlwks_integration.TRATBL_sdl_ph_as_watsons_inventory__test_lookup__ff');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_sm_goods','select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__null_test 
union all select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_CUST_CD
union all select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_JNJ_PC_PER_CUST_UNIT
union all select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_LST_PRICE_UNIT
union all select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_PL_JJ_MNTH_ID');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_robinsons_ds','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_ds__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_ds__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_ds__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_waltermart','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_waltermart__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_waltermart__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_waltermart__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_waltermart__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_svi_smc','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_svi_smc__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_svi_smc__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_svi_smc__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_svi_smc__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_shm','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_shm__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_shm__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_shm__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_shm__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_robinsons_sm','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_sm__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_sm__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_sm__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_robinsons_sm__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_landmark_sm','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_sm__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_sm__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_sm__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_sm__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_super_8','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_super_8__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_super_8__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_super_8__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_super_8__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_puregold','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_puregold__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_puregold__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_puregold__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_puregold__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_landmark_ds','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_ds__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_ds__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_ds__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_landmark_ds__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_non_ise_rustans','select * from phlwks_integration.TRATBL_sdl_ph_non_ise_rustans__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_rustans__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_rustans__test_date_format_odd_eve_leap
union all
select * from phlwks_integration.TRATBL_sdl_ph_non_ise_rustans__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_iop_trgt','select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__duplicate_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__format_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__lookup_test_brand
union all
select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__lookup_test_segment
union all
select * from phlwks_integration.TRATBL_sdl_ph_iop_trgt__lookup_test_customer_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_sfmc_open_data','select * from phlwks_integration.tratbl_sdl_ph_sfmc_open_data__test_null__ff
union all
select * from phlwks_integration.tratbl_sdl_ph_sfmc_open_data__test_duplicate__ff
union all
select * from phlwks_integration.tratbl_sdl_ph_sfmc_open_data__test_lookup__ff
union all
select * from phlwks_integration.tratbl_sdl_ph_sfmc_open_data_format_test');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_robinsons','select * from phlwks_integration.TRATBL_sdl_ph_pos_robinsons__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_robinsons__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_robinsons__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_mercury','select * from phlwks_integration.TRATBL_sdl_ph_pos_mercury__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_mercury__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_mercury__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_watsons','select * from phlwks_integration.tratbl_sdl_ph_pos_watsons__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_watsons__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_watsons__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_rustans','select * from phlwks_integration.tratbl_sdl_ph_pos_rustans__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_rustans__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_rustans__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_south_star','select * from phlwks_integration.tratbl_sdl_ph_pos_south_star__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_south_star__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_south_star__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_waltermart','select * from phlwks_integration.tratbl_sdl_ph_pos_waltermart__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_waltermart__lookup_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_waltermart__lookup_test_store_code;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_dyna_sales','select * from phlwks_integration.tratbl_sdl_ph_pos_dyna_sales__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_dyna_sales__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_711','select * from phlwks_integration.tratbl_sdl_ph_pos_711__null_test
union all
select * from phlwks_integration.TRATBL_sdl_ph_pos_711__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_ph_pos_puregold','select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_SAP_ITEM_CD
union all
select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_JNJ_PC_PER_CUST_UNIT
union all
select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_LST_PRICE_UNIT
union all
select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_PL_JJ_MNTH_ID
union all
select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__lookup_sm_CUST_CD
union all
select * from phlwks_integration.tratbl_sdl_ph_pos_sm_goods__null_test;');


insert into core_integration.dbtjobs_test_cdc_metadata values('ph_dms','67','(\'sdl_ph_cpg_calls\', \'sdl_ph_dms_sellout_sales_fact\', \'sdl_ph_dms_sellout_stock_fact\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_mds_itg_refresh','68','(\'sdl_mds_ph_targets_by_national_and_skus\', \'sdl_mds_ph_gt_customer\', \'sdl_mds_ph_ref_pos_primary_sold_to\', \'sdl_mds_ph_pos_product\', \'sdl_mds_ph_ref_parent_customer\', \'sdl_mds_ph_npi_peg_item\', \'sdl_mds_ph_npi_sales_groupings\', \'sdl_mds_ph_targets_by_account_and_skus\', \'sdl_mds_ph_distributor_product\', \'sdl_mds_ph_ref_repfranchise\', \'sdl_mds_ph_lav_customer\', \'sdl_mds_ph_pos_pricelist\', \'sdl_mds_ph_ps_weights\', \'sdl_mds_ph_msl_hdr\', \'sdl_mds_ph_msl_dtls\', \'sdl_mds_ph_ps_targets\', \'sdl_mds_ph_distributor_supervisors\', \'sdl_mds_ph_ref_repbrand\', \'sdl_mds_ph_lav_product\', \'sdl_mds_ph_pos_customers\', \'sdl_mds_ph_ref_rka_master\', \'sdl_mds_ph_retailer_soldto_map\', \'sdl_mds_ph_non_ise_weights\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_pos','69','(\'sdl_ph_pos_robinsons\', \'sdl_ph_pos_mercury\', \'sdl_ph_pos_watsons\', \'sdl_ph_pos_rustans\', \'sdl_ph_pos_south_star\', \'sdl_ph_pos_waltermart\', \'sdl_ph_pos_dyna_sales\', \'sdl_ph_pos_puregold\',\'sdl_ph_pos_711\',\'sdl_ph_pos_sm_goods\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_perfectstore','70','(\'sdl_ph_tbl_isebranchmaster\', \'sdl_ph_clobotics_task_raw_data\', \'sdl_ph_clobotics_store_raw_data\', \'sdl_ph_clobotics_survey_data\', \'sdl_ph_tbl_surveyisehdr\', \'sdl_ph_tbl_surveyisequestion\', \'sdl_ph_tbl_surveychoices\', \'sdl_ph_tbl_acctexec\', \'sdl_ph_tbl_surveycpg\', \'sdl_ph_tbl_surveyanswers\', \'sdl_ph_tbl_surveycustomers\', \'sdl_ph_tbl_surveynotes\', \'sdl_mds_ph_ref_distributors\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_sfmc','71','(\'sdl_ph_sfmc_bounce_data\', \'sdl_ph_sfmc_click_data\', \'sdl_ph_sfmc_complaint_data\', \'sdl_ph_sfmc_sent_data\', \'sdl_ph_sfmc_unsubscribe_data\', \'sdl_ph_sfmc_children_data\', \'sdl_ph_sfmc_open_data\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_crm','72','(\'sdl_ph_sfmc_consumer_master\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_iop_target','73','(\'sdl_ph_iop_trgt\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_bp_target','74','(\'sdl_ph_bp_trgt\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_non_pos','75','(\'sdl_ph_non_ise_robinsons_ds\', \'sdl_ph_non_ise_waltermart\', \'sdl_ph_non_ise_svi_smc\', \'sdl_ph_non_ise_shm\', \'sdl_ph_non_ise_robinsons_sm\', \'sdl_ph_non_ise_landmark_sm\', \'sdl_ph_non_ise_super_8\', \'sdl_ph_non_ise_puregold\', \'sdl_ph_non_ise_landmark_ds\', \'sdl_ph_non_ise_rustans\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('j_rg_watsons_inv_ingestion','76','(\'sdl_ph_as_watsons_inventory\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('j_os_dna_sdl_to_edw_wm_sales','77','(\'sdl_ph_pos_waltermart\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('ph_acommerce_data_ingestion','78','(\'sdl_ph_ecommerce_offtake_acommerce\')','0');

