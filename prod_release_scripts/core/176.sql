CREATE OR REPLACE TABLE aspedw_integration.edw_market_mirror_fact		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_edw.edw_market_mirror_fact
(
	utag VARCHAR(600)  		--//  ENCODE lzo
	,tag VARCHAR(600)  		--//  ENCODE lzo
	,market VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,channel_type VARCHAR(100)  		--//  ENCODE lzo
	,channel_source VARCHAR(100)  		--//  ENCODE lzo
	,channel_group VARCHAR(100)  		--//  ENCODE lzo
	,channel_description VARCHAR(1000)  		--//  ENCODE lzo
	,category VARCHAR(100)  		--//  ENCODE lzo
	,real_date DATE  		--//  ENCODE az64
	,period VARCHAR(50)  		--//  ENCODE lzo
	,time_period DATE  		--//  ENCODE az64
	,time_period_type VARCHAR(50)  		--//  ENCODE lzo
	,date_type VARCHAR(100)  		--//  ENCODE lzo
	,last_period DATE  		--//  ENCODE az64
	,supplier VARCHAR(100)  		--//  ENCODE lzo
	,source VARCHAR(100)  		--//  ENCODE lzo
	,product VARCHAR(3000)  		--//  ENCODE lzo
	,segment VARCHAR(300)  		--//  ENCODE lzo
	,manufacturer VARCHAR(300)  		--//  ENCODE lzo
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,sub_brand VARCHAR(300)  		--//  ENCODE lzo
	,packsize DOUBLE PRECISION
	,npd VARCHAR(10)  		--//  ENCODE lzo
	,launch_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,base_flag VARCHAR(10)  		--//  ENCODE lzo
	,exclude_flag VARCHAR(10)  		--//  ENCODE lzo
	,cat_unit_type VARCHAR(50)  		--//  ENCODE lzo
	,cat_selling_unit VARCHAR(50)  		--//  ENCODE lzo
	,unit VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_1_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_2_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_3 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_3_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_4 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_4_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_5 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_5_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_6 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_6_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_7 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_7_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_8 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_8_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_9 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_9_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_10 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_10_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_11 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_11_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_12 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_12_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_13 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_13_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_14 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_14_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_15 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_15_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_16 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_16_desc VARCHAR(200)  		--//  ENCODE lzo
	,attribute_17 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_17_desc VARCHAR(200)  		--//  ENCODE lzo
	,gph_gfo VARCHAR(250)  		--//  ENCODE lzo
	,gph_brand VARCHAR(250)  		--//  ENCODE lzo
	,gph_sub_brand VARCHAR(250)  		--//  ENCODE lzo
	,gph_variant VARCHAR(250)  		--//  ENCODE lzo
	,gph_need_state VARCHAR(250)  		--//  ENCODE lzo
	,gph_category VARCHAR(250)  		--//  ENCODE lzo
	,gph_sub_category VARCHAR(250)  		--//  ENCODE lzo
	,gph_segment VARCHAR(250)  		--//  ENCODE lzo
	,gph_sub_segment VARCHAR(250)  		--//  ENCODE lzo
	,ggh_country VARCHAR(250)  		--//  ENCODE lzo
	,ggh_region VARCHAR(250)  		--//  ENCODE lzo
	,ggh_cluster VARCHAR(250)  		--//  ENCODE lzo
	,ggh_sub_cluster VARCHAR(200)  		--//  ENCODE lzo
	,ggh_country_3_cd VARCHAR(250)  		--//  ENCODE lzo
	,ggh_country_2_cd VARCHAR(250)  		--//  ENCODE lzo
	,ggh_market_type VARCHAR(200)  		--//  ENCODE lzo
	,sku_value_sales_lc DOUBLE PRECISION
	,cat_value_sales_lc DOUBLE PRECISION
	,seg_value_sales_lc DOUBLE PRECISION
	,mnf_value_sales_lc DOUBLE PRECISION
	,mnf_seg_value_sales_lc DOUBLE PRECISION
	,brd_value_sales_lc DOUBLE PRECISION
	,brd_seg_value_sales_lc DOUBLE PRECISION
	,sku_value_sales_usd DOUBLE PRECISION
	,cat_value_sales_usd DOUBLE PRECISION
	,seg_value_sales_usd DOUBLE PRECISION
	,mnf_value_sales_usd DOUBLE PRECISION
	,mnf_seg_value_sales_usd DOUBLE PRECISION
	,brd_value_sales_usd DOUBLE PRECISION
	,brd_seg_value_sales_usd DOUBLE PRECISION
	,sku_promoted_value_sales_lc DOUBLE PRECISION
	,cat_promoted_value_sales_lc DOUBLE PRECISION
	,seg_promoted_value_sales_lc DOUBLE PRECISION
	,mnf_promoted_value_sales_lc DOUBLE PRECISION
	,mnf_seg_promoted_value_sales_lc DOUBLE PRECISION
	,brd_promoted_value_sales_lc DOUBLE PRECISION
	,brd_seg_promoted_value_sales_lc DOUBLE PRECISION
	,sku_promoted_value_sales_usd DOUBLE PRECISION
	,cat_promoted_value_sales_usd DOUBLE PRECISION
	,seg_promoted_value_sales_usd DOUBLE PRECISION
	,mnf_promoted_value_sales_usd DOUBLE PRECISION
	,mnf_seg_promoted_value_sales_usd DOUBLE PRECISION
	,brd_promoted_value_sales_usd DOUBLE PRECISION
	,brd_seg_promoted_value_sales_usd DOUBLE PRECISION
	,sku_base_value_sales_lc DOUBLE PRECISION
	,cat_base_value_sales_lc DOUBLE PRECISION
	,seg_base_value_sales_lc DOUBLE PRECISION
	,mnf_base_value_sales_lc DOUBLE PRECISION
	,mnf_seg_base_value_sales_lc DOUBLE PRECISION
	,brd_base_value_sales_lc DOUBLE PRECISION
	,brd_seg_base_value_sales_lc DOUBLE PRECISION
	,sku_base_value_sales_usd DOUBLE PRECISION
	,cat_base_value_sales_usd DOUBLE PRECISION
	,seg_base_value_sales_usd DOUBLE PRECISION
	,mnf_base_value_sales_usd DOUBLE PRECISION
	,mnf_seg_base_value_sales_usd DOUBLE PRECISION
	,brd_base_value_sales_usd DOUBLE PRECISION
	,brd_seg_base_value_sales_usd DOUBLE PRECISION
	,sku_unit_sales DOUBLE PRECISION
	,cat_unit_sales DOUBLE PRECISION
	,seg_unit_sales DOUBLE PRECISION
	,mnf_unit_sales DOUBLE PRECISION
	,mnf_seg_unit_sales DOUBLE PRECISION
	,brd_unit_sales DOUBLE PRECISION
	,brd_seg_unit_sales DOUBLE PRECISION
	,sku_promoted_unit_sales DOUBLE PRECISION
	,cat_promoted_unit_sales DOUBLE PRECISION
	,seg_promoted_unit_sales DOUBLE PRECISION
	,mnf_promoted_unit_sales DOUBLE PRECISION
	,mnf_seg_promoted_unit_sales DOUBLE PRECISION
	,brd_promoted_unit_sales DOUBLE PRECISION
	,brd_seg_promoted_unit_sales DOUBLE PRECISION
	,sku_base_unit_sales DOUBLE PRECISION
	,cat_base_unit_sales DOUBLE PRECISION
	,seg_base_unit_sales DOUBLE PRECISION
	,mnf_base_unit_sales DOUBLE PRECISION
	,mnf_seg_base_unit_sales DOUBLE PRECISION
	,brd_base_unit_sales DOUBLE PRECISION
	,brd_seg_base_unit_sales DOUBLE PRECISION
	,sku_volume_sales DOUBLE PRECISION
	,cat_volume_sales DOUBLE PRECISION
	,seg_volume_sales DOUBLE PRECISION
	,mnf_volume_sales DOUBLE PRECISION
	,mnf_seg_volume_sales DOUBLE PRECISION
	,brd_volume_sales DOUBLE PRECISION
	,brd_seg_volume_sales DOUBLE PRECISION
	,sku_promoted_volume_sales DOUBLE PRECISION
	,cat_promoted_volume_sales DOUBLE PRECISION
	,seg_promoted_volume_sales DOUBLE PRECISION
	,mnf_promoted_volume_sales DOUBLE PRECISION
	,mnf_seg_promoted_volume_sales DOUBLE PRECISION
	,brd_promoted_volume_sales DOUBLE PRECISION
	,brd_seg_promoted_volume_sales DOUBLE PRECISION
	,sku_base_volume_sales DOUBLE PRECISION
	,cat_base_volume_sales DOUBLE PRECISION
	,seg_base_volume_sales DOUBLE PRECISION
	,mnf_base_volume_sales DOUBLE PRECISION
	,mnf_seg_base_volume_sales DOUBLE PRECISION
	,brd_base_volume_sales DOUBLE PRECISION
	,brd_seg_base_volume_sales DOUBLE PRECISION
	,sku_numeric_distribution DOUBLE PRECISION
	,cat_numeric_distribution DOUBLE PRECISION
	,seg_numeric_distribution DOUBLE PRECISION
	,mnf_numeric_distribution DOUBLE PRECISION
	,mnf_seg_numeric_distribution DOUBLE PRECISION
	,brd_numeric_distribution DOUBLE PRECISION
	,brd_seg_numeric_distribution DOUBLE PRECISION
	,sku_weighted_distribution DOUBLE PRECISION
	,cat_weighted_distribution DOUBLE PRECISION
	,seg_weighted_distribution DOUBLE PRECISION
	,mnf_weighted_distribution DOUBLE PRECISION
	,mnf_seg_weighted_distribution DOUBLE PRECISION
	,brd_weighted_distribution DOUBLE PRECISION
	,brd_seg_weighted_distribution DOUBLE PRECISION
	,sku_cwd_feature_display_pct DOUBLE PRECISION
	,cat_cwd_feature_display_pct DOUBLE PRECISION
	,seg_cwd_feature_display_pct DOUBLE PRECISION
	,mnf_cwd_feature_display_pct DOUBLE PRECISION
	,mnf_seg_cwd_feature_display_pct DOUBLE PRECISION
	,brd_cwd_feature_display_pct DOUBLE PRECISION
	,brd_seg_cwd_feature_display_pct DOUBLE PRECISION
	,sku_cwd_display_pct DOUBLE PRECISION
	,cat_cwd_display_pct DOUBLE PRECISION
	,seg_cwd_display_pct DOUBLE PRECISION
	,mnf_cwd_display_pct DOUBLE PRECISION
	,mnf_seg_cwd_display_pct DOUBLE PRECISION
	,brd_cwd_display_pct DOUBLE PRECISION
	,brd_seg_cwd_display_pct DOUBLE PRECISION
	,sku_cwd_feature_pct DOUBLE PRECISION
	,cat_cwd_feature_pct DOUBLE PRECISION
	,seg_cwd_feature_pct DOUBLE PRECISION
	,mnf_cwd_feature_pct DOUBLE PRECISION
	,mnf_seg_cwd_feature_pct DOUBLE PRECISION
	,brd_cwd_feature_pct DOUBLE PRECISION
	,brd_seg_cwd_feature_pct DOUBLE PRECISION
	,msl VARCHAR(10)  		--//  ENCODE lzo
	,filename VARCHAR(200)  		--//  ENCODE lzo
	,file_time_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lcl_prod_hier_lvl_1 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_2 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_3 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_4 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_5 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_6 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_hier_lvl_7 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_attribute_1 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_attribute_2 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_attribute_3 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_attribute_4 VARCHAR(200)  		--//  ENCODE lzo
	,lcl_prod_attribute_5 VARCHAR(200)  		--//  ENCODE lzo
	,delivery_status VARCHAR(50)  		--//  ENCODE lzo
	,key VARCHAR(50)  		--//  ENCODE lzo
	,origin VARCHAR(50)  		--//  ENCODE lzo
	,gsr_description VARCHAR(300)  		--//  ENCODE lzo
	,gsr_level VARCHAR(100)  		--//  ENCODE lzo
	,gsr_flag BOOLEAN
	,launch_date DATE  		--//  ENCODE az64
	,sku_cwd_any_promo_pct DOUBLE PRECISION
	,cat_cwd_any_promo_pct DOUBLE PRECISION
	,seg_cwd_any_promo_pct DOUBLE PRECISION
	,mnf_cwd_any_promo_pct DOUBLE PRECISION
	,mnf_seg_cwd_any_promo_pct DOUBLE PRECISION
	,brd_cwd_any_promo_pct DOUBLE PRECISION
	,brd_seg_cwd_any_promo_pct DOUBLE PRECISION
	,sku_cwd_price_promo_pct DOUBLE PRECISION
	,cat_cwd_price_promo_pct DOUBLE PRECISION
	,seg_cwd_price_promo_pct DOUBLE PRECISION
	,mnf_cwd_price_promo_pct DOUBLE PRECISION
	,mnf_seg_cwd_price_promo_pct DOUBLE PRECISION
	,brd_cwd_price_promo_pct DOUBLE PRECISION
	,brd_seg_cwd_price_promo_pct DOUBLE PRECISION
	,sku_cwd_extra_qty_pct DOUBLE PRECISION
	,cat_cwd_extra_qty_pct DOUBLE PRECISION
	,seg_cwd_extra_qty_pct DOUBLE PRECISION
	,mnf_cwd_extra_qty_pct DOUBLE PRECISION
	,mnf_seg_cwd_extra_qty_pct DOUBLE PRECISION
	,brd_cwd_extra_qty_pct DOUBLE PRECISION
	,brd_seg_cwd_extra_qty_pct DOUBLE PRECISION
	,sku_cwd_display_other_pct DOUBLE PRECISION
	,cat_cwd_display_other_pct DOUBLE PRECISION
	,seg_cwd_display_other_pct DOUBLE PRECISION
	,mnf_cwd_display_other_pct DOUBLE PRECISION
	,mnf_seg_cwd_display_other_pct DOUBLE PRECISION
	,brd_cwd_display_other_pct DOUBLE PRECISION
	,brd_seg_cwd_display_other_pct DOUBLE PRECISION
	,sku_cwd_display_entry_pct DOUBLE PRECISION
	,cat_cwd_display_entry_pct DOUBLE PRECISION
	,seg_cwd_display_entry_pct DOUBLE PRECISION
	,mnf_cwd_display_entry_pct DOUBLE PRECISION
	,mnf_seg_cwd_display_entry_pct DOUBLE PRECISION
	,brd_cwd_display_entry_pct DOUBLE PRECISION
	,brd_seg_cwd_display_entry_pct DOUBLE PRECISION
	,sku_cwd_display_gondola_pct DOUBLE PRECISION
	,cat_cwd_display_gondola_pct DOUBLE PRECISION
	,seg_cwd_display_gondola_pct DOUBLE PRECISION
	,mnf_cwd_display_gondola_pct DOUBLE PRECISION
	,mnf_seg_cwd_display_gondola_pct DOUBLE PRECISION
	,brd_cwd_display_gondola_pct DOUBLE PRECISION
	,brd_seg_cwd_display_gondola_pct DOUBLE PRECISION
	,sku_cwd_display_check_out_pct DOUBLE PRECISION
	,cat_cwd_display_check_out_pct DOUBLE PRECISION
	,seg_cwd_display_check_out_pct DOUBLE PRECISION
	,mnf_cwd_display_check_out_pct DOUBLE PRECISION
	,mnf_seg_cwd_display_check_out_pct DOUBLE PRECISION
	,brd_cwd_display_check_out_pct DOUBLE PRECISION
	,brd_seg_cwd_display_check_out_pct DOUBLE PRECISION
	,sku_cwd_multibuy_pct DOUBLE PRECISION
	,cat_cwd_multibuy_pct DOUBLE PRECISION
	,seg_cwd_multibuy_pct DOUBLE PRECISION
	,mnf_cwd_multibuy_pct DOUBLE PRECISION
	,mnf_seg_cwd_multibuy_pct DOUBLE PRECISION
	,brd_cwd_multibuy_pct DOUBLE PRECISION
	,brd_seg_cwd_multibuy_pct DOUBLE PRECISION
	,sku_cwd_loyalty_card_pct DOUBLE PRECISION
	,cat_cwd_loyalty_card_pct DOUBLE PRECISION
	,seg_cwd_loyalty_card_pct DOUBLE PRECISION
	,mnf_cwd_loyalty_card_pct DOUBLE PRECISION
	,mnf_seg_cwd_loyalty_card_pct DOUBLE PRECISION
	,brd_cwd_loyalty_card_pct DOUBLE PRECISION
	,brd_seg_cwd_loyalty_card_pct DOUBLE PRECISION
	,channel_origin VARCHAR(50)  		--//  ENCODE lzo
	,sku_weighted_distribution_w_pct DOUBLE PRECISION
	,cat_weighted_distribution_w_pct DOUBLE PRECISION
	,seg_weighted_distribution_w_pct DOUBLE PRECISION
	,mnf_weighted_distribution_w_pct DOUBLE PRECISION
	,mnf_seg_weighted_distribution_w_pct DOUBLE PRECISION
	,brd_weighted_distribution_w_pct DOUBLE PRECISION
	,brd_seg_weighted_distribution_w_pct DOUBLE PRECISION
	,sku_weighted_distribution_4w_pct DOUBLE PRECISION
	,cat_weighted_distribution_4w_pct DOUBLE PRECISION
	,seg_weighted_distribution_4w_pct DOUBLE PRECISION
	,mnf_weighted_distribution_4w_pct DOUBLE PRECISION
	,mnf_seg_weighted_distribution_4w_pct DOUBLE PRECISION
	,brd_weighted_distribution_4w_pct DOUBLE PRECISION
	,brd_seg_weighted_distribution_4w_pct DOUBLE PRECISION
	,sku_weighted_distribution_12w_pct DOUBLE PRECISION
	,cat_weighted_distribution_12w_pct DOUBLE PRECISION
	,seg_weighted_distribution_12w_pct DOUBLE PRECISION
	,mnf_weighted_distribution_12w_pct DOUBLE PRECISION
	,mnf_seg_weighted_distribution_12w_pct DOUBLE PRECISION
	,brd_weighted_distribution_12w_pct DOUBLE PRECISION
	,brd_seg_weighted_distribution_12w_pct DOUBLE PRECISION
	,sku_weighted_distribution_pct DOUBLE PRECISION
	,cat_weighted_distribution_pct DOUBLE PRECISION
	,seg_weighted_distribution_pct DOUBLE PRECISION
	,mnf_weighted_distribution_pct DOUBLE PRECISION
	,mnf_seg_weighted_distribution_pct DOUBLE PRECISION
	,brd_weighted_distribution_pct DOUBLE PRECISION
	,brd_seg_weighted_distribution_pct DOUBLE PRECISION
	,manufacturer_local VARCHAR(300)  		--//  ENCODE lzo
	,manufacturer_global VARCHAR(300)  		--//  ENCODE lzo
	,brand_local VARCHAR(300)  		--//  ENCODE lzo
	,brand_global VARCHAR(300)  		--//  ENCODE lzo
	,need_state VARCHAR(100)  		--//  ENCODE lzo
	,gfo VARCHAR(100)  		--//  ENCODE lzo
	,cluster VARCHAR(100)  		--//  ENCODE lzo
	,sub_cluster VARCHAR(100)  		--//  ENCODE lzo
	,form VARCHAR(500)  		--//  ENCODE lzo
)
;

CREATE OR REPLACE TABLE aspedw_integration.edw_rpt_ecomm_oneview		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_edw.edw_rpt_ecomm_oneview
(
	data_type VARCHAR(20)  		--//  ENCODE lzo
	,dataset VARCHAR(20)  		--//  ENCODE lzo
	,data_level VARCHAR(20)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,period_type VARCHAR(10)  		--//  ENCODE lzo
	,fisc_year VARCHAR(5)  		--//  ENCODE lzo
	,cal_year VARCHAR(5)  		--//  ENCODE lzo
	,fisc_month VARCHAR(10)  		--//  ENCODE lzo
	,cal_month VARCHAR(10)  		--//  ENCODE lzo
	,fisc_day DATE  		--//  ENCODE az64
	,cal_day DATE  		--//  ENCODE az64
	,fisc_yr_per VARCHAR(10)  		--//  ENCODE lzo
	,cluster VARCHAR(100)  		--//  ENCODE lzo
	,market VARCHAR(40)  		--//  ENCODE lzo
	,sub_market VARCHAR(40)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,go_to_model VARCHAR(50)  		--//  ENCODE lzo
	,profit_center VARCHAR(50)  		--//  ENCODE lzo
	,company_code VARCHAR(20)  		--//  ENCODE lzo
	,sap_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_customer_name VARCHAR(250)  		--//  ENCODE lzo
	,banner VARCHAR(50)  		--//  ENCODE lzo
	,banner_format VARCHAR(50)  		--//  ENCODE lzo
	,platform_name VARCHAR(100)  		--//  ENCODE lzo
	,retailer_name VARCHAR(200)  		--//  ENCODE lzo
	,retailer_name_english VARCHAR(200)  		--//  ENCODE lzo
	,manufacturer_name VARCHAR(200)  		--//  ENCODE lzo
	,jj_manufacturer_flag VARCHAR(10)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(1000)  		--//  ENCODE lzo
	,product_minor_code VARCHAR(30)  		--//  ENCODE lzo
	,product_minor_name VARCHAR(200)  		--//  ENCODE lzo
	,material_number VARCHAR(50)  		--//  ENCODE lzo
	,ean VARCHAR(20)  		--//  ENCODE lzo
	,retailer_sku_code VARCHAR(200)  		--//  ENCODE lzo
	,product_key VARCHAR(100)  		--//  ENCODE lzo
	,product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,target_value NUMERIC(38,15)  		--//  ENCODE az64
	,actual_value VARCHAR(30)  		--//  ENCODE lzo
	,value_usd NUMERIC(38,15)  		--//  ENCODE az64
	,value_lcy NUMERIC(38,15)  		--//  ENCODE az64
	,salesweight NUMERIC(38,15)  		--//  ENCODE az64
	,from_crncy VARCHAR(5)  		--//  ENCODE lzo
	,to_crncy VARCHAR(5)  		--//  ENCODE lzo
	,account_number VARCHAR(50)  		--//  ENCODE lzo
	,account_name VARCHAR(200)  		--//  ENCODE lzo
	,account_description_l1 VARCHAR(200)  		--//  ENCODE lzo
	,account_description_l2 VARCHAR(200)  		--//  ENCODE lzo
	,account_description_l3 VARCHAR(300)  		--//  ENCODE lzo
	,additional_information VARCHAR(1000)  		--//  ENCODE lzo
	,ppm_role VARCHAR(100)  		--//  ENCODE lzo
)
;
