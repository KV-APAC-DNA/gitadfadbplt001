--DROP TABLE VNMWKS_INTEGRATION.wks_mds_vn_allchannel_siso_target_sku;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_MDS_VN_ALLCHANNEL_SISO_TARGET_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_mds_vn_allchannel_siso_target_sku
(
	cycle NUMERIC(31,0)  		--//  ENCODE az64
	,data_type VARCHAR(200)  		--//  ENCODE lzo
	,jnj_code NUMERIC(31,0)  		--//  ENCODE az64
	,description VARCHAR(200)  		--//  ENCODE lzo
	,dksh_mti NUMERIC(31,7)  		--//  ENCODE az64
	,dksh_ecom NUMERIC(31,7)  		--//  ENCODE az64
	,mtd NUMERIC(31,7)  		--//  ENCODE az64
	,gt_tien_thanh NUMERIC(31,7)  		--//  ENCODE az64
	,gt_duong_anh NUMERIC(31,7)  		--//  ENCODE az64
	,run_id VARCHAR(1)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,otc VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_mds_vn_ecom_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_MDS_VN_ECOM_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_mds_vn_ecom_target
(
	cycle NUMERIC(31,0)  		--//  ENCODE az64
	,platform VARCHAR(200)  		--//  ENCODE lzo
	,target NUMERIC(31,1)  		--//  ENCODE az64
	,run_id VARCHAR(14)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VIETNAM_REGIONAL_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(7)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(300)  		--//  ENCODE lzo
	,distributor_code VARCHAR(30)  		--//  ENCODE lzo
	,distributor_name VARCHAR(750)  		--//  ENCODE lzo
	,store_code VARCHAR(30)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(300)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(2)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(750)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(750)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(40)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity NUMERIC(38,4)  		--//  ENCODE az64
	,sellout_sales_value NUMERIC(38,23)  		--//  ENCODE az64
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_base;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VIETNAM_REGIONAL_SELLOUT_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_base
(
	data_src VARCHAR(8)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(7)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,soldto_code VARCHAR(300)  		--//  ENCODE lzo
	,distributor_code VARCHAR(30)  		--//  ENCODE lzo
	,distributor_name VARCHAR(750)  		--//  ENCODE lzo
	,store_cd VARCHAR(30)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(300)  		--//  ENCODE lzo
	,ean VARCHAR(40)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(750)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(750)  		--//  ENCODE lzo
	,so_sls_qty NUMERIC(15,4)  		--//  ENCODE az64
	,so_sls_value NUMERIC(38,23)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_npd;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VIETNAM_REGIONAL_SELLOUT_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(7)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(300)  		--//  ENCODE lzo
	,distributor_code VARCHAR(30)  		--//  ENCODE lzo
	,distributor_name VARCHAR(750)  		--//  ENCODE lzo
	,store_code VARCHAR(30)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(300)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(2)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(750)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(750)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(40)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity NUMERIC(38,4)  		--//  ENCODE az64
	,sellout_sales_value NUMERIC(38,23)  		--//  ENCODE az64
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,customer_min_date DATE  		--//  ENCODE az64
	,market_min_date DATE  		--//  ENCODE az64
	,rn_cus numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rn_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,customer_product_min_date DATE  		--//  ENCODE az64
	,market_product_min_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_offtake_npd;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VIETNAM_REGIONAL_SELLOUT_OFFTAKE_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vietnam_regional_sellout_offtake_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(7)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(300)  		--//  ENCODE lzo
	,distributor_code VARCHAR(30)  		--//  ENCODE lzo
	,distributor_name VARCHAR(750)  		--//  ENCODE lzo
	,store_code VARCHAR(30)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(300)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(2)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(2)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(750)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(750)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(40)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity NUMERIC(38,4)  		--//  ENCODE az64
	,sellout_sales_value NUMERIC(38,23)  		--//  ENCODE az64
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,sellout_value_list_price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellout_value_list_price_usd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_min_date DATE  		--//  ENCODE az64
	,customer_product_min_date DATE  		--//  ENCODE az64
	,market_min_date DATE  		--//  ENCODE az64
	,market_product_min_date DATE  		--//  ENCODE az64
	,rn_cus numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rn_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,first_scan_flag_parent_customer_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_parent_customer_level VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level VARCHAR(1)  		--//  ENCODE lzo
	,selling_price NUMERIC(38,19)  		--//  ENCODE az64
	,cnt_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_dksh_daily_sales;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_DKSH_DAILY_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_dksh_daily_sales
(
	group_ds VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,materialdescription VARCHAR(255)  		--//  ENCODE lzo
	,syslot VARCHAR(255)  		--//  ENCODE lzo
	,batchno VARCHAR(255)  		--//  ENCODE lzo
	,exp_date VARCHAR(255)  		--//  ENCODE lzo
	,total numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hcm VARCHAR(255)  		--//  ENCODE lzo
	,vsip VARCHAR(255)  		--//  ENCODE lzo
	,langha VARCHAR(255)  		--//  ENCODE lzo
	,thanhtri VARCHAR(255)  		--//  ENCODE lzo
	,danang VARCHAR(255)  		--//  ENCODE lzo
	,values_lc NUMERIC(24,10)  		--//  ENCODE az64
	,reason VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_dms_data_extract_summary;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_DMS_DATA_EXTRACT_SUMMARY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_dms_data_extract_summary
(
	source_file_name VARCHAR(256)  		--//  ENCODE zstd
	,date_of_extraction TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,record_count numeric(18,0)		--//  ENCODE zstd // INTEGER  
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_dms_record_reconcile;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_DMS_RECORD_RECONCILE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_dms_record_reconcile
(
	recon_date DATE  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,sourcefile_count numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,sdl_count numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,sdl_raw_count numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,itg_count numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,diff_src_bw_sdl numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,diff_sdl_bw_sdl_raw numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,diff_sdl_bw_itg numeric(18,0)		--//  ENCODE zstd // INTEGER  
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_gt_scorecard_details;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_GT_SCORECARD_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_gt_scorecard_details
(
	jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,dist_type VARCHAR(100)  		--//  ENCODE lzo
	,asm_id VARCHAR(200)  		--//  ENCODE lzo
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,so_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,ub_brand_lvl_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,so_target NUMERIC(38,6)  		--//  ENCODE az64
	,inv_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_value_prev NUMERIC(38,4)  		--//  ENCODE az64
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,ub numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,ub_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,pc numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,pc_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,total_stores numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,total_stores_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,sr_active numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sr_active_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,ss_active numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,ss_active_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,sr_inactive numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,ss_inactive numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sr_resigned_yearly_target NUMERIC(35,0)  		--//  ENCODE az64
	,ub_coverage_percent_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,pc_sr_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,ub_sr_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,revenue_sr_m_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,sr_attrition_percent_yearly_target NUMERIC(38,4)  		--//  ENCODE az64
	,mnth_start_date DATE  		--//  ENCODE az64
	,mnth_end_date DATE  		--//  ENCODE az64
	,exchg_rate NUMERIC(14,10)  		--//  ENCODE az64
	,curr_mnth_indc VARCHAR(1)  		--//  ENCODE lzo
	,curr_year_indc VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_gt_topdoor_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_GT_TOPDOOR_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_gt_topdoor_target
(
	td_id VARCHAR(100)  		--//  ENCODE lzo
	,td_zone VARCHAR(50)  		--//  ENCODE lzo
	,from_cycle VARCHAR(10)  		--//  ENCODE lzo
	,to_cycle VARCHAR(10)  		--//  ENCODE lzo
	,distributor_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,shoptype VARCHAR(100)  		--//  ENCODE lzo
	,target_value NUMERIC(18,2)  		--//  ENCODE az64
	,crt_dttm VARCHAR(100)  		--//  ENCODE lzo
	,upd_dttm VARCHAR(100)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_answers;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_ANSWERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_answers
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value NUMERIC(20,0)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,oos NUMERIC(20,0)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_branch;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_BRANCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_branch
(
	parent_cust_code VARCHAR(255)  		--//  ENCODE zstd
	,parent_cust_name VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,branch_name VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,sales_group VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,city VARCHAR(255)  		--//  ENCODE zstd
	,district VARCHAR(255)  		--//  ENCODE zstd
	,trade_type VARCHAR(255)  		--//  ENCODE zstd
	,store_prioritization VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(32,16)  		--//  ENCODE zstd
	,longitude NUMERIC(32,16)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_choices;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_CHOICES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_choices
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(20,0)  		--//  ENCODE zstd
	,channel_name VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,sku_group VARCHAR(255)  		--//  ENCODE zstd
	,rep_param VARCHAR(255)  		--//  ENCODE zstd
	,putup_id VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,sfa NUMERIC(20,0)  		--//  ENCODE zstd
	,brand_id VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_cpg;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_CPG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_cpg
(
	slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,visitdate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_customer_visited;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_CUSTOMER_VISITED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_customer_visited
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,created_date VARCHAR(255)  		--//  ENCODE zstd
	,visit_date VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_ise_header;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_ISE_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_ise_header
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ise_desc VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,startdate VARCHAR(255)  		--//  ENCODE zstd
	,enddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_notes;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_NOTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_notes
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_interface_question;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_INTERFACE_QUESTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_interface_question
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(10,0)  		--//  ENCODE zstd
	,channel VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_desc VARCHAR(255)  		--//  ENCODE zstd
	,standard_ques NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_class_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_class_desc VARCHAR(255)  		--//  ENCODE zstd
	,weigh NUMERIC(10,0)  		--//  ENCODE zstd
	,total_score NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_code VARCHAR(255)  		--//  ENCODE zstd
	,answer_desc VARCHAR(255)  		--//  ENCODE zstd
	,franchise_code VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_coop;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLIN_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_coop
(
	sku VARCHAR(20)  		--//  ENCODE lzo
	,idescr VARCHAR(200)  		--//  ENCODE lzo
	,vendor VARCHAR(50)  		--//  ENCODE lzo
	,asname VARCHAR(200)  		--//  ENCODE lzo
	,imfgr VARCHAR(50)  		--//  ENCODE lzo
	,store VARCHAR(50)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_dksh;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLIN_DKSH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_dksh
(
	supplier_code VARCHAR(10)  		--//  ENCODE zstd
	,supplier_name VARCHAR(100)  		--//  ENCODE zstd
	,plant VARCHAR(10)  		--//  ENCODE zstd
	,productid VARCHAR(20)  		--//  ENCODE zstd
	,product VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,sellin_category VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(100)  		--//  ENCODE zstd
	,product_sub_group VARCHAR(100)  		--//  ENCODE zstd
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE zstd
	,custcode VARCHAR(20)  		--//  ENCODE zstd
	,customer VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,zone VARCHAR(20)  		--//  ENCODE zstd
	,channel VARCHAR(10)  		--//  ENCODE zstd
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,cust_group VARCHAR(50)  		--//  ENCODE zstd
	,billing_no VARCHAR(20)  		--//  ENCODE zstd
	,invoice_date VARCHAR(20)  		--//  ENCODE zstd
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE zstd
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE zstd
	,order_type VARCHAR(20)  		--//  ENCODE zstd
	,red_invoice_no VARCHAR(20)  		--//  ENCODE zstd
	,expiry_date VARCHAR(20)  		--//  ENCODE zstd
	,order_no VARCHAR(20)  		--//  ENCODE zstd
	,order_date VARCHAR(20)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,sellout_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,group_account VARCHAR(50)  		--//  ENCODE zstd
	,account VARCHAR(50)  		--//  ENCODE zstd
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE zstd
	,zone_or_area VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,sellout_category VARCHAR(50)  		--//  ENCODE zstd
	,sub_cat VARCHAR(50)  		--//  ENCODE zstd
	,sub_brand VARCHAR(50)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,base_or_bundle VARCHAR(20)  		--//  ENCODE zstd
	,size VARCHAR(20)  		--//  ENCODE zstd
	,key_chain VARCHAR(20)  		--//  ENCODE zstd
	,status VARCHAR(20)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLIN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellin_target
(
	mtd_code VARCHAR(20)  		--//  ENCODE zstd
	,mti_code VARCHAR(20)  		--//  ENCODE zstd
	,target NUMERIC(20,5)  		--//  ENCODE az64
	,sellin_cycle numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellin_year VARCHAR(10)  		--//  ENCODE zstd
	,visit VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_aeon;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_AEON		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_aeon
(
	store VARCHAR(20)  		--//  ENCODE lzo
	,department VARCHAR(30)  		--//  ENCODE lzo
	,supplier_code VARCHAR(50)  		--//  ENCODE lzo
	,supplier_name VARCHAR(60)  		--//  ENCODE lzo
	,item VARCHAR(30)  		--//  ENCODE lzo
	,item_name VARCHAR(200)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_bhx;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_BHX		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_bhx
(
	pro_code VARCHAR(50)  		--//  ENCODE lzo
	,pro_name VARCHAR(200)  		--//  ENCODE lzo
	,cust_code VARCHAR(20)  		--//  ENCODE lzo
	,cust_name VARCHAR(200)  		--//  ENCODE lzo
	,cat_store VARCHAR(60)  		--//  ENCODE lzo
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_con_cung;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_CON_CUNG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_con_cung
(
	delivery_code VARCHAR(50)  		--//  ENCODE zstd
	,store VARCHAR(255)  		--//  ENCODE zstd
	,product_code VARCHAR(20)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_coop;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_coop
(
	thang VARCHAR(20)  		--//  ENCODE zstd
	,desc_a VARCHAR(20)  		--//  ENCODE zstd
	,idept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isdept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,iclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,tenvt VARCHAR(255)  		--//  ENCODE zstd
	,brand_spm VARCHAR(50)  		--//  ENCODE zstd
	,madv VARCHAR(20)  		--//  ENCODE zstd
	,sumoflg numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sumofttbvnckhd NUMERIC(20,5)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE zstd
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_guardian;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_GUARDIAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_guardian
(
	serial_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,description_vietnamese VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,division VARCHAR(50)  		--//  ENCODE zstd
	,department VARCHAR(50)  		--//  ENCODE zstd
	,category VARCHAR(50)  		--//  ENCODE zstd
	,sub_category VARCHAR(50)  		--//  ENCODE zstd
	,sales_supplier numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_lotte;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_LOTTE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_lotte
(
	str VARCHAR(10)  		--//  ENCODE zstd
	,str_nm VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_1 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_2 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_3 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_4 VARCHAR(50)  		--//  ENCODE zstd
	,prod_cd VARCHAR(100)  		--//  ENCODE zstd
	,sale_cd VARCHAR(100)  		--//  ENCODE zstd
	,prod_nm VARCHAR(100)  		--//  ENCODE zstd
	,ven VARCHAR(100)  		--//  ENCODE zstd
	,ven_nm VARCHAR(100)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,tot_sale_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_mega;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_MEGA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_mega
(
	site_no VARCHAR(20)  		--//  ENCODE zstd
	,site_name VARCHAR(255)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,art_no VARCHAR(20)  		--//  ENCODE zstd
	,art_sv_name VARCHAR(255)  		--//  ENCODE zstd
	,suppl_no VARCHAR(20)  		--//  ENCODE zstd
	,suppl_name VARCHAR(255)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cogs_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_vinmart;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_MT_SELLOUT_VINMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_mt_sellout_vinmart
(
	store VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,mch5_mc VARCHAR(20)  		--//  ENCODE zstd
	,mc VARCHAR(100)  		--//  ENCODE zstd
	,article VARCHAR(20)  		--//  ENCODE zstd
	,article_name VARCHAR(255)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,pos_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pos_revenue NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_oneview_otc;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_ONEVIEW_OTC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_oneview_otc
(
	plant VARCHAR(10)  		--//  ENCODE zstd
	,principalcode VARCHAR(100)  		--//  ENCODE zstd
	,principal VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,kunnr VARCHAR(100)  		--//  ENCODE zstd
	,name1 VARCHAR(100)  		--//  ENCODE zstd
	,name2 VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,zterm VARCHAR(100)  		--//  ENCODE zstd
	,kdgrp VARCHAR(100)  		--//  ENCODE zstd
	,custgroup VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,vbeln VARCHAR(100)  		--//  ENCODE zstd
	,billingdate VARCHAR(100)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,qty VARCHAR(100)  		--//  ENCODE zstd
	,dgle VARCHAR(100)  		--//  ENCODE zstd
	,pernr VARCHAR(100)  		--//  ENCODE zstd
	,vat VARCHAR(100)  		--//  ENCODE zstd
	,suom VARCHAR(100)  		--//  ENCODE zstd
	,custpayto VARCHAR(100)  		--//  ENCODE zstd
	,tt NUMERIC(21,5)  		--//  ENCODE zstd
	,nguyengia VARCHAR(100)  		--//  ENCODE zstd
	,ttv NUMERIC(21,5)  		--//  ENCODE zstd
	,discount NUMERIC(21,5)  		--//  ENCODE zstd
	,device_code VARCHAR(100)  		--//  ENCODE zstd
	,device VARCHAR(100)  		--//  ENCODE zstd
	,order_no VARCHAR(100)  		--//  ENCODE zstd
	,orginv VARCHAR(100)  		--//  ENCODE zstd
	,batch VARCHAR(100)  		--//  ENCODE zstd
	,charge VARCHAR(100)  		--//  ENCODE zstd
	,contact_name VARCHAR(100)  		--//  ENCODE zstd
	,userid VARCHAR(100)  		--//  ENCODE zstd
	,billinginst VARCHAR(100)  		--//  ENCODE zstd
	,distchannel VARCHAR(100)  		--//  ENCODE zstd
	,redinv VARCHAR(100)  		--//  ENCODE zstd
	,serial VARCHAR(100)  		--//  ENCODE zstd
	,potext VARCHAR(100)  		--//  ENCODE zstd
	,expdate VARCHAR(100)  		--//  ENCODE zstd
	,ret_so VARCHAR(100)  		--//  ENCODE zstd
	,vat_code VARCHAR(100)  		--//  ENCODE zstd
	,sodoc_date VARCHAR(100)  		--//  ENCODE zstd
	,itemnotes VARCHAR(100)  		--//  ENCODE zstd
	,mg1 VARCHAR(100)  		--//  ENCODE zstd
	,year VARCHAR(100)  		--//  ENCODE zstd
	,month VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_oneview_rpt;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_ONEVIEW_RPT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_oneview_rpt
(
	data_type VARCHAR(15)  		--//  ENCODE lzo
	,channel VARCHAR(20)  		--//  ENCODE lzo
	,sub_channel VARCHAR(300)  		--//  ENCODE lzo
	,jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,jj_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,jj_mnth_day numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mapped_spk VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,sap_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_name VARCHAR(500)  		--//  ENCODE lzo
	,bar_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(500)  		--//  ENCODE lzo
	,invoice_date DATE  		--//  ENCODE az64
	,salesman VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(100)  		--//  ENCODE lzo
	,si_gts_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_gts_excl_dm_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_nts_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_mnth_tgt_by_sku DOUBLE PRECISION
	,so_net_trd_sls_loc NUMERIC(38,5)  		--//  ENCODE az64
	,so_net_trd_sls_usd NUMERIC(38,10)  		--//  ENCODE az64
	,so_mnth_tgt NUMERIC(38,10)  		--//  ENCODE az64
	,so_avg_wk_tgt NUMERIC(28,6)  		--//  ENCODE az64
	,so_mnth_tgt_by_sku DOUBLE PRECISION
	,zone_manager_id VARCHAR(100)  		--//  ENCODE lzo
	,zone_manager_name VARCHAR(450)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,province VARCHAR(1125)  		--//  ENCODE lzo
	,region VARCHAR(1125)  		--//  ENCODE lzo
	,shop_type VARCHAR(100)  		--//  ENCODE lzo
	,mt_sub_channel VARCHAR(1125)  		--//  ENCODE lzo
	,retail_environment VARCHAR(450)  		--//  ENCODE lzo
	,group_account VARCHAR(1125)  		--//  ENCODE lzo
	,account VARCHAR(1125)  		--//  ENCODE lzo
	,franchise VARCHAR(30)  		--//  ENCODE lzo
	,brand VARCHAR(83)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(255)  		--//  ENCODE lzo
	,group_jb VARCHAR(6)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_product_mapping;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_product_mapping
(
	putupid VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(255)  		--//  ENCODE zstd
	,productname VARCHAR(2000)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_si_st_so_details;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_SI_ST_SO_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_si_st_so_details
(
	jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,jj_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,wks_in_qrtr numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,distributor_id_report VARCHAR(30)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_type VARCHAR(100)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_num VARCHAR(50)  		--//  ENCODE lzo
	,dstrbtr_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,trade_type VARCHAR(13)  		--//  ENCODE lzo
	,bill_doc VARCHAR(30)  		--//  ENCODE lzo
	,bill_date DATE  		--//  ENCODE az64
	,cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_cd VARCHAR(50)  		--//  ENCODE lzo
	,slsmn_nm VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_status VARCHAR(1)  		--//  ENCODE lzo
	,so_sls_qty NUMERIC(15,4)  		--//  ENCODE az64
	,so_ret_qty NUMERIC(15,4)  		--//  ENCODE az64
	,so_grs_trd_sls NUMERIC(15,4)  		--//  ENCODE az64
	,so_net_trd_sls NUMERIC(15,4)  		--//  ENCODE az64
	,so_avg_wk_tgt NUMERIC(28,6)  		--//  ENCODE az64
	,so_mnth_tgt NUMERIC(23,6)  		--//  ENCODE az64
	,supervisor_code VARCHAR(50)  		--//  ENCODE lzo
	,supervisor_name VARCHAR(100)  		--//  ENCODE lzo
	,asm_id VARCHAR(50)  		--//  ENCODE lzo
	,asm_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrb_region VARCHAR(20)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,shop_type VARCHAR(100)  		--//  ENCODE lzo
	,group_hierarchy VARCHAR(100)  		--//  ENCODE lzo
	,top_door_group VARCHAR(100)  		--//  ENCODE lzo
	,top_door_target NUMERIC(18,2)  		--//  ENCODE az64
	,top_door_flag VARCHAR(1)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_ret_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_nts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,si_mnth_tgt NUMERIC(20,4)  		--//  ENCODE az64
	,st_sls_qty NUMERIC(14,2)  		--//  ENCODE az64
	,st_ret_qty NUMERIC(14,2)  		--//  ENCODE az64
	,st_grs_trd_sls NUMERIC(15,4)  		--//  ENCODE az64
	,st_net_trd_sls NUMERIC(17,4)  		--//  ENCODE az64
	,st_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,st_mnth_tgt NUMERIC(20,4)  		--//  ENCODE az64
	,visit_call_date DATE  		--//  ENCODE az64
	,product_visit_call_date DATE  		--//  ENCODE az64
	,pc_target NUMERIC(20,4)  		--//  ENCODE az64
	,pc_target_by_week NUMERIC(20,4)  		--//  ENCODE az64
	,ce_target NUMERIC(20,4)  		--//  ENCODE az64
	,ce_target_by_week NUMERIC(20,4)  		--//  ENCODE az64
	,ub_weekly_target NUMERIC(35,24)  		--//  ENCODE az64
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(200)  		--//  ENCODE lzo
	,group_jb VARCHAR(20)  		--//  ENCODE lzo
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMWKS_INTEGRATION.wks_vn_si_st_so_details_forecast;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WKS_VN_SI_ST_SO_DETAILS_FORECAST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wks_vn_si_st_so_details_forecast
(
	jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,jj_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,wks_in_qrtr numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,distributor_id_report VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_type VARCHAR(100)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,trade_type VARCHAR(13)  		--//  ENCODE lzo
	,bill_doc VARCHAR(30)  		--//  ENCODE lzo
	,bill_date DATE  		--//  ENCODE az64
	,cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_cd VARCHAR(50)  		--//  ENCODE lzo
	,slsmn_nm VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_status VARCHAR(1)  		--//  ENCODE lzo
	,so_sls_qty NUMERIC(22,4)  		--//  ENCODE az64
	,so_ret_qty NUMERIC(22,4)  		--//  ENCODE az64
	,so_grs_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,so_net_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,so_avg_wk_tgt NUMERIC(28,6)  		--//  ENCODE az64
	,so_mnth_tgt NUMERIC(24,6)  		--//  ENCODE az64
	,supervisor_code VARCHAR(50)  		--//  ENCODE lzo
	,supervisor_name VARCHAR(100)  		--//  ENCODE lzo
	,asm_id VARCHAR(100)  		--//  ENCODE lzo
	,asm_name VARCHAR(200)  		--//  ENCODE lzo
	,dstrb_region VARCHAR(20)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,shop_type VARCHAR(100)  		--//  ENCODE lzo
	,group_hierarchy VARCHAR(100)  		--//  ENCODE lzo
	,top_door_group VARCHAR(100)  		--//  ENCODE lzo
	,top_door_target NUMERIC(20,2)  		--//  ENCODE az64
	,top_door_flag VARCHAR(1)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_ret_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_nts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,si_target NUMERIC(22,4)  		--//  ENCODE az64
	,st_sls_qty NUMERIC(20,2)  		--//  ENCODE az64
	,st_ret_qty NUMERIC(20,2)  		--//  ENCODE az64
	,st_grs_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,st_net_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,st_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,st_target NUMERIC(22,4)  		--//  ENCODE az64
	,visit_call_date DATE  		--//  ENCODE az64
	,product_visit_call_date DATE  		--//  ENCODE az64
	,pc_target NUMERIC(22,4)  		--//  ENCODE az64
	,pc_target_by_week NUMERIC(22,4)  		--//  ENCODE az64
	,ce_target NUMERIC(22,4)  		--//  ENCODE az64
	,ce_target_by_week NUMERIC(22,4)  		--//  ENCODE az64
	,ub_weekly_target NUMERIC(38,24)  		--//  ENCODE az64
	,franchise VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(200)  		--//  ENCODE lzo
	,group_jb VARCHAR(50)  		--//  ENCODE lzo
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
	,forecastso_mil NUMERIC(23,4)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_mnth_week;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_MNTH_WEEK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_mnth_week
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)
	,mnth_desc VARCHAR(15)  		--//  ENCODE lzo
	,mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_shrt VARCHAR(3)  		--//  ENCODE lzo
	,mnth_long VARCHAR(9)  		--//  ENCODE lzo
	,wk numeric(38,0)		--// BIGINT
	,mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,frst_day VARCHAR(13)  		--//  ENCODE lzo
	,lst_day VARCHAR(13)  		--//  ENCODE lzo
	,cnt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,p_1 VARCHAR(23)  		--//  ENCODE lzo
	,p_2 VARCHAR(23)  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	mnth_id
		--// 	, wk
		--// 	)
;		--// ;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_by_wkmnth_for_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLIN_BY_WKMNTH_FOR_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_by_wkmnth_for_target
(
	territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amount NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_target_p1;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLIN_TARGET_P1		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_target_p1
(
	p_2 VARCHAR(23)  		--//  ENCODE lzo
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,target_cyc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(38,4)  		--//  ENCODE az64
)

CLUSTER BY  (p_2)
;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_target_p2;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLIN_TARGET_P2		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellin_target_p2
(
	p_1 VARCHAR(23)  		--//  ENCODE lzo
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,target_cyc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(38,4)  		--//  ENCODE az64
)

CLUSTER BY  (p_1)
;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellout_by_wkmnth_for_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLOUT_BY_WKMNTH_FOR_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellout_by_wkmnth_for_target
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,salesrep_id VARCHAR(30)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amount NUMERIC(38,23)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_by_wkmnth_for_target;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLTHROUGH_BY_WKMNTH_FOR_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_by_wkmnth_for_target
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,mapped_spk VARCHAR(30)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amount NUMERIC(21,8)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_target_p1;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLTHROUGH_TARGET_P1		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_target_p1
(
	p_2 VARCHAR(23)  		--//  ENCODE lzo
	,mapped_spk VARCHAR(30)  		--//  ENCODE lzo
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,target_cyc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(21,8)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_target_p2;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_SELLTHROUGH_TARGET_P2		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_sellthrough_target_p2
(
	p_1 VARCHAR(23)  		--//  ENCODE lzo
	,mapped_spk VARCHAR(30)  		--//  ENCODE lzo
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,target_cyc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(21,8)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_target_p1;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_TARGET_P1		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_target_p1
(
	p_2 VARCHAR(23)  		--//  ENCODE lzo
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,saleman_code VARCHAR(50)  		--//  ENCODE lzo
	,target_cyc NUMERIC(18,0)  		--//  ENCODE az64
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(38,23)  		--//  ENCODE az64
)

;
--DROP TABLE VNMWKS_INTEGRATION.wrk_vn_target_p2;
CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.WRK_VN_TARGET_P2		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMWKS_INTEGRATION.wrk_vn_target_p2
(
	p_1 VARCHAR(23)  		--//  ENCODE lzo
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,saleman_code VARCHAR(50)  		--//  ENCODE lzo
	,target_cyc NUMERIC(18,0)  		--//  ENCODE az64
	,target_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_value NUMERIC(15,4)  		--//  ENCODE az64
	,sales_mnth VARCHAR(23)  		--//  ENCODE lzo
	,sales_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,amt_by_wk NUMERIC(38,23)  		--//  ENCODE az64
)

;
-----------------------------------------------------
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_dksh_stock;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_DKSH_STOCK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_dksh_stock
(
	cntry_cd VARCHAR(255)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(255)  		--//  ENCODE lzo
	,sold_to_code VARCHAR(30)  		--//  ENCODE lzo
	,group_ds VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,materialdescription VARCHAR(255)  		--//  ENCODE lzo
	,syslot VARCHAR(255)  		--//  ENCODE lzo
	,batchno VARCHAR(255)  		--//  ENCODE lzo
	,exp_date VARCHAR(255)  		--//  ENCODE lzo
	,total numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hcm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vsip numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,langha numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,thanhtri numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,danang numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,values_lc NUMERIC(24,10)  		--//  ENCODE az64
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,transaction_date DATE  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_gt_scorecard_details;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_GT_SCORECARD_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_gt_scorecard_details
(
	jj_year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,dist_type VARCHAR(256)  		--//  ENCODE lzo
	,asm_id VARCHAR(200)  		--//  ENCODE lzo
	,so_value NUMERIC(38,12)  		--//  ENCODE lzo
	,so_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,ub_brand_lvl_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,so_target NUMERIC(38,4)  		--//  ENCODE lzo
	,inv_value NUMERIC(38,4)  		--//  ENCODE lzo
	,inv_value_prev_day NUMERIC(38,4)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,ub numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,ub_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,pc numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,pc_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,total_stores numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_stores_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,sr_active numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,sr_active_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,ss_active numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,ss_active_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,sr_inactive numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,ss_inactive numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,sr_resigned_yearly_target NUMERIC(35,0)  		--//  ENCODE lzo
	,ub_coverage_percent_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,pc_sr_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,ub_sr_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,revenue_sr_m_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,sr_attrition_percent_yearly_target NUMERIC(38,4)  		--//  ENCODE lzo
	,mnth_start_date DATE  		--//  ENCODE lzo
	,mnth_end_date DATE  		--//  ENCODE lzo
	,exchg_rate NUMERIC(14,10)  		--//  ENCODE lzo
	,curr_mnth_indc VARCHAR(1)  		--//  ENCODE lzo
	,curr_year_indc VARCHAR(1)  		--//  ENCODE lzo
	,crdt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crdt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE lzo //  ENCODE lzo // character varying
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_mt_offtake_exception_report;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_MT_OFFTAKE_EXCEPTION_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_mt_offtake_exception_report
(
	account VARCHAR(13)  		--//  ENCODE lzo
	,month_id VARCHAR(23)  		--//  ENCODE lzo
	,product_cd VARCHAR(100)  		--//  ENCODE lzo
	,customer_cd VARCHAR(200)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(200)  		--//  ENCODE lzo
	,product_name VARCHAR(1125)  		--//  ENCODE lzo
	,franchise VARCHAR(1687)  		--//  ENCODE lzo
	,category VARCHAR(1687)  		--//  ENCODE lzo
	,sub_brand VARCHAR(1350)  		--//  ENCODE lzo
	,sub_category VARCHAR(1687)  		--//  ENCODE lzo
	,amount NUMERIC(38,9)  		--//  ENCODE az64
	,amount_usd NUMERIC(38,14)  		--//  ENCODE az64
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_mt_sellin_exception;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_MT_SELLIN_EXCEPTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_mt_sellin_exception
(
	data_source VARCHAR(6)  		--//  ENCODE lzo
	,data_type VARCHAR(7)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,productid VARCHAR(50)  		--//  ENCODE lzo
	,product_name VARCHAR(500)  		--//  ENCODE lzo
	,barcode VARCHAR(40)  		--//  ENCODE lzo
	,custcode VARCHAR(200)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,sub_channel VARCHAR(1125)  		--//  ENCODE lzo
	,region VARCHAR(1125)  		--//  ENCODE lzo
	,province VARCHAR(1125)  		--//  ENCODE lzo
	,kam VARCHAR(450)  		--//  ENCODE lzo
	,retail_environment VARCHAR(450)  		--//  ENCODE lzo
	,group_account VARCHAR(1125)  		--//  ENCODE lzo
	,account VARCHAR(1125)  		--//  ENCODE lzo
	,franchise VARCHAR(1125)  		--//  ENCODE lzo
	,category VARCHAR(1125)  		--//  ENCODE lzo
	,sub_category VARCHAR(1125)  		--//  ENCODE lzo
	,sub_brand VARCHAR(900)  		--//  ENCODE lzo
	,sales_amt_lcy NUMERIC(38,5)  		--//  ENCODE az64
	,sales_amt_usd NUMERIC(38,10)  		--//  ENCODE az64
	,target_lcy NUMERIC(28,10)  		--//  ENCODE az64
	,target_usd NUMERIC(28,10)  		--//  ENCODE az64
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_mt_sellin_sellout_analysis;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_MT_SELLIN_SELLOUT_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_mt_sellin_sellout_analysis
(
	data_source VARCHAR(6)  		--//  ENCODE lzo
	,data_type VARCHAR(7)  		--//  ENCODE lzo
	,invoice_date VARCHAR(20)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,mnth_desc VARCHAR(15)  		--//  ENCODE lzo
	,mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_shrt VARCHAR(3)  		--//  ENCODE lzo
	,mnth_long VARCHAR(9)  		--//  ENCODE lzo
	,wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,cal_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_qrtr_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_mnth_nm VARCHAR(9)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,cal_date_id VARCHAR(52)  		--//  ENCODE lzo
	,supplier_code VARCHAR(10)  		--//  ENCODE lzo
	,supplier_name VARCHAR(100)  		--//  ENCODE lzo
	,plant VARCHAR(10)  		--//  ENCODE lzo
	,productid VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(1125)  		--//  ENCODE lzo
	,barcode VARCHAR(200)  		--//  ENCODE lzo
	,jnj_sap_code VARCHAR(40)  		--//  ENCODE lzo
	,franchise VARCHAR(1687)  		--//  ENCODE lzo
	,category VARCHAR(1687)  		--//  ENCODE lzo
	,sub_category VARCHAR(1350)  		--//  ENCODE lzo
	,sub_brand VARCHAR(1687)  		--//  ENCODE lzo
	,size VARCHAR(400)  		--//  ENCODE lzo
	,channel VARCHAR(10)  		--//  ENCODE lzo
	,custcode VARCHAR(200)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,address VARCHAR(300)  		--//  ENCODE lzo
	,sub_channel VARCHAR(1125)  		--//  ENCODE lzo
	,group_account VARCHAR(1125)  		--//  ENCODE lzo
	,account VARCHAR(1125)  		--//  ENCODE lzo
	,region VARCHAR(1125)  		--//  ENCODE lzo
	,province VARCHAR(1125)  		--//  ENCODE lzo
	,retail_environment VARCHAR(450)  		--//  ENCODE lzo
	,district VARCHAR(1)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,period VARCHAR(20)  		--//  ENCODE lzo
	,sales_supervisor VARCHAR(450)  		--//  ENCODE lzo
	,kam VARCHAR(450)  		--//  ENCODE lzo
	,sales_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sales_amt_lcy NUMERIC(38,9)  		--//  ENCODE az64
	,sales_amt_usd NUMERIC(38,14)  		--//  ENCODE az64
	,target_lcy NUMERIC(28,10)  		--//  ENCODE az64
	,target_usd NUMERIC(28,10)  		--//  ENCODE az64
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_oneview_rpt;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_ONEVIEW_RPT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_oneview_rpt
(
	data_type VARCHAR(15)  		--//  ENCODE lzo
	,channel VARCHAR(20)  		--//  ENCODE lzo
	,sub_channel VARCHAR(300)  		--//  ENCODE lzo
	,jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,jj_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,jj_mnth_day numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mapped_spk VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,sap_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_name VARCHAR(500)  		--//  ENCODE lzo
	,bar_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(500)  		--//  ENCODE lzo
	,invoice_date DATE  		--//  ENCODE az64
	,salesman VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(100)  		--//  ENCODE lzo
	,si_gts_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_gts_excl_dm_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_nts_val NUMERIC(38,10)  		--//  ENCODE az64
	,si_mnth_tgt_by_sku DOUBLE PRECISION
	,so_net_trd_sls_loc NUMERIC(38,5)  		--//  ENCODE az64
	,so_net_trd_sls_usd NUMERIC(38,10)  		--//  ENCODE az64
	,so_mnth_tgt NUMERIC(38,10)  		--//  ENCODE az64
	,so_avg_wk_tgt NUMERIC(28,6)  		--//  ENCODE az64
	,so_mnth_tgt_by_sku DOUBLE PRECISION
	,zone_manager_id VARCHAR(100)  		--//  ENCODE lzo
	,zone_manager_name VARCHAR(450)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,province VARCHAR(1125)  		--//  ENCODE lzo
	,region VARCHAR(1125)  		--//  ENCODE lzo
	,shop_type VARCHAR(100)  		--//  ENCODE lzo
	,mt_sub_channel VARCHAR(1125)  		--//  ENCODE lzo
	,retail_environment VARCHAR(450)  		--//  ENCODE lzo
	,group_account VARCHAR(1125)  		--//  ENCODE lzo
	,account VARCHAR(1125)  		--//  ENCODE lzo
	,franchise VARCHAR(30)  		--//  ENCODE lzo
	,brand VARCHAR(83)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(255)  		--//  ENCODE lzo
	,group_jb VARCHAR(6)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMEDW_INTEGRATION.edw_vn_si_st_so_details;
CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.EDW_VN_SI_ST_SO_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMEDW_INTEGRATION.edw_vn_si_st_so_details
(
	jj_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,jj_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,wks_in_qrtr numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mnth_start_date DATE  		--//  ENCODE az64
	,mnth_end_date DATE  		--//  ENCODE az64
	,territory_dist VARCHAR(100)  		--//  ENCODE lzo
	,distributor_id_report VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(50)  		--//  ENCODE lzo
	,sap_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_matl_name VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,trade_type VARCHAR(13)  		--//  ENCODE lzo
	,bill_doc VARCHAR(30)  		--//  ENCODE lzo
	,bill_date DATE  		--//  ENCODE az64
	,exchg_rate NUMERIC(9,5)  		--//  ENCODE az64
	,slsmn_cd VARCHAR(50)  		--//  ENCODE lzo
	,slsmn_nm VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_status VARCHAR(1)  		--//  ENCODE lzo
	,so_sls_qty NUMERIC(22,4)  		--//  ENCODE az64
	,so_ret_qty NUMERIC(22,4)  		--//  ENCODE az64
	,so_grs_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,so_net_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,so_avg_wk_tgt NUMERIC(28,6)  		--//  ENCODE az64
	,so_mnth_tgt NUMERIC(24,6)  		--//  ENCODE az64
	,supervisor_code VARCHAR(50)  		--//  ENCODE lzo
	,supervisor_name VARCHAR(100)  		--//  ENCODE lzo
	,asm_id VARCHAR(100)  		--//  ENCODE lzo
	,asm_name VARCHAR(200)  		--//  ENCODE lzo
	,dstrb_region VARCHAR(20)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,shop_type VARCHAR(100)  		--//  ENCODE lzo
	,group_hierarchy VARCHAR(100)  		--//  ENCODE lzo
	,top_door_group VARCHAR(100)  		--//  ENCODE lzo
	,top_door_flag VARCHAR(1)  		--//  ENCODE lzo
	,top_door_target NUMERIC(20,2)  		--//  ENCODE az64
	,si_sls_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_ret_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_nts_val NUMERIC(38,4)  		--//  ENCODE az64
	,si_mnth_tgt NUMERIC(22,4)  		--//  ENCODE az64
	,si_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,st_sls_qty NUMERIC(20,2)  		--//  ENCODE az64
	,st_ret_qty NUMERIC(20,2)  		--//  ENCODE az64
	,st_grs_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,st_net_trd_sls NUMERIC(22,4)  		--//  ENCODE az64
	,st_mnth_tgt NUMERIC(22,4)  		--//  ENCODE az64
	,st_avg_wk_tgt NUMERIC(25,4)  		--//  ENCODE az64
	,pc_target NUMERIC(22,4)  		--//  ENCODE az64
	,pc_target_by_week NUMERIC(22,4)  		--//  ENCODE az64
	,ce_target NUMERIC(22,4)  		--//  ENCODE az64
	,ce_target_by_week NUMERIC(22,4)  		--//  ENCODE az64
	,ub_weekly_target NUMERIC(38,24)  		--//  ENCODE az64
	,visit_call_date DATE  		--//  ENCODE az64
	,product_visit_call_date DATE  		--//  ENCODE az64
	,forecastso_mil NUMERIC(23,4)  		--//  ENCODE az64
	,cntry_nm VARCHAR(40)  		--//  ENCODE lzo
	,sap_state_cd VARCHAR(150)  		--//  ENCODE lzo
	,sap_sls_org VARCHAR(4)  		--//  ENCODE lzo
	,sap_cmp_id VARCHAR(6)  		--//  ENCODE lzo
	,sap_cntry_cd VARCHAR(3)  		--//  ENCODE lzo
	,sap_cntry_nm VARCHAR(40)  		--//  ENCODE lzo
	,sap_addr VARCHAR(150)  		--//  ENCODE lzo
	,sap_region VARCHAR(150)  		--//  ENCODE lzo
	,sap_city VARCHAR(150)  		--//  ENCODE lzo
	,sap_post_cd VARCHAR(150)  		--//  ENCODE lzo
	,sap_chnl_cd VARCHAR(2)  		--//  ENCODE lzo
	,sap_chnl_desc VARCHAR(20)  		--//  ENCODE lzo
	,sap_sls_office_cd VARCHAR(4)  		--//  ENCODE lzo
	,sap_sls_office_desc VARCHAR(40)  		--//  ENCODE lzo
	,sap_sls_grp_cd VARCHAR(3)  		--//  ENCODE lzo
	,sap_sls_grp_desc VARCHAR(40)  		--//  ENCODE lzo
	,sap_curr_cd VARCHAR(5)  		--//  ENCODE lzo
	,gch_region VARCHAR(50)  		--//  ENCODE lzo
	,gch_cluster VARCHAR(50)  		--//  ENCODE lzo
	,gch_subcluster VARCHAR(50)  		--//  ENCODE lzo
	,gch_market VARCHAR(50)  		--//  ENCODE lzo
	,gch_retail_banner VARCHAR(50)  		--//  ENCODE lzo
	,sku VARCHAR(40)  		--//  ENCODE lzo
	,sku_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_mat_type_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_mat_type_desc VARCHAR(40)  		--//  ENCODE lzo
	,sap_base_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_prchse_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_prod_sgmt_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_prod_sgmt_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_base_prod_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_base_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_mega_brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_mega_brnd_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_brnd_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_vrnt_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_vrnt_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_put_up_cd VARCHAR(10)  		--//  ENCODE lzo
	,sap_put_up_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_grp_frnchse_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_grp_frnchse_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_frnchse_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_frnchse_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_frnchse_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_prod_frnchse_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_mjr_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_prod_mjr_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_mnr_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_prod_mnr_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_hier_cd VARCHAR(18)  		--//  ENCODE lzo
	,sap_prod_hier_desc VARCHAR(100)  		--//  ENCODE lzo
	,global_mat_region VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_put_up_cd VARCHAR(10)  		--//  ENCODE lzo
	,global_put_up_desc VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_need_state VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_size VARCHAR(20)  		--//  ENCODE lzo
	,global_prod_size_uom VARCHAR(20)  		--//  ENCODE lzo
	,local_prod_franchise VARCHAR(100)  		--//  ENCODE lzo
	,local_prod_brand VARCHAR(100)  		--//  ENCODE lzo
	,local_prod_variant VARCHAR(100)  		--//  ENCODE lzo
	,local_prod_grp VARCHAR(200)  		--//  ENCODE lzo
	,local_group_jb VARCHAR(50)  		--//  ENCODE lzo
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
	,curr_mnth_indc VARCHAR(1)  		--//  ENCODE lzo
	,curr_year_indc VARCHAR(1)  		--//  ENCODE lzo
)

;
--------------------------------------------------------------



CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_SPIRAL_MTI_OFFTAKE (		--// CREATE TABLE os_itg.itg_spiral_mti_offtake (
    stt varchar(100),		--//  ENCODE lzo // character varying
    area varchar(100),		--//  ENCODE lzo // character varying
    channelname varchar(100),		--//  ENCODE lzo // character varying
    customername varchar(100),		--//  ENCODE lzo // character varying
    shopcode varchar(100),		--//  ENCODE lzo // character varying
    shopname varchar(100),		--//  ENCODE lzo // character varying
    address varchar(100),		--//  ENCODE lzo // character varying
    supcode varchar(100),		--//  ENCODE lzo // character varying
    supname varchar(100),		--//  ENCODE lzo // character varying
    year varchar(100),		--//  ENCODE lzo // character varying
    month varchar(100),		--//  ENCODE lzo // character varying
    barcode varchar(100),		--//  ENCODE lzo // character varying
    productname varchar(100),		--//  ENCODE lzo // character varying
    franchise varchar(100),		--//  ENCODE lzo // character varying
    brand varchar(100),		--//  ENCODE lzo // character varying
    cate varchar(100),		--//  ENCODE lzo // character varying
    sub_cat varchar(100),		--//  ENCODE lzo // character varying
    sub_brand varchar(100),		--//  ENCODE lzo // character varying
    size varchar(100),		--//  ENCODE lzo // character varying
    quantity varchar(100),		--//  ENCODE lzo // character varying
    amount varchar(100),		--//  ENCODE lzo // character varying
    amountusd varchar(100),		--//  ENCODE lzo // character varying
    file_name varchar(100),		--//  ENCODE lzo // character varying
    crtd_name timestamp without time zone,		--//  ENCODE az64
    updt_dttm timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_call_details;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_CALL_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_call_details
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,salesrep_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,outlet_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,visit_date DATE NOT NULL 		--//  ENCODE zstd
	,checkin_time TIMESTAMP WITHOUT TIME ZONE NOT NULL 		--//  ENCODE zstd
	,checkout_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,distance numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,ordervisit VARCHAR(30)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, salesrep_id, outlet_id, visit_date, checkin_time)
)
		--// SORTKEY ( 
		--// 	visit_date
		--// 	)
;		--// ;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_d_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_D_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_d_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,cntry_code VARCHAR(2)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date DATE  		--//  ENCODE zstd
	,invoice_date DATE  		--//  ENCODE zstd
	,order_no VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(50)  		--//  ENCODE zstd
	,sale_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(50)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(50)  		--//  ENCODE zstd
	,material_code VARCHAR(50)  		--//  ENCODE zstd
	,uom VARCHAR(30)  		--//  ENCODE zstd
	,gross_price NUMERIC(15,4)  		--//  ENCODE zstd
	,orderqty NUMERIC(15,4)  		--//  ENCODE zstd
	,quantity NUMERIC(15,4)  		--//  ENCODE zstd
	,total_sellout_afvat_bfdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,discount NUMERIC(15,4)  		--//  ENCODE zstd
	,total_sellout_afvat_afdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,line_number VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,promotion_id VARCHAR(50)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, order_no, line_number)
)
		--// SORTKEY ( 
		--// 	order_date
		--// 	)
;		--// ;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_forecast;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_FORECAST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_forecast
(
	cycle NUMERIC(10,0) NOT NULL 		--//  ENCODE zstd
	,channel VARCHAR(30)  		--//  ENCODE zstd
	,territory_dist VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,warehouse VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,franchise VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,brand VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,variant VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,forecastso_mil VARCHAR(100)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (cycle, territory_dist, warehouse, franchise, brand, variant)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_h_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_H_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_h_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,cntry_code VARCHAR(2)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date DATE  		--//  ENCODE zstd
	,invoice_date DATE  		--//  ENCODE zstd
	,order_no VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sellout_afvat_bfdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,total_discount NUMERIC(15,4)  		--//  ENCODE zstd
	,invoice_discount NUMERIC(15,4)  		--//  ENCODE zstd
	,sellout_afvat_afdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, order_no)
)
		--// SORTKEY ( 
		--// 	order_date
		--// 	)
;		--// ;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_sales_stock_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_SALES_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_sales_stock_fact
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,cntry_code VARCHAR(2)  		--//  ENCODE zstd
	,wh_code VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,date DATE NOT NULL 		--//  ENCODE zstd
	,material_code VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,bat_number VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,expiry_date DATE  		--//  ENCODE zstd
	,quantity NUMERIC(14,2)  		--//  ENCODE zstd
	,uom VARCHAR(2)  		--//  ENCODE zstd
	,amount NUMERIC(15,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, wh_code, date, material_code, bat_number)
)
;
create or replace TABLE VNMITG_INTEGRATION.ITG_VN_DMS_SELLTHRGH_SALES_FACT (
	DSTRBTR_ID VARCHAR(30) NOT NULL,
	DSTRBTR_TYPE VARCHAR(30),
	MAPPED_SPK VARCHAR(30),
	DOC_NUMBER VARCHAR(30) NOT NULL,
	REF_NUMBER VARCHAR(30),
	RECEIPT_DATE DATE,
	ORDER_TYPE VARCHAR(2),
	VAT_INVOICE_NUMBER VARCHAR(30),
	VAT_INVOICE_NOTE VARCHAR(30),
	VAT_INVOICE_DATE DATE,
	PON_NUMBER VARCHAR(40),
	LINE_REF VARCHAR(10),
	PRODUCT_CODE VARCHAR(50) NOT NULL,
	UNIT VARCHAR(10),
	QUANTITY NUMBER(14,2),
	PRICE NUMBER(15,4),
	AMOUNT NUMBER(15,4),
	TAX_AMOUNT NUMBER(15,4),
	TAX_ID VARCHAR(10),
	TAX_RATE NUMBER(15,4),
	"values" NUMBER(15,4),
	LINE_DISCOUNT NUMBER(15,4),
	DOC_DISCOUNT NUMBER(15,4),
	STATUS VARCHAR(1),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	primary key (DSTRBTR_ID, DOC_NUMBER, PRODUCT_CODE)
);


--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_yearly_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_YEARLY_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_yearly_target
(
	year numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,kpi VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,category VARCHAR(200) NOT NULL 		--//  ENCODE zstd
	,target NUMERIC(38,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (year, kpi, category)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_productivity_call_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_PRODUCTIVITY_CALL_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_productivity_call_target
(
	jj_year numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE zstd
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE zstd
	,jj_mnth_no numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,jj_mnth_wk_no numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,no_of_week numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,no_of_days numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,visit_date DATE  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,target_by_day NUMERIC(20,4)  		--//  ENCODE zstd
	,target_by_week NUMERIC(20,4)  		--//  ENCODE zstd
	,target_by_month NUMERIC(20,4)  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_visit_call_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_VISIT_CALL_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_visit_call_target
(
	jj_year numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,jj_qrtr VARCHAR(14)  		--//  ENCODE zstd
	,jj_mnth_id VARCHAR(23)  		--//  ENCODE zstd
	,jj_mnth_no numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,jj_mnth_wk_no numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,no_of_week numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,no_of_days numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,visit_date DATE  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,target_by_day NUMERIC(20,4)  		--//  ENCODE zstd
	,target_by_week NUMERIC(20,4)  		--//  ENCODE zstd
	,target_by_month NUMERIC(20,4)  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellin_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_WEEKLY_SELLIN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellin_target
(
	territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,target_cyc numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,target_wk numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,tgt_by_month NUMERIC(15,4)  		--//  ENCODE zstd
	,prev_sales_mnth1 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk1 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk1 NUMERIC(20,4)  		--//  ENCODE zstd
	,prev_sales_mnth2 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk2 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk2 NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_by_wk NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_for_both_months NUMERIC(20,4)  		--//  ENCODE zstd
	,tgt_by_week NUMERIC(20,4)  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellout_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_WEEKLY_SELLOUT_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellout_target
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,saleman_code VARCHAR(30)  		--//  ENCODE zstd
	,target_cyc NUMERIC(18,0)  		--//  ENCODE zstd
	,target_wk numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,tgt_by_month NUMERIC(15,4)  		--//  ENCODE zstd
	,prev_sales_mnth1 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk1 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk1 NUMERIC(20,4)  		--//  ENCODE zstd
	,prev_sales_mnth2 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk2 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk2 NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_by_wk NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_for_both_months NUMERIC(20,4)  		--//  ENCODE zstd
	,tgt_by_week NUMERIC(20,4)  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellthrough_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_WEEKLY_SELLTHROUGH_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_weekly_sellthrough_target
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,target_cyc numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,target_wk numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,tgt_by_month NUMERIC(15,4)  		--//  ENCODE zstd
	,prev_sales_mnth1 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk1 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk1 NUMERIC(20,4)  		--//  ENCODE zstd
	,prev_sales_mnth2 VARCHAR(23)  		--//  ENCODE zstd
	,prev_sales_mnth_wk2 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prev_sales_amt_wk2 NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_by_wk NUMERIC(20,4)  		--//  ENCODE zstd
	,sum_for_both_months NUMERIC(20,4)  		--//  ENCODE zstd
	,tgt_by_week NUMERIC(20,4)  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_allchannel_siso_target_sku;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_ALLCHANNEL_SISO_TARGET_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_allchannel_siso_target_sku
(
	cycle NUMERIC(10,0)  		--//  ENCODE az64
	,data_type VARCHAR(30)  		--//  ENCODE lzo
	,jnj_code NUMERIC(31,0)  		--//  ENCODE az64
	,description VARCHAR(200)  		--//  ENCODE lzo
	,dksh_mti NUMERIC(31,7)  		--//  ENCODE az64
	,dksh_ecom NUMERIC(31,7)  		--//  ENCODE az64
	,mtd NUMERIC(31,7)  		--//  ENCODE az64
	,gt_tien_thanh NUMERIC(31,7)  		--//  ENCODE az64
	,gt_duong_anh NUMERIC(31,7)  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,otc VARCHAR(100)  		--//  ENCODE lzo
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_ecom_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_ECOM_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_ecom_target
(
	cycle NUMERIC(10,0)  		--//  ENCODE az64
	,platform VARCHAR(50)  		--//  ENCODE zstd
	,target NUMERIC(31,1)  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_gt_gts_ratio;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_GT_GTS_RATIO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_gt_gts_ratio
(
	distributor VARCHAR(255)  		--//  ENCODE lzo
	,from_month VARCHAR(10)  		--//  ENCODE lzo
	,to_month VARCHAR(10)  		--//  ENCODE lzo
	,percentage NUMERIC(18,4)  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_store_tagging;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_PS_STORE_TAGGING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_store_tagging
(
	parent_customer VARCHAR(256)  		--//  ENCODE lzo
	,store_code VARCHAR(256)  		--//  ENCODE lzo
	,store_name VARCHAR(256)  		--//  ENCODE lzo
	,status VARCHAR(256)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_targets;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_weights;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_cust_master;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_DKSH_CUST_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_cust_master
(
	account_code VARCHAR(500)  		--//  ENCODE zstd
	,account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_name VARCHAR(500)  		--//  ENCODE zstd
	,address VARCHAR(200)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_account_code VARCHAR(500)  		--//  ENCODE zstd
	,group_account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_account_name VARCHAR(500)  		--//  ENCODE zstd
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,province_code VARCHAR(500)  		--//  ENCODE zstd
	,province_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retail_environment VARCHAR(200)  		--//  ENCODE zstd
	,province_name VARCHAR(500)  		--//  ENCODE zstd
	,region_code VARCHAR(500)  		--//  ENCODE zstd
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(500)  		--//  ENCODE zstd
	,sub_channel_code VARCHAR(500)  		--//  ENCODE zstd
	,sub_channel_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sub_channel_name VARCHAR(500)  		--//  ENCODE zstd
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ten_st_ddp VARCHAR(200)  		--//  ENCODE zstd
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_cust_master_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_DKSH_CUST_MASTER_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_cust_master_wrk
(
	account_code VARCHAR(500)  		--//  ENCODE lzo
	,account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_name VARCHAR(500)  		--//  ENCODE lzo
	,address VARCHAR(200)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_account_code VARCHAR(500)  		--//  ENCODE lzo
	,group_account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_account_name VARCHAR(500)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,province_code VARCHAR(500)  		--//  ENCODE lzo
	,province_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retail_environment VARCHAR(200)  		--//  ENCODE lzo
	,province_name VARCHAR(500)  		--//  ENCODE lzo
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,sub_channel_code VARCHAR(500)  		--//  ENCODE lzo
	,sub_channel_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sub_channel_name VARCHAR(500)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ten_st_ddp VARCHAR(200)  		--//  ENCODE lzo
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_product_master;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_DKSH_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_product_master
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,code VARCHAR(500)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,barcode NUMERIC(31,0)  		--//  ENCODE az64
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,base_bundle VARCHAR(500)  		--//  ENCODE zstd
	,category VARCHAR(500)  		--//  ENCODE zstd
	,franchise VARCHAR(500)  		--//  ENCODE zstd
	,product_name VARCHAR(500)  		--//  ENCODE zstd
	,size VARCHAR(400)  		--//  ENCODE zstd
	,sub_brand VARCHAR(400)  		--//  ENCODE zstd
	,sub_category VARCHAR(500)  		--//  ENCODE zstd
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_product_master_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_DKSH_PRODUCT_MASTER_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_dksh_product_master_wrk
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,barcode NUMERIC(31,0)  		--//  ENCODE az64
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,base_bundle VARCHAR(500)  		--//  ENCODE lzo
	,category VARCHAR(500)  		--//  ENCODE lzo
	,franchise VARCHAR(500)  		--//  ENCODE lzo
	,product_name VARCHAR(500)  		--//  ENCODE lzo
	,size VARCHAR(400)  		--//  ENCODE lzo
	,sub_brand VARCHAR(400)  		--//  ENCODE lzo
	,sub_category VARCHAR(500)  		--//  ENCODE lzo
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;


--DROP TABLE VNMITG_INTEGRATION.itg_mds_log;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_LOG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_log
(
	cntry_cd VARCHAR(10)  		--//  ENCODE lzo
	,table_name VARCHAR(255)  		--//  ENCODE lzo
	,result VARCHAR(1000)  		--//  ENCODE lzo
	,status VARCHAR(50)  		--//  ENCODE lzo
	,rec_count numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crtd_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;

--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_customer_segmentation;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_CUSTOMER_SEGMENTATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_customer_segmentation
(
	cust_num VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_1_code VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_2_code VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE VNMITG_INTEGRATION.itg_mds_vn_gt_msl_shoptype_mapping;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_MDS_VN_GT_MSL_SHOPTYPE_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_mds_vn_gt_msl_shoptype_mapping
(
	shoptype_code VARCHAR(200)  		--//  ENCODE lzo
	,shoptype_name VARCHAR(200)  		--//  ENCODE lzo
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,subchannel VARCHAR(200)  		--//  ENCODE lzo
	,msl_subchannel VARCHAR(500)  		--//  ENCODE lzo
	,active NUMERIC(31,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_gt_topdoor_storetype_hierarchy;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_GT_TOPDOOR_STORETYPE_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_gt_topdoor_storetype_hierarchy
(
	storetype VARCHAR(100)  		--//  ENCODE lzo
	,group_hierarchy VARCHAR(100)  		--//  ENCODE lzo
	,top_door_group VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
------------------------------------------
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_history_saleout;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_HISTORY_SALEOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_history_saleout
(
	user_id VARCHAR(100)  		--//  ENCODE zstd
	,rsm_name VARCHAR(200)  		--//  ENCODE zstd
	,group_jb VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,dmsproduct_group VARCHAR(200)  		--//  ENCODE zstd
	,product_code VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,dmsproductid VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(100)  		--//  ENCODE zstd
	,tax NUMERIC(15,4)  		--//  ENCODE zstd
	,province VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,cycle numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,sellout_afvat_bfdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,sellout_afvat_afdisc NUMERIC(15,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (product_code, province, cycle)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_gt_topdoor_targets;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_GT_TOPDOOR_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_gt_topdoor_targets
(
	td_id VARCHAR(100)  		--//  ENCODE lzo
	,td_zone VARCHAR(50)  		--//  ENCODE lzo
	,from_cycle VARCHAR(10)  		--//  ENCODE lzo
	,to_cycle VARCHAR(10)  		--//  ENCODE lzo
	,distributor_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,shoptype VARCHAR(100)  		--//  ENCODE lzo
	,target_value NUMERIC(18,2)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_customer_sales_organization;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_CUSTOMER_SALES_ORGANIZATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_customer_sales_organization
(
	address VARCHAR(1200)  		--//  ENCODE zstd
	,code VARCHAR(500)  		--//  ENCODE zstd
	,code_sr_pg VARCHAR(60)  		--//  ENCODE zstd
	,code_ss VARCHAR(60)  		--//  ENCODE zstd
	,customer_name VARCHAR(600)  		--//  ENCODE zstd
	,district_name VARCHAR(200)  		--//  ENCODE zstd
	,dksh_jnj VARCHAR(200)  		--//  ENCODE zstd
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,kam VARCHAR(200)  		--//  ENCODE zstd
	,mtd_code NUMERIC(31,0)  		--//  ENCODE az64
	,mti_code NUMERIC(31,0)  		--//  ENCODE az64
	,name VARCHAR(500)  		--//  ENCODE zstd
	,rom VARCHAR(60)  		--//  ENCODE zstd
	,sales_man VARCHAR(400)  		--//  ENCODE zstd
	,sales_supervisor VARCHAR(200)  		--//  ENCODE zstd
	,status VARCHAR(60)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from DATE  		--//  ENCODE az64
	,effective_to DATE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_customer_sales_organization_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_CUSTOMER_SALES_ORGANIZATION_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_customer_sales_organization_wrk
(
	address VARCHAR(1200)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,code_sr_pg VARCHAR(60)  		--//  ENCODE lzo
	,code_ss VARCHAR(60)  		--//  ENCODE lzo
	,customer_name VARCHAR(600)  		--//  ENCODE lzo
	,district_name VARCHAR(200)  		--//  ENCODE lzo
	,dksh_jnj VARCHAR(200)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,kam VARCHAR(200)  		--//  ENCODE lzo
	,mtd_code NUMERIC(31,0)  		--//  ENCODE az64
	,mti_code NUMERIC(31,0)  		--//  ENCODE az64
	,name VARCHAR(500)  		--//  ENCODE lzo
	,rom VARCHAR(60)  		--//  ENCODE lzo
	,sales_man VARCHAR(400)  		--//  ENCODE lzo
	,sales_supervisor VARCHAR(200)  		--//  ENCODE lzo
	,status VARCHAR(60)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from DATE  		--//  ENCODE az64
	,effective_to DATE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_cust_master;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_CUST_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_cust_master
(
	chain VARCHAR(200)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE zstd
	,customer_code VARCHAR(200)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,customer_store_code VARCHAR(200)  		--//  ENCODE zstd
	,district VARCHAR(200)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,format VARCHAR(200)  		--//  ENCODE zstd
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,note_closed_store VARCHAR(200)  		--//  ENCODE zstd
	,plant VARCHAR(200)  		--//  ENCODE zstd
	,status VARCHAR(200)  		--//  ENCODE zstd
	,store_code VARCHAR(200)  		--//  ENCODE zstd
	,store_name VARCHAR(200)  		--//  ENCODE zstd
	,store_name_2 VARCHAR(200)  		--//  ENCODE zstd
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,wh VARCHAR(200)  		--//  ENCODE zstd
	,zone VARCHAR(200)  		--//  ENCODE zstd
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_cust_master_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_CUST_MASTER_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_cust_master_wrk
(
	chain VARCHAR(200)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE lzo
	,customer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,customer_store_code VARCHAR(200)  		--//  ENCODE lzo
	,district VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,format VARCHAR(200)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,note_closed_store VARCHAR(200)  		--//  ENCODE lzo
	,plant VARCHAR(200)  		--//  ENCODE lzo
	,status VARCHAR(200)  		--//  ENCODE lzo
	,store_code VARCHAR(200)  		--//  ENCODE lzo
	,store_name VARCHAR(200)  		--//  ENCODE lzo
	,store_name_2 VARCHAR(200)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,wh VARCHAR(200)  		--//  ENCODE lzo
	,zone VARCHAR(200)  		--//  ENCODE lzo
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_price_products;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_PRICE_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_price_products
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,code VARCHAR(500)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE zstd
	,franchise VARCHAR(60)  		--//  ENCODE zstd
	,brand VARCHAR(200)  		--//  ENCODE zstd
	,sku VARCHAR(500)  		--//  ENCODE zstd
	,bar_code VARCHAR(200)  		--//  ENCODE zstd
	,pc_per_case NUMERIC(31,2)  		--//  ENCODE zstd
	,price NUMERIC(31,5)  		--//  ENCODE zstd
	,product_id_concung VARCHAR(200)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_price_products_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_PRICE_PRODUCTS_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_price_products_wrk
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE az64
	,franchise VARCHAR(60)  		--//  ENCODE lzo
	,brand VARCHAR(200)  		--//  ENCODE lzo
	,sku VARCHAR(500)  		--//  ENCODE lzo
	,bar_code VARCHAR(200)  		--//  ENCODE lzo
	,pc_per_case NUMERIC(31,2)  		--//  ENCODE az64
	,price NUMERIC(31,5)  		--//  ENCODE az64
	,product_id_concung VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_product_master;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_product_master
(
	barcode VARCHAR(200)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE zstd
	,customer VARCHAR(200)  		--//  ENCODE zstd
	,customer_sku VARCHAR(200)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_product_master_wrk;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_PRODUCT_MASTER_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_product_master_wrk
(
	barcode VARCHAR(200)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE lzo
	,customer VARCHAR(200)  		--//  ENCODE lzo
	,customer_sku VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_coop;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_coop
(
	year VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,sku VARCHAR(20)  		--//  ENCODE lzo
	,idescr VARCHAR(200)  		--//  ENCODE lzo
	,vendor VARCHAR(50)  		--//  ENCODE lzo
	,asname VARCHAR(200)  		--//  ENCODE lzo
	,imfgr VARCHAR(50)  		--//  ENCODE lzo
	,store VARCHAR(50)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh
(
	supplier_code VARCHAR(10)  		--//  ENCODE lzo
	,supplier_name VARCHAR(100)  		--//  ENCODE lzo
	,plant VARCHAR(10)  		--//  ENCODE lzo
	,productid VARCHAR(20)  		--//  ENCODE lzo
	,product VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,sellin_category VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(100)  		--//  ENCODE lzo
	,product_sub_group VARCHAR(100)  		--//  ENCODE lzo
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE lzo
	,custcode VARCHAR(20)  		--//  ENCODE lzo
	,customer VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,district VARCHAR(100)  		--//  ENCODE lzo
	,province VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(20)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,channel VARCHAR(10)  		--//  ENCODE lzo
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,cust_group VARCHAR(50)  		--//  ENCODE lzo
	,billing_no VARCHAR(20)  		--//  ENCODE lzo
	,invoice_date VARCHAR(20)  		--//  ENCODE lzo
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE lzo
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE lzo
	,order_type VARCHAR(200)  		--//  ENCODE lzo
	,red_invoice_no VARCHAR(200)  		--//  ENCODE lzo
	,expiry_date VARCHAR(200)  		--//  ENCODE lzo
	,order_no VARCHAR(200)  		--//  ENCODE lzo
	,order_date VARCHAR(200)  		--//  ENCODE lzo
	,period VARCHAR(20)  		--//  ENCODE lzo
	,sellout_sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,group_account VARCHAR(50)  		--//  ENCODE lzo
	,account VARCHAR(50)  		--//  ENCODE lzo
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(20)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,sellout_category VARCHAR(50)  		--//  ENCODE lzo
	,sub_cat VARCHAR(50)  		--//  ENCODE lzo
	,sub_brand VARCHAR(50)  		--//  ENCODE lzo
	,barcode VARCHAR(20)  		--//  ENCODE lzo
	,base_or_bundle VARCHAR(20)  		--//  ENCODE lzo
	,size VARCHAR(20)  		--//  ENCODE lzo
	,key_chain VARCHAR(20)  		--//  ENCODE lzo
	,status VARCHAR(20)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh_fact
(
	supplier_code VARCHAR(10)  		--//  ENCODE lzo
	,supplier_name VARCHAR(100)  		--//  ENCODE lzo
	,plant VARCHAR(10)  		--//  ENCODE lzo
	,productid VARCHAR(20)  		--//  ENCODE lzo
	,product VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,sellin_category VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(100)  		--//  ENCODE lzo
	,product_sub_group VARCHAR(100)  		--//  ENCODE lzo
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE lzo
	,custcode VARCHAR(20)  		--//  ENCODE lzo
	,customer VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,district VARCHAR(100)  		--//  ENCODE lzo
	,province VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(20)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,channel VARCHAR(10)  		--//  ENCODE lzo
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,cust_group VARCHAR(50)  		--//  ENCODE lzo
	,billing_no VARCHAR(20)  		--//  ENCODE lzo
	,invoice_date VARCHAR(20)  		--//  ENCODE lzo
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE lzo
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE lzo
	,order_type VARCHAR(100)  		--//  ENCODE lzo
	,red_invoice_no VARCHAR(100)  		--//  ENCODE lzo
	,expiry_date VARCHAR(100)  		--//  ENCODE lzo
	,order_no VARCHAR(100)  		--//  ENCODE lzo
	,order_date VARCHAR(100)  		--//  ENCODE lzo
	,period VARCHAR(100)  		--//  ENCODE lzo
	,sellout_sub_channel VARCHAR(100)  		--//  ENCODE lzo
	,group_account VARCHAR(100)  		--//  ENCODE lzo
	,account VARCHAR(100)  		--//  ENCODE lzo
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(100)  		--//  ENCODE lzo
	,franchise VARCHAR(100)  		--//  ENCODE lzo
	,sellout_category VARCHAR(100)  		--//  ENCODE lzo
	,sub_cat VARCHAR(100)  		--//  ENCODE lzo
	,sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,base_or_bundle VARCHAR(100)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,key_chain VARCHAR(100)  		--//  ENCODE lzo
	,status VARCHAR(100)  		--//  ENCODE lzo
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_target
(
	mtd_code VARCHAR(20)  		--//  ENCODE zstd
	,mti_code VARCHAR(20)  		--//  ENCODE zstd
	,target NUMERIC(20,5)  		--//  ENCODE az64
	,sellin_cycle numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellin_year VARCHAR(10)  		--//  ENCODE zstd
	,visit VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_aeon;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_AEON		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_aeon
(
	year VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,store VARCHAR(20)  		--//  ENCODE lzo
	,department VARCHAR(30)  		--//  ENCODE lzo
	,supplier_code VARCHAR(50)  		--//  ENCODE lzo
	,supplier_name VARCHAR(60)  		--//  ENCODE lzo
	,item VARCHAR(30)  		--//  ENCODE lzo
	,item_name VARCHAR(200)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_bhx;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_BHX		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_bhx
(
	year VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,pro_code VARCHAR(50)  		--//  ENCODE lzo
	,pro_name VARCHAR(200)  		--//  ENCODE lzo
	,cust_code VARCHAR(20)  		--//  ENCODE lzo
	,cust_name VARCHAR(200)  		--//  ENCODE lzo
	,cat_store VARCHAR(60)  		--//  ENCODE lzo
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_con_cung;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_CON_CUNG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_con_cung
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,delivery_code VARCHAR(50)  		--//  ENCODE zstd
	,store VARCHAR(255)  		--//  ENCODE zstd
	,product_code VARCHAR(20)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_coop;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_coop
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,thang VARCHAR(20)  		--//  ENCODE zstd
	,desc_a VARCHAR(20)  		--//  ENCODE zstd
	,idept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isdept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,iclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,tenvt VARCHAR(255)  		--//  ENCODE zstd
	,brand_spm VARCHAR(50)  		--//  ENCODE zstd
	,madv VARCHAR(20)  		--//  ENCODE zstd
	,sumoflg numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sumofttbvnckhd NUMERIC(20,5)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE zstd
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_guardian;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_GUARDIAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_guardian
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,serial_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,description_vietnamese VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,division VARCHAR(50)  		--//  ENCODE zstd
	,department VARCHAR(50)  		--//  ENCODE zstd
	,category VARCHAR(50)  		--//  ENCODE zstd
	,sub_category VARCHAR(50)  		--//  ENCODE zstd
	,sales_supplier numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_lotte;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_LOTTE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_lotte
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,str VARCHAR(10)  		--//  ENCODE zstd
	,str_nm VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_1 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_2 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_3 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_4 VARCHAR(50)  		--//  ENCODE zstd
	,prod_cd VARCHAR(100)  		--//  ENCODE zstd
	,sale_cd VARCHAR(100)  		--//  ENCODE zstd
	,prod_nm VARCHAR(100)  		--//  ENCODE zstd
	,ven VARCHAR(100)  		--//  ENCODE zstd
	,ven_nm VARCHAR(100)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,tot_sale_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_mega;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_MEGA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_mega
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,site_no VARCHAR(20)  		--//  ENCODE zstd
	,site_name VARCHAR(255)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,art_no VARCHAR(20)  		--//  ENCODE zstd
	,art_sv_name VARCHAR(255)  		--//  ENCODE zstd
	,suppl_no VARCHAR(20)  		--//  ENCODE zstd
	,suppl_name VARCHAR(255)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cogs_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_vinmart;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_VINMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellout_vinmart
(
	year VARCHAR(20)  		--//  ENCODE zstd
	,month VARCHAR(20)  		--//  ENCODE zstd
	,store VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,mch5_mc VARCHAR(20)  		--//  ENCODE zstd
	,mc VARCHAR(100)  		--//  ENCODE zstd
	,article VARCHAR(20)  		--//  ENCODE zstd
	,article_name VARCHAR(255)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,pos_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pos_revenue NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
------------------------------------
--DROP TABLE VNMITG_INTEGRATION.itg_vn_distributor_sap_sold_to_mapping;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DISTRIBUTOR_SAP_SOLD_TO_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_distributor_sap_sold_to_mapping
(
	distributor_id VARCHAR(30)  		--//  ENCODE lzo
	,sap_sold_to_code VARCHAR(30)  		--//  ENCODE lzo
	,region VARCHAR(3)  		--//  ENCODE lzo
	,sap_ship_to_code VARCHAR(50)  		--//  ENCODE lzo
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dksh_daily_sales;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DKSH_DAILY_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dksh_daily_sales
(
	group_ds VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,materialdescription VARCHAR(255)  		--//  ENCODE lzo
	,syslot VARCHAR(255)  		--//  ENCODE lzo
	,batchno VARCHAR(255)  		--//  ENCODE lzo
	,exp_date VARCHAR(255)  		--//  ENCODE lzo
	,total numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hcm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vsip numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,langha numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,thanhtri numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,danang numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,values_lc NUMERIC(24,10)  		--//  ENCODE az64
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,file_date VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_customer_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_customer_dim
(
	sap_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,outlet_name VARCHAR(100)  		--//  ENCODE zstd
	,address_1 VARCHAR(200)  		--//  ENCODE zstd
	,address_2 VARCHAR(200)  		--//  ENCODE zstd
	,telephone VARCHAR(20)  		--//  ENCODE zstd
	,fax VARCHAR(20)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,postcode VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,channel_group VARCHAR(2)  		--//  ENCODE zstd
	,sub_channel VARCHAR(20)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,gps_lat VARCHAR(30)  		--//  ENCODE zstd
	,gps_long VARCHAR(30)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,district VARCHAR(30)  		--//  ENCODE zstd
	,province VARCHAR(30)  		--//  ENCODE zstd
	,crt_date DATE  		--//  ENCODE zstd
	,date_off DATE  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,shop_type VARCHAR(100)  		--//  ENCODE zstd
	,PRIMARY KEY (outlet_id)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_distributor_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_DISTRIBUTOR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_distributor_dim
(
	territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(100)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,dstrbtr_address VARCHAR(200)  		--//  ENCODE zstd
	,longitude VARCHAR(20)  		--//  ENCODE zstd
	,latitude VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_kpi;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_KPI		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_kpi
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,saleman_code VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,saleman_name VARCHAR(50)  		--//  ENCODE zstd
	,cycle NUMERIC(18,0) NOT NULL 		--//  ENCODE zstd
	,export_date DATE  		--//  ENCODE zstd
	,kpi_type VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,target_value NUMERIC(15,4)  		--//  ENCODE zstd
	,actual_value NUMERIC(15,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, saleman_code, cycle, kpi_type)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_msl;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_msl
(
	msl_id VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sub_channel VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,from_cycle numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,to_cycle numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,product_id VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,prouct_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
	,PRIMARY KEY (msl_id, sub_channel, from_cycle, to_cycle, product_id)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_order_promotion;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_ORDER_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_order_promotion
(
	branch_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,pro_id VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,ord_no VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,line_ref VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,disc_type VARCHAR(1)  		--//  ENCODE zstd
	,break_by VARCHAR(1)  		--//  ENCODE zstd
	,disc_break_line_ref VARCHAR(10)  		--//  ENCODE zstd
	,disct_bl_amt NUMERIC(15,4)  		--//  ENCODE zstd
	,disct_bl_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,free_item_code VARCHAR(20)  		--//  ENCODE zstd
	,free_item_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,disc_amt NUMERIC(15,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (branch_id, pro_id, ord_no, line_ref)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_product_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_PRODUCT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_product_dim
(
	product_code VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,productnamesap VARCHAR(100)  		--//  ENCODE zstd
	,unit VARCHAR(10)  		--//  ENCODE zstd
	,tax_rate VARCHAR(10)  		--//  ENCODE zstd
	,weight NUMERIC(15,4)  		--//  ENCODE zstd
	,volume NUMERIC(15,4)  		--//  ENCODE zstd
	,group_jb VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,active VARCHAR(1)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (product_code)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_promotion_list;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_PROMOTION_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_promotion_list
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,cntry_code VARCHAR(2)  		--//  ENCODE zstd
	,promotion_id VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,promotion_name VARCHAR(256)  		--//  ENCODE zstd
	,promotion_desc VARCHAR(256)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE zstd
	,end_date DATE  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, promotion_id)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_sales_org_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_SALES_ORG_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_sales_org_dim
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,salesrep_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,supervisor_code VARCHAR(50)  		--//  ENCODE zstd
	,salesrep_crtdate DATE  		--//  ENCODE zstd
	,salesrep_dateoff DATE  		--//  ENCODE zstd
	,supervisor_name VARCHAR(100)  		--//  ENCODE zstd
	,sup_active VARCHAR(1)  		--//  ENCODE zstd
	,sup_crtdate DATE  		--//  ENCODE zstd
	,sup_dateoff DATE  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,asm_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(1)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, salesrep_id)
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_answers;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_ANSWERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_answers
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value NUMERIC(20,0)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,oos NUMERIC(20,0)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_branch;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_BRANCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_branch
(
	parent_cust_code VARCHAR(255)  		--//  ENCODE zstd
	,parent_cust_name VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,branch_name VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,sales_group VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,city VARCHAR(255)  		--//  ENCODE zstd
	,district VARCHAR(255)  		--//  ENCODE zstd
	,trade_type VARCHAR(255)  		--//  ENCODE zstd
	,store_prioritization VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(32,16)  		--//  ENCODE zstd
	,longitude NUMERIC(32,16)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_choices;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_CHOICES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_choices
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(20,0)  		--//  ENCODE zstd
	,channel_name VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,sku_group VARCHAR(255)  		--//  ENCODE zstd
	,rep_param VARCHAR(255)  		--//  ENCODE zstd
	,putup_id VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,sfa NUMERIC(20,0)  		--//  ENCODE zstd
	,brand_id VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_cpg;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_CPG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_cpg
(
	slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,visitdate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_customer_visited;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_CUSTOMER_VISITED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_customer_visited
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,created_date VARCHAR(255)  		--//  ENCODE zstd
	,visit_date VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_ise_header;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_ISE_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_ise_header
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ise_desc VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,startdate VARCHAR(255)  		--//  ENCODE zstd
	,enddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_notes;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_NOTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_notes
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_interface_question;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_INTERFACE_QUESTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_interface_question
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(10,0)  		--//  ENCODE zstd
	,channel VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_desc VARCHAR(255)  		--//  ENCODE zstd
	,standard_ques NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_class_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_class_desc VARCHAR(255)  		--//  ENCODE zstd
	,weigh NUMERIC(10,0)  		--//  ENCODE zstd
	,total_score NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_code VARCHAR(255)  		--//  ENCODE zstd
	,answer_desc VARCHAR(255)  		--//  ENCODE zstd
	,franchise_code VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_union;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_POS_UNION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_pos_union
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account VARCHAR(20)  		--//  ENCODE zstd
	,customer_cd VARCHAR(200)  		--//  ENCODE zstd
	,store_name VARCHAR(200)  		--//  ENCODE zstd
	,product_cd VARCHAR(200)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh_history;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH_HISTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_mt_sellin_dksh_history
(
	supplier_code VARCHAR(10)  		--//  ENCODE lzo
	,supplier_name VARCHAR(100)  		--//  ENCODE lzo
	,plant VARCHAR(10)  		--//  ENCODE lzo
	,productid VARCHAR(20)  		--//  ENCODE lzo
	,product VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,sellin_category VARCHAR(100)  		--//  ENCODE lzo
	,product_group VARCHAR(100)  		--//  ENCODE lzo
	,product_sub_group VARCHAR(100)  		--//  ENCODE lzo
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE lzo
	,custcode VARCHAR(20)  		--//  ENCODE lzo
	,customer VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,district VARCHAR(100)  		--//  ENCODE lzo
	,province VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(20)  		--//  ENCODE lzo
	,zone VARCHAR(20)  		--//  ENCODE lzo
	,channel VARCHAR(10)  		--//  ENCODE lzo
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,cust_group VARCHAR(50)  		--//  ENCODE lzo
	,billing_no VARCHAR(20)  		--//  ENCODE lzo
	,invoice_date VARCHAR(20)  		--//  ENCODE lzo
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE lzo
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE lzo
	,order_type VARCHAR(200)  		--//  ENCODE lzo
	,red_invoice_no VARCHAR(200)  		--//  ENCODE lzo
	,expiry_date VARCHAR(200)  		--//  ENCODE lzo
	,order_no VARCHAR(200)  		--//  ENCODE lzo
	,order_date VARCHAR(200)  		--//  ENCODE lzo
	,period VARCHAR(20)  		--//  ENCODE lzo
	,sellout_sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,group_account VARCHAR(50)  		--//  ENCODE lzo
	,account VARCHAR(50)  		--//  ENCODE lzo
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(20)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,sellout_category VARCHAR(50)  		--//  ENCODE lzo
	,sub_cat VARCHAR(50)  		--//  ENCODE lzo
	,sub_brand VARCHAR(50)  		--//  ENCODE lzo
	,barcode VARCHAR(20)  		--//  ENCODE lzo
	,base_or_bundle VARCHAR(20)  		--//  ENCODE lzo
	,size VARCHAR(20)  		--//  ENCODE lzo
	,key_chain VARCHAR(20)  		--//  ENCODE lzo
	,status VARCHAR(20)  		--//  ENCODE lzo
	,ppg VARCHAR(20)  		--//  ENCODE lzo
	,sub_cat_monthly_sales VARCHAR(20)  		--//  ENCODE lzo
	,sales_sup VARCHAR(255)  		--//  ENCODE lzo
	,sp VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_oneview_otc;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_ONEVIEW_OTC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_oneview_otc
(
	plant VARCHAR(10)  		--//  ENCODE zstd
	,principalcode VARCHAR(100)  		--//  ENCODE zstd
	,principal VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,kunnr VARCHAR(100)  		--//  ENCODE zstd
	,name1 VARCHAR(100)  		--//  ENCODE zstd
	,name2 VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,zterm VARCHAR(100)  		--//  ENCODE zstd
	,kdgrp VARCHAR(100)  		--//  ENCODE zstd
	,custgroup VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,vbeln VARCHAR(100)  		--//  ENCODE zstd
	,billingdate VARCHAR(100)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,qty VARCHAR(100)  		--//  ENCODE zstd
	,dgle VARCHAR(100)  		--//  ENCODE zstd
	,pernr VARCHAR(100)  		--//  ENCODE zstd
	,vat VARCHAR(100)  		--//  ENCODE zstd
	,suom VARCHAR(100)  		--//  ENCODE zstd
	,custpayto VARCHAR(100)  		--//  ENCODE zstd
	,tt NUMERIC(21,5)  		--//  ENCODE zstd
	,nguyengia VARCHAR(100)  		--//  ENCODE zstd
	,ttv NUMERIC(21,5)  		--//  ENCODE zstd
	,discount NUMERIC(21,5)  		--//  ENCODE zstd
	,device_code VARCHAR(100)  		--//  ENCODE zstd
	,device VARCHAR(100)  		--//  ENCODE zstd
	,order_no VARCHAR(100)  		--//  ENCODE zstd
	,orginv VARCHAR(100)  		--//  ENCODE zstd
	,batch VARCHAR(100)  		--//  ENCODE zstd
	,charge VARCHAR(100)  		--//  ENCODE zstd
	,contact_name VARCHAR(100)  		--//  ENCODE zstd
	,userid VARCHAR(100)  		--//  ENCODE zstd
	,billinginst VARCHAR(100)  		--//  ENCODE zstd
	,distchannel VARCHAR(100)  		--//  ENCODE zstd
	,redinv VARCHAR(100)  		--//  ENCODE zstd
	,serial VARCHAR(100)  		--//  ENCODE zstd
	,potext VARCHAR(100)  		--//  ENCODE zstd
	,expdate VARCHAR(100)  		--//  ENCODE zstd
	,ret_so VARCHAR(100)  		--//  ENCODE zstd
	,vat_code VARCHAR(100)  		--//  ENCODE zstd
	,sodoc_date VARCHAR(100)  		--//  ENCODE zstd
	,itemnotes VARCHAR(100)  		--//  ENCODE zstd
	,mg1 VARCHAR(100)  		--//  ENCODE zstd
	,year VARCHAR(100)  		--//  ENCODE zstd
	,month VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,sap_matl_num VARCHAR(100)  		--//  ENCODE lzo
)
;
--DROP TABLE VNMITG_INTEGRATION.itg_vn_product_mapping;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_product_mapping
(
	putupid VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(255)  		--//  ENCODE zstd
	,productname VARCHAR(2000)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
;

--DROP TABLE VNMITG_INTEGRATION.itg_vn_dms_kpi_sellin_sellthrgh;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.ITG_VN_DMS_KPI_SELLIN_SELLTHRGH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.itg_vn_dms_kpi_sellin_sellthrgh
(
	dstrbtr_id VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(10)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,cycle numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,ordertype VARCHAR(30) NOT NULL 		--//  ENCODE zstd
	,sellin_tg NUMERIC(15,4)  		--//  ENCODE zstd
	,sellin_ac NUMERIC(15,4)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,PRIMARY KEY (dstrbtr_id, cycle, ordertype)
)
;



CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_SPIRAL_MTI_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_spiral_mti_offtake
(
	stt VARCHAR(100)  		--//  ENCODE lzo
	,area VARCHAR(100)  		--//  ENCODE lzo
	,channelname VARCHAR(100)  		--//  ENCODE lzo
	,customername VARCHAR(100)  		--//  ENCODE lzo
	,shopcode VARCHAR(100)  		--//  ENCODE lzo
	,shopname VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,supcode VARCHAR(100)  		--//  ENCODE lzo
	,supname VARCHAR(100)  		--//  ENCODE lzo
	,year VARCHAR(100)  		--//  ENCODE lzo
	,month VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,productname VARCHAR(100)  		--//  ENCODE lzo
	,franchise VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,cate VARCHAR(100)  		--//  ENCODE lzo
	,sub_cat VARCHAR(100)  		--//  ENCODE lzo
	,sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,quantity VARCHAR(100)  		--//  ENCODE lzo
	,amount VARCHAR(100)  		--//  ENCODE lzo
	,amountusd VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,crtd_name TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;

--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_call_details;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_CALL_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_call_details
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,visit_date VARCHAR(30)  		--//  ENCODE zstd
	,checkin_time VARCHAR(30)  		--//  ENCODE zstd
	,checkout_time VARCHAR(30)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,distance VARCHAR(30)  		--//  ENCODE zstd
	,ordervisit VARCHAR(30)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_customer_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_customer_dim
(
	sap_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,outlet_name VARCHAR(100)  		--//  ENCODE zstd
	,address_1 VARCHAR(200)  		--//  ENCODE zstd
	,address_2 VARCHAR(200)  		--//  ENCODE zstd
	,telephone VARCHAR(20)  		--//  ENCODE zstd
	,fax VARCHAR(20)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,postcode VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,channel_group VARCHAR(30)  		--//  ENCODE zstd
	,sub_channel VARCHAR(20)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,gps_lat VARCHAR(30)  		--//  ENCODE zstd
	,gps_long VARCHAR(30)  		--//  ENCODE zstd
	,status CHAR(1)  		--//  ENCODE zstd
	,district VARCHAR(30)  		--//  ENCODE zstd
	,province VARCHAR(30)  		--//  ENCODE zstd
	,crt_date VARCHAR(50)  		--//  ENCODE zstd
	,date_off VARCHAR(50)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
	,shop_type VARCHAR(100)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_d_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_D_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_d_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date VARCHAR(30)  		--//  ENCODE zstd
	,invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,order_no VARCHAR(30)  		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(30)  		--//  ENCODE zstd
	,sale_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(50)  		--//  ENCODE zstd
	,material_code VARCHAR(50)  		--//  ENCODE zstd
	,uom VARCHAR(30)  		--//  ENCODE zstd
	,gross_price VARCHAR(30)  		--//  ENCODE zstd
	,orderqty VARCHAR(30)  		--//  ENCODE zstd
	,quantity VARCHAR(30)  		--//  ENCODE zstd
	,total_sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,discount VARCHAR(30)  		--//  ENCODE zstd
	,total_sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,line_number VARCHAR(10)  		--//  ENCODE zstd
	,promotion_id VARCHAR(50)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_distributor_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_DISTRIBUTOR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_distributor_dim
(
	territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(10)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,dstrbtr_address VARCHAR(200)  		--//  ENCODE zstd
	,longitude VARCHAR(20)  		--//  ENCODE zstd
	,latitude VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_forecast;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_FORECAST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_forecast
(
	cycle VARCHAR(10)  		--//  ENCODE zstd
	,channel VARCHAR(30)  		--//  ENCODE zstd
	,territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,warehouse VARCHAR(100)  		--//  ENCODE zstd
	,franchise VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,forecastso_mil VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_h_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_H_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_h_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date VARCHAR(30)  		--//  ENCODE zstd
	,invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,order_no VARCHAR(30)  		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,total_discount VARCHAR(100)  		--//  ENCODE zstd
	,invoice_discount VARCHAR(100)  		--//  ENCODE zstd
	,sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_history_saleout;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_HISTORY_SALEOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_history_saleout
(
	user_id VARCHAR(100)  		--//  ENCODE zstd
	,rsm_name VARCHAR(200)  		--//  ENCODE zstd
	,group_jb VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,dmsproduct_group VARCHAR(200)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,dmsproductid VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(100)  		--//  ENCODE zstd
	,tax VARCHAR(10)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_kpi;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_KPI		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_kpi
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,saleman_code VARCHAR(30)  		--//  ENCODE zstd
	,saleman_name VARCHAR(100)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,export_date VARCHAR(30)  		--//  ENCODE zstd
	,kpi_type VARCHAR(20)  		--//  ENCODE zstd
	,target_value VARCHAR(100)  		--//  ENCODE zstd
	,actual_value VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_kpi_sellin_sellthrgh;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_KPI_SELLIN_SELLTHRGH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_kpi_sellin_sellthrgh
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(10)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,ordertype VARCHAR(30)  		--//  ENCODE zstd
	,sellin_tg VARCHAR(100)  		--//  ENCODE zstd
	,sellin_ac VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_msl;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_msl
(
	msl_id VARCHAR(20)  		--//  ENCODE zstd
	,sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,from_cycle VARCHAR(10)  		--//  ENCODE zstd
	,to_cycle VARCHAR(10)  		--//  ENCODE zstd
	,product_id VARCHAR(50)  		--//  ENCODE zstd
	,prouct_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_order_promotion;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_ORDER_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_order_promotion
(
	branch_id VARCHAR(30)  		--//  ENCODE zstd
	,pro_id VARCHAR(50)  		--//  ENCODE zstd
	,ord_no VARCHAR(20)  		--//  ENCODE zstd
	,line_ref VARCHAR(10)  		--//  ENCODE zstd
	,disc_type VARCHAR(5)  		--//  ENCODE zstd
	,break_by VARCHAR(5)  		--//  ENCODE zstd
	,disc_break_line_ref VARCHAR(10)  		--//  ENCODE zstd
	,disct_bl_amt VARCHAR(100)  		--//  ENCODE zstd
	,disct_bl_qty VARCHAR(20)  		--//  ENCODE zstd
	,free_item_code VARCHAR(20)  		--//  ENCODE zstd
	,free_item_qty VARCHAR(20)  		--//  ENCODE zstd
	,disc_amt VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_product_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_PRODUCT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_product_dim
(
	product_code VARCHAR(50)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,productnamesap VARCHAR(100)  		--//  ENCODE zstd
	,unit VARCHAR(10)  		--//  ENCODE zstd
	,tax_rate VARCHAR(10)  		--//  ENCODE zstd
	,weight VARCHAR(20)  		--//  ENCODE zstd
	,volume VARCHAR(20)  		--//  ENCODE zstd
	,groupjb VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,active VARCHAR(10)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_promotion_list;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_PROMOTION_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_promotion_list
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,promotion_id VARCHAR(50)  		--//  ENCODE zstd
	,promotion_name VARCHAR(200)  		--//  ENCODE zstd
	,promotion_desc VARCHAR(200)  		--//  ENCODE zstd
	,start_date VARCHAR(30)  		--//  ENCODE zstd
	,end_date VARCHAR(30)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sales_org_dim;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_SALES_ORG_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sales_org_dim
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,sup_code VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_crtdate VARCHAR(50)  		--//  ENCODE zstd
	,salesrep_dateoff VARCHAR(50)  		--//  ENCODE zstd
	,sup_name VARCHAR(100)  		--//  ENCODE zstd
	,sup_active VARCHAR(5)  		--//  ENCODE zstd
	,sup_crtdate VARCHAR(50)  		--//  ENCODE zstd
	,sup_dateoff VARCHAR(50)  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,asm_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sales_stock_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_SALES_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sales_stock_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,wh_code VARCHAR(10)  		--//  ENCODE zstd
	,date VARCHAR(30)  		--//  ENCODE zstd
	,material_code VARCHAR(50)  		--//  ENCODE zstd
	,bat_number VARCHAR(20)  		--//  ENCODE zstd
	,expiry_date VARCHAR(30)  		--//  ENCODE zstd
	,quantity VARCHAR(20)  		--//  ENCODE zstd
	,uom VARCHAR(10)  		--//  ENCODE zstd
	,amount VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sellthrgh_sales_fact;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_SELLTHRGH_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_sellthrgh_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(30)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,doc_number VARCHAR(30)  		--//  ENCODE zstd
	,ref_number VARCHAR(30)  		--//  ENCODE zstd
	,receipt_date VARCHAR(30)  		--//  ENCODE zstd
	,order_type VARCHAR(10)  		--//  ENCODE zstd
	,vat_invoice_number VARCHAR(30)  		--//  ENCODE zstd
	,vat_invoice_note VARCHAR(30)  		--//  ENCODE zstd
	,vat_invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,pon_number VARCHAR(50)  		--//  ENCODE zstd
	,line_ref VARCHAR(20)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,unit VARCHAR(10)  		--//  ENCODE zstd
	,quantity VARCHAR(20)  		--//  ENCODE zstd
	,price VARCHAR(100)  		--//  ENCODE zstd
	,amount VARCHAR(100)  		--//  ENCODE zstd
	,tax_amount VARCHAR(30)  		--//  ENCODE zstd
	,tax_id VARCHAR(30)  		--//  ENCODE zstd
	,tax_rate VARCHAR(30)  		--//  ENCODE zstd
	,"values" VARCHAR(100)  		--//  ENCODE zstd
	,line_discount VARCHAR(100)  		--//  ENCODE zstd
	,doc_discount VARCHAR(100)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_yearly_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_DMS_YEARLY_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_dms_yearly_target
(
	year VARCHAR(10)  		--//  ENCODE zstd
	,kpi VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(200)  		--//  ENCODE zstd
	,target VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_answers;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_ANSWERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_answers
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value NUMERIC(20,0)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,oos NUMERIC(20,0)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_branch;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_BRANCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_branch
(
	parent_cust_code VARCHAR(255)  		--//  ENCODE zstd
	,parent_cust_name VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,branch_name VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,sales_group VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,city VARCHAR(255)  		--//  ENCODE zstd
	,district VARCHAR(255)  		--//  ENCODE zstd
	,trade_type VARCHAR(255)  		--//  ENCODE zstd
	,store_prioritization VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(32,16)  		--//  ENCODE zstd
	,longitude NUMERIC(32,16)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_choices;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_CHOICES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_choices
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(20,0)  		--//  ENCODE zstd
	,channel_name VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,sku_group VARCHAR(255)  		--//  ENCODE zstd
	,rep_param VARCHAR(255)  		--//  ENCODE zstd
	,putup_id VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,sfa NUMERIC(20,0)  		--//  ENCODE zstd
	,brand_id VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_cpg;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_CPG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_cpg
(
	slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,visitdate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_customer_visited;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_CUSTOMER_VISITED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_customer_visited
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,created_date VARCHAR(255)  		--//  ENCODE zstd
	,visit_date VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_ise_header;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_ISE_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_ise_header
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ise_desc VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,startdate VARCHAR(255)  		--//  ENCODE zstd
	,enddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_notes;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_NOTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_notes
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_question;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_INTERFACE_QUESTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_interface_question
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(10,0)  		--//  ENCODE zstd
	,channel VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_desc VARCHAR(255)  		--//  ENCODE zstd
	,standard_ques NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_class_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_class_desc VARCHAR(255)  		--//  ENCODE zstd
	,weigh NUMERIC(10,0)  		--//  ENCODE zstd
	,total_score NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_code VARCHAR(255)  		--//  ENCODE zstd
	,answer_desc VARCHAR(255)  		--//  ENCODE zstd
	,franchise_code VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_coop;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLIN_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_coop
(
	sku VARCHAR(20)  		--//  ENCODE lzo
	,idescr VARCHAR(200)  		--//  ENCODE lzo
	,vendor VARCHAR(50)  		--//  ENCODE lzo
	,asname VARCHAR(200)  		--//  ENCODE lzo
	,imfgr VARCHAR(50)  		--//  ENCODE lzo
	,store VARCHAR(50)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_dksh;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLIN_DKSH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_dksh
(
	supplier_code VARCHAR(10)  		--//  ENCODE zstd
	,supplier_name VARCHAR(100)  		--//  ENCODE zstd
	,plant VARCHAR(10)  		--//  ENCODE zstd
	,productid VARCHAR(20)  		--//  ENCODE zstd
	,product VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,sellin_category VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(100)  		--//  ENCODE zstd
	,product_sub_group VARCHAR(100)  		--//  ENCODE zstd
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE zstd
	,custcode VARCHAR(20)  		--//  ENCODE zstd
	,customer VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,zone VARCHAR(20)  		--//  ENCODE zstd
	,channel VARCHAR(10)  		--//  ENCODE zstd
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,cust_group VARCHAR(50)  		--//  ENCODE zstd
	,billing_no VARCHAR(20)  		--//  ENCODE zstd
	,invoice_date VARCHAR(20)  		--//  ENCODE zstd
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE zstd
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE zstd
	,order_type VARCHAR(20)  		--//  ENCODE zstd
	,red_invoice_no VARCHAR(20)  		--//  ENCODE zstd
	,expiry_date VARCHAR(20)  		--//  ENCODE zstd
	,order_no VARCHAR(20)  		--//  ENCODE zstd
	,order_date VARCHAR(20)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,sellout_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,group_account VARCHAR(50)  		--//  ENCODE zstd
	,account VARCHAR(50)  		--//  ENCODE zstd
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE zstd
	,zone_or_area VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,sellout_category VARCHAR(50)  		--//  ENCODE zstd
	,sub_cat VARCHAR(50)  		--//  ENCODE zstd
	,sub_brand VARCHAR(50)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,base_or_bundle VARCHAR(20)  		--//  ENCODE zstd
	,size VARCHAR(20)  		--//  ENCODE zstd
	,key_chain VARCHAR(20)  		--//  ENCODE zstd
	,status VARCHAR(20)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_target;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLIN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellin_target
(
	mtd_code VARCHAR(20)  		--//  ENCODE zstd
	,mti_code VARCHAR(20)  		--//  ENCODE zstd
	,target NUMERIC(20,5)  		--//  ENCODE az64
	,sellin_cycle numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellin_year VARCHAR(10)  		--//  ENCODE zstd
	,visit VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_aeon;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_AEON		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_aeon
(
	store VARCHAR(20)  		--//  ENCODE lzo
	,department VARCHAR(30)  		--//  ENCODE lzo
	,supplier_code VARCHAR(50)  		--//  ENCODE lzo
	,supplier_name VARCHAR(60)  		--//  ENCODE lzo
	,item VARCHAR(30)  		--//  ENCODE lzo
	,item_name VARCHAR(200)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_bhx;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_BHX		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_bhx
(
	pro_code VARCHAR(50)  		--//  ENCODE lzo
	,pro_name VARCHAR(200)  		--//  ENCODE lzo
	,cust_code VARCHAR(20)  		--//  ENCODE lzo
	,cust_name VARCHAR(200)  		--//  ENCODE lzo
	,cat_store VARCHAR(60)  		--//  ENCODE lzo
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_con_cung;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_CON_CUNG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_con_cung
(
	delivery_code VARCHAR(50)  		--//  ENCODE zstd
	,store VARCHAR(255)  		--//  ENCODE zstd
	,product_code VARCHAR(20)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_coop;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_coop
(
	thang VARCHAR(20)  		--//  ENCODE zstd
	,desc_a VARCHAR(20)  		--//  ENCODE zstd
	,idept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isdept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,iclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,tenvt VARCHAR(255)  		--//  ENCODE zstd
	,brand_spm VARCHAR(50)  		--//  ENCODE zstd
	,madv VARCHAR(20)  		--//  ENCODE zstd
	,sumoflg numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sumofttbvnckhd NUMERIC(20,5)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE zstd
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_guardian;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_GUARDIAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_guardian
(
	serial_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,description_vietnamese VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,division VARCHAR(50)  		--//  ENCODE zstd
	,department VARCHAR(50)  		--//  ENCODE zstd
	,category VARCHAR(50)  		--//  ENCODE zstd
	,sub_category VARCHAR(50)  		--//  ENCODE zstd
	,sales_supplier numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_lotte;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_LOTTE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_lotte
(
	str VARCHAR(10)  		--//  ENCODE zstd
	,str_nm VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_1 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_2 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_3 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_4 VARCHAR(50)  		--//  ENCODE zstd
	,prod_cd VARCHAR(100)  		--//  ENCODE zstd
	,sale_cd VARCHAR(100)  		--//  ENCODE zstd
	,prod_nm VARCHAR(100)  		--//  ENCODE zstd
	,ven VARCHAR(100)  		--//  ENCODE zstd
	,ven_nm VARCHAR(100)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,tot_sale_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_mega;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_MEGA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_mega
(
	site_no VARCHAR(20)  		--//  ENCODE zstd
	,site_name VARCHAR(255)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,art_no VARCHAR(20)  		--//  ENCODE zstd
	,art_sv_name VARCHAR(255)  		--//  ENCODE zstd
	,suppl_no VARCHAR(20)  		--//  ENCODE zstd
	,suppl_name VARCHAR(255)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cogs_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_vinmart;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_MT_SELLOUT_VINMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_mt_sellout_vinmart
(
	store VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,mch5_mc VARCHAR(20)  		--//  ENCODE zstd
	,mc VARCHAR(100)  		--//  ENCODE zstd
	,article VARCHAR(20)  		--//  ENCODE zstd
	,article_name VARCHAR(255)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,pos_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pos_revenue NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE VNMITG_INTEGRATION.sdl_raw_vn_oneview_otc;
CREATE OR REPLACE TABLE VNMITG_INTEGRATION.SDL_RAW_VN_ONEVIEW_OTC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMITG_INTEGRATION.sdl_raw_vn_oneview_otc
(
	plant VARCHAR(10)  		--//  ENCODE zstd
	,principalcode VARCHAR(100)  		--//  ENCODE zstd
	,principal VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,kunnr VARCHAR(100)  		--//  ENCODE zstd
	,name1 VARCHAR(100)  		--//  ENCODE zstd
	,name2 VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,zterm VARCHAR(100)  		--//  ENCODE zstd
	,kdgrp VARCHAR(100)  		--//  ENCODE zstd
	,custgroup VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,vbeln VARCHAR(100)  		--//  ENCODE zstd
	,billingdate VARCHAR(100)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,qty VARCHAR(100)  		--//  ENCODE zstd
	,dgle VARCHAR(100)  		--//  ENCODE zstd
	,pernr VARCHAR(100)  		--//  ENCODE zstd
	,vat VARCHAR(100)  		--//  ENCODE zstd
	,suom VARCHAR(100)  		--//  ENCODE zstd
	,custpayto VARCHAR(100)  		--//  ENCODE zstd
	,tt VARCHAR(100)  		--//  ENCODE zstd
	,nguyengia VARCHAR(100)  		--//  ENCODE zstd
	,ttv VARCHAR(100)  		--//  ENCODE zstd
	,discount VARCHAR(100)  		--//  ENCODE zstd
	,device_code VARCHAR(100)  		--//  ENCODE zstd
	,device VARCHAR(100)  		--//  ENCODE zstd
	,order_no VARCHAR(100)  		--//  ENCODE zstd
	,orginv VARCHAR(100)  		--//  ENCODE zstd
	,batch VARCHAR(100)  		--//  ENCODE zstd
	,charge VARCHAR(100)  		--//  ENCODE zstd
	,contact_name VARCHAR(100)  		--//  ENCODE zstd
	,userid VARCHAR(100)  		--//  ENCODE zstd
	,billinginst VARCHAR(100)  		--//  ENCODE zstd
	,distchannel VARCHAR(100)  		--//  ENCODE zstd
	,redinv VARCHAR(100)  		--//  ENCODE zstd
	,serial VARCHAR(100)  		--//  ENCODE zstd
	,potext VARCHAR(100)  		--//  ENCODE zstd
	,expdate VARCHAR(100)  		--//  ENCODE zstd
	,ret_so VARCHAR(100)  		--//  ENCODE zstd
	,vat_code VARCHAR(100)  		--//  ENCODE zstd
	,sodoc_date VARCHAR(100)  		--//  ENCODE zstd
	,itemnotes VARCHAR(100)  		--//  ENCODE zstd
	,mg1 VARCHAR(100)  		--//  ENCODE zstd
	,year VARCHAR(100)  		--//  ENCODE zstd
	,month VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,groups VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
