--DROP TABLE IDNEDW_INTEGRATION.edw_indonesia_lppb_analysis;
CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.EDW_INDONESIA_LPPB_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.edw_indonesia_lppb_analysis
(
	jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_qrtr VARCHAR(24)  		--//  ENCODE zstd
	,jj_mnth_id NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth VARCHAR(25)  		--//  ENCODE zstd
	,jj_mnth_desc VARCHAR(20)  		--//  ENCODE zstd
	,jj_mnth_no NUMERIC(18,0)  		--//  ENCODE az64
	,dstrbtr_grp_cd VARCHAR(25)  		--//  ENCODE zstd
	,dstrbtr_grp_nm VARCHAR(155)  		--//  ENCODE zstd
	,jj_sap_dstrbtr_id VARCHAR(75)  		--//  ENCODE zstd
	,jj_sap_dstrbtr_nm VARCHAR(75)  		--//  ENCODE zstd
	,dstrbtr_cd_nm VARCHAR(72)  		--//  ENCODE zstd
	,area VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,bdm_nm VARCHAR(50)  		--//  ENCODE zstd
	,rbm_nm VARCHAR(50)  		--//  ENCODE zstd
	,dstrbtr_status VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,jj_sap_upgrd_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_upgrd_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,jj_sap_cd_mp_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_cd_mp_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,sap_prod_code_name VARCHAR(152)  		--//  ENCODE zstd
	,franchise VARCHAR(75)  		--//  ENCODE zstd
	,brand VARCHAR(75)  		--//  ENCODE zstd
	,sku_grp_or_variant VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp1_or_variant1 VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp2_or_variant2 VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp3_or_variant3 VARCHAR(62)  		--//  ENCODE zstd
	,prod_status VARCHAR(50)  		--//  ENCODE zstd
	,strt_inv_qty NUMERIC(18,4)  		--//  ENCODE az64
	,sellin_qty NUMERIC(18,4)  		--//  ENCODE az64
	,sellout_qty NUMERIC(18,4)  		--//  ENCODE az64
	,end_inv_qty NUMERIC(18,4)  		--//  ENCODE az64
	,strt_inv_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_strt_inv_val NUMERIC(18,4)  		--//  ENCODE az64
	,sellin_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_sellin_val NUMERIC(18,4)  		--//  ENCODE az64
	,sellout_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_sellout_val NUMERIC(18,4)  		--//  ENCODE az64
	,end_inv_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_end_inv_val NUMERIC(18,4)  		--//  ENCODE az64
	,sellout_last_two_mnths_qty NUMERIC(18,4)  		--//  ENCODE az64
	,sellout_last_two_mnths_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_sellout_last_two_mnths_val NUMERIC(18,4)  		--//  ENCODE az64
	,stock_on_hand_qty NUMERIC(18,4)  		--//  ENCODE az64
	,stock_on_hand_val NUMERIC(18,4)  		--//  ENCODE az64
	,variant VARCHAR(75)  		--//  ENCODE zstd
	,jj_mnth_long VARCHAR(75)  		--//  ENCODE zstd
	,bp_qtn NUMERIC(18,4)  		--//  ENCODE az64
	,bp_val NUMERIC(18,4)  		--//  ENCODE az64
	,s_op_qtn NUMERIC(18,4)  		--//  ENCODE az64
	,s_op_val NUMERIC(18,4)  		--//  ENCODE az64
	,trgt_hna NUMERIC(38,4)  		--//  ENCODE az64
	,trgt_niv NUMERIC(38,4)  		--//  ENCODE az64
	,trgt_bp_s_op_flag VARCHAR(1)  		--//  ENCODE zstd
	,trgt_dist_brnd_chnl_flag VARCHAR(1)  		--//  ENCODE zstd
	,saleable_stock_qty NUMERIC(18,4)  		--//  ENCODE az64
	,saleable_stock_value NUMERIC(18,4)  		--//  ENCODE az64
	,non_saleable_stock_qty NUMERIC(18,4)  		--//  ENCODE az64
	,non_saleable_stock_value NUMERIC(18,4)  		--//  ENCODE az64
	,intransit_qty NUMERIC(18,4)  		--//  ENCODE az64
	,intransit_hna NUMERIC(18,4)  		--//  ENCODE az64
	,intransit_niv NUMERIC(18,4)  		--//  ENCODE az64
	,stock_on_hand_niv NUMERIC(18,4)  		--//  ENCODE az64
	,p3m_sellout_val NUMERIC(18,4)  		--//  ENCODE az64
	,p3m_gross_sellout_val NUMERIC(18,4)  		--//  ENCODE az64
	,p6m_sellout_val NUMERIC(18,4)  		--//  ENCODE az64
	,p6m_gross_sellout_val NUMERIC(18,4)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE IDNEDW_INTEGRATION.edw_indonesia_noo_analysis;
CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.EDW_INDONESIA_NOO_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.edw_indonesia_noo_analysis
(
	jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_qrtr VARCHAR(24)  		--//  ENCODE lzo
	,jj_mnth VARCHAR(25)  		--//  ENCODE lzo
	,jj_wk NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_wk_no numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,jj_mnth_no NUMERIC(18,0)  		--//  ENCODE az64
	,bill_doc VARCHAR(100)  		--//  ENCODE lzo
	,bill_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dstrbtr_grp_cd VARCHAR(25)  		--//  ENCODE lzo
	,dstrbtr_grp_nm VARCHAR(155)  		--//  ENCODE lzo
	,jj_sap_dstrbtr_id VARCHAR(75)  		--//  ENCODE lzo
	,jj_sap_dstrbtr_nm VARCHAR(75)  		--//  ENCODE lzo
	,dstrbtr_cd_nm VARCHAR(152)  		--//  ENCODE lzo
	,area VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(50)  		--//  ENCODE lzo
	,bdm_nm VARCHAR(50)  		--//  ENCODE lzo
	,rbm_nm VARCHAR(50)  		--//  ENCODE lzo
	,dstrbtr_status VARCHAR(50)  		--//  ENCODE lzo
	,cust_id_map VARCHAR(100)  		--//  ENCODE lzo
	,cust_nm_map VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_cust_cd_nm VARCHAR(304)  		--//  ENCODE lzo
	,cust_grp VARCHAR(100)  		--//  ENCODE lzo
	,chnl VARCHAR(100)  		--//  ENCODE lzo
	,outlet_type VARCHAR(100)  		--//  ENCODE lzo
	,chnl_grp VARCHAR(100)  		--//  ENCODE lzo
	,jjid VARCHAR(100)  		--//  ENCODE lzo
	,chnl_grp2 VARCHAR(100)  		--//  ENCODE lzo
	,city VARCHAR(229)  		--//  ENCODE lzo
	,cust_status VARCHAR(8)  		--//  ENCODE lzo
	,jj_sap_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,jj_sap_upgrd_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_upgrd_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,jj_sap_cd_mp_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_cd_mp_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_code_name VARCHAR(152)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,variant1 VARCHAR(50)  		--//  ENCODE lzo
	,variant2 VARCHAR(50)  		--//  ENCODE lzo
	,variant VARCHAR(50)  		--//  ENCODE lzo
	,put_up VARCHAR(62)  		--//  ENCODE lzo
	,prod_status VARCHAR(50)  		--//  ENCODE lzo
	,slsmn_id VARCHAR(100)  		--//  ENCODE lzo
	,slsmn_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_qty NUMERIC(18,4)  		--//  ENCODE az64
	,hna NUMERIC(18,4)  		--//  ENCODE az64
	,niv NUMERIC(18,4)  		--//  ENCODE az64
	,trd_dscnt NUMERIC(18,4)  		--//  ENCODE az64
	,dstrbtr_niv NUMERIC(18,4)  		--//  ENCODE az64
	,rtrn_qty NUMERIC(18,4)  		--//  ENCODE az64
	,rtrn_val NUMERIC(18,4)  		--//  ENCODE az64
	,hsku_target_growth NUMERIC(18,4)  		--//  ENCODE az64
	,hsku_target_coverage NUMERIC(18,4)  		--//  ENCODE az64
	,jj_mnth_long VARCHAR(10)  		--//  ENCODE lzo
	,trgt_hna NUMERIC(38,4)  		--//  ENCODE az64
	,trgt_niv NUMERIC(38,4)  		--//  ENCODE az64
	,npi_flag VARCHAR(1)  		--//  ENCODE lzo
	,benchmark_sku_code VARCHAR(75)  		--//  ENCODE lzo
	,sku_benchmark VARCHAR(75)  		--//  ENCODE lzo
	,hero_sku_flag VARCHAR(1)  		--//  ENCODE lzo
	,trgt_dist_brnd_chnl_flag VARCHAR(1)  		--//  ENCODE lzo
	,tiering VARCHAR(100)  		--//  ENCODE lzo
	,count_sku_code numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mcs_status VARCHAR(20)  		--//  ENCODE lzo
	,local_variant VARCHAR(2000)  		--//  ENCODE lzo
	,count_local_variant numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salesman_key VARCHAR(70)  		--//  ENCODE lzo
	,sfa_id VARCHAR(255)  		--//  ENCODE lzo
	,latest_chnl VARCHAR(100)  		--//  ENCODE lzo
	,latest_outlet_type VARCHAR(100)  		--//  ENCODE lzo
	,latest_chnl_grp VARCHAR(100)  		--//  ENCODE lzo
	,latest_cust_grp2 VARCHAR(100)  		--//  ENCODE lzo
	,latest_cust_grp VARCHAR(100)  		--//  ENCODE lzo
	,latest_cust_nm_map VARCHAR(100)  		--//  ENCODE lzo
	,latest_region VARCHAR(100)  		--//  ENCODE lzo
	,latest_area VARCHAR(100)  		--//  ENCODE lzo
	,latest_rbm VARCHAR(100)  		--//  ENCODE lzo
	,latest_area_pic VARCHAR(100)  		--//  ENCODE lzo
	,latest_jjid VARCHAR(200)  		--//  ENCODE lzo
	,latest_put_up VARCHAR(200)  		--//  ENCODE lzo
	,latest_franchise VARCHAR(200)  		--//  ENCODE lzo
	,latest_brand VARCHAR(200)  		--//  ENCODE lzo
	,latest_msl VARCHAR(200)  		--//  ENCODE lzo
	,latest_count_local_variant VARCHAR(200)  		--//  ENCODE lzo
	,latest_chnl_grp2 VARCHAR(200)  		--//  ENCODE lzo
	,latest_distributor_group VARCHAR(200)  		--//  ENCODE lzo
	,latest_dstrbtr_grp_cd VARCHAR(200)  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	jj_year
		--// 	)
;		--// ;
--DROP TABLE IDNEDW_INTEGRATION.edw_order_sell_in_analysis;
CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.EDW_ORDER_SELL_IN_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.edw_order_sell_in_analysis
(
	datasrc VARCHAR(20)  		--//  ENCODE lzo
	,invoice_dt DATE  		--//  ENCODE az64
	,bill_doc VARCHAR(75)  		--//  ENCODE lzo
	,jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_qrtr VARCHAR(24)  		--//  ENCODE lzo
	,jj_mnth_id VARCHAR(24)  		--//  ENCODE lzo
	,jj_mnth VARCHAR(25)  		--//  ENCODE lzo
	,jj_mnth_desc VARCHAR(20)  		--//  ENCODE lzo
	,jj_mnth_no NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_long VARCHAR(10)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(500)  		--//  ENCODE lzo
	,dstrbtr_grp_nm VARCHAR(500)  		--//  ENCODE lzo
	,jj_sap_dstrbtr_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_dstrbtr_nm VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_cd_nm VARCHAR(100)  		--//  ENCODE lzo
	,area VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(100)  		--//  ENCODE lzo
	,bdm_nm VARCHAR(100)  		--//  ENCODE lzo
	,rbm_nm VARCHAR(100)  		--//  ENCODE lzo
	,dstrbtr_status VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,jj_sap_upgrd_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_upgrd_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,jj_sap_cd_mp_prod_id VARCHAR(50)  		--//  ENCODE lzo
	,jj_sap_cd_mp_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_code_name VARCHAR(200)  		--//  ENCODE lzo
	,franchise VARCHAR(750)  		--//  ENCODE lzo
	,brand VARCHAR(750)  		--//  ENCODE lzo
	,sku_grp_or_variant VARCHAR(100)  		--//  ENCODE lzo
	,sku_grp1_or_variant1 VARCHAR(100)  		--//  ENCODE lzo
	,sku_grp2_or_variant2 VARCHAR(100)  		--//  ENCODE lzo
	,sku_grp3_or_variant3 VARCHAR(100)  		--//  ENCODE lzo
	,prod_status VARCHAR(50)  		--//  ENCODE lzo
	,sellin_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sellin_val NUMERIC(38,4)  		--//  ENCODE az64
	,gross_sellin_val NUMERIC(38,4)  		--//  ENCODE az64
	,order_net NUMERIC(38,4)  		--//  ENCODE az64
	,order_qty NUMERIC(38,4)  		--//  ENCODE az64
	,order_gross NUMERIC(38,4)  		--//  ENCODE az64
	,nts_order_val NUMERIC(38,4)  		--//  ENCODE az64
	,order_dt DATE  		--//  ENCODE az64
	,order_doc VARCHAR(20)  		--//  ENCODE lzo
	,target_niv NUMERIC(25,7)  		--//  ENCODE az64
	,target_hna NUMERIC(25,7)  		--//  ENCODE az64
)

;
--DROP TABLE IDNEDW_INTEGRATION.edw_rpt_id_sellin_analysis;
CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.EDW_RPT_ID_SELLIN_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE IDNEDW_INTEGRATION.edw_rpt_id_sellin_analysis
(
	bill_dt DATE  		--//  ENCODE az64
	,bill_doc VARCHAR(100)  		--//  ENCODE zstd
	,jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_qrtr VARCHAR(24)  		--//  ENCODE zstd
	,jj_mnth_id NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth VARCHAR(25)  		--//  ENCODE zstd
	,jj_mnth_desc VARCHAR(20)  		--//  ENCODE zstd
	,jj_mnth_no NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_long VARCHAR(75)  		--//  ENCODE zstd
	,dstrbtr_grp_cd VARCHAR(25)  		--//  ENCODE zstd
	,dstrbtr_grp_nm VARCHAR(155)  		--//  ENCODE zstd
	,jj_sap_dstrbtr_id VARCHAR(75)  		--//  ENCODE zstd
	,jj_sap_dstrbtr_nm VARCHAR(75)  		--//  ENCODE zstd
	,dstrbtr_cd_nm VARCHAR(72)  		--//  ENCODE zstd
	,area VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,bdm_nm VARCHAR(50)  		--//  ENCODE zstd
	,rbm_nm VARCHAR(50)  		--//  ENCODE zstd
	,dstrbtr_status VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,jj_sap_upgrd_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_upgrd_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,jj_sap_cd_mp_prod_id VARCHAR(50)  		--//  ENCODE zstd
	,jj_sap_cd_mp_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,sap_prod_code_name VARCHAR(152)  		--//  ENCODE zstd
	,franchise VARCHAR(75)  		--//  ENCODE zstd
	,brand VARCHAR(75)  		--//  ENCODE zstd
	,sku_grp_or_variant VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp1_or_variant1 VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp2_or_variant2 VARCHAR(50)  		--//  ENCODE zstd
	,sku_grp3_or_variant3 VARCHAR(62)  		--//  ENCODE zstd
	,prod_status VARCHAR(50)  		--//  ENCODE zstd
	,sellin_qty NUMERIC(18,4)  		--//  ENCODE az64
	,sellin_val NUMERIC(18,4)  		--//  ENCODE az64
	,gross_sellin_val NUMERIC(18,4)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	jj_mnth_id
		--// 	)
;		--// ;
