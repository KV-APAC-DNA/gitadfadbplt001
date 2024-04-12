--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_pacific_perenso_ims_analysis;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_PACIFIC_PERENSO_IMS_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_pacific_perenso_ims_analysis
(
	order_type VARCHAR(255)  		--//  ENCODE lzo
	,delvry_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,time_id NUMERIC(18,0)  		--//  ENCODE az64
	,jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_qrtr NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth NUMERIC(18,0)  		--//  ENCODE az64
	,jj_wk NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,jj_mnth_id NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_tot NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_day NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_shrt VARCHAR(3)  		--//  ENCODE lzo
	,jj_mnth_long VARCHAR(10)  		--//  ENCODE lzo
	,cal_year NUMERIC(18,0)  		--//  ENCODE az64
	,cal_qrtr NUMERIC(18,0)  		--//  ENCODE az64
	,cal_mnth NUMERIC(18,0)  		--//  ENCODE az64
	,cal_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,cal_mnth_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,cal_mnth_id NUMERIC(18,0)  		--//  ENCODE az64
	,cal_mnth_nm VARCHAR(10)  		--//  ENCODE lzo
	,prod_key NUMERIC(10,0)  		--//  ENCODE az64
	,prod_probe_id NUMERIC(18,0)  		--//  ENCODE az64
	,prod_desc VARCHAR(256)  		--//  ENCODE lzo
	,prod_sapbw_code VARCHAR(50)  		--//  ENCODE lzo
	,prod_ean VARCHAR(50)  		--//  ENCODE lzo
	,prod_jj_franchise VARCHAR(100)  		--//  ENCODE lzo
	,prod_jj_category VARCHAR(100)  		--//  ENCODE lzo
	,prod_jj_brand VARCHAR(100)  		--//  ENCODE lzo
	,prod_sap_franchise VARCHAR(100)  		--//  ENCODE lzo
	,prod_sap_profit_centre VARCHAR(100)  		--//  ENCODE lzo
	,prod_sap_product_major VARCHAR(100)  		--//  ENCODE lzo
	,prod_grocery_franchise VARCHAR(100)  		--//  ENCODE lzo
	,prod_grocery_category VARCHAR(100)  		--//  ENCODE lzo
	,prod_grocery_brand VARCHAR(100)  		--//  ENCODE lzo
	,prod_active_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prod_pbs VARCHAR(100)  		--//  ENCODE lzo
	,prod_ims_brand VARCHAR(100)  		--//  ENCODE lzo
	,prod_nz_code VARCHAR(100)  		--//  ENCODE lzo
	,prod_metcash_code VARCHAR(100)  		--//  ENCODE lzo
	,prod_old_id VARCHAR(50)  		--//  ENCODE lzo
	,prod_old_ean VARCHAR(50)  		--//  ENCODE lzo
	,prod_tax VARCHAR(50)  		--//  ENCODE lzo
	,prod_bwp_aud VARCHAR(50)  		--//  ENCODE lzo
	,prod_bwp_nzd VARCHAR(50)  		--//  ENCODE lzo
	,prod_wholesaler_code VARCHAR(250)  		--//  ENCODE lzo
	,acct_key NUMERIC(10,0)  		--//  ENCODE az64
	,acct_probe_id NUMERIC(18,0)  		--//  ENCODE az64
	,acct_metcash_store_code VARCHAR(50)  		--//  ENCODE lzo
	,acct_display_name VARCHAR(256)  		--//  ENCODE lzo
	,acct_type_desc VARCHAR(50)  		--//  ENCODE lzo
	,acct_street_1 VARCHAR(256)  		--//  ENCODE lzo
	,acct_street_2 VARCHAR(256)  		--//  ENCODE lzo
	,acct_street_3 VARCHAR(256)  		--//  ENCODE lzo
	,acct_suburb VARCHAR(25)  		--//  ENCODE lzo
	,acct_postcode VARCHAR(25)  		--//  ENCODE lzo
	,acct_phone_number VARCHAR(50)  		--//  ENCODE lzo
	,acct_fax_number VARCHAR(50)  		--//  ENCODE lzo
	,acct_email VARCHAR(256)  		--//  ENCODE lzo
	,acct_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_region VARCHAR(256)  		--//  ENCODE lzo
	,acct_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_banner_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_banner_division VARCHAR(256)  		--//  ENCODE lzo
	,acct_banner_type VARCHAR(256)  		--//  ENCODE lzo
	,acct_banner VARCHAR(256)  		--//  ENCODE lzo
	,acct_type VARCHAR(256)  		--//  ENCODE lzo
	,acct_sub_type VARCHAR(256)  		--//  ENCODE lzo
	,acct_grade VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_pharma_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_pharma_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_pharma_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_groc_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_groc_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_nz_groc_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_ssr_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_ssr_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_ssr_team_leader VARCHAR(256)  		--//  ENCODE lzo
	,acct_ssr_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_ssr_sub_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_ind_groc_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_ind_groc_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_ind_groc_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_ind_groc_sub_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_ssr_country VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_ssr_state VARCHAR(256)  		--//  ENCODE lzo
	,acct_au_pharma_ssr_territory VARCHAR(256)  		--//  ENCODE lzo
	,acct_tsm VARCHAR(256)  		--//  ENCODE lzo
	,acct_terriroty VARCHAR(256)  		--//  ENCODE lzo
	,acct_store_code VARCHAR(256)  		--//  ENCODE lzo
	,acct_fax_opt_out VARCHAR(256)  		--//  ENCODE lzo
	,acct_email_opt_out VARCHAR(256)  		--//  ENCODE lzo
	,acct_contact_method VARCHAR(256)  		--//  ENCODE lzo
	,ssr_grade VARCHAR(256)  		--//  ENCODE lzo
	,order_key NUMERIC(25,0)  		--//  ENCODE az64
	,order_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,sent_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,delvry_instns VARCHAR(256)  		--//  ENCODE lzo
	,branch_key NUMERIC(18,0)  		--//  ENCODE az64
	,batch_key NUMERIC(18,0)  		--//  ENCODE az64
	,line_key NUMERIC(18,0)  		--//  ENCODE az64
	,unit_qty NUMERIC(22,4)  		--//  ENCODE az64
	,entered_qty NUMERIC(18,0)  		--//  ENCODE az64
	,entered_unit_key NUMERIC(18,0)  		--//  ENCODE az64
	,list_price NUMERIC(24,6)  		--//  ENCODE az64
	,nis DOUBLE PRECISION
	,rrp NUMERIC(20,2)  		--//  ENCODE az64
	,deal_desc VARCHAR(255)  		--//  ENCODE lzo
	,deal_start_date DATE  		--//  ENCODE az64
	,deal_end_date DATE  		--//  ENCODE az64
	,deal_short_desc VARCHAR(255)  		--//  ENCODE lzo
	,discount_desc VARCHAR(255)  		--//  ENCODE lzo
	,discount_per DOUBLE PRECISION
	,discount_amt DOUBLE PRECISION
	,order_status VARCHAR(255)  		--//  ENCODE lzo
	,batch_status VARCHAR(255)  		--//  ENCODE lzo
	,order_currency_cd VARCHAR(12)  		--//  ENCODE lzo
	,create_user_name VARCHAR(100)  		--//  ENCODE lzo
	,create_user_desc VARCHAR(100)  		--//  ENCODE lzo
	,create_user_email_address VARCHAR(100)  		--//  ENCODE lzo
	,distributor VARCHAR(255)  		--//  ENCODE lzo
	,distributor_display_name VARCHAR(255)  		--//  ENCODE lzo
	,pharmacy_msl_rank VARCHAR(5)  		--//  ENCODE lzo
	,pharmacy_msl_flag VARCHAR(5)  		--//  ENCODE lzo
	,exch_rate_to_aud NUMERIC(23,5)  		--//  ENCODE az64
	,aud_nis DOUBLE PRECISION
	,exch_rate_to_usd NUMERIC(23,5)  		--//  ENCODE az64
	,usd_nis DOUBLE PRECISION
	,exch_rate_to_nzd NUMERIC(23,5)  		--//  ENCODE az64
	,nzd_nis DOUBLE PRECISION
	,distribution_flag VARCHAR(5)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_pacific_perfect_store;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_PACIFIC_PERFECT_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_pacific_perfect_store
(
	dataset VARCHAR(128)  		--//  ENCODE lzo
	,merchandisingresponseid VARCHAR(128)  		--//  ENCODE lzo
	,surveyresponseid VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,salespersonid VARCHAR(128)  		--//  ENCODE lzo
	,visitid VARCHAR(128)  		--//  ENCODE lzo
	,mrch_resp_startdt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mrch_resp_enddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mrch_resp_status VARCHAR(256)  		--//  ENCODE lzo
	,mastersurveyid VARCHAR(128)  		--//  ENCODE lzo
	,survey_status VARCHAR(256)  		--//  ENCODE lzo
	,survey_enddate VARCHAR(256)  		--//  ENCODE lzo
	,questionkey VARCHAR(256)  		--//  ENCODE lzo
	,questiontext VARCHAR(256)  		--//  ENCODE lzo
	,valuekey VARCHAR(256)  		--//  ENCODE lzo
	,value VARCHAR(24)  		--//  ENCODE lzo
	,productid VARCHAR(40)  		--//  ENCODE lzo
	,mustcarryitem VARCHAR(256)  		--//  ENCODE lzo
	,answerscore VARCHAR(256)  		--//  ENCODE lzo
	,presence VARCHAR(256)  		--//  ENCODE lzo
	,outofstock VARCHAR(256)  		--//  ENCODE lzo
	,mastersurveyname VARCHAR(256)  		--//  ENCODE lzo
	,kpi VARCHAR(256)  		--//  ENCODE lzo
	,category VARCHAR(384)  		--//  ENCODE lzo
	,segment VARCHAR(384)  		--//  ENCODE lzo
	,vst_visitid VARCHAR(256)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,scheduledtime VARCHAR(1)  		--//  ENCODE lzo
	,duration VARCHAR(256)  		--//  ENCODE lzo
	,vst_status VARCHAR(256)  		--//  ENCODE lzo
	,fisc_yr DOUBLE PRECISION
	,fisc_per VARCHAR(21)
	,firstname VARCHAR(256)  		--//  ENCODE lzo
	,lastname VARCHAR(256)  		--//  ENCODE lzo
	,cust_remotekey VARCHAR(64)  		--//  ENCODE lzo
	,customername VARCHAR(256)  		--//  ENCODE lzo
	,country VARCHAR(510)  		--//  ENCODE lzo
	,state VARCHAR(256)  		--//  ENCODE lzo
	,county VARCHAR(256)  		--//  ENCODE lzo
	,district VARCHAR(256)  		--//  ENCODE lzo
	,city VARCHAR(256)  		--//  ENCODE lzo
	,storereference VARCHAR(384)  		--//  ENCODE lzo
	,storetype VARCHAR(256)  		--//  ENCODE lzo
	,channel VARCHAR(256)  		--//  ENCODE lzo
	,salesgroup VARCHAR(384)  		--//  ENCODE lzo
	,soldtoparty VARCHAR(256)  		--//  ENCODE lzo
	,brand VARCHAR(384)  		--//  ENCODE lzo
	,productname VARCHAR(256)  		--//  ENCODE lzo
	,eannumber VARCHAR(64)  		--//  ENCODE lzo
	,matl_num VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(384)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(384)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(384)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(323)  		--//  ENCODE lzo
	,kpi_chnl_wt NUMERIC(31,2)  		--//  ENCODE az64
	,mkt_share NUMERIC(31,3)  		--//  ENCODE az64
	,ques_desc VARCHAR(256)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(3)  		--//  ENCODE lzo
	,rej_reason VARCHAR(256)  		--//  ENCODE lzo
	,response VARCHAR(256)  		--//  ENCODE lzo
	,response_score VARCHAR(256)  		--//  ENCODE lzo
	,acc_rej_reason VARCHAR(256)  		--//  ENCODE lzo
	,actual DOUBLE PRECISION
	,target DOUBLE PRECISION
	,gcph_category VARCHAR(256)  		--//  ENCODE lzo
	,gcph_subcategory VARCHAR(256)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	fisc_per
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_sales_reporting;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_SALES_REPORTING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_sales_reporting
(
	snap_shot_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,pac_source_type VARCHAR(6)  		--//  ENCODE lzo
	,pac_subsource_type VARCHAR(13)  		--//  ENCODE lzo
	,jj_period NUMERIC(18,0)  		--//  ENCODE az64
	,jj_wk numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,jj_mnth NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_shrt VARCHAR(3)  		--//  ENCODE lzo
	,jj_mnth_long VARCHAR(10)  		--//  ENCODE lzo
	,jj_qrtr NUMERIC(18,0)  		--//  ENCODE az64
	,jj_year NUMERIC(18,0)  		--//  ENCODE az64
	,jj_mnth_tot NUMERIC(18,0)  		--//  ENCODE az64
	,prcnt_elpsd NUMERIC(38,3)  		--//  ENCODE az64
	,elpsd_jj_period NUMERIC(18,0)  		--//  ENCODE az64
	,cust_no VARCHAR(10)  		--//  ENCODE lzo
	,cmp_id VARCHAR(20)  		--//  ENCODE lzo
	,channel_cd VARCHAR(20)  		--//  ENCODE lzo
	,channel_desc VARCHAR(20)  		--//  ENCODE lzo
	,ctry_key VARCHAR(150)  		--//  ENCODE lzo
	,country VARCHAR(20)  		--//  ENCODE lzo
	,state_cd VARCHAR(150)  		--//  ENCODE lzo
	,post_cd VARCHAR(150)  		--//  ENCODE lzo
	,cust_suburb VARCHAR(150)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,sales_office_cd VARCHAR(20)  		--//  ENCODE lzo
	,sales_office_desc VARCHAR(30)  		--//  ENCODE lzo
	,sales_grp_cd VARCHAR(20)  		--//  ENCODE lzo
	,sales_grp_desc VARCHAR(30)  		--//  ENCODE lzo
	,mercia_ref VARCHAR(150)  		--//  ENCODE lzo
	,pln_cnnl_cd VARCHAR(20)  		--//  ENCODE lzo
	,pln_chnl_desc VARCHAR(20)  		--//  ENCODE lzo
	,cust_curr_cd VARCHAR(5)  		--//  ENCODE lzo
	,matl_id VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,master_code VARCHAR(18)  		--//  ENCODE lzo
	,parent_matl_id VARCHAR(18)  		--//  ENCODE lzo
	,parent_matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,mega_brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,mega_brnd_desc VARCHAR(100)  		--//  ENCODE lzo
	,brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,brnd_desc VARCHAR(100)  		--//  ENCODE lzo
	,base_prod_cd VARCHAR(10)  		--//  ENCODE lzo
	,base_prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,variant_cd VARCHAR(10)  		--//  ENCODE lzo
	,variant_desc VARCHAR(100)  		--//  ENCODE lzo
	,fran_cd VARCHAR(18)  		--//  ENCODE lzo
	,fran_desc VARCHAR(100)  		--//  ENCODE lzo
	,grp_fran_cd VARCHAR(18)  		--//  ENCODE lzo
	,grp_fran_desc VARCHAR(100)  		--//  ENCODE lzo
	,matl_type_cd VARCHAR(10)  		--//  ENCODE lzo
	,matl_type_desc VARCHAR(40)  		--//  ENCODE lzo
	,prod_fran_cd VARCHAR(18)  		--//  ENCODE lzo
	,prod_fran_desc VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_cd VARCHAR(40)  		--//  ENCODE lzo
	,prod_hier_desc VARCHAR(100)  		--//  ENCODE lzo
	,prod_mjr_cd VARCHAR(18)  		--//  ENCODE lzo
	,prod_mjr_desc VARCHAR(100)  		--//  ENCODE lzo
	,prod_mnr_cd VARCHAR(18)  		--//  ENCODE lzo
	,prod_mnr_desc VARCHAR(100)  		--//  ENCODE lzo
	,mercia_plan VARCHAR(10)  		--//  ENCODE lzo
	,putup_cd VARCHAR(10)  		--//  ENCODE lzo
	,putup_desc VARCHAR(100)  		--//  ENCODE lzo
	,bar_cd VARCHAR(40)  		--//  ENCODE lzo
	,prft_ctr VARCHAR(30)  		--//  ENCODE lzo
	,divest_flag VARCHAR(1)  		--//  ENCODE lzo
	,key_measure VARCHAR(40)  		--//  ENCODE lzo
	,ciw_ctgry VARCHAR(40)  		--//  ENCODE lzo
	,ciw_accnt_grp VARCHAR(40)  		--//  ENCODE lzo
	,px_gl_trans_desc VARCHAR(40)  		--//  ENCODE lzo
	,px_measure VARCHAR(40)  		--//  ENCODE lzo
	,px_bucket VARCHAR(40)  		--//  ENCODE lzo
	,sap_accnt VARCHAR(40)  		--//  ENCODE lzo
	,sap_accnt_desc VARCHAR(100)  		--//  ENCODE lzo
	,local_curr_cd VARCHAR(10)  		--//  ENCODE lzo
	,to_ccy VARCHAR(5)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,base_measure NUMERIC(38,7)  		--//  ENCODE az64
	,sales_qty NUMERIC(38,5)  		--//  ENCODE az64
	,gts NUMERIC(38,7)  		--//  ENCODE az64
	,gts_less_rtrn NUMERIC(38,7)  		--//  ENCODE az64
	,eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,nts_val NUMERIC(38,7)  		--//  ENCODE az64
	,tot_ciw_val NUMERIC(38,7)  		--//  ENCODE az64
	,posted_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,posted_con_free_goods_val NUMERIC(38,7)  		--//  ENCODE az64
	,posted_gp NUMERIC(38,7)  		--//  ENCODE az64
	,cogs NUMERIC(38,7)  		--//  ENCODE az64
	,gp NUMERIC(38,7)  		--//  ENCODE az64
	,futr_sls_qty NUMERIC(38,4)  		--//  ENCODE az64
	,futr_gts NUMERIC(38,9)  		--//  ENCODE az64
	,futr_ts NUMERIC(17,5)  		--//  ENCODE az64
	,futr_nts NUMERIC(38,9)  		--//  ENCODE az64
	,px_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,px_gts NUMERIC(38,7)  		--//  ENCODE az64
	,px_gts_less_rtrn NUMERIC(38,7)  		--//  ENCODE az64
	,px_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,px_nts NUMERIC(38,7)  		--//  ENCODE az64
	,px_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,px_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,px_gp NUMERIC(38,7)  		--//  ENCODE az64
	,projected_qty NUMERIC(38,5)  		--//  ENCODE az64
	,projected_gts_less_rtrn NUMERIC(38,7)  		--//  ENCODE az64
	,projected_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,projected_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,projected_nts NUMERIC(38,7)  		--//  ENCODE az64
	,projected_cogs_current NUMERIC(38,7)  		--//  ENCODE az64
	,projected_gp_current NUMERIC(38,7)  		--//  ENCODE az64
	,projected_cogs_actual NUMERIC(38,7)  		--//  ENCODE az64
	,projected_gp_actual NUMERIC(38,7)  		--//  ENCODE az64
	,goal_qty NUMERIC(18,0)  		--//  ENCODE az64
	,goal_gts NUMERIC(38,7)  		--//  ENCODE az64
	,goal_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,goal_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,goal_nts NUMERIC(38,7)  		--//  ENCODE az64
	,goal_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,goal_gp NUMERIC(38,7)  		--//  ENCODE az64
	,bp_qty NUMERIC(38,7)  		--//  ENCODE az64
	,bp_gts NUMERIC(38,7)  		--//  ENCODE az64
	,bp_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,bp_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,bp_nts NUMERIC(38,7)  		--//  ENCODE az64
	,bp_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,bp_gp NUMERIC(38,7)  		--//  ENCODE az64
	,ju_qty NUMERIC(38,7)  		--//  ENCODE az64
	,ju_gts NUMERIC(38,7)  		--//  ENCODE az64
	,ju_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,ju_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,ju_nts NUMERIC(38,7)  		--//  ENCODE az64
	,ju_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,ju_gp NUMERIC(38,7)  		--//  ENCODE az64
	,nu_qty NUMERIC(38,7)  		--//  ENCODE az64
	,nu_gts NUMERIC(38,7)  		--//  ENCODE az64
	,nu_eff_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_jgf_si_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_pmt_terms_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_datains_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_exp_adj_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_jgf_sd_val NUMERIC(38,7)  		--//  ENCODE az64
	,nu_ciw_tot NUMERIC(38,7)  		--//  ENCODE az64
	,nu_nts NUMERIC(38,7)  		--//  ENCODE az64
	,nu_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,nu_gp NUMERIC(38,7)  		--//  ENCODE az64
	,bme_trans NUMERIC(38,7)  		--//  ENCODE az64
	,incr_gts NUMERIC(38,7)  		--//  ENCODE az64
	,px_case_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,px_tot_planspend NUMERIC(38,7)  		--//  ENCODE az64
	,px_tot_paid NUMERIC(38,7)  		--//  ENCODE az64
	,px_committed_spend NUMERIC(38,7)  		--//  ENCODE az64
	,finance_cogs NUMERIC(38,7)  		--//  ENCODE az64
	,gcph_franchise VARCHAR(100)  		--//  ENCODE lzo
	,gcph_brand VARCHAR(100)  		--//  ENCODE lzo
	,gcph_subbrand VARCHAR(100)  		--//  ENCODE lzo
	,gcph_variant VARCHAR(100)  		--//  ENCODE lzo
	,gcph_needstate VARCHAR(100)  		--//  ENCODE lzo
	,gcph_category VARCHAR(100)  		--//  ENCODE lzo
	,gcph_subcategory VARCHAR(100)  		--//  ENCODE lzo
	,gcph_segment VARCHAR(100)  		--//  ENCODE lzo
	,gcph_subsegment VARCHAR(100)  		--//  ENCODE lzo
)

;

--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_sku_recom;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_SKU_RECOM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_sku_recom
(
	cust_cd VARCHAR(100)
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE zstd
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)
	,route_code VARCHAR(50)
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,region_name VARCHAR(200)  		--//  ENCODE zstd
	,zone_name VARCHAR(200)  		--//  ENCODE zstd
	,territory_name VARCHAR(200)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(50)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,product_category_name VARCHAR(255)  		--//  ENCODE zstd
	,variant_name VARCHAR(255)  		--//  ENCODE zstd
	,mothersku_name VARCHAR(255)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE delta // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE delta // INTEGER  
	,fisc_yr numeric(18,0)		--// INTEGER
	,month VARCHAR(50)
	,retailer_name VARCHAR(255)  		--//  ENCODE zstd
	,salesman_name VARCHAR(255)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_nr_val NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_amt NUMERIC(38,4)  		--//  ENCODE delta
	,num_packs VARCHAR(100)  		--//  ENCODE zstd
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,retailer_category_name VARCHAR(255)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(100)  		--//  ENCODE zstd
	,oos_flag VARCHAR(50)  		--//  ENCODE zstd
	,ms_flag VARCHAR(50)  		--//  ENCODE zstd
	,cs_flag VARCHAR(50)  		--//  ENCODE zstd
	,soq VARCHAR(50)  		--//  ENCODE zstd
	,hit_oos_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_ms_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_cs_flag VARCHAR(10)  		--//  ENCODE zstd
	,target_ms_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,target_cs_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,orange_percentage NUMERIC(6,2)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.rpt_in_perfect_store;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.RPT_IN_PERFECT_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.rpt_in_perfect_store
(
	dataset VARCHAR(22)  		--//  ENCODE zstd
	,customerid VARCHAR(255)  		--//  ENCODE zstd
	,salespersonid VARCHAR(255)  		--//  ENCODE zstd
	,mustcarryitem VARCHAR(4)  		--//  ENCODE zstd
	,answerscore VARCHAR(1)  		--//  ENCODE zstd
	,presence VARCHAR(5)  		--//  ENCODE zstd
	,outofstock VARCHAR(4)  		--//  ENCODE zstd
	,kpi VARCHAR(18)  		--//  ENCODE zstd
	,scheduleddate DATE  		--//  ENCODE az64
	,vst_status VARCHAR(9)  		--//  ENCODE zstd
	,fisc_yr VARCHAR(20)  		--//  ENCODE zstd
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,firstname VARCHAR(255)  		--//  ENCODE zstd
	,lastname VARCHAR(1)  		--//  ENCODE zstd
	,customername VARCHAR(511)  		--//  ENCODE zstd
	,country VARCHAR(5)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,storereference VARCHAR(255)  		--//  ENCODE zstd
	,storetype VARCHAR(255)  		--//  ENCODE zstd
	,channel VARCHAR(50)  		--//  ENCODE zstd
	,salesgroup VARCHAR(255)  		--//  ENCODE zstd
	,prod_hier_l1 VARCHAR(5)  		--//  ENCODE zstd
	,prod_hier_l2 VARCHAR(1)  		--//  ENCODE zstd
	,prod_hier_l3 VARCHAR(255)  		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(255)  		--//  ENCODE zstd
	,prod_hier_l5 VARCHAR(255)  		--//  ENCODE zstd
	,prod_hier_l6 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l7 VARCHAR(1)  		--//  ENCODE zstd
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE zstd
	,prod_hier_l9 VARCHAR(255)  		--//  ENCODE zstd
	,kpi_chnl_wt DOUBLE PRECISION  		--//  ENCODE zstd
	,ms_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hit_ms_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,"y/n_flag" VARCHAR(3)  		--//  ENCODE zstd
	,priority_store_flag VARCHAR(1)  		--//  ENCODE zstd
	,questiontext VARCHAR(255)  		--//  ENCODE zstd
	,ques_desc VARCHAR(11)  		--//  ENCODE zstd
	,value NUMERIC(14,0)  		--//  ENCODE az64
	,mkt_share NUMERIC(20,4)  		--//  ENCODE az64
	,rej_reason VARCHAR(511)  		--//  ENCODE zstd
	,photo_url VARCHAR(500)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_customer_dim;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_customer_dim
(
	customer_code VARCHAR(20)  		--//  ENCODE zstd
	,region_code NUMERIC(18,0)  		--//  ENCODE az64
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_code NUMERIC(18,0)  		--//  ENCODE az64
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_classification VARCHAR(50)  		--//  ENCODE zstd
	,territory_code NUMERIC(18,0)  		--//  ENCODE az64
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_classification VARCHAR(50)  		--//  ENCODE zstd
	,state_code NUMERIC(18,0)  		--//  ENCODE az64
	,state_name VARCHAR(50)  		--//  ENCODE zstd
	,town_name VARCHAR(50)  		--//  ENCODE zstd
	,town_classification VARCHAR(100)  		--//  ENCODE zstd
	,city VARCHAR(150)  		--//  ENCODE zstd
	,type_code NUMERIC(18,0)  		--//  ENCODE az64
	,type_name VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,customer_address1 VARCHAR(150)  		--//  ENCODE zstd
	,customer_address2 VARCHAR(150)  		--//  ENCODE zstd
	,customer_address3 VARCHAR(150)  		--//  ENCODE zstd
	,active_flag VARCHAR(7)  		--//  ENCODE zstd
	,active_start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,wholesalercode VARCHAR(50)  		--//  ENCODE zstd
	,super_stockiest VARCHAR(50)  		--//  ENCODE zstd
	,direct_account_flag VARCHAR(7)  		--//  ENCODE zstd
	,abi_code numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,abi_name VARCHAR(101)  		--//  ENCODE zstd
	,rds_size VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,num_of_retailers VARCHAR(20)  		--//  ENCODE zstd
	,customer_type VARCHAR(255)  		--//  ENCODE zstd
	,psnonps CHAR(1)  		--//  ENCODE zstd
	,suppliedby NUMERIC(18,0)  		--//  ENCODE az64
	,cfa VARCHAR(50)  		--//  ENCODE zstd
	,cfa_name VARCHAR(50)  		--//  ENCODE zstd
	,town_code VARCHAR(50)  		--//  ENCODE zstd
)

;

--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_brand_sls_target;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_BRAND_SLS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_brand_sls_target
(
	brnd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(30)  		--//  ENCODE lzo
	,cust_nm VARCHAR(30)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mo numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,brnd_trgt NUMERIC(16,5)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_catg_sls_target;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_CATG_SLS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_catg_sls_target
(
	sls_grp VARCHAR(30)  		--//  ENCODE lzo
	,brnd VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(2000)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,cust_num VARCHAR(30)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mo numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,sls_grp_cat_trgt NUMERIC(16,5)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_ecommerce_offtake;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_ECOMMERCE_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_ecommerce_offtake
(
	load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
	,retailer_sku_code VARCHAR(20) NOT NULL 		--//  ENCODE lzo
	,no_of_units_sold numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,ean VARCHAR(20)  		--//  ENCODE lzo
	,order_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,sales_value DOUBLE PRECISION
	,quantity numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,retailer_code VARCHAR(2000)  		--//  ENCODE lzo
	,retailer_name VARCHAR(2000)  		--//  ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE NOT NULL 		--//  ENCODE delta
	,order_number VARCHAR(255) NOT NULL 		--//  ENCODE lzo
	,product_code VARCHAR(255)  		--//  ENCODE lzo
	,product_title VARCHAR(20000)  		--//  ENCODE lzo
	,transaction_currency VARCHAR(10)  		--//  ENCODE lzo
	,country VARCHAR(10)  		--//  ENCODE lzo
	,sub_customer_name VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (retailer_sku_code, transaction_date, order_number)
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_fert_material_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_FERT_MATERIAL_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_fert_material_fact
(
	matl_num VARCHAR(18)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_hk_sellin_bp_le;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_HK_SELLIN_BP_LE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_hk_sellin_bp_le
(
	subsource_type VARCHAR(6)  		--//  ENCODE lzo
	,fisc_yr_per VARCHAR(22)  		--//  ENCODE lzo
	,mega_brnd_desc VARCHAR(100)  		--//  ENCODE lzo
	,brnd_desc VARCHAR(30)  		--//  ENCODE lzo
	,"go to model" VARCHAR(50)  		--//  ENCODE lzo
	,"banner format" VARCHAR(50)  		--//  ENCODE lzo
	,"sub channel" VARCHAR(50)  		--//  ENCODE lzo
	,"parent customer" VARCHAR(50)  		--//  ENCODE lzo
	,usd_rt NUMERIC(38,10)  		--//  ENCODE az64
	,lcl_rt NUMERIC(38,10)  		--//  ENCODE az64
	,tp_acc_start VARCHAR(20)  		--//  ENCODE lzo
	,tp_perc DOUBLE PRECISION
	,time_gone DOUBLE PRECISION
	,preww7_act DOUBLE PRECISION
	,nts_act DOUBLE PRECISION
	,tp_act DOUBLE PRECISION
	,tp_act_calc DOUBLE PRECISION
	,nts_bp DOUBLE PRECISION
	,tp_bp DOUBLE PRECISION
	,nts_le DOUBLE PRECISION
	,tp_le DOUBLE PRECISION
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_ims_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_IMS_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_ims_fact
(
	ims_txn_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(255)  		--//  ENCODE lzo
	,prod_nm VARCHAR(255)  		--//  ENCODE lzo
	,rpt_per_strt_dt DATE  		--//  ENCODE az64
	,rpt_per_end_dt DATE  		--//  ENCODE az64
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,unit_prc NUMERIC(21,5)  		--//  ENCODE az64
	,sls_amt NUMERIC(21,5)  		--//  ENCODE az64
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_amt NUMERIC(21,5)  		--//  ENCODE az64
	,ship_cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cls_grp VARCHAR(20)  		--//  ENCODE lzo
	,cust_sub_cls VARCHAR(20)  		--//  ENCODE lzo
	,prod_spec VARCHAR(50)  		--//  ENCODE lzo
	,itm_agn_nm VARCHAR(100)  		--//  ENCODE lzo
	,ordr_co VARCHAR(20)  		--//  ENCODE lzo
	,rtrn_rsn VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_ofc_nm VARCHAR(20)  		--//  ENCODE lzo
	,sls_grp_nm VARCHAR(20)  		--//  ENCODE lzo
	,acc_type VARCHAR(10)  		--//  ENCODE lzo
	,co_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,doc_dt DATE  		--//  ENCODE az64
	,doc_num VARCHAR(20)  		--//  ENCODE lzo
	,invc_num VARCHAR(15)  		--//  ENCODE lzo
	,remark_desc VARCHAR(100)  		--//  ENCODE lzo
	,gift_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_bfr_tax_amt NUMERIC(21,5)  		--//  ENCODE az64
	,sku_per_box NUMERIC(21,5)  		--//  ENCODE az64
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,prom_sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,prom_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sap_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_type VARCHAR(20)  		--//  ENCODE lzo
	,sub_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,sub_customer_name VARCHAR(100)  		--//  ENCODE lzo
	,sales_priority VARCHAR(10)  		--//  ENCODE lzo
	,sales_stores NUMERIC(21,5)  		--//  ENCODE az64
	,sales_rate NUMERIC(21,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_ims_inventory_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_IMS_INVENTORY_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_ims_inventory_fact
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(30)
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(200)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,invnt_amt NUMERIC(21,5)  		--//  ENCODE az64
	,avg_prc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,safety_stock NUMERIC(21,5)  		--//  ENCODE az64
	,bad_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,book_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,convs_amt NUMERIC(21,5)  		--//  ENCODE az64
	,prch_disc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,end_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,batch_no VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chn_uom VARCHAR(100)  		--//  ENCODE lzo
	,storage_name VARCHAR(50)  		--//  ENCODE lzo
	,area VARCHAR(50)  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	dstr_cd
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_internal_target_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_INTERNAL_TARGET_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_internal_target_fact
(
	fisc_yr_per VARCHAR(12)  		--//  ENCODE lzo
	,j_j_fisc_wk VARCHAR(50)  		--//  ENCODE lzo
	,co_cd VARCHAR(4)  		--//  ENCODE lzo
	,val_type VARCHAR(50)  		--//  ENCODE lzo
	,vers numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,man_type VARCHAR(10)  		--//  ENCODE lzo
	,crncy VARCHAR(5)  		--//  ENCODE lzo
	,sls_org VARCHAR(4)  		--//  ENCODE lzo
	,dstn_chnl VARCHAR(2)  		--//  ENCODE lzo
	,div VARCHAR(5)  		--//  ENCODE lzo
	,matl VARCHAR(10)  		--//  ENCODE lzo
	,mega_brnd_b1 VARCHAR(50)  		--//  ENCODE lzo
	,brnd_b2 VARCHAR(50)  		--//  ENCODE lzo
	,base_prod_b3 VARCHAR(50)  		--//  ENCODE lzo
	,vrnt_b4 VARCHAR(50)  		--//  ENCODE lzo
	,put_up_b5 VARCHAR(50)  		--//  ENCODE lzo
	,oper_grp VARCHAR(50)  		--//  ENCODE lzo
	,fran_grp VARCHAR(50)  		--//  ENCODE lzo
	,fran VARCHAR(50)  		--//  ENCODE lzo
	,prod_fran VARCHAR(50)  		--//  ENCODE lzo
	,prod_maj VARCHAR(50)  		--//  ENCODE lzo
	,prod_minor VARCHAR(50)  		--//  ENCODE lzo
	,cur_sls_emp VARCHAR(50)  		--//  ENCODE lzo
	,cust_num numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,lcl_cust_grp_1 VARCHAR(50)  		--//  ENCODE lzo
	,lcl_cust_grp_2 VARCHAR(50)  		--//  ENCODE lzo
	,lcl_cust_grp_3 VARCHAR(50)  		--//  ENCODE lzo
	,lcl_cust_grp_4 VARCHAR(50)  		--//  ENCODE lzo
	,lcl_cust_grp_5 VARCHAR(50)  		--//  ENCODE lzo
	,lcl_cust_grp_6 VARCHAR(50)  		--//  ENCODE lzo
	,sls_trgt NUMERIC(20,5)  		--//  ENCODE lzo
	,qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,unit VARCHAR(50)  		--//  ENCODE lzo
	,fisc_vrnt VARCHAR(5)  		--//  ENCODE lzo
	,fisc_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,ctry VARCHAR(5)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_intrm_calendar;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_INTRM_CALENDAR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_intrm_calendar
(
	cal_day DATE  		--//  ENCODE lzo
	,fisc_yr_vrnt VARCHAR(2)  		--//  ENCODE lzo
	,wkday numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_wk numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_mo_1 numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_mo_2 numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_qtr_1 numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_qtr_2 numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,half_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,pstng_per numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,rec_mode VARCHAR(1)  		--//  ENCODE lzo
	,promo_week numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,promo_month VARCHAR(3)  		--//  ENCODE lzo
	,promo_per VARCHAR(13)  		--//  ENCODE lzo
	,fisc_wk_num numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,max_wk_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_kr_dads_analysis;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_KR_DADS_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_kr_dads_analysis
(
	brand VARCHAR(200)  		--//  ENCODE zstd
	,keyword VARCHAR(200)  		--//  ENCODE zstd
	,search_area VARCHAR(200)  		--//  ENCODE zstd
	,ad_types VARCHAR(200)  		--//  ENCODE zstd
	,retailer VARCHAR(200)  		--//  ENCODE zstd
	,sub_retailer VARCHAR(200)  		--//  ENCODE zstd
	,product_name VARCHAR(200)  		--//  ENCODE zstd
	,barcode VARCHAR(200)  		--//  ENCODE zstd
	,keyword_group VARCHAR(60)  		--//  ENCODE zstd
	,fisc_wk_num VARCHAR(10)  		--//  ENCODE lzo
	,fisc_mnth numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fisc_day DATE  		--//  ENCODE lzo
	,category_1 VARCHAR(200)  		--//  ENCODE zstd
	,category_2 VARCHAR(200)  		--//  ENCODE zstd
	,category_3 VARCHAR(200)  		--//  ENCODE zstd
	,ranking VARCHAR(250)  		--//  ENCODE zstd
	,click NUMERIC(20,4)  		--//  ENCODE az64
	,impression NUMERIC(20,4)  		--//  ENCODE az64
	,ad_cost NUMERIC(20,4)  		--//  ENCODE az64
	,total_order NUMERIC(20,4)  		--//  ENCODE az64
	,total_order_sku NUMERIC(20,4)  		--//  ENCODE az64
	,total_conversion_sales NUMERIC(20,4)  		--//  ENCODE az64
	,total_conversion_sales_sku NUMERIC(20,4)  		--//  ENCODE az64
	,sales_gmv NUMERIC(20,4)  		--//  ENCODE az64
	,sales_gmv_sku NUMERIC(20,4)  		--//  ENCODE az64
	,pa_cost NUMERIC(20,4)  		--//  ENCODE az64
	,bpa_cost NUMERIC(20,4)  		--//  ENCODE az64
	,sa_cost NUMERIC(20,4)  		--//  ENCODE az64
	,observed_price NUMERIC(20,4)  		--//  ENCODE az64
	,rocket_wow_price NUMERIC(20,4)  		--//  ENCODE az64
	,total_monthly_search_volume NUMERIC(20,4)  		--//  ENCODE az64
	,payment_amount NUMERIC(20,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_kr_otc_inventory;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_KR_OTC_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_kr_otc_inventory
(
	mnth_id VARCHAR(20)  		--//  ENCODE zstd
	,matl_num VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,distributor_cd VARCHAR(50)  		--//  ENCODE zstd
	,unit_price NUMERIC(20,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(20,4)  		--//  ENCODE az64
	,inv_amt NUMERIC(20,4)  		--//  ENCODE az64
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_kr_sales_tgt;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_KR_SALES_TGT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_kr_sales_tgt
(
	ctry_cd VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,crncy_cd VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,sls_ofc_cd VARCHAR(4) NOT NULL 		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,channel VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,store_type VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,sls_grp_cd VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,sls_grp VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,target_type VARCHAR(4) NOT NULL 		--//  ENCODE zstd
	,prod_hier_l2 VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,fisc_yr numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,fisc_yr_per numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,target_amt NUMERIC(20,5)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (sls_ofc_cd, sls_grp_cd, target_type)
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_kr_trade_promotion;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_KR_TRADE_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_kr_trade_promotion
(
	ctry_cd VARCHAR(2)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(3)  		--//  ENCODE zstd
	,customer_code VARCHAR(20)  		--//  ENCODE zstd
	,activity_name VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,line_begin_date DATE  		--//  ENCODE delta
	,line_end_date DATE  		--//  ENCODE delta
	,or_tp_qty NUMERIC(38,5)  		--//  ENCODE delta
	,or_tp_rebate_a NUMERIC(38,5)  		--//  ENCODE delta
	,ttl_cost NUMERIC(38,5)  		--//  ENCODE delta
	,remark VARCHAR(500)  		--//  ENCODE zstd
	,sap_sgrp VARCHAR(10)  		--//  ENCODE zstd
	,or_tp_rebate NUMERIC(38,5)  		--//  ENCODE delta
	,application_code VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_onpck_trgt;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_ONPCK_TRGT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_onpck_trgt
(
	matl_num VARCHAR(50) NOT NULL
	,matl_desc VARCHAR(100)
	,acct_grp VARCHAR(100) NOT NULL
	,mo VARCHAR(10) NOT NULL
	,yr VARCHAR(10) NOT NULL
	,trgt_qty NUMERIC(22,2)
	,ctry_cd VARCHAR(20) NOT NULL
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (matl_num, acct_grp, mo, yr, ctry_cd)
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_pos_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_POS_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_pos_fact
(
	pos_dt DATE  		--//  ENCODE lzo
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE bytedict
	,unit_prc_amt NUMERIC(16,5)
	,sls_excl_vat_amt NUMERIC(16,5)
	,stk_rtrn_amt NUMERIC(16,5)
	,stk_recv_amt NUMERIC(16,5)
	,avg_sell_qty NUMERIC(16,5)
	,cum_ship_qty numeric(18,0)		--// INTEGER
	,cum_rtrn_qty numeric(18,0)		--// INTEGER
	,web_ordr_takn_qty numeric(18,0)		--// INTEGER
	,web_ordr_acpt_qty numeric(18,0)		--// INTEGER
	,dc_invnt_qty numeric(18,0)		--// INTEGER
	,invnt_qty numeric(18,0)		--// INTEGER
	,invnt_amt NUMERIC(16,5)
	,invnt_dt DATE
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--// INTEGER
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(100)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(100)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,hist_flg VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,prom_sls_amt NUMERIC(16,5)  		--//  ENCODE bytedict
	,prom_prc_amt NUMERIC(16,5)
	,channel VARCHAR(25)  		--//  ENCODE lzo
	,store_type VARCHAR(25)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(18)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_pos_inventory_fact;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_POS_INVENTORY_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_pos_inventory_fact
(
	invnt_dt DATE
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,per_box_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,cust_invnt_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,box_invnt_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,wk_hold_sls NUMERIC(16,5)  		--//  ENCODE lzo
	,wk_hold NUMERIC(16,5)  		--//  ENCODE lzo
	,fst_recv_dt VARCHAR(10)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(10)  		--//  ENCODE lzo
	,dc VARCHAR(40)  		--//  ENCODE lzo
	,stk_cls VARCHAR(40)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(100)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(100)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,prom_invnt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,prom_prc_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,hist_flg VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	invnt_dt
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_sfa_pm;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_RPT_SFA_PM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_sfa_pm
(
	id numeric(38,0) IDENTITY(6776559,1) 		--//  ENCODE az64 // character varying // BIGINT   
	,data_type VARCHAR(20)  		--//  ENCODE lzo
	,taskid VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(255)  		--//  ENCODE lzo
	,path VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,mrchr_visitdate VARCHAR(255)  		--//  ENCODE lzo
	,customername VARCHAR(255)  		--//  ENCODE lzo
	,salesgroup VARCHAR(255)  		--//  ENCODE lzo
	,storetype VARCHAR(255)  		--//  ENCODE lzo
	,dist_chnl VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,salescyclename VARCHAR(255)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,salesperson_firstname VARCHAR(128)  		--//  ENCODE lzo
	,salesperson_lastname VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,remotekey VARCHAR(64)  		--//  ENCODE lzo
	,secondarytradecode VARCHAR(64)  		--//  ENCODE lzo
	,secondarytradename VARCHAR(256)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_tw_sales_incentive;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_RPT_TW_SALES_INCENTIVE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_tw_sales_incentive
(
	source_type VARCHAR(15)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(5)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,to_crncy VARCHAR(5)  		--//  ENCODE lzo
	,psr_code VARCHAR(100)  		--//  ENCODE lzo
	,psr_name VARCHAR(255)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,report_to VARCHAR(500)  		--//  ENCODE lzo
	,reportto_name VARCHAR(500)  		--//  ENCODE lzo
	,reverse VARCHAR(500)  		--//  ENCODE lzo
	,monthly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_target NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_achievement DOUBLE PRECISION
	,monthly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_target NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_achievement DOUBLE PRECISION
	,quarterly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_sales_rep_route_plan;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_SALES_REP_ROUTE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_sales_rep_route_plan
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,sls_rep_typ VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_nm VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,visit_jj_mnth_id numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,visit_jj_wk_no numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,visit_jj_wkdy_no numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,visit_dt DATE NOT NULL 		--//  ENCODE delta
	,visit_end_dt DATE  		--//  ENCODE delta
	,effctv_strt_dt DATE NOT NULL 		--//  ENCODE delta
	,effctv_end_dt DATE  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_sfmc_naver_data;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_SFMC_NAVER_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_sfmc_naver_data
(
	cntry_cd VARCHAR(100)  		--//  ENCODE lzo
	,naver_id VARCHAR(50)  		--//  ENCODE lzo
	,birth_year VARCHAR(4)  		--//  ENCODE lzo
	,gender VARCHAR(50)  		--//  ENCODE lzo
	,total_purchase_amount VARCHAR(50)  		--//  ENCODE lzo
	,total_number_of_purchases VARCHAR(50)  		--//  ENCODE lzo
	,membership_grade_level VARCHAR(50)  		--//  ENCODE lzo
	,marketing_message_viewing_receiving VARCHAR(50)  		--//  ENCODE lzo
	,coupon_usage_issuance VARCHAR(50)  		--//  ENCODE lzo
	,number_of_reviews VARCHAR(50)  		--//  ENCODE lzo
	,number_of_comments VARCHAR(50)  		--//  ENCODE lzo
	,number_of_attendances VARCHAR(50)  		--//  ENCODE lzo
	,opt_in_for_jnj_communication VARCHAR(50)  		--//  ENCODE lzo
	,notification_subscription VARCHAR(50)  		--//  ENCODE lzo
	,acquisition_channel VARCHAR(100)  		--//  ENCODE lzo
	,reason_for_joining VARCHAR(100)  		--//  ENCODE lzo
	,skin_type_body VARCHAR(100)  		--//  ENCODE lzo
	,baby_skin_concerns VARCHAR(100)  		--//  ENCODE lzo
	,oral_health_concerns VARCHAR(100)  		--//  ENCODE lzo
	,skin_concerns_face VARCHAR(100)  		--//  ENCODE lzo
	,updated_date VARCHAR(50)  		--//  ENCODE lzo
	,membership_registration_date VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_sls_rep_dim;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_SLS_REP_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_sls_rep_dim
(
	ctry_cd VARCHAR(5)  		--//  ENCODE lzo
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,sls_rep_typ VARCHAR(20)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_store_dim;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_STORE_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_store_dim
(
	ctry_cd VARCHAR(5)  		--//  ENCODE lzo
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,store_cd VARCHAR(50)  		--//  ENCODE lzo
	,store_nm VARCHAR(100)  		--//  ENCODE lzo
	,store_class VARCHAR(3)  		--//  ENCODE lzo
	,dstr_cust_area VARCHAR(20)  		--//  ENCODE lzo
	,dstr_cust_clsn1 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn2 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn3 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn4 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn5 VARCHAR(100)  		--//  ENCODE lzo
	,effctv_strt_dt VARCHAR(52)  		--//  ENCODE lzo
	,effctv_end_dt VARCHAR(52)  		--//  ENCODE lzo
	,distributor_address VARCHAR(255)  		--//  ENCODE lzo
	,distributor_telephone VARCHAR(255)  		--//  ENCODE lzo
	,distributor_contact VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,hq VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_trd_promo_actl;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_TRD_PROMO_ACTL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_trd_promo_actl
(
	prft_ctr VARCHAR(100) NOT NULL
	,prft_ctr_desc VARCHAR(200)
	,cust_channel VARCHAR(100) NOT NULL
	,cust_hdqtr_cd VARCHAR(200) NOT NULL
	,ctry_cd VARCHAR(20) NOT NULL
	,crncy_cd VARCHAR(3) NOT NULL
	,yr VARCHAR(4) NOT NULL
	,jan_und_prcs NUMERIC(16,2)
	,jan_aprv_no_pmt NUMERIC(16,2)
	,jan_pmt NUMERIC(16,2)
	,jan_actl_trd_promo NUMERIC(16,2)
	,feb_und_prcs NUMERIC(16,2)
	,feb_aprv_no_pmt NUMERIC(16,2)
	,feb_pmt NUMERIC(16,2)
	,feb_actl_trd_promo NUMERIC(16,2)
	,mar_und_prcs NUMERIC(16,2)
	,mar_aprv_no_pmt NUMERIC(16,2)
	,mar_pmt NUMERIC(16,2)
	,mar_actl_trd_promo NUMERIC(16,2)
	,apr_und_prcs NUMERIC(16,2)
	,apr_aprv_no_pmt NUMERIC(16,2)
	,apr_pmt NUMERIC(16,2)
	,apr_actl_trd_promo NUMERIC(16,2)
	,may_und_prcs NUMERIC(16,2)
	,may_aprv_no_pmt NUMERIC(16,2)
	,may_pmt NUMERIC(16,2)
	,may_actl_trd_promo NUMERIC(16,2)
	,jun_und_prcs NUMERIC(16,2)
	,jun_aprv_no_pmt NUMERIC(16,2)
	,jun_pmt NUMERIC(16,2)
	,jun_actl_trd_promo NUMERIC(16,2)
	,jul_und_prcs NUMERIC(16,2)
	,jul_aprv_no_pmt NUMERIC(16,2)
	,jul_pmt NUMERIC(16,2)
	,jul_actl_trd_promo NUMERIC(16,2)
	,aug_und_prcs NUMERIC(16,2)
	,aug_aprv_no_pmt NUMERIC(16,2)
	,aug_pmt NUMERIC(16,2)
	,aug_actl_trd_promo NUMERIC(16,2)
	,sep_und_prcs NUMERIC(16,2)
	,sep_aprv_no_pmt NUMERIC(16,2)
	,sep_pmt NUMERIC(16,2)
	,sep_actl_trd_promo NUMERIC(16,2)
	,oct_und_prcs NUMERIC(16,2)
	,oct_aprv_no_pmt NUMERIC(16,2)
	,oct_pmt NUMERIC(16,2)
	,oct_actl_trd_promo NUMERIC(16,2)
	,nov_und_prcs NUMERIC(16,2)
	,nov_aprv_no_pmt NUMERIC(16,2)
	,nov_pmt NUMERIC(16,2)
	,nov_actl_trd_promo NUMERIC(16,2)
	,dec_und_prcs NUMERIC(16,2)
	,dec_aprv_no_pmt NUMERIC(16,2)
	,dec_pmt NUMERIC(16,2)
	,dec_actl_trd_promo NUMERIC(16,2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (prft_ctr, cust_channel, cust_hdqtr_cd, ctry_cd, yr)
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_trd_promo_pln;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_TRD_PROMO_PLN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_trd_promo_pln
(
	prft_ctr VARCHAR(100) NOT NULL
	,prft_ctr_nm VARCHAR(100)
	,cust_channel VARCHAR(100) NOT NULL
	,cust_channel_nm VARCHAR(100) NOT NULL
	,cust_hdqtr_cd VARCHAR(200) NOT NULL
	,cust_hdqtr_nm VARCHAR(200)
	,ctry_cd VARCHAR(20) NOT NULL
	,crncy_cd VARCHAR(3) NOT NULL
	,yr VARCHAR(4) NOT NULL
	,jan_trd_promo_pln NUMERIC(16,2)
	,feb_trd_promo_pln NUMERIC(16,2)
	,mar_trd_promo_pln NUMERIC(16,2)
	,apr_trd_promo_pln NUMERIC(16,2)
	,may_trd_promo_pln NUMERIC(16,2)
	,jun_trd_promo_pln NUMERIC(16,2)
	,jul_trd_promo_pln NUMERIC(16,2)
	,aug_trd_promo_pln NUMERIC(16,2)
	,sep_trd_promo_pln NUMERIC(16,2)
	,oct_trd_promo_pln NUMERIC(16,2)
	,nov_trd_promo_pln NUMERIC(16,2)
	,dec_trd_promo_pln NUMERIC(16,2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bp_target;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_TW_BP_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bp_target
(
	bp_version VARCHAR(5)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,lph_level_6 VARCHAR(255)  		--//  ENCODE zstd
	,pre_sales DOUBLE PRECISION  		--//  ENCODE zstd
	,tp DOUBLE PRECISION  		--//  ENCODE zstd
	,nts DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bu_forecast_prod_hier;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_TW_BU_FORECAST_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bu_forecast_prod_hier
(
	bu_version VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,lph_level_6 VARCHAR(255)  		--//  ENCODE zstd
	,price_off DOUBLE PRECISION  		--//  ENCODE zstd
	,display DOUBLE PRECISION  		--//  ENCODE zstd
	,dm DOUBLE PRECISION  		--//  ENCODE zstd
	,other_support DOUBLE PRECISION  		--//  ENCODE zstd
	,sr DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales_before_returns DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE ALL
;
--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bu_forecast_sku;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_TW_BU_FORECAST_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_tw_bu_forecast_sku
(
	bu_version VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,sap_code VARCHAR(30)  		--//  ENCODE zstd
	,system_list_price DOUBLE PRECISION  		--//  ENCODE zstd
	,gross_invoice_price DOUBLE PRECISION  		--//  ENCODE zstd
	,gross_invoice_price_lesst_terms DOUBLE PRECISION  		--//  ENCODE zstd
	,rf_sellout_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,rf_sellin_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,price_off DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales_before_returns DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE ALL
;
--DROP TABLE SNAPNTAEDW_INTEGRATION.pop6_kpi2data_mapping;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.POP6_KPI2DATA_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.pop6_kpi2data_mapping
(
	ctry VARCHAR(20)  		--//  ENCODE lzo
	,data_type VARCHAR(30)  		--//  ENCODE lzo
	,identifier VARCHAR(30)  		--//  ENCODE lzo
	,kpi_name VARCHAR(30)  		--//  ENCODE lzo
	,store_type VARCHAR(50)  		--//  ENCODE lzo
	,category VARCHAR(50)  		--//  ENCODE lzo
	,segment VARCHAR(50)  		--//  ENCODE lzo
	,value VARCHAR(50)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;


--DROP TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_th_perfect_store;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_RPT_TH_PERFECT_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.edw_rpt_th_perfect_store
(
	dataset VARCHAR(14)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(255)  		--//  ENCODE lzo
	,mustcarryitem VARCHAR(4)  		--//  ENCODE lzo
	,answerscore VARCHAR(1)  		--//  ENCODE lzo
	,presence VARCHAR(5)  		--//  ENCODE lzo
	,outofstock VARCHAR(4)  		--//  ENCODE lzo
	,kpi VARCHAR(255)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,vst_status VARCHAR(9)  		--//  ENCODE lzo
	,fisc_yr VARCHAR(13)  		--//  ENCODE lzo
	,fisc_per VARCHAR(255)  		--//  ENCODE lzo
	,firstname VARCHAR(513)  		--//  ENCODE lzo
	,lastname VARCHAR(1)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,country VARCHAR(8)  		--//  ENCODE lzo
	,state VARCHAR(255)  		--//  ENCODE lzo
	,storereference VARCHAR(255)  		--//  ENCODE lzo
	,storetype VARCHAR(300)  		--//  ENCODE lzo
	,channel VARCHAR(300)  		--//  ENCODE lzo
	,salesgroup VARCHAR(113)  		--//  ENCODE lzo
	,eannumber VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(30)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(30)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(513)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,"y/n_flag" VARCHAR(3)  		--//  ENCODE lzo
	,posm_execution_flag VARCHAR(1)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(1)  		--//  ENCODE lzo
	,questiontext VARCHAR(500)  		--//  ENCODE lzo
	,ques_desc VARCHAR(11)  		--//  ENCODE lzo
	,value VARCHAR(255)  		--//  ENCODE lzo
	,mkt_share NUMERIC(32,4)  		--//  ENCODE az64
	,rej_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo_url VARCHAR(1)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAEDW_INTEGRATION.v_rpt_my_perfect_store_snapshot;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.V_RPT_MY_PERFECT_STORE_SNAPSHOT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.v_rpt_my_perfect_store_snapshot
(
	dataset VARCHAR(22)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(50)  		--//  ENCODE lzo
	,mustcarryitem VARCHAR(4)  		--//  ENCODE lzo
	,answerscore VARCHAR(1)  		--//  ENCODE lzo
	,presence VARCHAR(5)  		--//  ENCODE lzo
	,outofstock VARCHAR(4)  		--//  ENCODE lzo
	,kpi VARCHAR(16)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,vst_status VARCHAR(9)  		--//  ENCODE lzo
	,fisc_yr VARCHAR(13)  		--//  ENCODE lzo
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,firstname VARCHAR(40)  		--//  ENCODE lzo
	,lastname VARCHAR(1)  		--//  ENCODE lzo
	,customername VARCHAR(511)  		--//  ENCODE lzo
	,country VARCHAR(8)  		--//  ENCODE lzo
	,state VARCHAR(255)  		--//  ENCODE lzo
	,storereference VARCHAR(255)  		--//  ENCODE lzo
	,storetype VARCHAR(382)  		--//  ENCODE lzo
	,channel VARCHAR(2)  		--//  ENCODE lzo
	,salesgroup VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(8)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(382)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(255)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,"y/n_flag" VARCHAR(21)  		--//  ENCODE lzo
	,posm_execution_flag VARCHAR(1)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(1)  		--//  ENCODE lzo
	,questiontext VARCHAR(255)  		--//  ENCODE lzo
	,ques_desc VARCHAR(11)  		--//  ENCODE lzo
	,value NUMERIC(38,2)  		--//  ENCODE az64
	,mkt_share NUMERIC(20,4)  		--//  ENCODE az64
	,rej_reason VARCHAR(255)  		--//  ENCODE lzo
)

;

--DROP TABLE rg_wks.prod_hier_data_new;
CREATE OR REPLACE TABLE RG_WKS.PROD_HIER_DATA_NEW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.prod_hier_data_new
(
	country VARCHAR(200)  		--//  ENCODE zstd
	,dataset VARCHAR(49)  		--//  ENCODE zstd
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,parent_customer VARCHAR(255)  		--//  ENCODE zstd
	,retail_environment VARCHAR(256)  		--//  ENCODE zstd
	,channel VARCHAR(255)  		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(510)  		--//  ENCODE zstd
	,priority_store_flag VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE rg_wks.wks_edw_customer_attr_flat_dim;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_CUSTOMER_ATTR_FLAT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_customer_attr_flat_dim
(
	aw_remote_key VARCHAR(10)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,street_num VARCHAR(2)  		--//  ENCODE lzo
	,street_nm VARCHAR(2)  		--//  ENCODE lzo
	,city VARCHAR(150)  		--//  ENCODE lzo
	,post_cd VARCHAR(150)  		--//  ENCODE lzo
	,dist VARCHAR(2)  		--//  ENCODE lzo
	,county VARCHAR(150)  		--//  ENCODE lzo
	,cntry VARCHAR(150)  		--//  ENCODE lzo
	,ph_num VARCHAR(150)  		--//  ENCODE lzo
	,email_id VARCHAR(2)  		--//  ENCODE lzo
	,website VARCHAR(2)  		--//  ENCODE lzo
	,store_typ VARCHAR(1)  		--//  ENCODE lzo
	,cust_store_ref VARCHAR(2)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,sls_grp VARCHAR(40)  		--//  ENCODE lzo
	,secondary_trade_cd VARCHAR(2)  		--//  ENCODE lzo
	,secondary_trade_nm VARCHAR(2)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(10)  		--//  ENCODE lzo
	,trgt_type VARCHAR(4)  		--//  ENCODE lzo
	,sls_ofc VARCHAR(4)  		--//  ENCODE lzo
	,sls_ofc_desc VARCHAR(40)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(3)  		--//  ENCODE lzo
	,sfa_cust_code VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
	,crt_ds VARCHAR(10)  		--//  ENCODE lzo
	,upd_ds VARCHAR(3)  		--//  ENCODE lzo
	,target_sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,priority_nm VARCHAR(3)  		--//  ENCODE lzo
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_data_reformat;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_DATA_REFORMAT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_data_reformat
(
	dataset VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(255)  		--//  ENCODE lzo
	,visitid VARCHAR(255)  		--//  ENCODE lzo
	,questiontext VARCHAR(513)  		--//  ENCODE lzo
	,productid VARCHAR(255)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,latestdate DATE  		--//  ENCODE az64
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,merchandiser_name VARCHAR(770)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,country VARCHAR(200)  		--//  ENCODE lzo
	,state VARCHAR(256)  		--//  ENCODE lzo
	,parent_customer VARCHAR(382)  		--//  ENCODE lzo
	,retail_environment VARCHAR(382)  		--//  ENCODE lzo
	,channel VARCHAR(382)  		--//  ENCODE lzo
	,retailer VARCHAR(382)  		--//  ENCODE lzo
	,business_unit VARCHAR(200)  		--//  ENCODE lzo
	,eannumber VARCHAR(255)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(2000)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(2258)  		--//  ENCODE lzo
	,ques_type VARCHAR(382)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(150)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(256)  		--//  ENCODE lzo
	,add_info VARCHAR(65535)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,response_score VARCHAR(65535)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,mkt_share DOUBLE PRECISION
	,salience_val NUMERIC(20,4)  		--//  ENCODE az64
	,channel_weightage NUMERIC(31,2)  		--//  ENCODE az64
	,actual_value NUMERIC(14,4)  		--//  ENCODE az64
	,ref_value NUMERIC(22,4)  		--//  ENCODE az64
	,kpi_actual_wt_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_wt_val VARCHAR(150)  		--//  ENCODE lzo
	,photo_url VARCHAR(500)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_hash;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_HASH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_hash
(
	hashkey VARCHAR(32)  		--//  ENCODE lzo
	,hash_row numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,dataset VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(255)  		--//  ENCODE lzo
	,visitid VARCHAR(255)  		--//  ENCODE lzo
	,questiontext VARCHAR(513)  		--//  ENCODE lzo
	,productid VARCHAR(255)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,latestdate DATE  		--//  ENCODE az64
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,merchandiser_name VARCHAR(770)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,country VARCHAR(200)  		--//  ENCODE lzo
	,state VARCHAR(256)  		--//  ENCODE lzo
	,parent_customer VARCHAR(382)  		--//  ENCODE lzo
	,retail_environment VARCHAR(382)  		--//  ENCODE lzo
	,channel VARCHAR(382)  		--//  ENCODE lzo
	,retailer VARCHAR(382)  		--//  ENCODE lzo
	,business_unit VARCHAR(200)  		--//  ENCODE lzo
	,eannumber VARCHAR(255)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(2000)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(2258)  		--//  ENCODE lzo
	,ques_type VARCHAR(382)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(150)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(256)  		--//  ENCODE lzo
	,add_info VARCHAR(65535)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,response_score VARCHAR(65535)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,mkt_share DOUBLE PRECISION
	,salience_val NUMERIC(20,4)  		--//  ENCODE az64
	,channel_weightage NUMERIC(31,2)  		--//  ENCODE az64
	,actual_value NUMERIC(14,4)  		--//  ENCODE az64
	,ref_value NUMERIC(22,4)  		--//  ENCODE az64
	,kpi_actual_wt_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_wt_val VARCHAR(150)  		--//  ENCODE lzo
	,photo_url VARCHAR(500)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
)

CLUSTER BY(hashkey)
;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_AGG_WT_MNTH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,total_weight NUMERIC(38,4)  		--//  ENCODE az64
	,calc_weight NUMERIC(38,33)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth_cname;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_AGG_WT_MNTH_CNAME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth_cname
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,total_weight NUMERIC(38,4)  		--//  ENCODE az64
	,calc_weight NUMERIC(38,33)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_REBASED_WT_MNTH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,chnl_wt NUMERIC(8,4)  		--//  ENCODE az64
	,total_weight NUMERIC(38,4)  		--//  ENCODE az64
	,calc_weight NUMERIC(38,33)  		--//  ENCODE az64
	,weight_msl NUMERIC(38,37)  		--//  ENCODE az64
	,weight_oos NUMERIC(38,37)  		--//  ENCODE az64
	,weight_soa NUMERIC(38,37)  		--//  ENCODE az64
	,weight_sos NUMERIC(38,37)  		--//  ENCODE az64
	,weight_promo NUMERIC(38,37)  		--//  ENCODE az64
	,weight_planogram NUMERIC(38,37)  		--//  ENCODE az64
	,weight_display NUMERIC(38,37)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_REBASED_WT_MNTH_CNAME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,chnl_wt NUMERIC(8,4)  		--//  ENCODE az64
	,total_weight NUMERIC(38,4)  		--//  ENCODE az64
	,calc_weight NUMERIC(38,33)  		--//  ENCODE az64
	,weight_msl NUMERIC(38,37)  		--//  ENCODE az64
	,weight_oos NUMERIC(38,37)  		--//  ENCODE az64
	,weight_soa NUMERIC(38,37)  		--//  ENCODE az64
	,weight_sos NUMERIC(38,37)  		--//  ENCODE az64
	,weight_promo NUMERIC(38,37)  		--//  ENCODE az64
	,weight_planogram NUMERIC(38,37)  		--//  ENCODE az64
	,weight_display NUMERIC(38,37)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_wt_mnth;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_WT_MNTH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_wt_mnth
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,chnl_wt NUMERIC(8,4)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PERFECT_STORE_KPI_WT_MNTH_CNAME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
(
	country VARCHAR(200)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,scheduledmonth VARCHAR(14)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,chnl_wt NUMERIC(8,4)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_edw_product_attr_dim;
CREATE OR REPLACE TABLE RG_WKS.WKS_EDW_PRODUCT_ATTR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_edw_product_attr_dim
(
	aw_remote_key VARCHAR(64)  		--//  ENCODE lzo
	,awrefs_prod_remotekey VARCHAR(64)  		--//  ENCODE lzo
	,awrefs_buss_unit VARCHAR(256)  		--//  ENCODE lzo
	,sap_matl_num VARCHAR(18)  		--//  ENCODE lzo
	,cntry VARCHAR(256)  		--//  ENCODE lzo
	,ean VARCHAR(64)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(256)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(256)  		--//  ENCODE lzo
	,lcl_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE rg_wks.wks_hk_inventory_health_analysis_propagation;
CREATE OR REPLACE TABLE RG_WKS.WKS_HK_INVENTORY_HEALTH_ANALYSIS_PROPAGATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_hk_inventory_health_analysis_propagation
(
	cal_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,cal_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cntry_nm VARCHAR(9)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(50)  		--//  ENCODE lzo
	,dstrbtr_grp_cd_name VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_desc VARCHAR(30)  		--//  ENCODE lzo
	,sku_cd VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(100)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,product_key VARCHAR(68)  		--//  ENCODE lzo
	,product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_ccy VARCHAR(3)  		--//  ENCODE lzo
	,to_ccy VARCHAR(3)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sap_prnt_cust_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_prnt_cust_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_cust_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_sub_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_frmt_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_frmt_desc VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(8)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(8)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,so_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls_usd NUMERIC(30,0)  		--//  ENCODE az64
	,si_all_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_3months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_hk_inventory_health_analysis_propagation_final;
CREATE OR REPLACE TABLE RG_WKS.WKS_HK_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_hk_inventory_health_analysis_propagation_final
(
	cal_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_id VARCHAR(23)  		--//  ENCODE lzo
	,cal_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cntry_nm VARCHAR(9)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(50)  		--//  ENCODE lzo
	,dstrbtr_grp_cd_name VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_prod_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(50)  		--//  ENCODE lzo
	,global_prod_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_desc VARCHAR(30)  		--//  ENCODE lzo
	,sku_cd VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(100)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,product_key VARCHAR(68)  		--//  ENCODE lzo
	,product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_ccy VARCHAR(3)  		--//  ENCODE lzo
	,to_ccy VARCHAR(3)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sap_prnt_cust_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_prnt_cust_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_cust_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_sub_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_frmt_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_frmt_desc VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(8)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(8)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,so_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls_usd NUMERIC(30,0)  		--//  ENCODE az64
	,si_all_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_3months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,healthy_inventory VARCHAR(1)  		--//  ENCODE lzo
	,min_date DATE  		--//  ENCODE az64
	,diff_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l12m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l6m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l3m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l12m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l6m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l3m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l6m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l3m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l6m_weeks_avg_sales_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l3m_weeks_avg_sales_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE rg_wks.wks_perfect_store_sos_soa_custid_ind;
CREATE OR REPLACE TABLE RG_WKS.WKS_PERFECT_STORE_SOS_SOA_CUSTID_IND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_perfect_store_sos_soa_custid_ind
(
	hashkey VARCHAR(32)  		--//  ENCODE lzo
	,hash_row numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,dataset VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(255)  		--//  ENCODE lzo
	,visitid VARCHAR(255)  		--//  ENCODE lzo
	,questiontext VARCHAR(513)  		--//  ENCODE lzo
	,productid VARCHAR(255)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,latestdate DATE  		--//  ENCODE az64
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,merchandiser_name VARCHAR(770)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,country VARCHAR(200)  		--//  ENCODE lzo
	,state VARCHAR(256)  		--//  ENCODE lzo
	,parent_customer VARCHAR(382)  		--//  ENCODE lzo
	,retail_environment VARCHAR(382)  		--//  ENCODE lzo
	,channel VARCHAR(382)  		--//  ENCODE lzo
	,retailer VARCHAR(382)  		--//  ENCODE lzo
	,business_unit VARCHAR(200)  		--//  ENCODE lzo
	,eannumber VARCHAR(255)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(2000)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(2258)  		--//  ENCODE lzo
	,ques_type VARCHAR(382)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(150)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(256)  		--//  ENCODE lzo
	,add_info VARCHAR(65535)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,response_score VARCHAR(65535)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,mkt_share DOUBLE PRECISION
	,salience_val NUMERIC(20,4)  		--//  ENCODE az64
	,channel_weightage NUMERIC(31,2)  		--//  ENCODE az64
	,actual_value NUMERIC(14,4)  		--//  ENCODE az64
	,ref_value NUMERIC(22,4)  		--//  ENCODE az64
	,kpi_actual_wt_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_wt_val VARCHAR(150)  		--//  ENCODE lzo
	,photo_url VARCHAR(500)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
)

CLUSTER BY(hashkey)
;
--DROP TABLE rg_wks.wks_perfect_store_sos_soa_mnth;
CREATE OR REPLACE TABLE RG_WKS.WKS_PERFECT_STORE_SOS_SOA_MNTH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_perfect_store_sos_soa_mnth
(
	hashkey VARCHAR(32)  		--//  ENCODE lzo
	,hash_row numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,dataset VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(255)  		--//  ENCODE lzo
	,salespersonid VARCHAR(255)  		--//  ENCODE lzo
	,visitid VARCHAR(255)  		--//  ENCODE lzo
	,questiontext VARCHAR(513)  		--//  ENCODE lzo
	,productid VARCHAR(255)  		--//  ENCODE lzo
	,kpi VARCHAR(573)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,latestdate DATE  		--//  ENCODE az64
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,merchandiser_name VARCHAR(770)  		--//  ENCODE lzo
	,customername VARCHAR(771)  		--//  ENCODE lzo
	,country VARCHAR(200)  		--//  ENCODE lzo
	,state VARCHAR(256)  		--//  ENCODE lzo
	,parent_customer VARCHAR(382)  		--//  ENCODE lzo
	,retail_environment VARCHAR(382)  		--//  ENCODE lzo
	,channel VARCHAR(382)  		--//  ENCODE lzo
	,retailer VARCHAR(382)  		--//  ENCODE lzo
	,business_unit VARCHAR(200)  		--//  ENCODE lzo
	,eannumber VARCHAR(255)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(510)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(2000)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(500)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(2258)  		--//  ENCODE lzo
	,ques_type VARCHAR(382)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(150)  		--//  ENCODE lzo
	,priority_store_flag VARCHAR(256)  		--//  ENCODE lzo
	,add_info VARCHAR(65535)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,response_score VARCHAR(65535)  		--//  ENCODE lzo
	,kpi_chnl_wt DOUBLE PRECISION
	,mkt_share DOUBLE PRECISION
	,salience_val NUMERIC(20,4)  		--//  ENCODE az64
	,channel_weightage NUMERIC(31,2)  		--//  ENCODE az64
	,actual_value NUMERIC(14,4)  		--//  ENCODE az64
	,ref_value NUMERIC(22,4)  		--//  ENCODE az64
	,kpi_actual_wt_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_val VARCHAR(40)  		--//  ENCODE lzo
	,kpi_ref_wt_val VARCHAR(150)  		--//  ENCODE lzo
	,photo_url VARCHAR(500)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
)

CLUSTER BY(hashkey)
;
--DROP TABLE rg_wks.wks_sotp_propagation_hash;
CREATE OR REPLACE TABLE RG_WKS.WKS_SOTP_PROPAGATION_HASH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_sotp_propagation_hash
(
	dataset VARCHAR(49)
	,country VARCHAR(200)
	,parent_customer VARCHAR(255)
	,retail_environment VARCHAR(256)
	,channel VARCHAR(255)
	,prod_hier_l4 VARCHAR(510)
	,fisc_yr numeric(18,0)		--// INTEGER
	,fisc_per numeric(18,0)		--// INTEGER
	,compliance NUMERIC(14,3)  		--//  ENCODE az64
	,hashkey VARCHAR(32)  		--//  ENCODE lzo
	,complaince_max NUMERIC(14,3)  		--//  ENCODE az64
)

CLUSTER BY(hashkey)
		--// SORTKEY ( 
		--// 	dataset
		--// 	, country
		--// 	, parent_customer
		--// 	, retail_environment
		--// 	, channel
		--// 	, prod_hier_l4
		--// 	, fisc_yr
		--// 	, fisc_per
		--// 	)
;		--// ;
--DROP TABLE rg_wks.wks_tw_inventory_health_analysis_propagation;
CREATE OR REPLACE TABLE RG_WKS.WKS_TW_INVENTORY_HEALTH_ANALYSIS_PROPAGATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_tw_inventory_health_analysis_propagation
(
	cal_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_id VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cntry_nm VARCHAR(6)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(12)  		--//  ENCODE lzo
	,dstrbtr_grp_cd_name VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_franchise VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_brand VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_sub_brand VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_subsegment VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_subcategory VARCHAR(2)  		--//  ENCODE lzo
	,global_put_up_desc VARCHAR(30)  		--//  ENCODE lzo
	,sku_cd VARCHAR(100)  		--//  ENCODE lzo
	,sku_description VARCHAR(100)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,product_key VARCHAR(68)  		--//  ENCODE lzo
	,product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_ccy VARCHAR(3)  		--//  ENCODE lzo
	,to_ccy VARCHAR(3)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sap_prnt_cust_key VARCHAR(13)  		--//  ENCODE lzo
	,sap_prnt_cust_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_cust_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_sub_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_frmt_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_frmt_desc VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(6)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(6)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,so_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_3months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE rg_wks.wks_tw_inventory_health_analysis_propagation_final;
CREATE OR REPLACE TABLE RG_WKS.WKS_TW_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE rg_wks.wks_tw_inventory_health_analysis_propagation_final
(
	cal_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_id VARCHAR(11)  		--//  ENCODE lzo
	,cal_mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cntry_nm VARCHAR(6)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(12)  		--//  ENCODE lzo
	,dstrbtr_grp_cd_name VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_franchise VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_brand VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_sub_brand VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_subsegment VARCHAR(2)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_subcategory VARCHAR(2)  		--//  ENCODE lzo
	,global_put_up_desc VARCHAR(30)  		--//  ENCODE lzo
	,sku_cd VARCHAR(100)  		--//  ENCODE lzo
	,sku_description VARCHAR(100)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,product_key VARCHAR(68)  		--//  ENCODE lzo
	,product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_ccy VARCHAR(3)  		--//  ENCODE lzo
	,to_ccy VARCHAR(3)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sap_prnt_cust_key VARCHAR(13)  		--//  ENCODE lzo
	,sap_prnt_cust_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_cust_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_cust_sub_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_chnl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,sap_bnr_frmt_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_bnr_frmt_desc VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(6)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(6)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,so_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls NUMERIC(38,5)  		--//  ENCODE az64
	,so_trd_sls_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_all_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_inv_db_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_3months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,healthy_inventory VARCHAR(1)  		--//  ENCODE lzo
	,min_date DATE  		--//  ENCODE az64
	,diff_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l12m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l6m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l3m_weeks numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,l12m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l6m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l3m_weeks_avg_sales NUMERIC(38,4)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l6m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l3m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_qty NUMERIC(38,4)  		--//  ENCODE az64
	,l6m_weeks_avg_sales_qty NUMERIC(38,4)  		--//  ENCODE az64
	,l3m_weeks_avg_sales_qty NUMERIC(38,4)  		--//  ENCODE az64
)

;

--DROP TABLE cn_edw.edw_cn_perfect_store;
CREATE OR REPLACE TABLE SNAPNTAEDW_INTEGRATION.EDW_CN_PERFECT_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE cn_edw.edw_cn_perfect_store
(
	dataset VARCHAR(22)  		--//  ENCODE lzo
	,customerid VARCHAR(200)  		--//  ENCODE lzo
	,salespersonid VARCHAR(1)  		--//  ENCODE lzo
	,mrch_resp_startdt VARCHAR(1)  		--//  ENCODE lzo
	,mrch_resp_enddt VARCHAR(1)  		--//  ENCODE lzo
	,survey_enddate VARCHAR(1)  		--//  ENCODE lzo
	,questiontext VARCHAR(28)  		--//  ENCODE lzo
	,value VARCHAR(40)  		--//  ENCODE lzo
	,mustcarryitem VARCHAR(4)  		--//  ENCODE lzo
	,presence VARCHAR(5)  		--//  ENCODE lzo
	,outofstock VARCHAR(4)  		--//  ENCODE lzo
	,kpi VARCHAR(20)  		--//  ENCODE lzo
	,scheduleddate DATE  		--//  ENCODE az64
	,vst_status VARCHAR(9)  		--//  ENCODE lzo
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,firstname VARCHAR(100)  		--//  ENCODE lzo
	,lastname VARCHAR(1)  		--//  ENCODE lzo
	,customername VARCHAR(500)  		--//  ENCODE lzo
	,country VARCHAR(5)  		--//  ENCODE lzo
	,state VARCHAR(100)  		--//  ENCODE lzo
	,storereference VARCHAR(100)  		--//  ENCODE lzo
	,storetype VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(2)  		--//  ENCODE lzo
	,salesgroup VARCHAR(100)  		--//  ENCODE lzo
	,bu VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l1 VARCHAR(5)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(30)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(300)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(200)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(100)  		--//  ENCODE lzo
	,productname VARCHAR(100)  		--//  ENCODE lzo
	,eannumber VARCHAR(100)  		--//  ENCODE lzo
	,category VARCHAR(300)  		--//  ENCODE lzo
	,segment VARCHAR(1)  		--//  ENCODE lzo
	,kpi_chnl_wt NUMERIC(38,5)  		--//  ENCODE az64
	,actual VARCHAR(40)  		--//  ENCODE lzo
	,target VARCHAR(40)  		--//  ENCODE lzo
	,mkt_share NUMERIC(20,4)  		--//  ENCODE az64
	,ques_desc VARCHAR(11)  		--//  ENCODE lzo
	,"y/n_flag" VARCHAR(3)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_pacific_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_PACIFIC_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_pacific_ps_targets
(
	market VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,valid_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,valid_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_pacific_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_PACIFIC_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_pacific_ps_weights
(
	market VARCHAR(510)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)
;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_cn_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_CN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_cn_ps_weights
(
	re VARCHAR(20)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,weight NUMERIC(38,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,channel VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_cn_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_CN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_cn_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_id_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_ID_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_id_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_id_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_ID_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_id_ps_weights
(
	channel VARCHAR(20)  		--//  ENCODE zstd
	,weight NUMERIC(38,5)  		--//  ENCODE az64
	,kpi VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_msl;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IN_PERFECTSTORE_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_msl
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(10,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_code VARCHAR(255)  		--//  ENCODE lzo
	,product_name VARCHAR(255)  		--//  ENCODE lzo
	,msl VARCHAR(255)  		--//  ENCODE lzo
	,cost_inr NUMERIC(12,4)  		--//  ENCODE az64
	,quantity NUMERIC(10,0)  		--//  ENCODE az64
	,amount_inr NUMERIC(12,4)  		--//  ENCODE az64
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_paid_display;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IN_PERFECTSTORE_PAID_DISPLAY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_paid_display
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(14,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,asset VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,posm_brand VARCHAR(255)  		--//  ENCODE lzo
	,start_date VARCHAR(255)  		--//  ENCODE lzo
	,end_date VARCHAR(255)  		--//  ENCODE lzo
	,audit_status VARCHAR(255)  		--//  ENCODE lzo
	,is_available VARCHAR(255)  		--//  ENCODE lzo
	,availability_points VARCHAR(255)  		--//  ENCODE lzo
	,visibility_type VARCHAR(255)  		--//  ENCODE lzo
	,visibility_condition VARCHAR(255)  		--//  ENCODE lzo
	,is_planogram_availbale VARCHAR(255)  		--//  ENCODE lzo
	,select_brand VARCHAR(255)  		--//  ENCODE lzo
	,is_correct_brand_displayed VARCHAR(255)  		--//  ENCODE lzo
	,brandavailability_points VARCHAR(255)  		--//  ENCODE lzo
	,stock_status VARCHAR(255)  		--//  ENCODE lzo
	,stock_points VARCHAR(255)  		--//  ENCODE lzo
	,is_near_category VARCHAR(255)  		--//  ENCODE lzo
	,nearcategory_points VARCHAR(255)  		--//  ENCODE lzo
	,audit_score VARCHAR(255)  		--//  ENCODE lzo
	,paid_visibility_score VARCHAR(255)  		--//  ENCODE lzo
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_promo;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IN_PERFECTSTORE_PROMO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_promo
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(10,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_name VARCHAR(255)  		--//  ENCODE lzo
	,ispromotionavailable VARCHAR(255)  		--//  ENCODE lzo
	,photopath VARCHAR(500)  		--//  ENCODE lzo
	,countoffacing NUMERIC(10,0)  		--//  ENCODE az64
	,promotionoffertype VARCHAR(255)  		--//  ENCODE lzo
	,notavailablereason VARCHAR(255)  		--//  ENCODE lzo
	,price_off VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_sos;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IN_PERFECTSTORE_SOS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_in_perfectstore_sos
(
	visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjisp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(14,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,prod_facings NUMERIC(14,0)  		--//  ENCODE az64
	,total_facings NUMERIC(14,0)  		--//  ENCODE az64
	,facing_contribution VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_in_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_IN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_in_ps_targets
(
	kpi VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(200)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(50)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_in_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_IN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_in_ps_weights
(
	kpi VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_udcdetails;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_UDCDETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_udcdetails
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,masterid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,mastername VARCHAR(200)  		--//  ENCODE zstd
	,mastervaluecode VARCHAR(200)
	,mastervaluename VARCHAR(200)  		--//  ENCODE zstd
	,columnname VARCHAR(100)  		--//  ENCODE zstd
	,columnvalue VARCHAR(100)  		--//  ENCODE zstd
	,uploadflag VARCHAR(1)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE delta32k
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	mastervaluecode
		--// 	)
;		--// ;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_jp_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_JP_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_jp_ps_targets
(
	kpi VARCHAR(200)  		--//  ENCODE zstd
	,attribute_1 VARCHAR(255)  		--//  ENCODE zstd
	,attribute_2 VARCHAR(255)  		--//  ENCODE zstd
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_jp_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_JP_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_jp_ps_weights
(
	channel VARCHAR(255)  		--//  ENCODE zstd
	,kpi VARCHAR(255)  		--//  ENCODE zstd
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crted_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_direct_sales_rep_route_plan;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_DIRECT_SALES_REP_ROUTE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_direct_sales_rep_route_plan
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sl_no numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,sls_rep_cd_nm VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE zstd
	,store_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_nm VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,week numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,day VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,effctv_strt_dt DATE NOT NULL 		--//  ENCODE delta
	,effctv_end_dt DATE NOT NULL 		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_gt_msl_items;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_GT_MSL_ITEMS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_gt_msl_items
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,dstr_prod_cd VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,sap_matl_cd VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,prod_desc_eng VARCHAR(100)  		--//  ENCODE zstd
	,prod_desc_chnse VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,msl_flg VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,strt_yr_mnth VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,end_yr_mnth VARCHAR(20)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (ctry_cd, dstr_cd)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_ims;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IMS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_ims
(
	ims_txn_dt DATE  		--//  ENCODE lzo
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,rpt_per_strt_dt DATE  		--//  ENCODE lzo
	,rpt_per_end_dt DATE  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,unit_prc NUMERIC(21,5)  		--//  ENCODE lzo
	,sls_amt NUMERIC(21,5)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,rtrn_qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,rtrn_amt NUMERIC(21,5)  		--//  ENCODE lzo
	,ship_cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cls_grp VARCHAR(20)  		--//  ENCODE lzo
	,cust_sub_cls VARCHAR(20)  		--//  ENCODE lzo
	,prod_spec VARCHAR(50)  		--//  ENCODE lzo
	,itm_agn_nm VARCHAR(100)  		--//  ENCODE lzo
	,ordr_co VARCHAR(20)  		--//  ENCODE lzo
	,rtrn_rsn VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_ofc_nm VARCHAR(20)  		--//  ENCODE lzo
	,sls_grp_nm VARCHAR(20)  		--//  ENCODE lzo
	,acc_type VARCHAR(10)  		--//  ENCODE lzo
	,co_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,doc_dt DATE  		--//  ENCODE lzo
	,doc_type VARCHAR(10)  		--//  ENCODE lzo
	,doc_num VARCHAR(20)  		--//  ENCODE lzo
	,invc_num VARCHAR(15)  		--//  ENCODE lzo
	,remark_desc VARCHAR(100)  		--//  ENCODE lzo
	,gift_qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,sls_bfr_tax_amt NUMERIC(21,5)  		--//  ENCODE lzo
	,sku_per_box NUMERIC(21,5)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_ims_dstr_cust_attr;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IMS_DSTR_CUST_ATTR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_ims_dstr_cust_attr
(
	dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(20)  		--//  ENCODE lzo
	,dstr_cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,dstr_cust_nm VARCHAR(50)  		--//  ENCODE lzo
	,dstr_cust_area VARCHAR(20)  		--//  ENCODE lzo
	,dstr_cust_clsn1 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn2 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn3 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn4 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_cust_clsn5 VARCHAR(100)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,distributor_address VARCHAR(255)  		--//  ENCODE lzo
	,distributor_telephone VARCHAR(255)  		--//  ENCODE lzo
	,distributor_contact VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,hq VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_ims_invnt;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_IMS_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_ims_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(30)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(200)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,invnt_amt NUMERIC(21,5)  		--//  ENCODE az64
	,avg_prc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,safety_stock NUMERIC(21,5)  		--//  ENCODE az64
	,bad_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,book_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,convs_amt NUMERIC(21,5)  		--//  ENCODE az64
	,prch_disc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,end_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,batch_no VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chn_uom VARCHAR(100)  		--//  ENCODE lzo
	,storage_name VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_indirect_sales_rep_route_plan;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_INDIRECT_SALES_REP_ROUTE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_indirect_sales_rep_route_plan
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd_nm VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,store_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_nm VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3)  		--//  ENCODE zstd
	,visit_freq numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,week numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,day VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,effctv_strt_dt DATE  		--//  ENCODE delta
	,effctv_end_dt DATE  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_brand_ranking;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_BRAND_RANKING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_brand_ranking
(
	category_depth1 VARCHAR(30)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(30)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(50)  		--//  ENCODE zstd
	,ranking VARCHAR(4)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,jnj_brand VARCHAR(20)  		--//  ENCODE zstd
	,rank_change VARCHAR(5)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(20)  		--//  ENCODE zstd
	,data_granularity VARCHAR(10)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_customer_brand_trend;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_CUSTOMER_BRAND_TREND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_customer_brand_trend
(
	date_yyyymm VARCHAR(6)  		--//  ENCODE zstd
	,coupang_id VARCHAR(15)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(30)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,new_user_count VARCHAR(10)  		--//  ENCODE zstd
	,curr_user_count VARCHAR(10)  		--//  ENCODE zstd
	,tot_user_count VARCHAR(10)  		--//  ENCODE zstd
	,new_user_sales_amt NUMERIC(15,2)  		--//  ENCODE az64
	,curr_user_sales_amt NUMERIC(15,2)  		--//  ENCODE az64
	,new_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
	,curr_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
	,tot_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(20)  		--//  ENCODE zstd
	,data_granularity VARCHAR(10)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_daily_brand_reviews;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_DAILY_BRAND_REVIEWS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_daily_brand_reviews
(
	review_date VARCHAR(10)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,coupang_id VARCHAR(15)  		--//  ENCODE zstd
	,coupang_product_name VARCHAR(200)  		--//  ENCODE zstd
	,review_score_star VARCHAR(1)  		--//  ENCODE zstd
	,review_contents VARCHAR(65535)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(20)  		--//  ENCODE zstd
	,data_granularity VARCHAR(10)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_master
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_daily;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_RANKING_DAILY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_daily
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,data_granularity VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_monthly;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_RANKING_MONTHLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_monthly
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,data_granularity VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_weekly;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_RANKING_WEEKLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_ranking_weekly
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,data_granularity VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_summary_monthly;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_SUMMARY_MONTHLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_summary_monthly
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_summary_weekly;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCT_SUMMARY_WEEKLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_product_summary_weekly
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_productsalereport;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PRODUCTSALEREPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_productsalereport
(
	country_cd VARCHAR(255)  		--//  ENCODE lzo
	,transaction_date DATE  		--//  ENCODE az64
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_people VARCHAR(255)  		--//  ENCODE lzo
	,vendor_item_id VARCHAR(255)  		--//  ENCODE lzo
	,vendor_item VARCHAR(255)  		--//  ENCODE lzo
	,product_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,product_category_high VARCHAR(255)  		--//  ENCODE lzo
	,product_category_mid VARCHAR(255)  		--//  ENCODE lzo
	,product_category_low VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,sales_gmv NUMERIC(18,6)  		--//  ENCODE az64
	,cost_of_purchase NUMERIC(18,6)  		--//  ENCODE az64
	,units_sold NUMERIC(18,0)  		--//  ENCODE az64
	,shipping_sales_gmv VARCHAR(255)  		--//  ENCODE lzo
	,shipping_weight_percent VARCHAR(255)  		--//  ENCODE lzo
	,sns_cogs VARCHAR(255)  		--//  ENCODE lzo
	,sns_units_sold VARCHAR(255)  		--//  ENCODE lzo
	,no_of_product_reviews VARCHAR(255)  		--//  ENCODE lzo
	,avg_product_rating VARCHAR(255)  		--//  ENCODE lzo
	,transaction_currency VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_search_keyword_by_category;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_SEARCH_KEYWORD_BY_CATEGORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_search_keyword_by_category
(
	by_search_keyword VARCHAR(255)  		--//  ENCODE zstd
	,by_product_ranking VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,search_keyword VARCHAR(2000)  		--//  ENCODE zstd
	,product_ranking VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(2000)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,data_granularity VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_search_keyword_by_product;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_SEARCH_KEYWORD_BY_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_search_keyword_by_product
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(2000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,search_keyword VARCHAR(255)  		--//  ENCODE zstd
	,click_rate NUMERIC(18,5)  		--//  ENCODE az64
	,cart_transition_rate NUMERIC(18,5)  		--//  ENCODE az64
	,purchase_conversion_rate NUMERIC(18,5)  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,data_granularity VARCHAR(255)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecom_dstr_inventory;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOM_DSTR_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecom_dstr_inventory
(
	dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,matl_num VARCHAR(30)  		--//  ENCODE lzo
	,ean VARCHAR(30)  		--//  ENCODE lzo
	,brand_name VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(500)  		--//  ENCODE zstd
	,inv_date DATE  		--//  ENCODE az64
	,inventory_qty NUMERIC(38,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecom_dstr_sellout;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOM_DSTR_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecom_dstr_sellout
(
	dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,matl_num VARCHAR(30)  		--//  ENCODE lzo
	,ean VARCHAR(30)  		--//  ENCODE lzo
	,brand_name VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(500)  		--//  ENCODE zstd
	,so_date DATE  		--//  ENCODE az64
	,so_qty NUMERIC(38,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_coupang_transaction;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOMMERCE_OFFTAKE_COUPANG_TRANSACTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_coupang_transaction
(
	load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,product_id VARCHAR(20)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(20)  		--//  ENCODE lzo
	,vendor_item_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_name VARCHAR(20000)  		--//  ENCODE lzo
	,vendor_item_name VARCHAR(20000)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,sales_value DOUBLE PRECISION
	,regular_shipping_sales_sns_gmv DOUBLE PRECISION
	,regular_shipping_weight DOUBLE PRECISION
	,purchase_cost_cogs DOUBLE PRECISION
	,periodic_shipping_cost_sns_cogs DOUBLE PRECISION
	,quantity DOUBLE PRECISION
	,regular_shippment_sns_units_sold DOUBLE PRECISION
	,reviews DOUBLE PRECISION
	,average_product_rating DOUBLE PRECISION
	,active_customers numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,new_customers numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,transaction_currency VARCHAR(10)  		--//  ENCODE lzo
	,country VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_product_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOMMERCE_OFFTAKE_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_product_master
(
	load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
	,retailer_sku_code VARCHAR(100)  		--//  ENCODE lzo
	,jnj_sku_code VARCHAR(255)  		--//  ENCODE lzo
	,retailer_type VARCHAR(255)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,retailer_barcode VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,retailer_sku_name VARCHAR(2000)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_sales_ebay;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOMMERCE_OFFTAKE_SALES_EBAY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_offtake_sales_ebay
(
	load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(20)  		--//  ENCODE lzo
	,no_of_units_sold numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,order_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,sales_value DOUBLE PRECISION
	,option VARCHAR(20)  		--//  ENCODE lzo
	,quantity numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,retailer_code VARCHAR(2000)  		--//  ENCODE lzo
	,retailer_name VARCHAR(2000)  		--//  ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,order_number VARCHAR(255)  		--//  ENCODE lzo
	,product_code VARCHAR(255)  		--//  ENCODE lzo
	,price_per_package DOUBLE PRECISION
	,product_title VARCHAR(2000)  		--//  ENCODE lzo
	,affiliates VARCHAR(255)  		--//  ENCODE lzo
	,delivery_cost DOUBLE PRECISION
	,payment_number VARCHAR(2000)  		--//  ENCODE lzo
	,transaction_currency VARCHAR(10)  		--//  ENCODE lzo
	,country VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_sellout;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_ECOMMERCE_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_ecommerce_sellout
(
	customer_name VARCHAR(100)  		--//  ENCODE lzo
	,customer_code VARCHAR(20)  		--//  ENCODE lzo
	,sub_customer_name VARCHAR(100)  		--//  ENCODE lzo
	,ean_number VARCHAR(20)  		--//  ENCODE lzo
	,sap_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_type VARCHAR(20)  		--//  ENCODE lzo
	,brand_categories VARCHAR(255)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,year VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,week VARCHAR(20)  		--//  ENCODE lzo
	,transaction_date DATE  		--//  ENCODE delta
	,sellout_qty DOUBLE PRECISION
	,sellout_amount DOUBLE PRECISION
	,sold_to VARCHAR(255)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_daiso_price;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_GT_DAISO_PRICE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_daiso_price
(
	cntry_cd VARCHAR(2)  		--//  ENCODE zstd
	,dstr_nm VARCHAR(30)  		--//  ENCODE zstd
	,ean VARCHAR(50)  		--//  ENCODE zstd
	,unit_price VARCHAR(50)  		--//  ENCODE zstd
	,created_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_dpt_daiso;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_GT_DPT_DAISO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_dpt_daiso
(
	cntry_cd VARCHAR(2)  		--//  ENCODE zstd
	,sub_customer_name VARCHAR(100)  		--//  ENCODE zstd
	,sub_customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,created_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_food_ws;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_GT_FOOD_WS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_food_ws
(
	cntry_cd VARCHAR(2)  		--//  ENCODE zstd
	,sales_grp VARCHAR(20)  		--//  ENCODE zstd
	,sub_customer_name VARCHAR(100)  		--//  ENCODE zstd
	,sub_customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,created_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_nacf_cust_dim;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_GT_NACF_CUST_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_nacf_cust_dim
(
	cntry_cd VARCHAR(2)  		--//  ENCODE zstd
	,dstr_nm VARCHAR(30)  		--//  ENCODE zstd
	,nacf_customer_code VARCHAR(50)  		--//  ENCODE zstd
	,sap_customer_code VARCHAR(50)  		--//  ENCODE zstd
	,created_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_sellout;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_GT_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_gt_sellout
(
	ims_txn_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(255)  		--//  ENCODE lzo
	,prod_nm VARCHAR(255)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,unit_prc NUMERIC(21,5)  		--//  ENCODE az64
	,sls_amt NUMERIC(21,5)  		--//  ENCODE az64
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sub_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,sub_customer_name VARCHAR(100)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,sales_priority VARCHAR(10)  		--//  ENCODE lzo
	,sales_stores NUMERIC(21,5)  		--//  ENCODE az64
	,sales_rate NUMERIC(21,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_jnj_price_guide;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_JNJ_PRICE_GUIDE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_jnj_price_guide
(
	prod_desc VARCHAR(255)  		--//  ENCODE lzo
	,qty_of_bundle VARCHAR(20)  		--//  ENCODE lzo
	,prod_desc_and_qty_of_bundle VARCHAR(500)  		--//  ENCODE lzo
	,jnj_price_guide_line DOUBLE PRECISION
	,from_date DATE  		--//  ENCODE az64
	,to_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_otc_inventory;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_OTC_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_otc_inventory
(
	mnth_id VARCHAR(20)  		--//  ENCODE zstd
	,matl_num VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,distributor_cd VARCHAR(50)  		--//  ENCODE zstd
	,unit_price NUMERIC(20,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(20,4)  		--//  ENCODE az64
	,inv_amt NUMERIC(20,4)  		--//  ENCODE az64
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_price_tracker;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_PRICE_TRACKER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_price_tracker
(
	option VARCHAR(20)  		--//  ENCODE lzo
	,id__by_product_page VARCHAR(255)  		--//  ENCODE lzo
	,id_by_option_on_page VARCHAR(255)  		--//  ENCODE lzo
	,data_collection_date VARCHAR(8)  		--//  ENCODE lzo
	,data_collection_day VARCHAR(20)  		--//  ENCODE lzo
	,product_description_on_the_website VARCHAR(255)  		--//  ENCODE lzo
	,option_description VARCHAR(255)  		--//  ENCODE lzo
	,product_url VARCHAR(2000)  		--//  ENCODE lzo
	,bundle_or_not VARCHAR(20)  		--//  ENCODE lzo
	,bundle_type VARCHAR(255)  		--//  ENCODE lzo
	,total_volume_1 DOUBLE PRECISION
	,gift VARCHAR(1)  		--//  ENCODE lzo
	,price DOUBLE PRECISION
	,option_price DOUBLE PRECISION
	,final_selling_price__including_shipping_fee DOUBLE PRECISION
	,shipping_fee DOUBLE PRECISION
	,shipping_fee_included_or_not VARCHAR(5)  		--//  ENCODE lzo
	,sell_out_qty DOUBLE PRECISION
	,total_sales_price DOUBLE PRECISION
	,promotional_site VARCHAR(255)  		--//  ENCODE lzo
	,customer_rating VARCHAR(20)  		--//  ENCODE lzo
	,count_of_product_review VARCHAR(20)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,manufacturer__including_competitors VARCHAR(255)  		--//  ENCODE lzo
	,brand_name VARCHAR(255)  		--//  ENCODE lzo
	,product_description VARCHAR(255)  		--//  ENCODE lzo
	,qty_of_bundle VARCHAR(255)  		--//  ENCODE lzo
	,volume_per_unit_qty VARCHAR(20)  		--//  ENCODE lzo
	,total_volume_2 VARCHAR(20)  		--//  ENCODE lzo
	,price_per_ml DOUBLE PRECISION
	,price_per_qty DOUBLE PRECISION
	,recommended_consumer_price DOUBLE PRECISION
	,selling_price_relative_to_the_regular_price DOUBLE PRECISION
	,total_sales_qty DOUBLE PRECISION
	,type_of_ecom__open_market__social_commerce VARCHAR(20)  		--//  ENCODE lzo
	,ecom_mall_name VARCHAR(255)  		--//  ENCODE lzo
	,name_of_seller VARCHAR(255)  		--//  ENCODE lzo
	,name_of_representative VARCHAR(255)  		--//  ENCODE lzo
	,name_of_seller_s_company VARCHAR(255)  		--//  ENCODE lzo
	,seller_type__black_seller___official_seller VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_store_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_STORE_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_store_map
(
	sls_ofc VARCHAR(50)  		--//  ENCODE lzo
	,sls_ofc_desc VARCHAR(200)  		--//  ENCODE lzo
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,store_type VARCHAR(200)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(200)  		--//  ENCODE lzo
	,sales_group_nm VARCHAR(200)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_brand;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_TARGET_AM_BRAND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_brand
(
	brnd VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(30)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,jan_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,feb_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,mar_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,apr_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,may_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jun_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jul_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,aug_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,sep_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,oct_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,nov_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,dec_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_cust_link;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_TARGET_AM_CUST_LINK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_cust_link
(
	cust_num VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(50)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_sls_grp;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_TARGET_AM_SLS_GRP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_target_am_sls_grp
(
	sls_grp VARCHAR(30)  		--//  ENCODE lzo
	,brnd VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(2000)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,jan_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,feb_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,mar_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,apr_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,may_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jun_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jul_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,aug_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,sep_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,oct_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,nov_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,dec_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_tgt;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_TGT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_tgt
(
	ctry_cd VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,crncy_cd VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,sls_ofc_cd VARCHAR(4) NOT NULL 		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,channel VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,store_type VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,sls_grp_cd VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,sls_grp VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,target_type VARCHAR(4) NOT NULL 		--//  ENCODE zstd
	,prod_hier_l2 VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,fisc_yr numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,fisc_yr_per numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,target_amt NUMERIC(20,5)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (sls_ofc_cd, sls_grp_cd, target_type)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sfmc_naver_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SFMC_NAVER_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sfmc_naver_data
(
	cntry_cd VARCHAR(100)  		--//  ENCODE zstd
	,naver_id VARCHAR(50)  		--//  ENCODE zstd
	,birth_year VARCHAR(4)  		--//  ENCODE zstd
	,gender VARCHAR(50)  		--//  ENCODE zstd
	,total_purchase_amount VARCHAR(50)  		--//  ENCODE zstd
	,total_number_of_purchases VARCHAR(50)  		--//  ENCODE zstd
	,membership_grade_level VARCHAR(50)  		--//  ENCODE zstd
	,marketing_message_viewing_receiving VARCHAR(50)  		--//  ENCODE zstd
	,coupon_usage_issuance VARCHAR(50)  		--//  ENCODE zstd
	,number_of_reviews VARCHAR(50)  		--//  ENCODE zstd
	,number_of_comments VARCHAR(50)  		--//  ENCODE zstd
	,number_of_attendances VARCHAR(50)  		--//  ENCODE zstd
	,opt_in_for_jnj_communication VARCHAR(50)  		--//  ENCODE zstd
	,notification_subscription VARCHAR(50)  		--//  ENCODE zstd
	,updated_date VARCHAR(50)  		--//  ENCODE zstd
	,membership_registration_date VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sfmc_naver_data_additional;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SFMC_NAVER_DATA_ADDITIONAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sfmc_naver_data_additional
(
	cntry_cd VARCHAR(100)  		--//  ENCODE zstd
	,naver_id VARCHAR(100)  		--//  ENCODE zstd
	,attribute_name VARCHAR(100)  		--//  ENCODE zstd
	,attribute_value VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_tp_tracker_target;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_TP_TRACKER_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_tp_tracker_target
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,country_name VARCHAR(30)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(10)  		--//  ENCODE zstd
	,channel VARCHAR(50)  		--//  ENCODE zstd
	,store_type VARCHAR(100)  		--//  ENCODE zstd
	,sales_group_cd VARCHAR(10)  		--//  ENCODE zstd
	,sales_group_name VARCHAR(100)  		--//  ENCODE zstd
	,target_type VARCHAR(10)  		--//  ENCODE zstd
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,tgt_date DATE  		--//  ENCODE az64
	,ytd_target_fy NUMERIC(18,6)  		--//  ENCODE az64
	,tgt_value NUMERIC(18,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,brand VARCHAR(200)  		--//  ENCODE zstd
	,target_category_code VARCHAR(100)  		--//  ENCODE zstd
	,target_category VARCHAR(100)  		--//  ENCODE zstd
	,target_amt NUMERIC(31,3)  		--//  ENCODE az64
	,ytd_target_amt NUMERIC(31,3)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_trade_promotion;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_TRADE_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_trade_promotion
(
	ctry_cd VARCHAR(2)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(3)  		--//  ENCODE zstd
	,customer_code VARCHAR(20)  		--//  ENCODE zstd
	,activity_name VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,line_begin_date DATE  		--//  ENCODE delta
	,line_end_date DATE  		--//  ENCODE delta
	,or_tp_qty NUMERIC(38,5)  		--//  ENCODE delta
	,or_tp_rebate_a NUMERIC(38,5)  		--//  ENCODE delta
	,ttl_cost NUMERIC(38,5)  		--//  ENCODE delta
	,remark VARCHAR(500)  		--//  ENCODE zstd
	,sap_sgrp VARCHAR(10)  		--//  ENCODE zstd
	,or_tp_rebate NUMERIC(38,5)  		--//  ENCODE delta
	,application_code VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_customer_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_CUSTOMER_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_customer_hierarchy
(
	customer_code VARCHAR(500)  		--//  ENCODE lzo
	,customer_name VARCHAR(500)  		--//  ENCODE lzo
	,parent_customer VARCHAR(200)  		--//  ENCODE lzo
	,local_customer_segmentation_1 VARCHAR(500)  		--//  ENCODE lzo
	,local_customer_segmentation_2 VARCHAR(500)  		--//  ENCODE lzo
	,channel1 VARCHAR(500)  		--//  ENCODE lzo
	,channel2 VARCHAR(500)  		--//  ENCODE lzo
	,channel3 VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_le_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_LE_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_le_targets
(
	year VARCHAR(30)  		--//  ENCODE lzo
	,month_no VARCHAR(30)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,channel_type_nm VARCHAR(255)  		--//  ENCODE lzo
	,brand_cd VARCHAR(50)  		--//  ENCODE lzo
	,brand_nm VARCHAR(255)  		--//  ENCODE lzo
	,target_type_cd VARCHAR(50)  		--//  ENCODE lzo
	,metric_cd VARCHAR(30)  		--//  ENCODE lzo
	,metric_nm VARCHAR(255)  		--//  ENCODE lzo
	,target_value NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_pos_product_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_POS_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_pos_product_mapping
(
	name VARCHAR(500)  		--//  ENCODE lzo
	,jnj_sku_code VARCHAR(200)  		--//  ENCODE lzo
	,age_group_name VARCHAR(500)  		--//  ENCODE lzo
	,category_name VARCHAR(500)  		--//  ENCODE lzo
	,prod_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(500)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_pos_promo_calendar;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_POS_PROMO_CALENDAR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_pos_promo_calendar
(
	calendardate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,promoyear NUMERIC(31,0)  		--//  ENCODE az64
	,promomonth NUMERIC(31,0)  		--//  ENCODE az64
	,promoweekday NUMERIC(31,0)  		--//  ENCODE az64
	,promomonthweeknumber NUMERIC(31,0)  		--//  ENCODE az64
	,promoweeknumber NUMERIC(31,0)  		--//  ENCODE az64
	,customer_name VARCHAR(500)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_product_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_PRODUCT_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_product_hierarchy
(
	sap_brand VARCHAR(200)  		--//  ENCODE lzo
	,sap_base_product VARCHAR(200)  		--//  ENCODE lzo
	,hk_brand_code VARCHAR(500)  		--//  ENCODE lzo
	,hk_base_product_code VARCHAR(500)  		--//  ENCODE lzo
	,hk_base_product_name VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ref_pos_accounts;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_HK_REF_POS_ACCOUNTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_hk_ref_pos_accounts
(
	name VARCHAR(500)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_brand_campaign_promotion;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_BRAND_CAMPAIGN_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_brand_campaign_promotion
(
	code VARCHAR(500)  		--//  ENCODE lzo
	,brand_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_name VARCHAR(500)  		--//  ENCODE lzo
	,brand_id VARCHAR(500)  		--//  ENCODE lzo
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_cost_of_goods;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_COST_OF_GOODS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_cost_of_goods
(
	sap_code VARCHAR(200)  		--//  ENCODE zstd
	,sku_name VARCHAR(510)  		--//  ENCODE zstd
	,free_good_value NUMERIC(31,3)  		--//  ENCODE az64
	,pre_apsc_cogs NUMERIC(31,3)  		--//  ENCODE az64
	,package_cost NUMERIC(31,3)  		--//  ENCODE az64
	,labour_cost NUMERIC(31,3)  		--//  ENCODE az64
	,attribute_value NUMERIC(31,3)  		--//  ENCODE az64
	,valid_from DATE  		--//  ENCODE az64
	,valid_to DATE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_keyword_classifications;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_KEYWORD_CLASSIFICATIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_keyword_classifications
(
	code VARCHAR(500)  		--//  ENCODE lzo
	,keyword VARCHAR(200)  		--//  ENCODE lzo
	,keyword_group VARCHAR(200)  		--//  ENCODE lzo
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_naver_product_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_NAVER_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_naver_product_master
(
	code VARCHAR(100)  		--//  ENCODE zstd
	,category_l_name VARCHAR(100)  		--//  ENCODE zstd
	,category_m_name VARCHAR(100)  		--//  ENCODE zstd
	,category_s_name VARCHAR(100)  		--//  ENCODE zstd
	,brands_name VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(1000)  		--//  ENCODE zstd
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,refresh_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_product_ean_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_PRODUCT_EAN_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_product_ean_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE lzo
	,vendoritemid NUMERIC(31,0)  		--//  ENCODE az64
	,skuid NUMERIC(31,0)  		--//  ENCODE az64
	,ean VARCHAR(200)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_product_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_PRODUCT_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_product_hierarchy
(
	prft_ctr VARCHAR(500)  		--//  ENCODE lzo
	,local_brand_classification VARCHAR(200)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_sub_customer_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_KR_SUB_CUSTOMER_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_kr_sub_customer_master
(
	code VARCHAR(500)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,sap_customer_code VARCHAR(200)  		--//  ENCODE lzo
	,retailer VARCHAR(200)  		--//  ENCODE lzo
	,outlet_code VARCHAR(200)  		--//  ENCODE lzo
	,pharmacy_name VARCHAR(200)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_log;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_LOG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_log
(
	cntry_cd VARCHAR(10)  		--//  ENCODE lzo
	,table_name VARCHAR(255)  		--//  ENCODE lzo
	,result VARCHAR(1000)  		--//  ENCODE lzo
	,status VARCHAR(50)  		--//  ENCODE lzo
	,rec_count numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crtd_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_CUSTOMER_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_hierarchy
(
	customer_strategic_typeb VARCHAR(500)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,local_customer_segmentation_1 VARCHAR(500)  		--//  ENCODE lzo
	,local_customer_segmentation_2 VARCHAR(500)  		--//  ENCODE lzo
	,channel1 VARCHAR(500)  		--//  ENCODE lzo
	,sales_office_code VARCHAR(500)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_sales_rep_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_CUSTOMER_SALES_REP_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_sales_rep_map
(
	year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,month numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,month_name VARCHAR(200)  		--//  ENCODE zstd
	,ec_code VARCHAR(200)  		--//  ENCODE zstd
	,ec_name VARCHAR(200)  		--//  ENCODE zstd
	,offtake_inc VARCHAR(200)  		--//  ENCODE zstd
	,offtake_inc_name VARCHAR(200)  		--//  ENCODE zstd
	,customer_code VARCHAR(200)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,customer_c_name VARCHAR(200)  		--//  ENCODE zstd
	,customer_s_name VARCHAR(200)  		--//  ENCODE zstd
	,psr_code01 VARCHAR(200)  		--//  ENCODE zstd
	,psr_name01 VARCHAR(200)  		--//  ENCODE zstd
	,psr_code02 VARCHAR(200)  		--//  ENCODE zstd
	,psr_name02 VARCHAR(200)  		--//  ENCODE zstd
	,psr_code03 VARCHAR(200)  		--//  ENCODE zstd
	,psr_name03 VARCHAR(200)  		--//  ENCODE zstd
	,psr_code04 VARCHAR(200)  		--//  ENCODE zstd
	,psr_name04 VARCHAR(200)  		--//  ENCODE zstd
	,psr_code05 VARCHAR(200)  		--//  ENCODE zstd
	,psr_name05 VARCHAR(200)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_sales_rep_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_CUSTOMER_SALES_REP_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_customer_sales_rep_mapping
(
	year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,month numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,customer_code VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(255)  		--//  ENCODE zstd
	,customers_name VARCHAR(255)  		--//  ENCODE zstd
	,psr_code VARCHAR(100)  		--//  ENCODE zstd
	,psr_name VARCHAR(255)  		--//  ENCODE zstd
	,ec_code VARCHAR(255)  		--//  ENCODE zstd
	,offtake_inc VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_incentive_schemes;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_INCENTIVE_SCHEMES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_incentive_schemes
(
	incentive_type VARCHAR(100)  		--//  ENCODE zstd
	,begin NUMERIC(28,4)  		--//  ENCODE az64
	,end NUMERIC(28,4)  		--//  ENCODE az64
	,nts_si NUMERIC(28,4)  		--//  ENCODE az64
	,offtake_si NUMERIC(28,4)  		--//  ENCODE az64
	,tp_si NUMERIC(28,4)  		--//  ENCODE az64
	,ciw_si NUMERIC(28,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sales_representative;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_SALES_REPRESENTATIVE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sales_representative
(
	psr_code VARCHAR(500)  		--//  ENCODE zstd
	,psr_name VARCHAR(500)  		--//  ENCODE zstd
	,report_to VARCHAR(500)  		--//  ENCODE zstd
	,reportto_name VARCHAR(500)  		--//  ENCODE zstd
	,reverse VARCHAR(500)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_channel;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_SFMC_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_channel
(
	channel_raw VARCHAR(200)  		--//  ENCODE lzo
	,channel_standard VARCHAR(200)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_gender;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_SFMC_GENDER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_gender
(
	gender_raw VARCHAR(100)  		--//  ENCODE lzo
	,gender_standard VARCHAR(100)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_member_tier;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TW_SFMC_MEMBER_TIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_tw_sfmc_member_tier
(
	tier_raw VARCHAR(100)  		--//  ENCODE lzo
	,tier_standard VARCHAR(100)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_onpck_trgt;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_ONPCK_TRGT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_onpck_trgt
(
	matl_num VARCHAR(50) NOT NULL
	,matl_desc VARCHAR(1000)
	,acct_grp VARCHAR(100) NOT NULL
	,mo VARCHAR(10) NOT NULL
	,yr VARCHAR(10) NOT NULL
	,trgt_qty NUMERIC(22,2)
	,ctry_cd VARCHAR(20) NOT NULL
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (matl_num, acct_grp, mo, yr, ctry_cd)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_photo_mgmnt_url;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_PHOTO_MGMNT_URL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_photo_mgmnt_url
(
	original_photo_key VARCHAR(1031)  		--//  ENCODE zstd
	,original_response VARCHAR(65535)  		--//  ENCODE zstd
	,photo_key VARCHAR(1053)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,url_cnt numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,create_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upload_photo_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos
(
	pos_dt DATE  		--//  ENCODE zstd
	,vend_cd VARCHAR(40)  		--//  ENCODE zstd
	,vend_nm VARCHAR(100)  		--//  ENCODE zstd
	,prod_nm VARCHAR(100)  		--//  ENCODE zstd
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE zstd
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE zstd
	,brnd_nm VARCHAR(40)  		--//  ENCODE zstd
	,ean_num VARCHAR(100)  		--//  ENCODE zstd
	,str_cd VARCHAR(40)  		--//  ENCODE zstd
	,str_nm VARCHAR(100)  		--//  ENCODE zstd
	,sls_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE zstd
	,cum_ship_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,invnt_dt DATE  		--//  ENCODE zstd
	,serial_num VARCHAR(40)  		--//  ENCODE zstd
	,prod_delv_type VARCHAR(40)  		--//  ENCODE zstd
	,prod_type VARCHAR(40)  		--//  ENCODE zstd
	,dept_cd VARCHAR(40)  		--//  ENCODE zstd
	,dept_nm VARCHAR(100)  		--//  ENCODE zstd
	,spec_1_desc VARCHAR(100)  		--//  ENCODE zstd
	,spec_2_desc VARCHAR(100)  		--//  ENCODE zstd
	,cat_big VARCHAR(100)  		--//  ENCODE zstd
	,cat_mid VARCHAR(40)  		--//  ENCODE zstd
	,cat_small VARCHAR(40)  		--//  ENCODE zstd
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE zstd
	,cust_dtls VARCHAR(100)  		--//  ENCODE zstd
	,dist_cd VARCHAR(40)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(10)  		--//  ENCODE zstd
	,src_txn_sts VARCHAR(40)  		--//  ENCODE zstd
	,src_seq_num numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,src_sys_cd VARCHAR(30)  		--//  ENCODE zstd
	,ctry_cd VARCHAR(10)  		--//  ENCODE zstd
	,src_mesg_no VARCHAR(35)  		--//  ENCODE zstd
	,src_mesg_code VARCHAR(3)  		--//  ENCODE zstd
	,src_mesg_func_code VARCHAR(3)  		--//  ENCODE zstd
	,src_mesg_date DATE  		--//  ENCODE zstd
	,src_sale_date_form VARCHAR(3)  		--//  ENCODE zstd
	,src_send_code VARCHAR(10)  		--//  ENCODE zstd
	,src_send_ean_code VARCHAR(13)  		--//  ENCODE zstd
	,src_send_name VARCHAR(30)  		--//  ENCODE zstd
	,src_recv_qual VARCHAR(13)  		--//  ENCODE zstd
	,src_recv_ean_code VARCHAR(10)  		--//  ENCODE zstd
	,src_recv_name VARCHAR(35)  		--//  ENCODE zstd
	,src_part_qual VARCHAR(3)  		--//  ENCODE zstd
	,src_part_ean_code VARCHAR(13)  		--//  ENCODE zstd
	,src_part_id VARCHAR(10)  		--//  ENCODE zstd
	,src_part_name VARCHAR(30)  		--//  ENCODE zstd
	,src_sender_id VARCHAR(35)  		--//  ENCODE zstd
	,src_recv_date VARCHAR(10)  		--//  ENCODE zstd
	,src_recv_time VARCHAR(6)  		--//  ENCODE zstd
	,src_file_size NUMERIC(8,0)  		--//  ENCODE zstd
	,src_file_path VARCHAR(128)  		--//  ENCODE zstd
	,src_lega_tran VARCHAR(1)  		--//  ENCODE zstd
	,src_regi_date VARCHAR(10)  		--//  ENCODE zstd
	,src_line_no NUMERIC(6,0)  		--//  ENCODE zstd
	,src_instore_code VARCHAR(20)  		--//  ENCODE zstd
	,src_mnth_sale_amnt NUMERIC(15,0)  		--//  ENCODE zstd
	,src_qty_unit VARCHAR(3)  		--//  ENCODE zstd
	,src_mnth_sale_qty NUMERIC(10,0)  		--//  ENCODE zstd
	,unit_of_pkg_sales VARCHAR(5)  		--//  ENCODE zstd
	,doc_send_date DATE  		--//  ENCODE zstd
	,unit_of_pkg_invt VARCHAR(5)  		--//  ENCODE zstd
	,doc_fun VARCHAR(6)  		--//  ENCODE zstd
	,doc_no VARCHAR(40)  		--//  ENCODE zstd
	,doc_fun_cd VARCHAR(6)  		--//  ENCODE zstd
	,buye_loc_cd VARCHAR(40)  		--//  ENCODE zstd
	,vend_loc_cd VARCHAR(40)  		--//  ENCODE zstd
	,provider_loc_cd VARCHAR(40)  		--//  ENCODE zstd
	,comp_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,unit_of_pkg_comp VARCHAR(5)  		--//  ENCODE zstd
	,order_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,unit_of_pkg_order VARCHAR(5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_cust_prod_cd_ean_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_CUST_PROD_CD_EAN_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_cust_prod_cd_ean_map
(
	cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_hier_cd VARCHAR(100)  		--//  ENCODE lzo
	,cust_prod_cd VARCHAR(100)  		--//  ENCODE lzo
	,barcd VARCHAR(100)  		--//  ENCODE lzo
	,sap_prod_cd VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_cust_prod_to_sap_prod_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_CUST_PROD_TO_SAP_PROD_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_cust_prod_to_sap_prod_map
(
	matl VARCHAR(50)  		--//  ENCODE lzo
	,matl_desc VARCHAR(200)  		--//  ENCODE lzo
	,cust_matl_no VARCHAR(50)  		--//  ENCODE lzo
	,m_b VARCHAR(20)  		--//  ENCODE lzo
	,brnd VARCHAR(20)  		--//  ENCODE lzo
	,bp VARCHAR(20)  		--//  ENCODE lzo
	,vrnt VARCHAR(20)  		--//  ENCODE lzo
	,ptup VARCHAR(20)  		--//  ENCODE lzo
	,mtyp VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,customer_code VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_invnt;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_invnt
(
	invnt_dt DATE  		--//  ENCODE lzo
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,per_box_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,cust_invnt_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,box_invnt_qty NUMERIC(16,5)  		--//  ENCODE lzo
	,wk_hold_sls NUMERIC(16,5)  		--//  ENCODE lzo
	,wk_hold NUMERIC(16,5)  		--//  ENCODE lzo
	,fst_recv_dt VARCHAR(10)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(10)  		--//  ENCODE lzo
	,dc VARCHAR(40)  		--//  ENCODE lzo
	,stk_cls VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_invoice_prc_lookup;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_INVOICE_PRC_LOOKUP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_invoice_prc_lookup
(
	pos_dt DATE NOT NULL 		--//  ENCODE lzo
	,ean_num VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,sold_to_party VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,calc_invoice_price NUMERIC(16,5)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,wave numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(18)  		--//  ENCODE lzo
	,pricing_sold_to VARCHAR(100)  		--//  ENCODE lzo
	,PRIMARY KEY (pos_dt, ean_num, sold_to_party)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_prc_condition_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_PRC_CONDITION_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_prc_condition_map
(
	sls_org VARCHAR(25)  		--//  ENCODE zstd
	,sales_grp_cd VARCHAR(18) NOT NULL 		--//  ENCODE bytedict
	,cnd_type VARCHAR(25)  		--//  ENCODE zstd
	,matl_num VARCHAR(40)  		--//  ENCODE zstd
	,sold_to_cust_cd VARCHAR(100)  		--//  ENCODE zstd
	,price numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,vld_frm DATE  		--//  ENCODE bytedict
	,vld_to DATE  		--//  ENCODE bytedict
	,cond_curr VARCHAR(15)  		--//  ENCODE zstd
	,doc_currcy VARCHAR(15)  		--//  ENCODE zstd
	,recordmode CHAR(1)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(25)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_prom_prc_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_PROM_PRC_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_prom_prc_map
(
	cust VARCHAR(20)  		--//  ENCODE lzo
	,barcd VARCHAR(20)  		--//  ENCODE lzo
	,cust_prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prom_prc NUMERIC(30,4)  		--//  ENCODE lzo
	,prom_strt_dt DATE  		--//  ENCODE lzo
	,prom_end_dt DATE  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_store_product;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_STORE_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_store_product
(
	line_no VARCHAR(10)  		--//  ENCODE zstd
	,product_cd VARCHAR(20)  		--//  ENCODE zstd
	,product_nm VARCHAR(60)  		--//  ENCODE zstd
	,store_cd VARCHAR(10)  		--//  ENCODE zstd
	,store_nm VARCHAR(60)  		--//  ENCODE zstd
	,vendor VARCHAR(20)  		--//  ENCODE zstd
	,prm_strt_dt DATE  		--//  ENCODE zstd
	,prm_end_dt DATE  		--//  ENCODE zstd
	,sales_tgt VARCHAR(10)  		--//  ENCODE zstd
	,amt_order VARCHAR(10)  		--//  ENCODE zstd
	,warehouse_dt VARCHAR(10)  		--//  ENCODE zstd
	,item_type VARCHAR(3)  		--//  ENCODE zstd
	,unit_of_pkg_item VARCHAR(20)  		--//  ENCODE zstd
	,pack_size VARCHAR(10)  		--//  ENCODE zstd
	,delivery_method VARCHAR(1)  		--//  ENCODE zstd
	,style VARCHAR(10)  		--//  ENCODE zstd
	,occ_no VARCHAR(20)  		--//  ENCODE zstd
	,src_sys_cd VARCHAR(30)  		--//  ENCODE zstd
	,ctry_cd VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_str_cd_sold_to_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_STR_CD_SOLD_TO_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_str_cd_sold_to_map
(
	clnt VARCHAR(50)  		--//  ENCODE lzo
	,seqid VARCHAR(50)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(50)  		--//  ENCODE lzo
	,conv_sys_cd VARCHAR(50)  		--//  ENCODE lzo
	,str_type VARCHAR(50)  		--//  ENCODE lzo
	,cust_str_cd VARCHAR(50)  		--//  ENCODE lzo
	,sold_to_cd VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_pos_str_sls_grp_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_POS_STR_SLS_GRP_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_pos_str_sls_grp_map
(
	src_sys_cd VARCHAR(50)
	,str_type VARCHAR(50)
	,mysls_str_type VARCHAR(50)
	,sls_grp_cd VARCHAR(20)
	,sls_grp_nm VARCHAR(100)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_query_parameters;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_QUERY_PARAMETERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_query_parameters
(
	country_code VARCHAR(10)  		--//  ENCODE lzo
	,parameter_name VARCHAR(300)  		--//  ENCODE lzo
	,parameter_value VARCHAR(300)  		--//  ENCODE lzo
	,parameter_type VARCHAR(300)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sales_cust_prod_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SALES_CUST_PROD_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sales_cust_prod_master
(
	sales_grp_cd VARCHAR(18)  		--//  ENCODE zstd
	,src_sys_cd VARCHAR(100)  		--//  ENCODE zstd
	,product_nm VARCHAR(100)  		--//  ENCODE zstd
	,cust_prod_cd VARCHAR(25)  		--//  ENCODE zstd
	,ean_cd VARCHAR(25)  		--//  ENCODE zstd
	,ctry_cd VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sales_rep_so_target_fact;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SALES_REP_SO_TARGET_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sales_rep_so_target_fact
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,crncy_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,jj_mnth_id numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,sls_trgt_val NUMERIC(20,5) NOT NULL 		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (ctry_cd, crncy_cd, dstr_cd)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sales_store_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SALES_STORE_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sales_store_master
(
	channel VARCHAR(25)  		--//  ENCODE lzo
	,store_type VARCHAR(25)  		--//  ENCODE lzo
	,sales_grp_cd VARCHAR(18)  		--//  ENCODE lzo
	,sold_to VARCHAR(25)  		--//  ENCODE lzo
	,store_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_store_cd VARCHAR(18)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sls_grp_to_customer_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SLS_GRP_TO_CUSTOMER_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sls_grp_to_customer_mapping
(
	sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_code VARCHAR(100)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_trd_promo_actl;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TRD_PROMO_ACTL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_trd_promo_actl
(
	prft_ctr VARCHAR(100) NOT NULL
	,prft_ctr_desc VARCHAR(200)
	,cust_channel VARCHAR(100) NOT NULL
	,cust_hdqtr_cd VARCHAR(200) NOT NULL
	,ctry_cd VARCHAR(20) NOT NULL
	,crncy_cd VARCHAR(3) NOT NULL
	,yr VARCHAR(4) NOT NULL
	,jan_und_prcs NUMERIC(16,2)
	,jan_aprv_no_pmt NUMERIC(16,2)
	,jan_pmt NUMERIC(16,2)
	,jan_actl_trd_promo NUMERIC(16,2)
	,feb_und_prcs NUMERIC(16,2)
	,feb_aprv_no_pmt NUMERIC(16,2)
	,feb_pmt NUMERIC(16,2)
	,feb_actl_trd_promo NUMERIC(16,2)
	,mar_und_prcs NUMERIC(16,2)
	,mar_aprv_no_pmt NUMERIC(16,2)
	,mar_pmt NUMERIC(16,2)
	,mar_actl_trd_promo NUMERIC(16,2)
	,apr_und_prcs NUMERIC(16,2)
	,apr_aprv_no_pmt NUMERIC(16,2)
	,apr_pmt NUMERIC(16,2)
	,apr_actl_trd_promo NUMERIC(16,2)
	,may_und_prcs NUMERIC(16,2)
	,may_aprv_no_pmt NUMERIC(16,2)
	,may_pmt NUMERIC(16,2)
	,may_actl_trd_promo NUMERIC(16,2)
	,jun_und_prcs NUMERIC(16,2)
	,jun_aprv_no_pmt NUMERIC(16,2)
	,jun_pmt NUMERIC(16,2)
	,jun_actl_trd_promo NUMERIC(16,2)
	,jul_und_prcs NUMERIC(16,2)
	,jul_aprv_no_pmt NUMERIC(16,2)
	,jul_pmt NUMERIC(16,2)
	,jul_actl_trd_promo NUMERIC(16,2)
	,aug_und_prcs NUMERIC(16,2)
	,aug_aprv_no_pmt NUMERIC(16,2)
	,aug_pmt NUMERIC(16,2)
	,aug_actl_trd_promo NUMERIC(16,2)
	,sep_und_prcs NUMERIC(16,2)
	,sep_aprv_no_pmt NUMERIC(16,2)
	,sep_pmt NUMERIC(16,2)
	,sep_actl_trd_promo NUMERIC(16,2)
	,oct_und_prcs NUMERIC(16,2)
	,oct_aprv_no_pmt NUMERIC(16,2)
	,oct_pmt NUMERIC(16,2)
	,oct_actl_trd_promo NUMERIC(16,2)
	,nov_und_prcs NUMERIC(16,2)
	,nov_aprv_no_pmt NUMERIC(16,2)
	,nov_pmt NUMERIC(16,2)
	,nov_actl_trd_promo NUMERIC(16,2)
	,dec_und_prcs NUMERIC(16,2)
	,dec_aprv_no_pmt NUMERIC(16,2)
	,dec_pmt NUMERIC(16,2)
	,dec_actl_trd_promo NUMERIC(16,2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (prft_ctr, cust_channel, cust_hdqtr_cd, ctry_cd, yr)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_trd_promo_pln;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TRD_PROMO_PLN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_trd_promo_pln
(
	prft_ctr VARCHAR(100) NOT NULL
	,prft_ctr_nm VARCHAR(100)
	,cust_channel VARCHAR(100) NOT NULL
	,cust_channel_nm VARCHAR(100) NOT NULL
	,cust_hdqtr_cd VARCHAR(200) NOT NULL
	,cust_hdqtr_nm VARCHAR(200)
	,ctry_cd VARCHAR(20) NOT NULL
	,crncy_cd VARCHAR(3) NOT NULL
	,yr VARCHAR(4) NOT NULL
	,jan_trd_promo_pln NUMERIC(16,2)
	,feb_trd_promo_pln NUMERIC(16,2)
	,mar_trd_promo_pln NUMERIC(16,2)
	,apr_trd_promo_pln NUMERIC(16,2)
	,may_trd_promo_pln NUMERIC(16,2)
	,jun_trd_promo_pln NUMERIC(16,2)
	,jul_trd_promo_pln NUMERIC(16,2)
	,aug_trd_promo_pln NUMERIC(16,2)
	,sep_trd_promo_pln NUMERIC(16,2)
	,oct_trd_promo_pln NUMERIC(16,2)
	,nov_trd_promo_pln NUMERIC(16,2)
	,dec_trd_promo_pln NUMERIC(16,2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (prft_ctr, cust_channel, cust_hdqtr_cd, ctry_cd, yr)
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tsi_target_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TSI_TARGET_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tsi_target_data
(
	year_month numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,ec VARCHAR(10)  		--//  ENCODE zstd
	,offtake VARCHAR(10)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(100)  		--//  ENCODE zstd
	,customer_cname VARCHAR(100)  		--//  ENCODE zstd
	,customer_sname VARCHAR(100)  		--//  ENCODE zstd
	,nts NUMERIC(34,8)  		--//  ENCODE az64
	,"offtake/sellout" NUMERIC(34,8)  		--//  ENCODE az64
	,gts NUMERIC(34,8)  		--//  ENCODE az64
	,pre_sales NUMERIC(34,8)  		--//  ENCODE az64
	,prs_code_01 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_02 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_03 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_04 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_05 VARCHAR(50)  		--//  ENCODE zstd
	,tp NUMERIC(34,8)  		--//  ENCODE zstd
	,tp_percent DOUBLE PRECISION  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_as_watsons_inventory;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_AS_WATSONS_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_as_watsons_inventory
(
	year VARCHAR(30)  		--//  ENCODE lzo
	,week_no VARCHAR(30)  		--//  ENCODE lzo
	,supplier VARCHAR(30)  		--//  ENCODE lzo
	,item_cd VARCHAR(30)  		--//  ENCODE lzo
	,buy_code VARCHAR(30)  		--//  ENCODE lzo
	,home_cdesc VARCHAR(255)  		--//  ENCODE lzo
	,prdt_grp VARCHAR(30)  		--//  ENCODE lzo
	,grp_desc VARCHAR(255)  		--//  ENCODE lzo
	,prdt_cat VARCHAR(30)  		--//  ENCODE lzo
	,category_desc VARCHAR(255)  		--//  ENCODE lzo
	,item_desc VARCHAR(255)  		--//  ENCODE lzo
	,type VARCHAR(50)  		--//  ENCODE lzo
	,avg_sls_cost_value NUMERIC(20,4)  		--//  ENCODE az64
	,total_stock_qty NUMERIC(20,4)  		--//  ENCODE az64
	,total_stock_value NUMERIC(20,4)  		--//  ENCODE az64
	,weeks_holding_sales NUMERIC(20,4)  		--//  ENCODE az64
	,weeks_holding NUMERIC(20,4)  		--//  ENCODE az64
	,first_recv_date DATE  		--//  ENCODE az64
	,turn_type_sales VARCHAR(100)  		--//  ENCODE lzo
	,turn_type VARCHAR(100)  		--//  ENCODE lzo
	,uda73 VARCHAR(50)  		--//  ENCODE lzo
	,discontinue_date DATE  		--//  ENCODE az64
	,stock_class VARCHAR(30)  		--//  ENCODE lzo
	,pog VARCHAR(30)  		--//  ENCODE lzo
	,ean_num VARCHAR(30)  		--//  ENCODE lzo
	,filename VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_bp_target;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_BP_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_bp_target
(
	bp_version VARCHAR(5)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,lph_level_6 VARCHAR(255)  		--//  ENCODE zstd
	,pre_sales DOUBLE PRECISION  		--//  ENCODE zstd
	,tp DOUBLE PRECISION  		--//  ENCODE zstd
	,nts DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_bu_forecast_prod_hier;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_BU_FORECAST_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_bu_forecast_prod_hier
(
	bu_version VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,lph_level_6 VARCHAR(255)  		--//  ENCODE zstd
	,price_off DOUBLE PRECISION  		--//  ENCODE zstd
	,display DOUBLE PRECISION  		--//  ENCODE zstd
	,dm DOUBLE PRECISION  		--//  ENCODE zstd
	,other_support DOUBLE PRECISION  		--//  ENCODE zstd
	,sr DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales_before_returns DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE ALL
;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_bu_forecast_sku;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_BU_FORECAST_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_bu_forecast_sku
(
	bu_version VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_on_month VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_year VARCHAR(10)  		--//  ENCODE zstd
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE zstd
	,sls_grp VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(100)  		--//  ENCODE zstd
	,sls_ofc_desc VARCHAR(255)  		--//  ENCODE zstd
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE zstd
	,sap_code VARCHAR(30)  		--//  ENCODE zstd
	,system_list_price DOUBLE PRECISION  		--//  ENCODE zstd
	,gross_invoice_price DOUBLE PRECISION  		--//  ENCODE zstd
	,gross_invoice_price_lesst_terms DOUBLE PRECISION  		--//  ENCODE zstd
	,rf_sellout_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,rf_sellin_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,price_off DOUBLE PRECISION  		--//  ENCODE zstd
	,pre_sales_before_returns DOUBLE PRECISION  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE ALL
;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_customer_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_CUSTOMER_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_customer_mapping
(
	distributor_code VARCHAR(255)  		--//  ENCODE lzo
	,distributor_name VARCHAR(255)  		--//  ENCODE lzo
	,distributors_customer_code VARCHAR(255)  		--//  ENCODE lzo
	,distributors_customer_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,hq VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_prod_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_PROD_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_prod_map
(
	dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(20)  		--//  ENCODE lzo
	,dstr_prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,dstr_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_cd VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_prod_price_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_PROD_PRICE_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_ims_dstr_prod_price_map
(
	dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(20)  		--//  ENCODE lzo
	,ean_cd VARCHAR(20)  		--//  ENCODE lzo
	,dstr_prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,dstr_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,sell_out_price_manual NUMERIC(30,4)  		--//  ENCODE lzo
	,promotion_start_date DATE  		--//  ENCODE lzo
	,promotion_end_date DATE  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_pos_watson_store;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_POS_WATSON_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_pos_watson_store
(
	store_no VARCHAR(40)  		--//  ENCODE lzo
	,store_name VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,product_description VARCHAR(500)  		--//  ENCODE lzo
	,selling_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,selling_amt NUMERIC(16,5)  		--//  ENCODE az64
	,department VARCHAR(40)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,filename VARCHAR(60)  		--//  ENCODE lzo
	,run_id VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_tw_strategic_cust_hier;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_TW_STRATEGIC_CUST_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_tw_strategic_cust_hier
(
	strategy_customer_hierachy_code VARCHAR(10)  		--//  ENCODE lzo
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE lzo
	,cust_cd VARCHAR(10)  		--//  ENCODE lzo
	,cust_nm VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(20)  		--//  ENCODE lzo
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_coupang_price;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_COUPANG_PRICE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_coupang_price
(
	report_date VARCHAR(100)  		--//  ENCODE zstd
	,trusted_upc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_rpc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_mpc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_product_description VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,online_store VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(100)  		--//  ENCODE zstd
	,dimension1 VARCHAR(100)  		--//  ENCODE zstd
	,sub_category VARCHAR(100)  		--//  ENCODE zstd
	,brand_subcategory VARCHAR(100)  		--//  ENCODE zstd
	,dimension4 VARCHAR(100)  		--//  ENCODE zstd
	,dimension5 VARCHAR(100)  		--//  ENCODE zstd
	,dimension6 VARCHAR(100)  		--//  ENCODE zstd
	,seller VARCHAR(100)  		--//  ENCODE zstd
	,power_sku VARCHAR(100)  		--//  ENCODE zstd
	,availability_status VARCHAR(100)  		--//  ENCODE zstd
	,currency VARCHAR(100)  		--//  ENCODE zstd
	,observed_price VARCHAR(100)  		--//  ENCODE zstd
	,store_list_price VARCHAR(100)  		--//  ENCODE zstd
	,min_price VARCHAR(100)  		--//  ENCODE zstd
	,max_price VARCHAR(100)  		--//  ENCODE zstd
	,min_max_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,min_max_diff_price VARCHAR(100)  		--//  ENCODE zstd
	,msrp VARCHAR(100)  		--//  ENCODE zstd
	,msrp_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,msrp_diff_amount VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_price VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_diff_amount VARCHAR(100)  		--//  ENCODE zstd
	,promotion_text VARCHAR(100)  		--//  ENCODE zstd
	,url VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_coupang_search_keyword;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_COUPANG_SEARCH_KEYWORD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_coupang_search_keyword
(
	search_keyword_criteria VARCHAR(250)  		--//  ENCODE zstd
	,product_keyword_criteria VARCHAR(250)  		--//  ENCODE zstd
	,category1 VARCHAR(250)  		--//  ENCODE zstd
	,category2 VARCHAR(250)  		--//  ENCODE zstd
	,category3 VARCHAR(250)  		--//  ENCODE zstd
	,ranking VARCHAR(250)  		--//  ENCODE zstd
	,search_keyword VARCHAR(250)  		--//  ENCODE zstd
	,product_ranking VARCHAR(250)  		--//  ENCODE zstd
	,product_name VARCHAR(500)  		--//  ENCODE zstd
	,my_product VARCHAR(250)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_linkprice;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_LINKPRICE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_linkprice
(
	campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,group_name VARCHAR(100)  		--//  ENCODE zstd
	,material_id VARCHAR(100)  		--//  ENCODE zstd
	,product_number VARCHAR(250)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,impressison_area VARCHAR(100)  		--//  ENCODE zstd
	,keyword VARCHAR(100)  		--//  ENCODE zstd
	,impression VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,impression_ranking VARCHAR(100)  		--//  ENCODE zstd
	,avg_click_rate VARCHAR(100)  		--//  ENCODE zstd
	,consumed_cost VARCHAR(100)  		--//  ENCODE zstd
	,conversion_count VARCHAR(100)  		--//  ENCODE zstd
	,conversion_rate VARCHAR(100)  		--//  ENCODE zstd
	,purchased_amount VARCHAR(100)  		--//  ENCODE zstd
	,roas VARCHAR(100)  		--//  ENCODE zstd
	,previous_roas VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_gmv;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_NAVER_GMV		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_gmv
(
	product_category_l VARCHAR(100)  		--//  ENCODE zstd
	,product_category_m VARCHAR(100)  		--//  ENCODE zstd
	,product_category_s VARCHAR(100)  		--//  ENCODE zstd
	,product_category_detail VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,product_id VARCHAR(100)  		--//  ENCODE zstd
	,number_of_payments VARCHAR(100)  		--//  ENCODE zstd
	,quantity_of_products_paid VARCHAR(100)  		--//  ENCODE zstd
	,mobile_ratio_qty_prdt_paid VARCHAR(100)  		--//  ENCODE zstd
	,payment_amount VARCHAR(100)  		--//  ENCODE zstd
	,mobile_ratio_payment_amount VARCHAR(100)  		--//  ENCODE zstd
	,pymt_amt_per_prdt_allowance VARCHAR(100)  		--//  ENCODE zstd
	,coupon_total VARCHAR(100)  		--//  ENCODE zstd
	,product_coupon VARCHAR(100)  		--//  ENCODE zstd
	,order_coupon VARCHAR(100)  		--//  ENCODE zstd
	,refund_number VARCHAR(100)  		--//  ENCODE zstd
	,refund_amount VARCHAR(100)  		--//  ENCODE zstd
	,refund_rate VARCHAR(100)  		--//  ENCODE zstd
	,refund_qty VARCHAR(100)  		--//  ENCODE zstd
	,refund_qty_paid_product VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_keyword_search_volume;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_NAVER_KEYWORD_SEARCH_VOLUME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_keyword_search_volume
(
	no VARCHAR(255)  		--//  ENCODE zstd
	,keyword VARCHAR(255)  		--//  ENCODE zstd
	,total_monthly_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,monthly_desktop_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,monthly_mobile_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,average_daily_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,keyword_first_appearance_date VARCHAR(255)  		--//  ENCODE zstd
	,keyword_class VARCHAR(255)  		--//  ENCODE zstd
	,keyword_for_adult VARCHAR(255)  		--//  ENCODE zstd
	,blog_recent_mnthly_publications VARCHAR(255)  		--//  ENCODE zstd
	,blog_total_publications VARCHAR(255)  		--//  ENCODE zstd
	,cafe_recent_mnthly_publications VARCHAR(255)  		--//  ENCODE zstd
	,cafe_total_publications VARCHAR(255)  		--//  ENCODE zstd
	,view_recent_mnthly_publications VARCHAR(255)  		--//  ENCODE zstd
	,view_total_publications VARCHAR(255)  		--//  ENCODE zstd
	,search_volume_until_yesterday VARCHAR(255)  		--//  ENCODE zstd
	,search_volume_until_endofmonth VARCHAR(255)  		--//  ENCODE zstd
	,blog_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,cafe_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,view_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,relative_keyword VARCHAR(4000)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_search_channel;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_DADS_NAVER_SEARCH_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_dads_naver_search_channel
(
	channel_property VARCHAR(250)  		--//  ENCODE zstd
	,channel_group VARCHAR(250)  		--//  ENCODE zstd
	,channel_name VARCHAR(250)  		--//  ENCODE zstd
	,keyword VARCHAR(250)  		--//  ENCODE zstd
	,customer_count VARCHAR(250)  		--//  ENCODE zstd
	,inflow_count VARCHAR(250)  		--//  ENCODE zstd
	,page_count VARCHAR(250)  		--//  ENCODE zstd
	,pages_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,number_of_payment VARCHAR(250)  		--//  ENCODE zstd
	,payment_rate_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,count_of_payments_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_rate_per_inflow_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_per_inflow_14d VARCHAR(250)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_pa_report;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_PA_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_pa_report
(
	date VARCHAR(100)  		--//  ENCODE zstd
	,bidding_type VARCHAR(100)  		--//  ENCODE zstd
	,sales_method VARCHAR(100)  		--//  ENCODE zstd
	,ad_types VARCHAR(100)  		--//  ENCODE zstd
	,campaign_id VARCHAR(100)  		--//  ENCODE zstd
	,campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,ad_groups VARCHAR(250)  		--//  ENCODE zstd
	,ad_execution_product_name VARCHAR(250)  		--//  ENCODE zstd
	,ad_execution_option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_con_revenue_gen_product_nm VARCHAR(250)  		--//  ENCODE zstd
	,ad_con_revenue_gen_product_option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_impression_area VARCHAR(100)  		--//  ENCODE zstd
	,keyword VARCHAR(100)  		--//  ENCODE zstd
	,impression_count VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ad_cost VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_quantity_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_quantity_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_quantity_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_quantity_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,campaign_start_date VARCHAR(100)  		--//  ENCODE zstd
	,campaign_end_date VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_bpa_report;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_COUPANG_BPA_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_coupang_bpa_report
(
	date VARCHAR(100)  		--//  ENCODE zstd
	,bidding_type VARCHAR(100)  		--//  ENCODE zstd
	,sales_method VARCHAR(100)  		--//  ENCODE zstd
	,campaign_start_date VARCHAR(100)  		--//  ENCODE zstd
	,campaign_end_date VARCHAR(100)  		--//  ENCODE zstd
	,ad_objectives VARCHAR(100)  		--//  ENCODE zstd
	,campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,campaign_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_group VARCHAR(100)  		--//  ENCODE zstd
	,ad_group_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_name VARCHAR(100)  		--//  ENCODE zstd
	,template_type VARCHAR(100)  		--//  ENCODE zstd
	,advertisement_id VARCHAR(100)  		--//  ENCODE zstd
	,impression_area VARCHAR(100)  		--//  ENCODE zstd
	,material_type VARCHAR(100)  		--//  ENCODE zstd
	,material VARCHAR(100)  		--//  ENCODE zstd
	,material_id VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_execution_product_name VARCHAR(100)  		--//  ENCODE zstd
	,ad_execution_option_id VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_type VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_name VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_id VARCHAR(100)  		--//  ENCODE zstd
	,impressed_keywords VARCHAR(100)  		--//  ENCODE zstd
	,input_keywords VARCHAR(100)  		--//  ENCODE zstd
	,keyword_extension_type VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(100)  		--//  ENCODE zstd
	,impression_count VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,ad_cost VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_qty_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_qty_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_qty_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_qty_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_store_map;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_KR_SALES_STORE_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_kr_sales_store_map
(
	sls_ofc VARCHAR(4)
	,sls_ofc_desc VARCHAR(40)
	,channel VARCHAR(25)
	,store_type VARCHAR(25)
	,sales_group_code VARCHAR(18)
	,sales_grp_nm VARCHAR(100)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,customer_segmentation_code VARCHAR(256)  		--//  ENCODE lzo
	,customer_segmentation_level_2_code VARCHAR(256)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_ap_customer360_config;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_ap_customer360_config
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
	,param_value VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,create_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_sfmc_attributes;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_RG_SFMC_ATTRIBUTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_sfmc_attributes
(
	subject VARCHAR(200)  		--//  ENCODE zstd
	,original_value VARCHAR(200)  		--//  ENCODE zstd
	,mapped_value VARCHAR(200)  		--//  ENCODE zstd
	,market VARCHAR(200)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_parameter_reg_inventory;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_PARAMETER_REG_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_parameter_reg_inventory
(
	country_name VARCHAR(30)  		--//  ENCODE lzo
	,parameter_name VARCHAR(200)  		--//  ENCODE lzo
	,parameter_value VARCHAR(300)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_query_parameters;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_QUERY_PARAMETERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_query_parameters
(
	country_code VARCHAR(10)  		--//  ENCODE zstd
	,parameter_name VARCHAR(300)  		--//  ENCODE zstd
	,parameter_value VARCHAR(300)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sap_billing_condition;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SAP_BILLING_CONDITION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sap_billing_condition
(
	bill_num VARCHAR(10)  		--//  ENCODE lzo
	,bill_item VARCHAR(6)  		--//  ENCODE lzo
	,zstepnum VARCHAR(3)  		--//  ENCODE lzo
	,kncounter VARCHAR(2)  		--//  ENCODE lzo
	,doc_number VARCHAR(10)  		--//  ENCODE lzo
	,s_ord_item VARCHAR(6)  		--//  ENCODE lzo
	,knart VARCHAR(4)  		--//  ENCODE lzo
	,ch_on VARCHAR(8)  		--//  ENCODE lzo
	,comp_code VARCHAR(4)  		--//  ENCODE lzo
	,sales_dist VARCHAR(6)  		--//  ENCODE lzo
	,bill_type VARCHAR(4)  		--//  ENCODE lzo
	,bill_date VARCHAR(8)  		--//  ENCODE lzo
	,bill_cat VARCHAR(1)  		--//  ENCODE lzo
	,loc_currcy VARCHAR(5)  		--//  ENCODE lzo
	,cust_group VARCHAR(2)  		--//  ENCODE lzo
	,sold_to VARCHAR(10)  		--//  ENCODE lzo
	,payer VARCHAR(10)  		--//  ENCODE lzo
	,exrate_acc VARCHAR(17)  		--//  ENCODE lzo
	,stat_curr VARCHAR(5)  		--//  ENCODE lzo
	,doc_categ VARCHAR(2)  		--//  ENCODE lzo
	,salesorg VARCHAR(4)  		--//  ENCODE lzo
	,distr_chan VARCHAR(2)  		--//  ENCODE lzo
	,doc_currcy VARCHAR(5)  		--//  ENCODE lzo
	,createdon VARCHAR(8)  		--//  ENCODE lzo
	,co_area VARCHAR(4)  		--//  ENCODE lzo
	,costcenter VARCHAR(10)  		--//  ENCODE lzo
	,trans_date VARCHAR(8)  		--//  ENCODE lzo
	,exchg_rate VARCHAR(16)  		--//  ENCODE lzo
	,cust_grp1 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp2 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp3 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp4 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp5 VARCHAR(3)  		--//  ENCODE lzo
	,matl_group VARCHAR(9)  		--//  ENCODE lzo
	,material VARCHAR(18)  		--//  ENCODE lzo
	,mat_entrd VARCHAR(18)  		--//  ENCODE lzo
	,matl_grp_1 VARCHAR(3)  		--//  ENCODE lzo
	,matl_grp_2 VARCHAR(3)  		--//  ENCODE lzo
	,matl_grp_3 VARCHAR(3)  		--//  ENCODE lzo
	,matl_grp_4 VARCHAR(3)  		--//  ENCODE lzo
	,matl_grp_5 VARCHAR(3)  		--//  ENCODE lzo
	,billtoprty VARCHAR(10)  		--//  ENCODE lzo
	,ship_to VARCHAR(10)  		--//  ENCODE lzo
	,itm_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier VARCHAR(18)  		--//  ENCODE lzo
	,prov_group VARCHAR(2)  		--//  ENCODE lzo
	,price_date VARCHAR(8)  		--//  ENCODE lzo
	,item_categ VARCHAR(4)  		--//  ENCODE lzo
	,div_head VARCHAR(2)  		--//  ENCODE lzo
	,division VARCHAR(2)  		--//  ENCODE lzo
	,stat_date VARCHAR(8)  		--//  ENCODE lzo
	,refer_doc VARCHAR(10)  		--//  ENCODE lzo
	,refer_itm VARCHAR(6)  		--//  ENCODE lzo
	,sales_off VARCHAR(4)  		--//  ENCODE lzo
	,sales_grp VARCHAR(3)  		--//  ENCODE lzo
	,wbs_elemt VARCHAR(24)  		--//  ENCODE lzo
	,calday VARCHAR(8)  		--//  ENCODE lzo
	,calmonth VARCHAR(6)  		--//  ENCODE lzo
	,calweek VARCHAR(6)  		--//  ENCODE lzo
	,fiscper VARCHAR(7)  		--//  ENCODE lzo
	,fiscvarnt VARCHAR(2)  		--//  ENCODE lzo
	,knclass VARCHAR(4)  		--//  ENCODE lzo
	,knorigin VARCHAR(4)  		--//  ENCODE lzo
	,kntyp VARCHAR(4)  		--//  ENCODE lzo
	,knval NUMERIC(17,3)  		--//  ENCODE lzo
	,kprice NUMERIC(17,3)  		--//  ENCODE lzo
	,kinak VARCHAR(1)  		--//  ENCODE lzo
	,kstat VARCHAR(1)  		--//  ENCODE lzo
	,storno VARCHAR(1)  		--//  ENCODE lzo
	,rt_promo VARCHAR(10)  		--//  ENCODE lzo
	,rebate_grp VARCHAR(2)  		--//  ENCODE lzo
	,bwapplnm VARCHAR(30)  		--//  ENCODE lzo
	,processkey VARCHAR(3)  		--//  ENCODE lzo
	,eanupc VARCHAR(18)  		--//  ENCODE lzo
	,createdby VARCHAR(12)  		--//  ENCODE lzo
	,serv_date VARCHAR(8)  		--//  ENCODE lzo
	,inv_qty NUMERIC(17,3)  		--//  ENCODE lzo
	,actual_quantity_pc NUMERIC(17,3)  		--//  ENCODE lzo
	,forwagent VARCHAR(10)  		--//  ENCODE lzo
	,salesemply VARCHAR(8)  		--//  ENCODE lzo
	,sales_unit VARCHAR(3)  		--//  ENCODE lzo
	,kappl VARCHAR(2)  		--//  ENCODE lzo
	,acrn_id VARCHAR(2)  		--//  ENCODE lzo
	,recordmode VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_bounce_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_BOUNCE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_bounce_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(20)  		--//  ENCODE zstd
	,job_id VARCHAR(20)  		--//  ENCODE zstd
	,list_id VARCHAR(10)  		--//  ENCODE zstd
	,batch_id VARCHAR(10)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(20)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,is_unique VARCHAR(10)  		--//  ENCODE zstd
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,bounce_category_id VARCHAR(10)  		--//  ENCODE zstd
	,bounce_category VARCHAR(30)  		--//  ENCODE zstd
	,bounce_subcategory_id VARCHAR(10)  		--//  ENCODE zstd
	,bounce_subcategory VARCHAR(30)  		--//  ENCODE zstd
	,bounce_type_id VARCHAR(10)  		--//  ENCODE zstd
	,bounce_type VARCHAR(30)  		--//  ENCODE zstd
	,smtp_bounce_reason VARCHAR(1000)  		--//  ENCODE zstd
	,smtp_message VARCHAR(200)  		--//  ENCODE zstd
	,smtp_code VARCHAR(10)  		--//  ENCODE zstd
	,triggerer_send_definition_object_id VARCHAR(50)  		--//  ENCODE zstd
	,triggered_send_customer_key VARCHAR(10)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,bcc_email VARCHAR(50)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_id VARCHAR(20)  		--//  ENCODE zstd
	,email_address VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_children_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_CHILDREN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_children_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,parent_key VARCHAR(200)  		--//  ENCODE zstd
	,child_nm VARCHAR(200)  		--//  ENCODE zstd
	,child_birth_mnth VARCHAR(10)  		--//  ENCODE zstd
	,child_birth_year VARCHAR(10)  		--//  ENCODE zstd
	,child_gender VARCHAR(10)  		--//  ENCODE zstd
	,child_number VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_click_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_CLICK_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_click_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(50)  		--//  ENCODE zstd
	,job_id VARCHAR(50)  		--//  ENCODE zstd
	,list_id VARCHAR(50)  		--//  ENCODE zstd
	,batch_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,url VARCHAR(1000)  		--//  ENCODE zstd
	,link_name VARCHAR(200)  		--//  ENCODE zstd
	,link_content VARCHAR(1000)  		--//  ENCODE zstd
	,is_unique VARCHAR(10)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_complaint_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_COMPLAINT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_complaint_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(50)  		--//  ENCODE zstd
	,job_id VARCHAR(50)  		--//  ENCODE zstd
	,list_id VARCHAR(50)  		--//  ENCODE zstd
	,batch_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,is_unique VARCHAR(10)  		--//  ENCODE zstd
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_id VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_consumer_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_CONSUMER_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_consumer_master
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,first_name VARCHAR(200)  		--//  ENCODE zstd
	,last_name VARCHAR(200)  		--//  ENCODE zstd
	,mobile_num VARCHAR(30)  		--//  ENCODE zstd
	,mobile_cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,birthday_mnth VARCHAR(10)  		--//  ENCODE zstd
	,birthday_year VARCHAR(10)  		--//  ENCODE zstd
	,address_1 VARCHAR(255)  		--//  ENCODE zstd
	,address_2 VARCHAR(255)  		--//  ENCODE zstd
	,address_city VARCHAR(100)  		--//  ENCODE zstd
	,address_zipcode VARCHAR(30)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,website_unique_id VARCHAR(150)  		--//  ENCODE zstd
	,source VARCHAR(100)  		--//  ENCODE zstd
	,medium VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(200)  		--//  ENCODE zstd
	,address_cntry VARCHAR(100)  		--//  ENCODE zstd
	,campaign_id VARCHAR(100)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,unsubscribe_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,email VARCHAR(100)  		--//  ENCODE zstd
	,full_name VARCHAR(200)  		--//  ENCODE zstd
	,last_logon_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,remaining_points NUMERIC(10,4)  		--//  ENCODE az64
	,redeemed_points NUMERIC(10,4)  		--//  ENCODE az64
	,total_points NUMERIC(10,4)  		--//  ENCODE az64
	,gender VARCHAR(20)  		--//  ENCODE zstd
	,line_id VARCHAR(50)  		--//  ENCODE zstd
	,line_name VARCHAR(200)  		--//  ENCODE zstd
	,line_email VARCHAR(100)  		--//  ENCODE zstd
	,line_channel_id VARCHAR(50)  		--//  ENCODE zstd
	,address_region VARCHAR(100)  		--//  ENCODE zstd
	,tier VARCHAR(100)  		--//  ENCODE zstd
	,opt_in_for_communication VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,have_kid VARCHAR(20)  		--//  ENCODE zstd
	,age numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,valid_from TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,valid_from TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,valid_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,delete_flag VARCHAR(10)  		--//  ENCODE zstd
	,subscriber_status VARCHAR(100)  		--//  ENCODE zstd
	,opt_in_for_jnj_communication VARCHAR(100)  		--//  ENCODE zstd
	,opt_in_for_campaign VARCHAR(100)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_consumer_master_additional;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_CONSUMER_MASTER_ADDITIONAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_consumer_master_additional
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,attribute_name VARCHAR(100)  		--//  ENCODE zstd
	,attribute_value VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_invoice_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_INVOICE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_invoice_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,purchase_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,channel VARCHAR(200)  		--//  ENCODE zstd
	,product VARCHAR(200)  		--//  ENCODE zstd
	,status VARCHAR(50)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,completed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,points numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,show_record VARCHAR(20)  		--//  ENCODE zstd
	,qty NUMERIC(20,4)  		--//  ENCODE az64
	,invoice_type VARCHAR(200)  		--//  ENCODE zstd
	,seller_nm VARCHAR(255)  		--//  ENCODE zstd
	,product_category VARCHAR(200)  		--//  ENCODE zstd
	,website_unique_id VARCHAR(150)  		--//  ENCODE zstd
	,invoice_num VARCHAR(50)  		--//  ENCODE zstd
	,epsilon_price_per_unit NUMERIC(20,4)  		--//  ENCODE az64
	,epsilon_amount NUMERIC(20,4)  		--//  ENCODE az64
	,epsilon_total_amount NUMERIC(20,4)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_open_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_OPEN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_open_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(50)  		--//  ENCODE zstd
	,job_id VARCHAR(50)  		--//  ENCODE zstd
	,list_id VARCHAR(30)  		--//  ENCODE zstd
	,batch_id VARCHAR(30)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,bcc_email VARCHAR(50)  		--//  ENCODE zstd
	,email_id VARCHAR(30)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,is_unique VARCHAR(10)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_redemption_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_REDEMPTION_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_redemption_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,prod_nm VARCHAR(255)  		--//  ENCODE zstd
	,redeemed_points NUMERIC(20,4)  		--//  ENCODE az64
	,qty NUMERIC(20,4)  		--//  ENCODE az64
	,redeemed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,status VARCHAR(100)  		--//  ENCODE zstd
	,completed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,order_num VARCHAR(50)  		--//  ENCODE zstd
	,website_unique_id VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_sent_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_SENT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_sent_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(50)  		--//  ENCODE zstd
	,job_id VARCHAR(50)  		--//  ENCODE zstd
	,list_id VARCHAR(30)  		--//  ENCODE zstd
	,batch_id VARCHAR(30)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,triggerer_send_definition_object_id VARCHAR(50)  		--//  ENCODE zstd
	,triggered_send_customer_key VARCHAR(10)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,email_id VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_unsubscribe_data;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SFMC_UNSUBSCRIBE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sfmc_unsubscribe_data
(
	cntry_cd VARCHAR(10)  		--//  ENCODE zstd
	,oyb_account_id VARCHAR(50)  		--//  ENCODE zstd
	,job_id VARCHAR(50)  		--//  ENCODE zstd
	,list_id VARCHAR(30)  		--//  ENCODE zstd
	,batch_id VARCHAR(30)  		--//  ENCODE zstd
	,subscriber_id VARCHAR(50)  		--//  ENCODE zstd
	,subscriber_key VARCHAR(100)  		--//  ENCODE zstd
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE zstd
	,email_name VARCHAR(100)  		--//  ENCODE zstd
	,email_subject VARCHAR(200)  		--//  ENCODE zstd
	,email_id VARCHAR(30)  		--//  ENCODE zstd
	,is_unique VARCHAR(10)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_sls_grp_text;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_SLS_GRP_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_sls_grp_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,sls_grp VARCHAR(3) NOT NULL
	,de VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, sls_grp)
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_ps_channel_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_RG_PS_CHANNEL_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_ps_channel_weights
(
	country VARCHAR(500)  		--//  ENCODE lzo
	,channel_re VARCHAR(200)  		--//  ENCODE lzo
	,channel_re_value VARCHAR(200)  		--//  ENCODE lzo
	,weight NUMERIC(31,2)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_ps_market_coverage;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_RG_PS_MARKET_COVERAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_rg_ps_market_coverage
(
	cnty_nm VARCHAR(255)  		--//  ENCODE zstd
	,channel VARCHAR(25)  		--//  ENCODE zstd
	,coverage NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;

--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_my_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_MY_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_my_ps_targets
(
	kpi VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(200)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(50)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_my_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_MY_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_my_ps_weights
(
	kpi VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(255)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_ph_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_PH_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_ph_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_ph_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_PH_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_ph_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,valid_from DATE  		--//  ENCODE az64
	,valid_to DATE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_customer_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_CUSTOMER_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_customer_hierarchy
(
	customer_number VARCHAR(10)  		--//  ENCODE lzo
	,customer_name VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,customer_group_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_group_name VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,customer_segmentation_code VARCHAR(256)  		--//  ENCODE lzo
	,customer_segmentation_level_2_code VARCHAR(256)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_product_hierarchy;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_PRODUCT_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_product_hierarchy
(
	material VARCHAR(10)  		--//  ENCODE lzo
	,material_description VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,category VARCHAR(50)  		--//  ENCODE lzo
	,producttype VARCHAR(50)  		--//  ENCODE lzo
	,productvarient VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_product_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_product_mapping
(
	customer_name VARCHAR(100)  		--//  ENCODE lzo
	,customer_brand VARCHAR(200)  		--//  ENCODE lzo
	,customer_product_code VARCHAR(300)  		--//  ENCODE lzo
	,customer_product_name VARCHAR(500)  		--//  ENCODE lzo
	,master_code VARCHAR(300)  		--//  ENCODE lzo
	,material_code VARCHAR(300)  		--//  ENCODE lzo
	,product_name VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_store_master;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_SG_STORE_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_sg_store_master
(
	customer_name VARCHAR(200)  		--//  ENCODE lzo
	,customer_store_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_store_name VARCHAR(200)  		--//  ENCODE lzo
	,customer_store_location VARCHAR(200)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_th_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TH_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_th_ps_targets
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(100)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,valid_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,valid_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_th_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_TH_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_th_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_store_tagging;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_VN_PS_STORE_TAGGING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_store_tagging
(
	parent_customer VARCHAR(256)  		--//  ENCODE lzo
	,store_code VARCHAR(256)  		--//  ENCODE lzo
	,store_name VARCHAR(256)  		--//  ENCODE lzo
	,status VARCHAR(256)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_targets;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_VN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_targets
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_weights;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_MDS_VN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_mds_vn_ps_weights
(
	channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_answers;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_ANSWERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_answers
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_branch;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_BRANCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_branch
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_choices;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_CHOICES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_choices
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_ise_header;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_ISE_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_ise_header
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_notes;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_NOTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_notes
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_question;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_INTERFACE_QUESTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_interface_question
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
--DROP TABLE SNAPNTAITG_INTEGRATION.itg_vn_product_mapping;
CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.ITG_VN_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAITG_INTEGRATION.itg_vn_product_mapping
(
	putupid VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(255)  		--//  ENCODE zstd
	,productname VARCHAR(2000)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;


--DROP TABLE SNAPNTAWKS_INTEGRATION.hk_propagate_from_to;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.HK_PROPAGATE_FROM_TO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.hk_propagate_from_to
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,latest_month VARCHAR(23)  		--//  ENCODE lzo
	,propagate_to VARCHAR(23)  		--//  ENCODE lzo
	,propagate_from VARCHAR(23)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,diff_month numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,reason VARCHAR(29)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.hk_propagate_to;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.HK_PROPAGATE_TO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.hk_propagate_to
(
	sap_parent_customer_key VARCHAR(50)
	,month VARCHAR(23)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,latest_month VARCHAR(23)  		--//  ENCODE lzo
)

CLUSTER BY(sap_parent_customer_key)
		--// SORTKEY ( 
		--// 	sap_parent_customer_key
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAWKS_INTEGRATION.notify_mising_cust_prod_cd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.NOTIFY_MISING_CUST_PROD_CD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.notify_mising_cust_prod_cd
(
	src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE lzo
	,record_count numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,min_pos_dt DATE  		--//  ENCODE lzo
	,max_pos_dt DATE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.notify_mising_store_cd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.NOTIFY_MISING_STORE_CD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.notify_mising_store_cd
(
	src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,record_count numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,min_pos_dt DATE  		--//  ENCODE lzo
	,max_pos_dt DATE  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.taiwan_propagate_from_to;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.TAIWAN_PROPAGATE_FROM_TO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.taiwan_propagate_from_to
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,latest_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,propagate_to numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,diff_month numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,reason VARCHAR(29)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.taiwan_propagate_to;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.TAIWAN_PROPAGATE_TO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.taiwan_propagate_to
(
	sap_parent_customer_key VARCHAR(12)
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,latest_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

CLUSTER BY(sap_parent_customer_key)
		--// SORTKEY ( 
		--// 	sap_parent_customer_key
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAWKS_INTEGRATION.tw_ims_distributor_ingestion_metadata;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.TW_IMS_DISTRIBUTOR_INGESTION_METADATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.tw_ims_distributor_ingestion_metadata
(
	distributor_code VARCHAR(255)
	,s3_raw_layer_path VARCHAR(255)
	,flag VARCHAR(2)
	,subject_area VARCHAR(255)
	,ctry_cd VARCHAR(255)
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_direct_sales_rep_route_plan;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_DIRECT_SALES_REP_ROUTE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_direct_sales_rep_route_plan
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sl_no numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sls_rep_cd_nm VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,store_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_nm VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,week numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,day VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,file_rec_dt DATE NOT NULL 		--//  ENCODE delta
	,period numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,file_eff_dt DATE NOT NULL 		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_brand_sls_target;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_BRAND_SLS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_brand_sls_target
(
	brnd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(30)  		--//  ENCODE lzo
	,cust_num VARCHAR(30)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mo numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,brnd_trgt NUMERIC(16,5)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_catg_sls_target;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_CATG_SLS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_catg_sls_target
(
	sls_grp VARCHAR(30)  		--//  ENCODE lzo
	,brnd VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(2000)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,cust_num VARCHAR(30)  		--//  ENCODE lzo
	,yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mo numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,sls_grp_cat_trgt NUMERIC(16,5)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_invnt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_IMS_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(30)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(200)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,invnt_amt NUMERIC(21,5)  		--//  ENCODE az64
	,avg_prc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,safety_stock NUMERIC(21,5)  		--//  ENCODE az64
	,bad_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,book_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,convs_amt NUMERIC(21,5)  		--//  ENCODE az64
	,prch_disc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,end_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,batch_no VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chn_uom VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_invnt_std;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_IMS_INVNT_STD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_invnt_std
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(30)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(200)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,invnt_amt NUMERIC(21,5)  		--//  ENCODE az64
	,avg_prc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,safety_stock NUMERIC(21,5)  		--//  ENCODE az64
	,bad_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,book_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,convs_amt NUMERIC(21,5)  		--//  ENCODE az64
	,prch_disc_amt NUMERIC(21,5)  		--//  ENCODE az64
	,end_invnt_qty NUMERIC(21,5)  		--//  ENCODE az64
	,batch_no VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chn_uom VARCHAR(100)  		--//  ENCODE lzo
	,storage_name VARCHAR(200)  		--//  ENCODE lzo
	,area VARCHAR(300)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_sls;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_IMS_SLS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_sls
(
	ims_txn_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,rpt_per_strt_dt DATE  		--//  ENCODE az64
	,rpt_per_end_dt DATE  		--//  ENCODE az64
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,unit_prc NUMERIC(21,5)  		--//  ENCODE az64
	,sls_amt NUMERIC(21,5)  		--//  ENCODE az64
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_amt NUMERIC(24,5)  		--//  ENCODE az64
	,ship_cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cls_grp VARCHAR(20)  		--//  ENCODE lzo
	,cust_sub_cls VARCHAR(20)  		--//  ENCODE lzo
	,prod_spec VARCHAR(50)  		--//  ENCODE lzo
	,itm_agn_nm VARCHAR(100)  		--//  ENCODE lzo
	,ordr_co VARCHAR(20)  		--//  ENCODE lzo
	,rtrn_rsn VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_ofc_nm VARCHAR(20)  		--//  ENCODE lzo
	,sls_grp_nm VARCHAR(20)  		--//  ENCODE lzo
	,acc_type VARCHAR(10)  		--//  ENCODE lzo
	,co_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,doc_dt DATE  		--//  ENCODE az64
	,doc_num VARCHAR(20)  		--//  ENCODE lzo
	,invc_num VARCHAR(15)  		--//  ENCODE lzo
	,remark_desc VARCHAR(100)  		--//  ENCODE lzo
	,gift_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_bfr_tax_amt NUMERIC(22,5)  		--//  ENCODE az64
	,sku_per_box NUMERIC(21,5)  		--//  ENCODE az64
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,prom_sls_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_rtrn_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(31,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_sls_std;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_IMS_SLS_STD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_ims_sls_std
(
	ims_txn_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(10)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,rpt_per_strt_dt DATE  		--//  ENCODE az64
	,rpt_per_end_dt DATE  		--//  ENCODE az64
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,unit_prc NUMERIC(21,5)  		--//  ENCODE az64
	,sls_amt NUMERIC(21,5)  		--//  ENCODE az64
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_amt NUMERIC(24,5)  		--//  ENCODE az64
	,ship_cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_cls_grp VARCHAR(20)  		--//  ENCODE lzo
	,cust_sub_cls VARCHAR(20)  		--//  ENCODE lzo
	,prod_spec VARCHAR(50)  		--//  ENCODE lzo
	,itm_agn_nm VARCHAR(100)  		--//  ENCODE lzo
	,ordr_co VARCHAR(20)  		--//  ENCODE lzo
	,rtrn_rsn VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_ofc_nm VARCHAR(20)  		--//  ENCODE lzo
	,sls_grp_nm VARCHAR(20)  		--//  ENCODE lzo
	,acc_type VARCHAR(10)  		--//  ENCODE lzo
	,co_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(20)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,doc_dt DATE  		--//  ENCODE az64
	,doc_num VARCHAR(20)  		--//  ENCODE lzo
	,invc_num VARCHAR(15)  		--//  ENCODE lzo
	,remark_desc VARCHAR(100)  		--//  ENCODE lzo
	,gift_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_bfr_tax_amt NUMERIC(22,5)  		--//  ENCODE az64
	,sku_per_box NUMERIC(21,5)  		--//  ENCODE az64
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,prom_sls_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_rtrn_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(31,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_POS_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(300)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(500)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,prom_sls_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(31,5)  		--//  ENCODE az64
	,hist_flg VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact_korea;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_POS_FACT_KOREA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact_korea
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(100)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(500)  		--//  ENCODE lzo
	,matl_num VARCHAR(1)  		--//  ENCODE lzo
	,matl_desc VARCHAR(1)  		--//  ENCODE lzo
	,prom_sls_amt VARCHAR(1)  		--//  ENCODE lzo
	,prom_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,hist_flg VARCHAR(1)  		--//  ENCODE lzo
	,channel VARCHAR(25)  		--//  ENCODE lzo
	,store_type VARCHAR(25)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact_watson_store;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_POS_FACT_WATSON_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_fact_watson_store
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(300)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(500)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,prom_sls_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(31,5)  		--//  ENCODE az64
	,hist_flg VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_inventory_fact;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_POS_INVENTORY_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_pos_inventory_fact
(
	invnt_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(300)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,per_box_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cust_invnt_qty NUMERIC(16,5)  		--//  ENCODE az64
	,box_invnt_qty NUMERIC(16,5)  		--//  ENCODE az64
	,wk_hold_sls NUMERIC(16,5)  		--//  ENCODE az64
	,wk_hold NUMERIC(16,5)  		--//  ENCODE az64
	,fst_recv_dt VARCHAR(10)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(10)  		--//  ENCODE lzo
	,dc VARCHAR(40)  		--//  ENCODE lzo
	,stk_cls VARCHAR(40)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(500)  		--//  ENCODE lzo
	,matl_num VARCHAR(100)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,prom_invnt_amt NUMERIC(38,4)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(31,5)  		--//  ENCODE az64
	,hist_flg VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_trd_promo_actl;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_TRD_PROMO_ACTL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_trd_promo_actl
(
	prft_ctr VARCHAR(100)  		--//  ENCODE lzo
	,prft_ctr_desc VARCHAR(200)  		--//  ENCODE lzo
	,cust_channel VARCHAR(100)  		--//  ENCODE lzo
	,cust_hdqtr_cd VARCHAR(200)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(20)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,yr VARCHAR(4)  		--//  ENCODE lzo
	,jan_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,may_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,may_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,may_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,may_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_und_prcs NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_aprv_no_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_pmt NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_actl_trd_promo NUMERIC(16,2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_edw_trd_promo_pln;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_EDW_TRD_PROMO_PLN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_edw_trd_promo_pln
(
	prft_ctr VARCHAR(100)  		--//  ENCODE lzo
	,prft_ctr_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_channel VARCHAR(100)  		--//  ENCODE lzo
	,cust_channel_nm VARCHAR(100)  		--//  ENCODE lzo
	,cust_hdqtr_cd VARCHAR(200)  		--//  ENCODE lzo
	,cust_hdqtr_nm VARCHAR(200)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(20)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,yr VARCHAR(4)  		--//  ENCODE lzo
	,jan_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,may_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_trd_promo_pln NUMERIC(16,2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY(cust_channel)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_gt_msl_items;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_GT_MSL_ITEMS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_gt_msl_items
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,brand VARCHAR(60)  		--//  ENCODE zstd
	,dstr_prod_cd VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,sap_matl_cd VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,prod_desc_eng VARCHAR(100)  		--//  ENCODE zstd
	,prod_desc_chnse VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3) NOT NULL 		--//  ENCODE zstd
	,msl_flg VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,strt_yr_mnth numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,end_yr_mnth numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,file_rec_dt DATE NOT NULL 		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_allmonths_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_ALLMONTHS_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_allmonths_base
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_base
(
	ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,dstr_cd VARCHAR(50)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,4)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,9)  		--//  ENCODE az64
	,so_sls_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_trd_sls NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_base_detail;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_BASE_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_base_detail
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,base_matl_num VARCHAR(255)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_viva_sel_out;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_IMS_VIVA_SEL_OUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_viva_sel_out
(
	calendar_sid DATE  		--//  ENCODE zstd
	,sales_office VARCHAR(10)  		--//  ENCODE zstd
	,sales_group VARCHAR(10)  		--//  ENCODE zstd
	,sales_office_name VARCHAR(15)  		--//  ENCODE zstd
	,sales_group_name VARCHAR(15)  		--//  ENCODE zstd
	,account_types VARCHAR(10)  		--//  ENCODE zstd
	,sales_volume NUMERIC(21,5)  		--//  ENCODE zstd
	,sales_order_quantity NUMERIC(21,5)  		--//  ENCODE zstd
	,net_trade_sales NUMERIC(21,5)  		--//  ENCODE zstd
	,customer_name VARCHAR(100)  		--//  ENCODE zstd
	,customer_number VARCHAR(15)  		--//  ENCODE zstd
	,base_product VARCHAR(50)  		--//  ENCODE zstd
	,variant VARCHAR(60)  		--//  ENCODE zstd
	,mvgr1_base VARCHAR(10)  		--//  ENCODE zstd
	,mvgr2_variant VARCHAR(10)  		--//  ENCODE zstd
	,mega_brand VARCHAR(20)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,mvgr4_mega VARCHAR(10)  		--//  ENCODE zstd
	,mvgr5_brand VARCHAR(10)  		--//  ENCODE zstd
	,product_description VARCHAR(100)  		--//  ENCODE zstd
	,product_number VARCHAR(15)  		--//  ENCODE zstd
	,local_curr_exch_rate NUMERIC(21,5)  		--//  ENCODE zstd
	,employee VARCHAR(10)  		--//  ENCODE zstd
	,employee_name VARCHAR(50)  		--//  ENCODE zstd
	,transactiontype VARCHAR(10)  		--//  ENCODE zstd
	,return_reason VARCHAR(30)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,currency VARCHAR(3)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_wingkeung_inv;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_IMS_WINGKEUNG_INV		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_wingkeung_inv
(
	date DATE  		--//  ENCODE zstd
	,stk_code VARCHAR(10)  		--//  ENCODE zstd
	,prod_code VARCHAR(18)  		--//  ENCODE zstd
	,chn_desp VARCHAR(50)  		--//  ENCODE zstd
	,chn_uom VARCHAR(100)  		--//  ENCODE zstd
	,closing DOUBLE PRECISION  		--//  ENCODE zstd
	,amount DOUBLE PRECISION  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_wingkeung_sel_out;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_IMS_WINGKEUNG_SEL_OUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_ims_wingkeung_sel_out
(
	calendar_sid DATE  		--//  ENCODE zstd
	,sales_office VARCHAR(10)  		--//  ENCODE zstd
	,sales_group VARCHAR(10)  		--//  ENCODE zstd
	,sales_office_name VARCHAR(15)  		--//  ENCODE zstd
	,sales_group_name VARCHAR(15)  		--//  ENCODE zstd
	,account_types VARCHAR(10)  		--//  ENCODE zstd
	,sales_volume NUMERIC(21,5)  		--//  ENCODE zstd
	,sales_order_quantity numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,net_trade_sales NUMERIC(21,5)  		--//  ENCODE zstd
	,customer_name VARCHAR(100)  		--//  ENCODE zstd
	,customer_number VARCHAR(15)  		--//  ENCODE zstd
	,base_product VARCHAR(50)  		--//  ENCODE zstd
	,variant VARCHAR(60)  		--//  ENCODE zstd
	,mvgr1_base VARCHAR(10)  		--//  ENCODE zstd
	,mvgr2_variant VARCHAR(10)  		--//  ENCODE zstd
	,mega_brand VARCHAR(20)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,mvgr4_mega VARCHAR(10)  		--//  ENCODE zstd
	,mvgr5_brand VARCHAR(10)  		--//  ENCODE zstd
	,product_description VARCHAR(100)  		--//  ENCODE zstd
	,product_number VARCHAR(15)  		--//  ENCODE zstd
	,local_curr_exch_rate NUMERIC(21,5)  		--//  ENCODE zstd
	,employee VARCHAR(10)  		--//  ENCODE zstd
	,employee_name VARCHAR(50)  		--//  ENCODE zstd
	,transactiontype VARCHAR(10)  		--//  ENCODE zstd
	,return_reason VARCHAR(30)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,currency VARCHAR(3)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_inventory_healthy_unhealthy_analysis;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_INVENTORY_HEALTHY_UNHEALTHY_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_inventory_healthy_unhealthy_analysis
(
	month VARCHAR(23)  		--//  ENCODE lzo
	,dstrbtr_grp_cd VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(30)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,segment VARCHAR(50)  		--//  ENCODE lzo
	,prod_category VARCHAR(50)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,sap_prnt_cust_key VARCHAR(12)  		--//  ENCODE lzo
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,healthy_inventory VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_lastnmonths;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_LASTNMONTHS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_lastnmonths
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_pos_scorecard_mannings;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_POS_SCORECARD_MANNINGS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_pos_scorecard_mannings
(
	vendorid VARCHAR(40)  		--//  ENCODE zstd
	,vendordesc VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(40)  		--//  ENCODE zstd
	,productid VARCHAR(40)  		--//  ENCODE zstd
	,productdesc VARCHAR(100)  		--//  ENCODE zstd
	,date VARCHAR(100)  		--//  ENCODE zstd
	,salesqty NUMERIC(16,5)  		--//  ENCODE zstd
	,salesvalue NUMERIC(16,5)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_pos_scorecard_mannings_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_POS_SCORECARD_MANNINGS_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_pos_scorecard_mannings_temp
(
	vendorid VARCHAR(40)  		--//  ENCODE zstd
	,vendordesc VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(40)  		--//  ENCODE zstd
	,productid VARCHAR(40)  		--//  ENCODE zstd
	,productdesc VARCHAR(100)  		--//  ENCODE zstd
	,date VARCHAR(100)  		--//  ENCODE zstd
	,salesqty NUMERIC(16,5)  		--//  ENCODE zstd
	,salesvalue NUMERIC(16,5)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_sellout_for_inv_analysis;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_SELLOUT_FOR_INV_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_sellout_for_inv_analysis
(
	min_date DATE  		--//  ENCODE az64
	,dstrbtr_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,brand VARCHAR(30)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,segment VARCHAR(50)  		--//  ENCODE lzo
	,prod_category VARCHAR(50)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,sap_prnt_cust_key VARCHAR(12)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_final;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_SISO_PROPAGATE_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_final
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
	,existing_so_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_inv_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_sell_in_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_sell_in_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_3months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_6months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_12months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_3months_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_6months_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_12months_so_value NUMERIC(38,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_to_details;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_SISO_PROPAGATE_TO_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_to_details
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagation_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from VARCHAR(23)  		--//  ENCODE lzo
	,reason VARCHAR(29)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_to_existing_dtls;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HK_SISO_PROPAGATE_TO_EXISTING_DTLS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hk_siso_propagate_to_existing_dtls
(
	sap_parent_customer_key VARCHAR(50)  		--//  ENCODE lzo
	,month VARCHAR(23)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,so_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_6months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_12months_so numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagate_from VARCHAR(23)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_indirect_sales_rep_route_plan;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_INDIRECT_SALES_REP_ROUTE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_indirect_sales_rep_route_plan
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd_nm VARCHAR(100) NOT NULL 		--//  ENCODE zstd
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,store_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,store_nm VARCHAR(100)  		--//  ENCODE zstd
	,store_class VARCHAR(3)  		--//  ENCODE zstd
	,visit_freq numeric(18,0)		--//  ENCODE delta // INTEGER  
	,week numeric(18,0) NOT NULL 		--//  ENCODE zstd // INTEGER 
	,day VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,file_rec_dt DATE NOT NULL 		--//  ENCODE delta
	,period numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,file_eff_dt DATE NOT NULL 		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_hk_ims_dstr_cust_attr;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_HK_IMS_DSTR_CUST_ATTR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_hk_ims_dstr_cust_attr
(
	dstr_code VARCHAR(10)  		--//  ENCODE lzo
	,dstr_name VARCHAR(1)  		--//  ENCODE lzo
	,dstr_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,dstr_customer_name VARCHAR(50)  		--//  ENCODE lzo
	,dstr_customer_area VARCHAR(1)  		--//  ENCODE lzo
	,dstr_customer_clsn1 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_customer_clsn2 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_customer_clsn3 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_customer_clsn4 VARCHAR(100)  		--//  ENCODE lzo
	,dstr_customer_clsn5 VARCHAR(100)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_invnt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_IMS_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(6)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(32)  		--//  ENCODE lzo
	,prod_cd VARCHAR(18)  		--//  ENCODE lzo
	,prod_nm VARCHAR(50)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,cust_nm VARCHAR(4)  		--//  ENCODE lzo
	,invnt_qty DOUBLE PRECISION
	,invnt_amt DOUBLE PRECISION
	,avg_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,safety_stock VARCHAR(1)  		--//  ENCODE lzo
	,bad_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,book_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,convs_amt VARCHAR(1)  		--//  ENCODE lzo
	,prch_disc_amt VARCHAR(1)  		--//  ENCODE lzo
	,end_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,batch_no VARCHAR(1)  		--//  ENCODE lzo
	,uom VARCHAR(1)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(1)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(1)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,chn_uom VARCHAR(100)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_invnt_std;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_IMS_INVNT_STD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_invnt_std
(
	invnt_dt DATE  		--//  ENCODE az64
	,dstr_cd VARCHAR(20)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(108)  		--//  ENCODE lzo
	,prod_cd VARCHAR(20)  		--//  ENCODE lzo
	,prod_nm VARCHAR(200)  		--//  ENCODE lzo
	,ean_num VARCHAR(20)  		--//  ENCODE lzo
	,cust_nm VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(38,5)  		--//  ENCODE az64
	,invnt_amt NUMERIC(38,5)  		--//  ENCODE az64
	,avg_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,safety_stock VARCHAR(1)  		--//  ENCODE lzo
	,bad_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,book_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,convs_amt VARCHAR(1)  		--//  ENCODE lzo
	,prch_disc_amt VARCHAR(1)  		--//  ENCODE lzo
	,end_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,batch_no VARCHAR(1)  		--//  ENCODE lzo
	,uom VARCHAR(1)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(1)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(1)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,chn_uom VARCHAR(10)  		--//  ENCODE lzo
	,storage_name VARCHAR(200)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_sls;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_IMS_SLS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_ims_sls
(
	ims_txn_dt DATE  		--//  ENCODE az64
	,cust_cd VARCHAR(15)  		--//  ENCODE lzo
	,cust_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_cd VARCHAR(15)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,rpt_per_strt_dt DATE  		--//  ENCODE az64
	,rpt_per_end_dt DATE  		--//  ENCODE az64
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,uom VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc VARCHAR(1)  		--//  ENCODE lzo
	,sls_amt NUMERIC(38,5)  		--//  ENCODE az64
	,sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,ship_cust_nm VARCHAR(1)  		--//  ENCODE lzo
	,cust_cls_grp VARCHAR(1)  		--//  ENCODE lzo
	,cust_sub_cls VARCHAR(1)  		--//  ENCODE lzo
	,prod_spec VARCHAR(1)  		--//  ENCODE lzo
	,itm_agn_nm VARCHAR(1)  		--//  ENCODE lzo
	,ordr_co VARCHAR(1)  		--//  ENCODE lzo
	,rtrn_rsn VARCHAR(30)  		--//  ENCODE lzo
	,sls_ofc_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_ofc_nm VARCHAR(15)  		--//  ENCODE lzo
	,sls_grp_nm VARCHAR(15)  		--//  ENCODE lzo
	,acc_type VARCHAR(10)  		--//  ENCODE lzo
	,co_cd VARCHAR(1)  		--//  ENCODE lzo
	,sls_rep_cd VARCHAR(10)  		--//  ENCODE lzo
	,sls_rep_nm VARCHAR(50)  		--//  ENCODE lzo
	,doc_dt DATE  		--//  ENCODE az64
	,doc_type VARCHAR(7)  		--//  ENCODE lzo
	,doc_num VARCHAR(1)  		--//  ENCODE lzo
	,invc_num VARCHAR(1)  		--//  ENCODE lzo
	,invc_prc VARCHAR(1)  		--//  ENCODE lzo
	,invc_amt VARCHAR(1)  		--//  ENCODE lzo
	,channel VARCHAR(1)  		--//  ENCODE lzo
	,remark_desc VARCHAR(1)  		--//  ENCODE lzo
	,prom_type VARCHAR(1)  		--//  ENCODE lzo
	,ship_to_party VARCHAR(1)  		--//  ENCODE lzo
	,prom_prc VARCHAR(1)  		--//  ENCODE lzo
	,ordr_num VARCHAR(1)  		--//  ENCODE lzo
	,ordr_box VARCHAR(1)  		--//  ENCODE lzo
	,ordr_pc VARCHAR(1)  		--//  ENCODE lzo
	,gift_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_bfr_tax_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_bal_amt VARCHAR(1)  		--//  ENCODE lzo
	,sku_per_box VARCHAR(1)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,dstr_cd VARCHAR(6)  		--//  ENCODE lzo
	,dstr_nm VARCHAR(30)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(ims_txn_dt)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_brand;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_KR_SALES_TARGET_AM_BRAND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_brand
(
	brand VARCHAR(30)  		--//  ENCODE lzo
	,account_manager VARCHAR(30)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,jan_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,feb_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,mar_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,apr_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,may_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jun_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jul_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,aug_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,sep_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,oct_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,nov_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,dec_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,country_code VARCHAR(10)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_cust_link;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_KR_SALES_TARGET_AM_CUST_LINK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_cust_link
(
	customer_code VARCHAR(30)  		--//  ENCODE lzo
	,account_manager VARCHAR(50)  		--//  ENCODE lzo
	,country_code VARCHAR(10)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_sls_grp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_KR_SALES_TARGET_AM_SLS_GRP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_kr_sales_target_am_sls_grp
(
	sales_group VARCHAR(30)  		--//  ENCODE lzo
	,brnd VARCHAR(30)  		--//  ENCODE lzo
	,acct_mgr VARCHAR(2000)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,jan_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,feb_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,mar_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,apr_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,may_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jun_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,jul_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,aug_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,sep_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,oct_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,nov_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,dec_trgt_amt NUMERIC(16,5)  		--//  ENCODE lzo
	,country_code VARCHAR(10)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY(acct_mgr)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_onpck_trgt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_ONPCK_TRGT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_onpck_trgt
(
	material_number VARCHAR(50)  		--//  ENCODE lzo
	,description VARCHAR(200)  		--//  ENCODE lzo
	,account_group VARCHAR(100)  		--//  ENCODE lzo
	,month VARCHAR(10)  		--//  ENCODE lzo
	,year VARCHAR(10)  		--//  ENCODE lzo
	,target_quantity NUMERIC(22,2)  		--//  ENCODE az64
	,country_code VARCHAR(20)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(account_group)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(23,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(23,5)  		--//  ENCODE az64
	,stk_rtrn_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stk_recv_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,avg_sell_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(40)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(40)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(17)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,src_mesg_no VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_func_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_date VARCHAR(1)  		--//  ENCODE lzo
	,src_sale_date_form VARCHAR(1)  		--//  ENCODE lzo
	,src_send_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_name VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_name VARCHAR(1)  		--//  ENCODE lzo
	,src_part_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_part_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_part_id VARCHAR(1)  		--//  ENCODE lzo
	,src_part_name VARCHAR(1)  		--//  ENCODE lzo
	,src_sender_id VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_date VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_time VARCHAR(1)  		--//  ENCODE lzo
	,src_file_size VARCHAR(1)  		--//  ENCODE lzo
	,src_file_path VARCHAR(1)  		--//  ENCODE lzo
	,src_lega_tran VARCHAR(1)  		--//  ENCODE lzo
	,src_regi_date VARCHAR(1)  		--//  ENCODE lzo
	,src_line_no VARCHAR(1)  		--//  ENCODE lzo
	,src_instore_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_amnt VARCHAR(1)  		--//  ENCODE lzo
	,src_qty_unit VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_sales VARCHAR(1)  		--//  ENCODE lzo
	,doc_send_date VARCHAR(1)  		--//  ENCODE lzo
	,unit_of_pkg_invt VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun VARCHAR(1)  		--//  ENCODE lzo
	,doc_no VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun_cd VARCHAR(1)  		--//  ENCODE lzo
	,buye_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,provider_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,comp_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_comp VARCHAR(1)  		--//  ENCODE lzo
	,order_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_order VARCHAR(1)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_7eleven;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_7ELEVEN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_7eleven
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(4)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_carrefour;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_CARREFOUR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_carrefour
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(40)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(19)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_cosmed;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_COSMED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_cosmed
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(16)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_costco;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_COSTCO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_costco
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(10)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty NUMERIC(10,0)  		--//  ENCODE az64
	,sls_amt VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty NUMERIC(10,0)  		--//  ENCODE az64
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(6)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,src_mesg_no VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_func_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_date VARCHAR(1)  		--//  ENCODE lzo
	,src_sale_date_form VARCHAR(1)  		--//  ENCODE lzo
	,src_send_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_name VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_name VARCHAR(1)  		--//  ENCODE lzo
	,src_part_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_part_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_part_id VARCHAR(1)  		--//  ENCODE lzo
	,src_part_name VARCHAR(1)  		--//  ENCODE lzo
	,src_sender_id VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_date VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_time VARCHAR(1)  		--//  ENCODE lzo
	,src_file_size VARCHAR(1)  		--//  ENCODE lzo
	,src_file_path VARCHAR(1)  		--//  ENCODE lzo
	,src_lega_tran VARCHAR(1)  		--//  ENCODE lzo
	,src_regi_date VARCHAR(1)  		--//  ENCODE lzo
	,src_line_no VARCHAR(1)  		--//  ENCODE lzo
	,src_instore_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_amnt VARCHAR(1)  		--//  ENCODE lzo
	,src_qty_unit VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_qty VARCHAR(1)  		--//  ENCODE lzo
	,src_unit_of_pkg_sales VARCHAR(5)  		--//  ENCODE lzo
	,src_doc_send_date DATE  		--//  ENCODE az64
	,src_unit_of_pkg_invt VARCHAR(5)  		--//  ENCODE lzo
	,src_doc_fun VARCHAR(6)  		--//  ENCODE lzo
	,src_doc_no VARCHAR(40)  		--//  ENCODE lzo
	,src_doc_fun_cd VARCHAR(6)  		--//  ENCODE lzo
	,src_buye_loc_cd VARCHAR(40)  		--//  ENCODE lzo
	,src_vend_loc_cd VARCHAR(40)  		--//  ENCODE lzo
	,src_provider_loc_cd VARCHAR(40)  		--//  ENCODE lzo
	,src_comp_qty NUMERIC(10,0)  		--//  ENCODE az64
	,src_unit_of_pkg_comp VARCHAR(5)  		--//  ENCODE lzo
	,src_order_qty NUMERIC(10,0)  		--//  ENCODE az64
	,src_unit_of_pkg_order VARCHAR(5)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY(ean_num)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_cust_prod_cd_ean_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_CUST_PROD_CD_EAN_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_cust_prod_cd_ean_map
(
	customer VARCHAR(200)  		--//  ENCODE lzo
	,customer_hierarchy_code VARCHAR(1)  		--//  ENCODE lzo
	,cust_prod_cd VARCHAR(200)  		--//  ENCODE lzo
	,barcode VARCHAR(200)  		--//  ENCODE lzo
	,sap_product_code VARCHAR(1)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_ec;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_EC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_ec
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt NUMERIC(15,2)  		--//  ENCODE az64
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(2)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(pos_dt)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_emart;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_EMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_emart
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(140)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(14)  		--//  ENCODE lzo
	,str_cd VARCHAR(13)  		--//  ENCODE lzo
	,str_nm VARCHAR(50)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stk_rtrn_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stk_recv_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,avg_sell_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(5)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,src_mesg_no VARCHAR(35)  		--//  ENCODE lzo
	,src_mesg_code VARCHAR(3)  		--//  ENCODE lzo
	,src_mesg_func_code VARCHAR(3)  		--//  ENCODE lzo
	,src_mesg_date DATE  		--//  ENCODE az64
	,src_sale_date_form VARCHAR(3)  		--//  ENCODE lzo
	,src_send_code VARCHAR(10)  		--//  ENCODE lzo
	,src_send_ean_code VARCHAR(13)  		--//  ENCODE lzo
	,src_send_name VARCHAR(30)  		--//  ENCODE lzo
	,src_recv_qual VARCHAR(13)  		--//  ENCODE lzo
	,src_recv_ean_code VARCHAR(10)  		--//  ENCODE lzo
	,src_recv_name VARCHAR(35)  		--//  ENCODE lzo
	,src_part_qual VARCHAR(3)  		--//  ENCODE lzo
	,src_part_ean_code VARCHAR(13)  		--//  ENCODE lzo
	,src_part_id VARCHAR(10)  		--//  ENCODE lzo
	,src_part_name VARCHAR(30)  		--//  ENCODE lzo
	,src_sender_id VARCHAR(35)  		--//  ENCODE lzo
	,src_recv_date VARCHAR(10)  		--//  ENCODE lzo
	,src_recv_time VARCHAR(6)  		--//  ENCODE lzo
	,src_file_size NUMERIC(8,0)  		--//  ENCODE az64
	,src_file_path VARCHAR(128)  		--//  ENCODE lzo
	,src_lega_tran VARCHAR(1)  		--//  ENCODE lzo
	,src_regi_date VARCHAR(10)  		--//  ENCODE lzo
	,src_line_no NUMERIC(6,0)  		--//  ENCODE az64
	,src_instore_code VARCHAR(20)  		--//  ENCODE lzo
	,src_mnth_sale_amnt NUMERIC(15,0)  		--//  ENCODE az64
	,src_qty_unit VARCHAR(3)  		--//  ENCODE lzo
	,src_mnth_sale_qty NUMERIC(10,0)  		--//  ENCODE az64
	,unit_of_pkg_sales VARCHAR(1)  		--//  ENCODE lzo
	,doc_send_date VARCHAR(1)  		--//  ENCODE lzo
	,unit_of_pkg_invt VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun VARCHAR(1)  		--//  ENCODE lzo
	,doc_no VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun_cd VARCHAR(1)  		--//  ENCODE lzo
	,buye_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,provider_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,comp_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_comp VARCHAR(1)  		--//  ENCODE lzo
	,order_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_order VARCHAR(1)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_emart_ecvan_ssg;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_EMART_ECVAN_SSG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_emart_ecvan_ssg
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(3)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(255)  		--//  ENCODE lzo
	,str_cd VARCHAR(255)  		--//  ENCODE lzo
	,str_nm VARCHAR(255)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(20,4)  		--//  ENCODE az64
	,unit_prc_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_excl_vat_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stk_rtrn_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stk_recv_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,avg_sell_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(255)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(5)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,src_mesg_no VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_func_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mesg_date VARCHAR(1)  		--//  ENCODE lzo
	,src_sale_date_form VARCHAR(1)  		--//  ENCODE lzo
	,src_send_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_send_name VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_name VARCHAR(1)  		--//  ENCODE lzo
	,src_part_qual VARCHAR(1)  		--//  ENCODE lzo
	,src_part_ean_code VARCHAR(1)  		--//  ENCODE lzo
	,src_part_id VARCHAR(1)  		--//  ENCODE lzo
	,src_part_name VARCHAR(1)  		--//  ENCODE lzo
	,src_sender_id VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_date VARCHAR(1)  		--//  ENCODE lzo
	,src_recv_time VARCHAR(1)  		--//  ENCODE lzo
	,src_file_size VARCHAR(1)  		--//  ENCODE lzo
	,src_file_path VARCHAR(1)  		--//  ENCODE lzo
	,src_lega_tran VARCHAR(1)  		--//  ENCODE lzo
	,src_regi_date VARCHAR(1)  		--//  ENCODE lzo
	,src_line_no VARCHAR(1)  		--//  ENCODE lzo
	,src_instore_code VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_amnt VARCHAR(1)  		--//  ENCODE lzo
	,src_qty_unit VARCHAR(1)  		--//  ENCODE lzo
	,src_mnth_sale_qty VARCHAR(1)  		--//  ENCODE lzo
	,unit_of_pkg_sales VARCHAR(1)  		--//  ENCODE lzo
	,doc_send_date VARCHAR(1)  		--//  ENCODE lzo
	,unit_of_pkg_invt VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun VARCHAR(1)  		--//  ENCODE lzo
	,doc_no VARCHAR(1)  		--//  ENCODE lzo
	,doc_fun_cd VARCHAR(1)  		--//  ENCODE lzo
	,buye_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,provider_loc_cd VARCHAR(1)  		--//  ENCODE lzo
	,comp_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_comp VARCHAR(1)  		--//  ENCODE lzo
	,order_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unit_of_pkg_order VARCHAR(1)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_mannings;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_MANNINGS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_mannings
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty NUMERIC(16,5)  		--//  ENCODE az64
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(8)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_poya;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_POYA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_poya
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(11)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_poya_invnt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_POYA_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_poya_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(1)  		--//  ENCODE lzo
	,str_nm VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,per_box_qty VARCHAR(1)  		--//  ENCODE lzo
	,cust_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,box_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,wk_hold_sls VARCHAR(1)  		--//  ENCODE lzo
	,wk_hold VARCHAR(1)  		--//  ENCODE lzo
	,fst_recv_dt VARCHAR(1)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(1)  		--//  ENCODE lzo
	,dc VARCHAR(1)  		--//  ENCODE lzo
	,stk_cls VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(11)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_promotional_price_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_PROMOTIONAL_PRICE_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_promotional_price_map
(
	customer VARCHAR(200)  		--//  ENCODE lzo
	,barcode VARCHAR(200)  		--//  ENCODE lzo
	,cust_prod_cd VARCHAR(200)  		--//  ENCODE lzo
	,promotional_price NUMERIC(31,4)  		--//  ENCODE az64
	,promotion_start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,promotion_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(customer)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_px_civila;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_PX_CIVILA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_px_civila
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(1)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(9)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_pxcivilia_invnt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_PXCIVILIA_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_pxcivilia_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(40)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,per_box_qty VARCHAR(1)  		--//  ENCODE lzo
	,cust_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,box_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,wk_hold_sls VARCHAR(1)  		--//  ENCODE lzo
	,wk_hold VARCHAR(1)  		--//  ENCODE lzo
	,fst_recv_dt VARCHAR(1)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(1)  		--//  ENCODE lzo
	,dc VARCHAR(40)  		--//  ENCODE lzo
	,stk_cls VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(9)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rank;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_RANK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rank
(
	barcode VARCHAR(100)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code_vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,date_of_preparation DATE  		--//  ENCODE az64
	,distribution_code VARCHAR(40)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pos_date DATE  		--//  ENCODE az64
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,product_status VARCHAR(1)  		--//  ENCODE lzo
	,product_volume VARCHAR(40)  		--//  ENCODE lzo
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
	,sales_rvenue_excl_vat NUMERIC(23,5)  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,sl_no VARCHAR(1)  		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,unit_price NUMERIC(23,5)  		--//  ENCODE az64
	,currency VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,src_sys_cd VARCHAR(17)  		--//  ENCODE lzo
	,rnk numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rt_mart;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_RT_MART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rt_mart
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(60)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,src_sys_cd VARCHAR(17)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rtmart_invnt;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_RTMART_INVNT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_rtmart_invnt
(
	invnt_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(60)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,per_box_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cust_invnt_qty NUMERIC(16,5)  		--//  ENCODE az64
	,box_invnt_qty NUMERIC(16,5)  		--//  ENCODE az64
	,wk_hold_sls NUMERIC(16,5)  		--//  ENCODE az64
	,wk_hold NUMERIC(16,5)  		--//  ENCODE az64
	,fst_recv_dt VARCHAR(10)  		--//  ENCODE lzo
	,dsct_dt VARCHAR(10)  		--//  ENCODE lzo
	,dc VARCHAR(40)  		--//  ENCODE lzo
	,stk_cls VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(17)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(vend_prod_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_store_product;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_STORE_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_store_product
(
	line_no VARCHAR(10)  		--//  ENCODE lzo
	,product_cd VARCHAR(20)  		--//  ENCODE lzo
	,product_nm VARCHAR(60)  		--//  ENCODE lzo
	,store_cd VARCHAR(10)  		--//  ENCODE lzo
	,store_nm VARCHAR(60)  		--//  ENCODE lzo
	,vendor VARCHAR(20)  		--//  ENCODE lzo
	,prm_strt_dt DATE  		--//  ENCODE az64
	,prm_end_dt DATE  		--//  ENCODE az64
	,sales_tgt VARCHAR(10)  		--//  ENCODE lzo
	,amt_order VARCHAR(10)  		--//  ENCODE lzo
	,warehouse_dt VARCHAR(10)  		--//  ENCODE lzo
	,item_type VARCHAR(3)  		--//  ENCODE lzo
	,unit_of_pkg_item VARCHAR(20)  		--//  ENCODE lzo
	,pack_size VARCHAR(10)  		--//  ENCODE lzo
	,delivery_method VARCHAR(1)  		--//  ENCODE lzo
	,style VARCHAR(10)  		--//  ENCODE lzo
	,occ_no VARCHAR(20)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(6)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_str_cd_sold_to_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_STR_CD_SOLD_TO_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_str_cd_sold_to_map
(
	name VARCHAR(500)  		--//  ENCODE lzo
	,seqid NUMERIC(31,0)  		--//  ENCODE az64
	,str_nm VARCHAR(200)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(200)  		--//  ENCODE lzo
	,conv_sys_cd VARCHAR(200)  		--//  ENCODE lzo
	,str_type VARCHAR(200)  		--//  ENCODE lzo
	,cust_str_cd VARCHAR(200)  		--//  ENCODE lzo
	,sold_to_cd VARCHAR(200)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY(cust_str_cd)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_watson_store;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_WATSON_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_pos_watson_store
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(1)  		--//  ENCODE lzo
	,vend_nm VARCHAR(1)  		--//  ENCODE lzo
	,prod_nm VARCHAR(1)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(500)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(1)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(40)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt VARCHAR(1)  		--//  ENCODE lzo
	,sls_excl_vat_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_rtrn_amt VARCHAR(1)  		--//  ENCODE lzo
	,stk_recv_amt VARCHAR(1)  		--//  ENCODE lzo
	,avg_sell_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_ship_qty VARCHAR(1)  		--//  ENCODE lzo
	,cum_rtrn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_takn_qty VARCHAR(1)  		--//  ENCODE lzo
	,web_ordr_acpt_qty VARCHAR(1)  		--//  ENCODE lzo
	,dc_invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_qty VARCHAR(1)  		--//  ENCODE lzo
	,invnt_amt VARCHAR(1)  		--//  ENCODE lzo
	,invnt_dt VARCHAR(1)  		--//  ENCODE lzo
	,serial_num VARCHAR(1)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(1)  		--//  ENCODE lzo
	,prod_type VARCHAR(1)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(1)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(1)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(1)  		--//  ENCODE lzo
	,cat_big VARCHAR(1)  		--//  ENCODE lzo
	,cat_mid VARCHAR(1)  		--//  ENCODE lzo
	,cat_small VARCHAR(1)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(1)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(1)  		--//  ENCODE lzo
	,dist_cd VARCHAR(1)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(3)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(1)  		--//  ENCODE lzo
	,src_seq_num VARCHAR(1)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(300)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_prc_condition_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_PRC_CONDITION_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_prc_condition_map
(
	sls_org VARCHAR(25)  		--//  ENCODE lzo
	,sales_grp_cd VARCHAR(18)  		--//  ENCODE lzo
	,cnd_type VARCHAR(25)  		--//  ENCODE lzo
	,matl_num VARCHAR(25)  		--//  ENCODE lzo
	,sold_to_cust_cd VARCHAR(25)  		--//  ENCODE lzo
	,price NUMERIC(14,2)  		--//  ENCODE az64
	,vld_frm DATE  		--//  ENCODE az64
	,vld_to DATE  		--//  ENCODE az64
	,cond_curr VARCHAR(15)  		--//  ENCODE lzo
	,doc_currcy VARCHAR(15)  		--//  ENCODE lzo
	,recordmode CHAR(1)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(25)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_sales_cust_prod_master;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_SALES_CUST_PROD_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_sales_cust_prod_master
(
	sales_grp_cd VARCHAR(200)  		--//  ENCODE lzo
	,src_sys_cd VARCHAR(200)  		--//  ENCODE lzo
	,product_nm VARCHAR(200)  		--//  ENCODE lzo
	,cust_prod_cd VARCHAR(200)  		--//  ENCODE lzo
	,ean_cd VARCHAR(200)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(200)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_sales_store_master;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_SALES_STORE_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_sales_store_master
(
	channel VARCHAR(200)  		--//  ENCODE lzo
	,store_type VARCHAR(200)  		--//  ENCODE lzo
	,sales_grp_cd VARCHAR(200)  		--//  ENCODE lzo
	,sold_to VARCHAR(200)  		--//  ENCODE lzo
	,store_nm VARCHAR(200)  		--//  ENCODE lzo
	,cust_store_cd VARCHAR(200)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(200)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm VARCHAR(1)  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_trd_promo_actl;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_TRD_PROMO_ACTL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_trd_promo_actl
(
	profit_center VARCHAR(100)  		--//  ENCODE lzo
	,profit_center_desc VARCHAR(200)  		--//  ENCODE lzo
	,customer_channel VARCHAR(100)  		--//  ENCODE lzo
	,customer_headquarter_code VARCHAR(200)  		--//  ENCODE lzo
	,country_code VARCHAR(20)  		--//  ENCODE lzo
	,currency_code VARCHAR(3)  		--//  ENCODE lzo
	,year VARCHAR(4)  		--//  ENCODE lzo
	,jan_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,jan_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,may_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,may_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,may_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,may_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_under_processing NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_approved_but_payment_not_yet NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_payment NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_actual_trade_promotion NUMERIC(16,2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_trd_promo_pln;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_TRD_PROMO_PLN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_trd_promo_pln
(
	profit_center VARCHAR(100)  		--//  ENCODE lzo
	,profit_center_nm VARCHAR(100)  		--//  ENCODE lzo
	,customer_channel VARCHAR(100)  		--//  ENCODE lzo
	,customer_channel_nm VARCHAR(100)  		--//  ENCODE lzo
	,customer_hq_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_hq_nm VARCHAR(200)  		--//  ENCODE lzo
	,country_code VARCHAR(20)  		--//  ENCODE lzo
	,currency_code VARCHAR(3)  		--//  ENCODE lzo
	,year VARCHAR(4)  		--//  ENCODE lzo
	,jan_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,feb_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,mar_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,apr_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,may_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,jun_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,jul_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,aug_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,sep_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,oct_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,nov_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,dec_tp_plan NUMERIC(16,2)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_bu_forecast_prod_hier;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_TW_BU_FORECAST_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_bu_forecast_prod_hier
(
	bu_version VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_month VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc VARCHAR(4)  		--//  ENCODE lzo
	,sls_ofc_desc VARCHAR(40)  		--//  ENCODE lzo
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE lzo
	,lph_level_6 VARCHAR(255)  		--//  ENCODE lzo
	,price_off DOUBLE PRECISION
	,display DOUBLE PRECISION
	,dm DOUBLE PRECISION
	,other_support DOUBLE PRECISION
	,sr DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
	,pre_sales DOUBLE PRECISION
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_bu_forecast_sku;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_TW_BU_FORECAST_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_bu_forecast_sku
(
	bu_version VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_month VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,sls_ofc VARCHAR(4)  		--//  ENCODE lzo
	,sls_ofc_desc VARCHAR(40)  		--//  ENCODE lzo
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE lzo
	,sap_code VARCHAR(30)  		--//  ENCODE lzo
	,system_list_price DOUBLE PRECISION
	,gross_invoice_price DOUBLE PRECISION
	,gross_invoice_price_less_terms DOUBLE PRECISION
	,rf_sell_out_qty DOUBLE PRECISION
	,rf_sell_in_qty DOUBLE PRECISION
	,price_off DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_ims_dstr_prod_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_ITG_TW_IMS_DSTR_PROD_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_itg_tw_ims_dstr_prod_map
(
	dstr_code VARCHAR(10)  		--//  ENCODE lzo
	,dstr_name VARCHAR(20)  		--//  ENCODE lzo
	,dstr_product_code VARCHAR(20)  		--//  ENCODE lzo
	,dstr_product_name VARCHAR(100)  		--//  ENCODE lzo
	,ean_code VARCHAR(20)  		--//  ENCODE lzo
	,tgt_crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY(dstr_code)
;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_bpa_report;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_BPA_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_bpa_report
(
	date VARCHAR(100)  		--//  ENCODE zstd
	,bidding_type VARCHAR(100)  		--//  ENCODE zstd
	,sales_method VARCHAR(100)  		--//  ENCODE zstd
	,campaign_start_date VARCHAR(100)  		--//  ENCODE zstd
	,campaign_end_date VARCHAR(100)  		--//  ENCODE zstd
	,ad_objectives VARCHAR(100)  		--//  ENCODE zstd
	,campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,campaign_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_group VARCHAR(100)  		--//  ENCODE zstd
	,ad_group_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_name VARCHAR(100)  		--//  ENCODE zstd
	,template_type VARCHAR(100)  		--//  ENCODE zstd
	,advertisement_id VARCHAR(100)  		--//  ENCODE zstd
	,impression_area VARCHAR(100)  		--//  ENCODE zstd
	,material_type VARCHAR(100)  		--//  ENCODE zstd
	,material VARCHAR(100)  		--//  ENCODE zstd
	,material_id VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_execution_product_name VARCHAR(100)  		--//  ENCODE zstd
	,ad_execution_option_id VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_type VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_name VARCHAR(100)  		--//  ENCODE zstd
	,landing_page_id VARCHAR(100)  		--//  ENCODE zstd
	,impressed_keywords VARCHAR(100)  		--//  ENCODE zstd
	,input_keywords VARCHAR(100)  		--//  ENCODE zstd
	,keyword_extension_type VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(100)  		--//  ENCODE zstd
	,impression_count VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,ad_cost VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_qty_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_qty_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_qty_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_qty_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_brand_ranking;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_BRAND_RANKING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_brand_ranking
(
	category_depth1 VARCHAR(30)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(30)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(50)  		--//  ENCODE zstd
	,ranking VARCHAR(4)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,jnj_brand VARCHAR(20)  		--//  ENCODE zstd
	,rank_change VARCHAR(5)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_customer_brand_trend;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_CUSTOMER_BRAND_TREND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_customer_brand_trend
(
	date_yyyymm VARCHAR(6)  		--//  ENCODE zstd
	,coupang_id VARCHAR(15)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(30)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,new_user_count VARCHAR(10)  		--//  ENCODE zstd
	,curr_user_count VARCHAR(10)  		--//  ENCODE zstd
	,tot_user_count VARCHAR(10)  		--//  ENCODE zstd
	,new_user_sales_amt NUMERIC(15,2)  		--//  ENCODE az64
	,curr_user_sales_amt NUMERIC(15,2)  		--//  ENCODE az64
	,new_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
	,curr_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
	,tot_user_avg_product_sales_price NUMERIC(10,2)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_daily_brand_reviews;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_DAILY_BRAND_REVIEWS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_daily_brand_reviews
(
	review_date VARCHAR(10)  		--//  ENCODE zstd
	,brand VARCHAR(30)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(15)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(200)  		--//  ENCODE zstd
	,review_score_star VARCHAR(1)  		--//  ENCODE zstd
	,review_contents VARCHAR(65535)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_pa_report;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PA_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_pa_report
(
	date VARCHAR(100)  		--//  ENCODE zstd
	,bidding_type VARCHAR(100)  		--//  ENCODE zstd
	,sales_method VARCHAR(100)  		--//  ENCODE zstd
	,ad_types VARCHAR(100)  		--//  ENCODE zstd
	,campaign_id VARCHAR(100)  		--//  ENCODE zstd
	,campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,ad_groups VARCHAR(250)  		--//  ENCODE zstd
	,ad_execution_product_name VARCHAR(250)  		--//  ENCODE zstd
	,ad_execution_option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_con_revenue_gen_product_nm VARCHAR(250)  		--//  ENCODE zstd
	,ad_con_revenue_gen_product_option_id VARCHAR(100)  		--//  ENCODE zstd
	,ad_impression_area VARCHAR(100)  		--//  ENCODE zstd
	,keyword VARCHAR(100)  		--//  ENCODE zstd
	,impression_count VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ad_cost VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_quantity_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_quantity_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_orders_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_sales_quantity_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_sales_quantity_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_conversion_sales_14d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_1d VARCHAR(100)  		--//  ENCODE zstd
	,total_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,direct_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,indirect_ad_return_14d VARCHAR(100)  		--//  ENCODE zstd
	,campaign_start_date VARCHAR(100)  		--//  ENCODE zstd
	,campaign_end_date VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_master;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_master
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_daily;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_RANKING_DAILY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_daily
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_monthly;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_RANKING_MONTHLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_monthly
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_weekly;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_RANKING_WEEKLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_ranking_weekly
(
	product_ranking_date VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_summary_monthly;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_SUMMARY_MONTHLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_summary_monthly
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_summary_weekly;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCT_SUMMARY_WEEKLY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_product_summary_weekly
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,all_brand VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_id VARCHAR(255)  		--//  ENCODE zstd
	,coupang_sku_name VARCHAR(20000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_productsalereport;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_PRODUCTSALEREPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_productsalereport
(
	transaction_date VARCHAR(255)  		--//  ENCODE lzo
	,product_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,skuid VARCHAR(255)  		--//  ENCODE lzo
	,sku_name VARCHAR(255)  		--//  ENCODE lzo
	,vendor_item_id VARCHAR(255)  		--//  ENCODE lzo
	,vendor_item_name VARCHAR(255)  		--//  ENCODE lzo
	,regular_delivery VARCHAR(255)  		--//  ENCODE lzo
	,product_category_high VARCHAR(255)  		--//  ENCODE lzo
	,product_category_mid VARCHAR(255)  		--//  ENCODE lzo
	,product_category_low VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,gmv VARCHAR(255)  		--//  ENCODE lzo
	,units_sold VARCHAR(255)  		--//  ENCODE lzo
	,return_units VARCHAR(255)  		--//  ENCODE lzo
	,cogs VARCHAR(255)  		--//  ENCODE lzo
	,amv VARCHAR(255)  		--//  ENCODE lzo
	,coupon_discount VARCHAR(255)  		--//  ENCODE lzo
	,instant_discount_price VARCHAR(255)  		--//  ENCODE lzo
	,asp VARCHAR(255)  		--//  ENCODE lzo
	,order_count VARCHAR(255)  		--//  ENCODE lzo
	,ordered_customer_count VARCHAR(255)  		--//  ENCODE lzo
	,unit_price VARCHAR(255)  		--//  ENCODE lzo
	,conversion_rate VARCHAR(255)  		--//  ENCODE lzo
	,pv VARCHAR(255)  		--//  ENCODE lzo
	,reg_dlvry_gmv VARCHAR(255)  		--//  ENCODE lzo
	,reg_dlvry_cogs VARCHAR(255)  		--//  ENCODE lzo
	,reg_dlvry_rate VARCHAR(255)  		--//  ENCODE lzo
	,reg_dlvry_units_sold VARCHAR(255)  		--//  ENCODE lzo
	,reg_dlvry_return_units VARCHAR(255)  		--//  ENCODE lzo
	,review_count VARCHAR(255)  		--//  ENCODE lzo
	,avg_product_review_rate VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_search_keyword_by_category;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_SEARCH_KEYWORD_BY_CATEGORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_search_keyword_by_category
(
	by_search_keyword VARCHAR(255)  		--//  ENCODE zstd
	,by_product_ranking VARCHAR(255)  		--//  ENCODE zstd
	,category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,search_keyword VARCHAR(2000)  		--//  ENCODE zstd
	,product_ranking VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(2000)  		--//  ENCODE zstd
	,jnj_product_flag VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_search_keyword_by_product;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_COUPANG_SEARCH_KEYWORD_BY_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_coupang_search_keyword_by_product
(
	category_depth1 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth2 VARCHAR(255)  		--//  ENCODE zstd
	,category_depth3 VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(2000)  		--//  ENCODE zstd
	,ranking VARCHAR(255)  		--//  ENCODE zstd
	,search_keyword VARCHAR(255)  		--//  ENCODE zstd
	,click_rate NUMERIC(18,5)  		--//  ENCODE az64
	,cart_transition_rate NUMERIC(18,5)  		--//  ENCODE az64
	,purchase_conversion_rate NUMERIC(18,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_coupang_price;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_COUPANG_PRICE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_coupang_price
(
	report_date VARCHAR(100)  		--//  ENCODE zstd
	,trusted_upc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_rpc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_mpc VARCHAR(100)  		--//  ENCODE zstd
	,trusted_product_description VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,online_store VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(100)  		--//  ENCODE zstd
	,dimension1 VARCHAR(100)  		--//  ENCODE zstd
	,sub_category VARCHAR(100)  		--//  ENCODE zstd
	,brand_subcategory VARCHAR(100)  		--//  ENCODE zstd
	,dimension4 VARCHAR(100)  		--//  ENCODE zstd
	,dimension5 VARCHAR(100)  		--//  ENCODE zstd
	,dimension6 VARCHAR(100)  		--//  ENCODE zstd
	,seller VARCHAR(100)  		--//  ENCODE zstd
	,power_sku VARCHAR(100)  		--//  ENCODE zstd
	,availability_status VARCHAR(100)  		--//  ENCODE zstd
	,currency VARCHAR(100)  		--//  ENCODE zstd
	,observed_price VARCHAR(100)  		--//  ENCODE zstd
	,store_list_price VARCHAR(100)  		--//  ENCODE zstd
	,min_price VARCHAR(100)  		--//  ENCODE zstd
	,max_price VARCHAR(100)  		--//  ENCODE zstd
	,min_max_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,min_max_diff_price VARCHAR(100)  		--//  ENCODE zstd
	,msrp VARCHAR(100)  		--//  ENCODE zstd
	,msrp_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,msrp_diff_amount VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_price VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_diff_pct VARCHAR(100)  		--//  ENCODE zstd
	,previous_day_diff_amount VARCHAR(100)  		--//  ENCODE zstd
	,promotion_text VARCHAR(100)  		--//  ENCODE zstd
	,url VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_coupang_search_keyword;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_COUPANG_SEARCH_KEYWORD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_coupang_search_keyword
(
	by_search_term_ranking VARCHAR(250)  		--//  ENCODE zstd
	,product_ranking_criteria VARCHAR(250)  		--//  ENCODE zstd
	,category_1 VARCHAR(250)  		--//  ENCODE zstd
	,category_2 VARCHAR(250)  		--//  ENCODE zstd
	,category_3 VARCHAR(250)  		--//  ENCODE zstd
	,ranking VARCHAR(250)  		--//  ENCODE zstd
	,query VARCHAR(250)  		--//  ENCODE zstd
	,product_standings VARCHAR(250)  		--//  ENCODE zstd
	,goods VARCHAR(250)  		--//  ENCODE zstd
	,my_products VARCHAR(250)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_date VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_linkprice;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_LINKPRICE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_linkprice
(
	temp1 VARCHAR(100)  		--//  ENCODE zstd
	,temp2 VARCHAR(100)  		--//  ENCODE zstd
	,campaign_name VARCHAR(100)  		--//  ENCODE zstd
	,group_name VARCHAR(100)  		--//  ENCODE zstd
	,material_id VARCHAR(100)  		--//  ENCODE zstd
	,product_number VARCHAR(250)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,impressison_area VARCHAR(100)  		--//  ENCODE zstd
	,keyword VARCHAR(100)  		--//  ENCODE zstd
	,impression VARCHAR(100)  		--//  ENCODE zstd
	,click_count VARCHAR(100)  		--//  ENCODE zstd
	,ctr VARCHAR(100)  		--//  ENCODE zstd
	,impression_ranking VARCHAR(100)  		--//  ENCODE zstd
	,avg_click_rate VARCHAR(100)  		--//  ENCODE zstd
	,consumed_cost VARCHAR(100)  		--//  ENCODE zstd
	,conversion_count VARCHAR(100)  		--//  ENCODE zstd
	,conversion_rate VARCHAR(100)  		--//  ENCODE zstd
	,purchased_amount VARCHAR(100)  		--//  ENCODE zstd
	,roas VARCHAR(100)  		--//  ENCODE zstd
	,previous_roas VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_date VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_gmv;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_NAVER_GMV		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_gmv
(
	product_category_l VARCHAR(100)  		--//  ENCODE zstd
	,product_category_m VARCHAR(100)  		--//  ENCODE zstd
	,product_category_s VARCHAR(100)  		--//  ENCODE zstd
	,product_category_detail VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,product_id VARCHAR(100)  		--//  ENCODE zstd
	,number_of_payments VARCHAR(100)  		--//  ENCODE zstd
	,quantity_of_products_paid VARCHAR(100)  		--//  ENCODE zstd
	,mobile_ratio_qty_prdt_paid VARCHAR(100)  		--//  ENCODE zstd
	,payment_amount VARCHAR(100)  		--//  ENCODE zstd
	,mobile_ratio_payment_amount VARCHAR(100)  		--//  ENCODE zstd
	,pymt_amt_per_prdt_allowance VARCHAR(100)  		--//  ENCODE zstd
	,coupon_total VARCHAR(100)  		--//  ENCODE zstd
	,product_coupon VARCHAR(100)  		--//  ENCODE zstd
	,order_coupon VARCHAR(100)  		--//  ENCODE zstd
	,refund_number VARCHAR(100)  		--//  ENCODE zstd
	,refund_amount VARCHAR(100)  		--//  ENCODE zstd
	,refund_rate VARCHAR(100)  		--//  ENCODE zstd
	,refund_qty VARCHAR(100)  		--//  ENCODE zstd
	,refund_qty_paid_product VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_date VARCHAR(10)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_keyword_search_volume;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_NAVER_KEYWORD_SEARCH_VOLUME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_keyword_search_volume
(
	no VARCHAR(255)  		--//  ENCODE zstd
	,keyword VARCHAR(255)  		--//  ENCODE zstd
	,total_monthly_searches VARCHAR(255)  		--//  ENCODE zstd
	,monthly_desktop_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,monthl_mobile_searches VARCHAR(255)  		--//  ENCODE zstd
	,average_daily_search_volume VARCHAR(255)  		--//  ENCODE zstd
	,keyword_first_appearance_date VARCHAR(255)  		--//  ENCODE zstd
	,keyword_rating VARCHAR(255)  		--//  ENCODE zstd
	,adult_keywords VARCHAR(255)  		--//  ENCODE zstd
	,blogrecent_mnthly_publications VARCHAR(255)  		--//  ENCODE zstd
	,blogtotal_period_pbliction_vol VARCHAR(255)  		--//  ENCODE zstd
	,caferecent_monthly_issue VARCHAR(255)  		--//  ENCODE zstd
	,cafe_total_period_issue VARCHAR(255)  		--//  ENCODE zstd
	,view_recent_mnthly_publication VARCHAR(255)  		--//  ENCODE zstd
	,view_total_period_issuance VARCHAR(255)  		--//  ENCODE zstd
	,search_volume_until_yesterday VARCHAR(255)  		--//  ENCODE zstd
	,search_vol_end_of_the_month VARCHAR(255)  		--//  ENCODE zstd
	,blog_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,cafe_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,view_saturation_index VARCHAR(255)  		--//  ENCODE zstd
	,related_keywords VARCHAR(4000)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_date VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_search_channel;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_DADS_NAVER_SEARCH_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_dads_naver_search_channel
(
	channel_properties VARCHAR(250)  		--//  ENCODE zstd
	,channel_groups VARCHAR(250)  		--//  ENCODE zstd
	,channel_name VARCHAR(250)  		--//  ENCODE zstd
	,keyword VARCHAR(250)  		--//  ENCODE zstd
	,customers VARCHAR(250)  		--//  ENCODE zstd
	,inlet_water VARCHAR(250)  		--//  ENCODE zstd
	,number_of_pages VARCHAR(250)  		--//  ENCODE zstd
	,pages_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,number_of_payments VARCHAR(250)  		--//  ENCODE zstd
	,payment_rate_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_per_inflow VARCHAR(250)  		--//  ENCODE zstd
	,no_of_payments_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_rate_per_inflow_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_14d VARCHAR(250)  		--//  ENCODE zstd
	,payment_amount_per_inflow_14d VARCHAR(250)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_date VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_coupang_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_COUPANG_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_coupang_temp
(
	ean_number VARCHAR(50)  		--//  ENCODE lzo
	,seller VARCHAR(256)  		--//  ENCODE lzo
	,order_date VARCHAR(50)  		--//  ENCODE lzo
	,seller_product_code VARCHAR(50)  		--//  ENCODE lzo
	,seller_product_name VARCHAR(256)  		--//  ENCODE lzo
	,product_name VARCHAR(256)  		--//  ENCODE lzo
	,brand VARCHAR(256)  		--//  ENCODE lzo
	,seller_product_option VARCHAR(256)  		--//  ENCODE lzo
	,order_qty VARCHAR(256)  		--//  ENCODE lzo
	,product_qty VARCHAR(100)  		--//  ENCODE lzo
	,product_value VARCHAR(100)  		--//  ENCODE lzo
	,invoice_value VARCHAR(100)  		--//  ENCODE lzo
	,delivery_service_company VARCHAR(100)  		--//  ENCODE lzo
	,shipment VARCHAR(100)  		--//  ENCODE lzo
	,delivery_date VARCHAR(100)  		--//  ENCODE lzo
	,supply VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_inventory;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_DSTR_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_inventory
(
	dstr_cd VARCHAR(10)  		--//  ENCODE zstd
	,matl_num VARCHAR(30)  		--//  ENCODE zstd
	,ean VARCHAR(30)  		--//  ENCODE zstd
	,brand_name VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(500)  		--//  ENCODE zstd
	,inv_date DATE  		--//  ENCODE az64
	,inventory_qty NUMERIC(38,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_DSTR_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_sellout
(
	dstr_cd VARCHAR(10)  		--//  ENCODE zstd
	,matl_num VARCHAR(30)  		--//  ENCODE zstd
	,ean VARCHAR(30)  		--//  ENCODE zstd
	,brand_name VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(500)  		--//  ENCODE zstd
	,so_date DATE  		--//  ENCODE az64
	,so_qty NUMERIC(38,5)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_sellout_stock;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_DSTR_SELLOUT_STOCK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_dstr_sellout_stock
(
	sap VARCHAR(50)  		--//  ENCODE zstd
	,ean_code VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,sku_name VARCHAR(255)  		--//  ENCODE zstd
	,remark VARCHAR(255)  		--//  ENCODE zstd
	,dstr_cd VARCHAR(50)  		--//  ENCODE zstd
	,data_src VARCHAR(50)  		--//  ENCODE zstd
	,quantity NUMERIC(38,5)  		--//  ENCODE az64
	,transaction_date DATE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_naver_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_NAVER_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_naver_sellout
(
	transaction_date VARCHAR(50)  		--//  ENCODE lzo
	,ean VARCHAR(30)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,sales_qty NUMERIC(20,4)  		--//  ENCODE az64
	,year VARCHAR(20)  		--//  ENCODE lzo
	,day VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,week VARCHAR(20)  		--//  ENCODE lzo
	,invoice NUMERIC(20,4)  		--//  ENCODE az64
	,sales_amount NUMERIC(20,4)  		--//  ENCODE az64
	,brand_name VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_naver_sellout_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_ECOM_NAVER_SELLOUT_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_ecom_naver_sellout_temp
(
	cust_nm VARCHAR(5)  		--//  ENCODE lzo
	,cust_code VARCHAR(1)  		--//  ENCODE lzo
	,sub_cust_nm VARCHAR(1)  		--//  ENCODE lzo
	,ean_num VARCHAR(30)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,prod_desc VARCHAR(100)  		--//  ENCODE lzo
	,year VARCHAR(20)  		--//  ENCODE lzo
	,monthno VARCHAR(2)  		--//  ENCODE lzo
	,weekint numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day VARCHAR(20)  		--//  ENCODE lzo
	,transaction_date VARCHAR(50)  		--//  ENCODE lzo
	,sellout_qty NUMERIC(20,4)  		--//  ENCODE az64
	,sellout_amount NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(37)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_otc_inventory;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_OTC_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_otc_inventory
(
	mnth_id VARCHAR(20)  		--//  ENCODE lzo
	,matl_num VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,product_name VARCHAR(255)  		--//  ENCODE lzo
	,distributor_cd VARCHAR(50)  		--//  ENCODE lzo
	,unit_price VARCHAR(100)  		--//  ENCODE lzo
	,inv_qty VARCHAR(100)  		--//  ENCODE lzo
	,inv_amt VARCHAR(100)  		--//  ENCODE lzo
	,filename VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_otc_inventory_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_OTC_INVENTORY_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_otc_inventory_temp
(
	matl_num VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,distributor_cd VARCHAR(50)  		--//  ENCODE zstd
	,unit_price VARCHAR(100)  		--//  ENCODE zstd
	,inv_qty VARCHAR(100)  		--//  ENCODE zstd
	,inv_amt VARCHAR(100)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_costco_vmimst;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_COSTCO_VMIMST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_costco_vmimst
(
	line_no VARCHAR(10)  		--//  ENCODE zstd
	,product_cd VARCHAR(20)  		--//  ENCODE zstd
	,product_nm VARCHAR(60)  		--//  ENCODE zstd
	,store_cd VARCHAR(10)  		--//  ENCODE zstd
	,store_nm VARCHAR(60)  		--//  ENCODE zstd
	,vendor VARCHAR(20)  		--//  ENCODE zstd
	,prm_strt_dt DATE  		--//  ENCODE zstd
	,prm_end_dt DATE  		--//  ENCODE zstd
	,sales_tgt VARCHAR(10)  		--//  ENCODE zstd
	,amt_order VARCHAR(10)  		--//  ENCODE zstd
	,warehouse_dt VARCHAR(10)  		--//  ENCODE zstd
	,item_type VARCHAR(3)  		--//  ENCODE zstd
	,unit_of_pkg_item VARCHAR(20)  		--//  ENCODE zstd
	,pack_size VARCHAR(10)  		--//  ENCODE zstd
	,delivery_method VARCHAR(1)  		--//  ENCODE zstd
	,style VARCHAR(10)  		--//  ENCODE zstd
	,occ_no VARCHAR(20)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_eland;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_ELAND		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_eland
(
	pos_date DATE  		--//  ENCODE az64
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,currency VARCHAR(10)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_rvenue_incl_vat NUMERIC(16,5)  		--//  ENCODE az64
	,sales_rvenue_excl_vat NUMERIC(16,5)  		--//  ENCODE az64
	,filename VARCHAR(60)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_eland_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_ELAND_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_eland_temp
(
	pos_date DATE NOT NULL 		--//  ENCODE az64
	,store_code VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,currency VARCHAR(10)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_rvenue_incl_vat NUMERIC(16,5)  		--//  ENCODE az64
	,sales_rvenue_excl_vat NUMERIC(16,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_emart_ssg;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_EMART_SSG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_emart_ssg
(
	str_nm VARCHAR(255)  		--//  ENCODE zstd
	,str_cd VARCHAR(255)  		--//  ENCODE zstd
	,team_nm VARCHAR(255)  		--//  ENCODE zstd
	,lrg_classification_nm VARCHAR(255)  		--//  ENCODE zstd
	,mid_classification_nm VARCHAR(255)  		--//  ENCODE zstd
	,sub_classified_nm VARCHAR(255)  		--//  ENCODE zstd
	,offline_ean VARCHAR(255)  		--//  ENCODE zstd
	,ean VARCHAR(255)  		--//  ENCODE zstd
	,prod_nm VARCHAR(255)  		--//  ENCODE zstd
	,pos_dt VARCHAR(255)  		--//  ENCODE zstd
	,sellout_qty VARCHAR(255)  		--//  ENCODE zstd
	,sellout_amt VARCHAR(255)  		--//  ENCODE zstd
	,suppliers VARCHAR(255)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_emart_ssg_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_EMART_SSG_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_emart_ssg_temp
(
	str_nm VARCHAR(255)  		--//  ENCODE lzo
	,team_nm VARCHAR(255)  		--//  ENCODE lzo
	,lrg_classification_nm VARCHAR(255)  		--//  ENCODE lzo
	,mid_classification_nm VARCHAR(255)  		--//  ENCODE lzo
	,sub_classified_nm VARCHAR(255)  		--//  ENCODE lzo
	,offline_ean VARCHAR(255)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,prod_nm VARCHAR(255)  		--//  ENCODE lzo
	,pos_dt VARCHAR(255)  		--//  ENCODE lzo
	,sellout_qty VARCHAR(255)  		--//  ENCODE lzo
	,sellout_amt VARCHAR(255)  		--//  ENCODE lzo
	,suppliers VARCHAR(255)  		--//  ENCODE lzo
	,product_type VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_HOMEPLUS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus
(
	pos_date DATE NOT NULL 		--//  ENCODE az64
	,store_code VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,bar_code VARCHAR(100)  		--//  ENCODE lzo
	,unit_price VARCHAR(20)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue VARCHAR(20)  		--//  ENCODE lzo
	,date_of_preparation DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,distribution_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_online;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_HOMEPLUS_ONLINE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_online
(
	pos_date VARCHAR(20)  		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,bar_code VARCHAR(40)  		--//  ENCODE lzo
	,unit_price VARCHAR(20)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue VARCHAR(20)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_online_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_HOMEPLUS_ONLINE_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_online_temp
(
	posdate VARCHAR(20)  		--//  ENCODE lzo
	,store_category VARCHAR(40)  		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,storename VARCHAR(100)  		--//  ENCODE lzo
	,bar_code VARCHAR(40)  		--//  ENCODE lzo
	,tpnb VARCHAR(40)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,size VARCHAR(10)  		--//  ENCODE lzo
	,color VARCHAR(25)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,total VARCHAR(20)  		--//  ENCODE lzo
	,employee_discount_amount VARCHAR(20)  		--//  ENCODE lzo
	,sales_revenue VARCHAR(20)  		--//  ENCODE lzo
	,online_sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,online_sales_amount VARCHAR(20)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_HOMEPLUS_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_homeplus_temp
(
	pos_date DATE NOT NULL 		--//  ENCODE az64
	,store_code VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,bar_code VARCHAR(100)  		--//  ENCODE lzo
	,unit_price VARCHAR(20)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue VARCHAR(20)  		--//  ENCODE lzo
	,date_of_preparation DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,distribution_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lohbs;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOHBS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lohbs
(
	store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,product_volume VARCHAR(40)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
	,pos_date VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lohbs_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOHBS_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lohbs_temp
(
	store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,product_volume VARCHAR(40)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_mart;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOTTE_MART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_mart
(
	store_name VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
	,pos_date VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_mart_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOTTE_MART_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_mart_temp
(
	store_name VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_super;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOTTE_SUPER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_super
(
	store_name VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,product_volume VARCHAR(40)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
	,pos_date VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_super_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_LOTTE_SUPER_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_lotte_super_temp
(
	store_name VARCHAR(100) NOT NULL 		--//  ENCODE lzo
	,store_code VARCHAR(40)  		--//  ENCODE lzo
	,customer_code VARCHAR(40)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,product_volume VARCHAR(40)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_revenue VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_olive_young;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_OLIVE_YOUNG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_olive_young
(
	pos_date DATE NOT NULL 		--//  ENCODE az64
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_price NUMERIC(16,5)  		--//  ENCODE az64
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
	,filename VARCHAR(60)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_olive_young_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_OLIVE_YOUNG_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_olive_young_temp
(
	pos_date DATE NOT NULL 		--//  ENCODE az64
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,number_of_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_price NUMERIC(16,5)  		--//  ENCODE az64
	,sales_revenue NUMERIC(16,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_prc_condition_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_POS_PRC_CONDITION_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_pos_prc_condition_map
(
	sls_org VARCHAR(25)  		--//  ENCODE zstd
	,sales_grp_cd VARCHAR(18)  		--//  ENCODE zstd
	,cnd_type VARCHAR(25)  		--//  ENCODE zstd
	,matl_num VARCHAR(25)  		--//  ENCODE zstd
	,sold_to_cust_cd VARCHAR(25)  		--//  ENCODE zstd
	,price VARCHAR(25)  		--//  ENCODE zstd
	,vld_frm DATE NOT NULL 		--//  ENCODE zstd
	,vld_to DATE NOT NULL 		--//  ENCODE zstd
	,cond_curr VARCHAR(15)  		--//  ENCODE zstd
	,doc_currcy VARCHAR(15)  		--//  ENCODE zstd
	,recordmode CHAR(1)  		--//  ENCODE zstd
	,ctry_cd VARCHAR(25)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_sfmc_naver_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_SFMC_NAVER_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_sfmc_naver_data
(
	naver_id VARCHAR(50)  		--//  ENCODE zstd
	,birth_year VARCHAR(4)  		--//  ENCODE zstd
	,gender VARCHAR(50)  		--//  ENCODE zstd
	,total_purchase_amount VARCHAR(50)  		--//  ENCODE zstd
	,total_number_of_purchases VARCHAR(50)  		--//  ENCODE zstd
	,membership_grade_level VARCHAR(50)  		--//  ENCODE zstd
	,marketing_message_viewing_receiving VARCHAR(50)  		--//  ENCODE zstd
	,coupon_usage_issuance VARCHAR(50)  		--//  ENCODE zstd
	,number_of_reviews VARCHAR(50)  		--//  ENCODE zstd
	,number_of_comments VARCHAR(50)  		--//  ENCODE zstd
	,number_of_attendances VARCHAR(50)  		--//  ENCODE zstd
	,opt_in_for_jnj_communication VARCHAR(50)  		--//  ENCODE zstd
	,notification_subscription VARCHAR(50)  		--//  ENCODE zstd
	,updated_date VARCHAR(50)  		--//  ENCODE zstd
	,membership_registration_date VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_kr_sfmc_naver_data_additional;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KR_SFMC_NAVER_DATA_ADDITIONAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_kr_sfmc_naver_data_additional
(
	naver_id VARCHAR(100)  		--//  ENCODE zstd
	,attribute_name VARCHAR(100)  		--//  ENCODE zstd
	,attribute_value VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_parameter_gt_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_PARAMETER_GT_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_parameter_gt_sellout
(
	country_cd VARCHAR(5)  		--//  ENCODE zstd
	,parameter_name VARCHAR(50)  		--//  ENCODE zstd
	,dstr_nm VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_photo_mgmnt_url_wrk1;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_PHOTO_MGMNT_URL_WRK1		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_photo_mgmnt_url_wrk1
(
	original_photo_key VARCHAR(1031)  		--//  ENCODE lzo
	,original_response VARCHAR(65535)  		--//  ENCODE lzo
	,photo_key VARCHAR(1053)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,url_cnt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,create_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upload_photo_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_photo_mgmnt_url_wrk2;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_PHOTO_MGMNT_URL_WRK2		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_photo_mgmnt_url_wrk2
(
	photo_key VARCHAR(1031)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,url_cnt numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,change_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_pos_fact_korea_pcmap;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_POS_FACT_KOREA_PCMAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_pos_fact_korea_pcmap
(
	pos_dt DATE  		--//  ENCODE az64
	,vend_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_nm VARCHAR(100)  		--//  ENCODE lzo
	,prod_nm VARCHAR(100)  		--//  ENCODE lzo
	,vend_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,vend_prod_nm VARCHAR(600)  		--//  ENCODE lzo
	,brnd_nm VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,str_cd VARCHAR(40)  		--//  ENCODE lzo
	,str_nm VARCHAR(100)  		--//  ENCODE lzo
	,sold_to_party VARCHAR(100)  		--//  ENCODE lzo
	,sls_grp VARCHAR(100)  		--//  ENCODE lzo
	,mysls_brnd_nm VARCHAR(500)  		--//  ENCODE lzo
	,sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt NUMERIC(16,5)  		--//  ENCODE az64
	,unit_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,sls_excl_vat_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_rtrn_amt NUMERIC(16,5)  		--//  ENCODE az64
	,stk_recv_amt NUMERIC(16,5)  		--//  ENCODE az64
	,avg_sell_qty NUMERIC(16,5)  		--//  ENCODE az64
	,cum_ship_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cum_rtrn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_takn_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,web_ordr_acpt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dc_invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invnt_amt NUMERIC(16,5)  		--//  ENCODE az64
	,invnt_dt DATE  		--//  ENCODE az64
	,serial_num VARCHAR(40)  		--//  ENCODE lzo
	,prod_delv_type VARCHAR(40)  		--//  ENCODE lzo
	,prod_type VARCHAR(40)  		--//  ENCODE lzo
	,dept_cd VARCHAR(40)  		--//  ENCODE lzo
	,dept_nm VARCHAR(100)  		--//  ENCODE lzo
	,spec_1_desc VARCHAR(100)  		--//  ENCODE lzo
	,spec_2_desc VARCHAR(100)  		--//  ENCODE lzo
	,cat_big VARCHAR(100)  		--//  ENCODE lzo
	,cat_mid VARCHAR(40)  		--//  ENCODE lzo
	,cat_small VARCHAR(40)  		--//  ENCODE lzo
	,dc_prod_cd VARCHAR(40)  		--//  ENCODE lzo
	,cust_dtls VARCHAR(100)  		--//  ENCODE lzo
	,dist_cd VARCHAR(40)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,src_txn_sts VARCHAR(40)  		--//  ENCODE lzo
	,src_seq_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,src_sys_cd VARCHAR(30)  		--//  ENCODE lzo
	,ctry_cd VARCHAR(10)  		--//  ENCODE lzo
	,mysls_catg VARCHAR(500)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(100)  		--//  ENCODE lzo
	,prom_sls_amt NUMERIC(27,5)  		--//  ENCODE az64
	,prom_prc_amt NUMERIC(16,5)  		--//  ENCODE az64
	,hist_flg VARCHAR(1)  		--//  ENCODE lzo
	,sls_grp_cd VARCHAR(20)  		--//  ENCODE lzo
	,channel VARCHAR(25)  		--//  ENCODE lzo
	,store_type VARCHAR(25)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_pos_gross_prc_condition_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_POS_GROSS_PRC_CONDITION_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_pos_gross_prc_condition_map
(
	pos_dt DATE  		--//  ENCODE az64
	,sold_to_party VARCHAR(100)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,cnd_type VARCHAR(25)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,matl_desc VARCHAR(40)  		--//  ENCODE lzo
	,calc_price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vld_frm DATE  		--//  ENCODE az64
	,vld_to DATE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_pos_prc_condition_map;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_POS_PRC_CONDITION_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_pos_prc_condition_map
(
	sls_org VARCHAR(25)  		--//  ENCODE lzo
	,sales_grp_cd VARCHAR(18)  		--//  ENCODE lzo
	,cnd_type VARCHAR(25)  		--//  ENCODE lzo
	,matl_num VARCHAR(40)  		--//  ENCODE lzo
	,sold_to_cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vld_frm DATE  		--//  ENCODE az64
	,vld_to DATE  		--//  ENCODE az64
	,ctry_cd VARCHAR(25)  		--//  ENCODE lzo
	,calc_price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_org_msd VARCHAR(4)  		--//  ENCODE lzo
	,matl_num_msd VARCHAR(18)  		--//  ENCODE lzo
	,matl_desc VARCHAR(40)  		--//  ENCODE lzo
	,ean_num VARCHAR(18)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_sales_rep_so_target_fact;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_SALES_REP_SO_TARGET_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_sales_rep_so_target_fact
(
	ctry_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,dstr_cd VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,crncy_cd VARCHAR(5) NOT NULL 		--//  ENCODE zstd
	,jj_mnth_id numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,sls_rep_cd VARCHAR(20) NOT NULL 		--//  ENCODE zstd
	,sls_rep_nm VARCHAR(50) NOT NULL 		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,sls_trgt_val NUMERIC(38,7) NOT NULL 		--//  ENCODE delta
	,file_rec_dt DATE NOT NULL 		--//  ENCODE delta
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_sdl_kr_pos_ecvan_ssg;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_SDL_KR_POS_ECVAN_SSG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_sdl_kr_pos_ecvan_ssg
(
	pos_dt VARCHAR(255)  		--//  ENCODE lzo
	,prod_nm VARCHAR(255)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,vend_nm VARCHAR(3)  		--//  ENCODE lzo
	,str_cd VARCHAR(255)  		--//  ENCODE lzo
	,str_nm VARCHAR(255)  		--//  ENCODE lzo
	,product_type VARCHAR(255)  		--//  ENCODE lzo
	,sellout_qty VARCHAR(255)  		--//  ENCODE lzo
	,sellout_amt VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_allmonths_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_ALLMONTHS_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_allmonths_base
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_base
(
	ctry_cd VARCHAR(2)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,cal_month VARCHAR(14)  		--//  ENCODE lzo
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,si_qty NUMERIC(38,4)  		--//  ENCODE az64
	,si_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_base_detail;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_BASE_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_base_detail
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,base_matl_num VARCHAR(100)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_lastnmonths;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_LASTNMONTHS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_lastnmonths
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_sellout_for_inv_analysis;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_SELLOUT_FOR_INV_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_sellout_for_inv_analysis
(
	min_date DATE  		--//  ENCODE az64
	,global_prod_brand VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(500)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,sap_prnt_cust_key VARCHAR(13)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_final;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_SISO_PROPAGATE_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_final
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagate_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(100)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
	,existing_so_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_inv_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_sell_in_qty NUMERIC(38,5)  		--//  ENCODE az64
	,existing_sell_in_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_3months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_6months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_12months_so NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_3months_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_6months_so_value NUMERIC(38,5)  		--//  ENCODE az64
	,existing_last_12months_so_value NUMERIC(38,5)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_to_details;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_SISO_PROPAGATE_TO_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_to_details
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagation_flag VARCHAR(1)  		--//  ENCODE lzo
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,reason VARCHAR(29)  		--//  ENCODE lzo
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_to_existing_dtls;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_SISO_PROPAGATE_TO_EXISTING_DTLS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_siso_propagate_to_existing_dtls
(
	sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_desc VARCHAR(50)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_desc VARCHAR(50)  		--//  ENCODE lzo
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ean_num VARCHAR(100)  		--//  ENCODE lzo
	,so_qty NUMERIC(38,4)  		--//  ENCODE az64
	,so_value NUMERIC(38,4)  		--//  ENCODE az64
	,inv_qty NUMERIC(38,5)  		--//  ENCODE az64
	,inv_value NUMERIC(38,9)  		--//  ENCODE az64
	,sell_in_qty NUMERIC(38,4)  		--//  ENCODE az64
	,sell_in_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so NUMERIC(38,4)  		--//  ENCODE az64
	,last_3months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_value NUMERIC(38,4)  		--//  ENCODE az64
	,propagate_from numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,replicated_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tsi_target_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TSI_TARGET_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tsi_target_data
(
	date VARCHAR(255)  		--//  ENCODE zstd
	,ec VARCHAR(255)  		--//  ENCODE zstd
	,offtake VARCHAR(255)  		--//  ENCODE zstd
	,customer_code VARCHAR(255)  		--//  ENCODE zstd
	,customer_name VARCHAR(255)  		--//  ENCODE zstd
	,customer_cname VARCHAR(255)  		--//  ENCODE zstd
	,customer_sname VARCHAR(255)  		--//  ENCODE zstd
	,nts VARCHAR(255)  		--//  ENCODE zstd
	,"offtake(sell_out)" VARCHAR(255)  		--//  ENCODE zstd
	,gts VARCHAR(255)  		--//  ENCODE zstd
	,pre_sales VARCHAR(255)  		--//  ENCODE zstd
	,prs_code_01 VARCHAR(255)  		--//  ENCODE zstd
	,prs_code_02 VARCHAR(255)  		--//  ENCODE zstd
	,prs_code_03 VARCHAR(255)  		--//  ENCODE zstd
	,prs_code_04 VARCHAR(255)  		--//  ENCODE zstd
	,prs_code_05 VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tsi_target_data_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TSI_TARGET_DATA_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tsi_target_data_temp
(
	date VARCHAR(10)  		--//  ENCODE zstd
	,ec VARCHAR(10)  		--//  ENCODE zstd
	,offtake VARCHAR(10)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(100)  		--//  ENCODE zstd
	,customer_cname VARCHAR(100)  		--//  ENCODE zstd
	,customer_sname VARCHAR(100)  		--//  ENCODE zstd
	,nts NUMERIC(34,8)  		--//  ENCODE az64
	,"offtake/sellout" NUMERIC(34,8)  		--//  ENCODE az64
	,gts NUMERIC(34,8)  		--//  ENCODE az64
	,pre_sales NUMERIC(34,8)  		--//  ENCODE az64
	,prs_code_01 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_02 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_03 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_04 VARCHAR(50)  		--//  ENCODE zstd
	,prs_code_05 VARCHAR(50)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_as_watsons_inventory;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_AS_WATSONS_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_as_watsons_inventory
(
	supplier VARCHAR(255)  		--//  ENCODE lzo
	,sku_no VARCHAR(255)  		--//  ENCODE lzo
	,buy_code VARCHAR(255)  		--//  ENCODE lzo
	,home_cdesc VARCHAR(255)  		--//  ENCODE lzo
	,prdt_grp VARCHAR(255)  		--//  ENCODE lzo
	,grp_desc VARCHAR(255)  		--//  ENCODE lzo
	,prdt_cat VARCHAR(255)  		--//  ENCODE lzo
	,cat_desc VARCHAR(255)  		--//  ENCODE lzo
	,c_cdesc VARCHAR(255)  		--//  ENCODE lzo
	,type VARCHAR(255)  		--//  ENCODE lzo
	,avge_sales_cost_value VARCHAR(255)  		--//  ENCODE lzo
	,total_stock_qty VARCHAR(255)  		--//  ENCODE lzo
	,total_stock_value VARCHAR(255)  		--//  ENCODE lzo
	,weeks_holding_sales VARCHAR(255)  		--//  ENCODE lzo
	,weeks_holding VARCHAR(255)  		--//  ENCODE lzo
	,first_recv_date VARCHAR(255)  		--//  ENCODE lzo
	,turn_type_sales VARCHAR(255)  		--//  ENCODE lzo
	,turn_type VARCHAR(255)  		--//  ENCODE lzo
	,uda73 VARCHAR(255)  		--//  ENCODE lzo
	,discontinue_date VARCHAR(255)  		--//  ENCODE lzo
	,stockclass VARCHAR(255)  		--//  ENCODE lzo
	,pog VARCHAR(255)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_as_watsons_inventory_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_AS_WATSONS_INVENTORY_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_as_watsons_inventory_temp
(
	supplier VARCHAR(255)  		--//  ENCODE zstd
	,sku_no VARCHAR(255)  		--//  ENCODE zstd
	,buy_code VARCHAR(255)  		--//  ENCODE zstd
	,home_cdesc VARCHAR(255)  		--//  ENCODE zstd
	,prdt_grp VARCHAR(255)  		--//  ENCODE zstd
	,grp_desc VARCHAR(255)  		--//  ENCODE zstd
	,prdt_cat VARCHAR(255)  		--//  ENCODE zstd
	,cat_desc VARCHAR(255)  		--//  ENCODE zstd
	,c_cdesc VARCHAR(255)  		--//  ENCODE zstd
	,type VARCHAR(255)  		--//  ENCODE zstd
	,avge_sales_cost_value VARCHAR(255)  		--//  ENCODE zstd
	,total_stock_qty VARCHAR(255)  		--//  ENCODE zstd
	,total_stock_value VARCHAR(255)  		--//  ENCODE zstd
	,weeks_holding_sales VARCHAR(255)  		--//  ENCODE zstd
	,weeks_holding VARCHAR(255)  		--//  ENCODE zstd
	,first_recv_date VARCHAR(255)  		--//  ENCODE zstd
	,turn_type_sales VARCHAR(255)  		--//  ENCODE zstd
	,turn_type VARCHAR(255)  		--//  ENCODE zstd
	,uda73 VARCHAR(255)  		--//  ENCODE zstd
	,discontinue_date VARCHAR(255)  		--//  ENCODE zstd
	,stockclass VARCHAR(255)  		--//  ENCODE zstd
	,pog VARCHAR(255)  		--//  ENCODE zstd
	,ean VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bp_forecast;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BP_FORECAST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bp_forecast
(
	bp_version VARCHAR(5)  		--//  ENCODE lzo
	,forecast_on_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_month VARCHAR(10)  		--//  ENCODE lzo
	,region VARCHAR(100)  		--//  ENCODE lzo
	,prod_name VARCHAR(255)  		--//  ENCODE lzo
	,lph_level_6 VARCHAR(255)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,pre_sales DOUBLE PRECISION
	,tp DOUBLE PRECISION
	,nts DOUBLE PRECISION
	,filename VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bp_forecast_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BP_FORECAST_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bp_forecast_temp
(
	region VARCHAR(100)  		--//  ENCODE lzo
	,prod_name VARCHAR(255)  		--//  ENCODE lzo
	,lph_level_6 VARCHAR(255)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,pre_sales DOUBLE PRECISION
	,tp DOUBLE PRECISION
	,nts DOUBLE PRECISION
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_prod_hier;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BU_FORECAST_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_prod_hier
(
	bu_version VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_month VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,lph_level_6 VARCHAR(255)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(10)  		--//  ENCODE lzo
	,price_off DOUBLE PRECISION
	,display DOUBLE PRECISION
	,dm DOUBLE PRECISION
	,other_support DOUBLE PRECISION
	,sr DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
	,pre_sales DOUBLE PRECISION
	,filename VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_prod_hier_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BU_FORECAST_PROD_HIER_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_prod_hier_temp
(
	lph_level_6 VARCHAR(255)  		--//  ENCODE lzo
	,product VARCHAR(255)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(6)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,price_off DOUBLE PRECISION
	,display DOUBLE PRECISION
	,dm DOUBLE PRECISION
	,other_support DOUBLE PRECISION
	,sr DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
	,pre_sales DOUBLE PRECISION
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_sku;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BU_FORECAST_SKU		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_sku
(
	bu_version VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_on_month VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,sap_code VARCHAR(30)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(6)  		--//  ENCODE lzo
	,system_list_price DOUBLE PRECISION
	,gross_invoice_price DOUBLE PRECISION
	,gross_invoice_price_less_terms DOUBLE PRECISION
	,rf_sell_out_qty DOUBLE PRECISION
	,rf_sell_in_qty DOUBLE PRECISION
	,price_off DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
	,filename VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_sku_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_BU_FORECAST_SKU_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_bu_forecast_sku_temp
(
	sap_code VARCHAR(255)  		--//  ENCODE lzo
	,representative_cust_no VARCHAR(6)  		--//  ENCODE lzo
	,forecast_for_year VARCHAR(10)  		--//  ENCODE lzo
	,forecast_for_mnth VARCHAR(10)  		--//  ENCODE lzo
	,system_list_price DOUBLE PRECISION
	,gross_invoice_price DOUBLE PRECISION
	,gross_invoice_price_less_terms DOUBLE PRECISION
	,rf_sell_out_qty DOUBLE PRECISION
	,rf_sell_in_qty DOUBLE PRECISION
	,price_off DOUBLE PRECISION
	,pre_sales_before_returns DOUBLE PRECISION
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_customer;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_IMS_DSTR_STD_CUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_customer
(
	distributor_code VARCHAR(10)  		--//  ENCODE zstd
	,distributor_cusotmer_code VARCHAR(50)  		--//  ENCODE zstd
	,distributor_customer_name VARCHAR(50)  		--//  ENCODE zstd
	,distributor_address VARCHAR(255)  		--//  ENCODE zstd
	,distributor_telephone VARCHAR(255)  		--//  ENCODE zstd
	,distributor_contact VARCHAR(255)  		--//  ENCODE zstd
	,distributor_sales_area VARCHAR(20)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_sel_out;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_IMS_DSTR_STD_SEL_OUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_sel_out
(
	transaction_date DATE NOT NULL 		--//  ENCODE zstd
	,distributor_code VARCHAR(10)  		--//  ENCODE zstd
	,distributor_name VARCHAR(100)  		--//  ENCODE zstd
	,distributors_customer_code VARCHAR(50)  		--//  ENCODE zstd
	,distributors_customer_name VARCHAR(100)  		--//  ENCODE zstd
	,distributors_product_code VARCHAR(20)  		--//  ENCODE zstd
	,distributors_product_name VARCHAR(100)  		--//  ENCODE zstd
	,report_period_start_date DATE  		--//  ENCODE zstd
	,report_period_end_date DATE  		--//  ENCODE zstd
	,ean VARCHAR(20)  		--//  ENCODE zstd
	,uom VARCHAR(10)  		--//  ENCODE zstd
	,unit_price NUMERIC(21,5)  		--//  ENCODE zstd
	,sales_amount NUMERIC(21,5)  		--//  ENCODE zstd
	,sales_qty NUMERIC(21,5)  		--//  ENCODE zstd
	,return_qty NUMERIC(21,5)  		--//  ENCODE zstd
	,return_amount NUMERIC(21,5)  		--//  ENCODE zstd
	,sales_rep_code VARCHAR(20)  		--//  ENCODE zstd
	,sales_rep_name VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_stock;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_IMS_DSTR_STD_STOCK		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_ims_dstr_std_stock
(
	distributor_code VARCHAR(20)  		--//  ENCODE lzo
	,ean VARCHAR(20)  		--//  ENCODE lzo
	,distributor_product_code VARCHAR(20)  		--//  ENCODE lzo
	,quantity NUMERIC(21,5)  		--//  ENCODE az64
	,total_cost NUMERIC(21,5)  		--//  ENCODE az64
	,inventory_date DATE NOT NULL 		--//  ENCODE az64
	,distributors_product_name VARCHAR(200)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,storage_name VARCHAR(30)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(14)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_inventory_healthy_unhealthy_analysis;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_INVENTORY_HEALTHY_UNHEALTHY_ANALYSIS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_inventory_healthy_unhealthy_analysis
(
	month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,global_prod_brand VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_variant VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_segment VARCHAR(500)  		--//  ENCODE lzo
	,global_prod_category VARCHAR(500)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,sap_prnt_cust_key VARCHAR(13)  		--//  ENCODE lzo
	,last_3months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_6months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_12months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,last_36months_so_val NUMERIC(38,4)  		--//  ENCODE az64
	,healthy_inventory VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_7eleven;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_7ELEVEN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_7eleven
(
	pos_date DATE NOT NULL 		--//  ENCODE zstd
	,product_code VARCHAR(40)  		--//  ENCODE zstd
	,product_description VARCHAR(100)  		--//  ENCODE zstd
	,sales_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,vendor_code VARCHAR(40)  		--//  ENCODE zstd
	,vendor_description VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_carrefour;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_CARREFOUR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_carrefour
(
	pos_date DATE NOT NULL 		--//  ENCODE zstd
	,gln VARCHAR(40)  		--//  ENCODE zstd
	,vendor VARCHAR(40)  		--//  ENCODE zstd
	,store_no VARCHAR(40)  		--//  ENCODE zstd
	,store_name VARCHAR(40)  		--//  ENCODE zstd
	,product_code VARCHAR(40)  		--//  ENCODE zstd
	,product_name_english VARCHAR(100)  		--//  ENCODE zstd
	,product_name_chinese VARCHAR(100)  		--//  ENCODE zstd
	,ean_code VARCHAR(40)  		--//  ENCODE zstd
	,amount NUMERIC(16,5)  		--//  ENCODE zstd
	,qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_cosmed;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_COSMED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_cosmed
(
	product_code VARCHAR(40)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,wk1_strt_dt DATE  		--//  ENCODE zstd
	,wk1_end_dt DATE  		--//  ENCODE zstd
	,wk1_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,wk2_strt_dt DATE  		--//  ENCODE zstd
	,wk2_end_dt DATE  		--//  ENCODE zstd
	,wk2_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,wk3_strt_dt DATE  		--//  ENCODE zstd
	,wk3_end_dt DATE  		--//  ENCODE zstd
	,wk3_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,wk4_strt_dt DATE  		--//  ENCODE zstd
	,wk4_end_dt DATE  		--//  ENCODE zstd
	,wk4_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_ec;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_EC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_ec
(
	customer_ec_platfom VARCHAR(40)  		--//  ENCODE zstd
	,pos_date DATE NOT NULL 		--//  ENCODE zstd
	,product_code VARCHAR(40)  		--//  ENCODE zstd
	,brand VARCHAR(40)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,selling_amt_before_tax NUMERIC(16,5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_poya;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_POYA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_poya
(
	vendor_code VARCHAR(40)  		--//  ENCODE zstd
	,department VARCHAR(40)  		--//  ENCODE zstd
	,category_small VARCHAR(40)  		--//  ENCODE zstd
	,customer_product_code VARCHAR(40)  		--//  ENCODE zstd
	,ean_code VARCHAR(40)  		--//  ENCODE zstd
	,product_description VARCHAR(100)  		--//  ENCODE zstd
	,selling_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,selling_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,inventory numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,change_code VARCHAR(40)  		--//  ENCODE zstd
	,change_date DATE  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE zstd
	,end_date DATE  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_pxcivilia;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_PXCIVILIA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_pxcivilia
(
	store_code VARCHAR(40)  		--//  ENCODE zstd
	,store_name_chinese VARCHAR(100)  		--//  ENCODE zstd
	,pos_date DATE NOT NULL 		--//  ENCODE zstd
	,ean_code VARCHAR(40)  		--//  ENCODE zstd
	,civilian_product_code VARCHAR(40)  		--//  ENCODE zstd
	,brand VARCHAR(40)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,dc VARCHAR(40)  		--//  ENCODE zstd
	,unit_price NUMERIC(16,5)  		--//  ENCODE zstd
	,stock_receive_qty_by_store numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,stock_selling_qty_by_store numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,stock_inventory_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,stock_return_qty_by_store numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,stock_receive_amt_by_store NUMERIC(16,5)  		--//  ENCODE zstd
	,stock_selling_amt_by_store NUMERIC(16,5)  		--//  ENCODE zstd
	,stock_inventory_amt NUMERIC(16,5)  		--//  ENCODE zstd
	,stock_return_amt_by_store NUMERIC(16,5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	filename
		--// 	)
;		--// ;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_rt_mart;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_RT_MART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_rt_mart
(
	vendor_code VARCHAR(40)  		--//  ENCODE zstd
	,vendor_name VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(40)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,ean_code VARCHAR(60)  		--//  ENCODE zstd
	,store_no VARCHAR(40)  		--//  ENCODE zstd
	,store_name VARCHAR(100)  		--//  ENCODE zstd
	,department VARCHAR(40)  		--//  ENCODE zstd
	,department_name VARCHAR(100)  		--//  ENCODE zstd
	,pos_date DATE NOT NULL 		--//  ENCODE zstd
	,stock_receive_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,selling_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,inventory_qty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,average_selling_qty NUMERIC(16,5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_watson_store;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_POS_WATSON_STORE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_pos_watson_store
(
	store_no VARCHAR(40)  		--//  ENCODE lzo
	,store_name VARCHAR(500)  		--//  ENCODE lzo
	,product_code VARCHAR(40)  		--//  ENCODE lzo
	,product_description VARCHAR(500)  		--//  ENCODE lzo
	,selling_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,selling_amt NUMERIC(16,5)  		--//  ENCODE az64
	,department VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(60)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sales_incentive_offtake;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SALES_INCENTIVE_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sales_incentive_offtake
(
	source_type VARCHAR(12)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(5)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(10)  		--//  ENCODE lzo
	,to_crncy VARCHAR(5)  		--//  ENCODE lzo
	,psr_code VARCHAR(100)  		--//  ENCODE lzo
	,psr_name VARCHAR(255)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,report_to VARCHAR(500)  		--//  ENCODE lzo
	,reportto_name VARCHAR(500)  		--//  ENCODE lzo
	,reverse VARCHAR(500)  		--//  ENCODE lzo
	,monthly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_target NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_achievement NUMERIC(38,2)  		--//  ENCODE az64
	,monthly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_target NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_achievement NUMERIC(38,2)  		--//  ENCODE az64
	,quarterly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sales_incentive_tp_ciw;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SALES_INCENTIVE_TP_CIW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sales_incentive_tp_ciw
(
	source_type VARCHAR(7)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(5)  		--//  ENCODE lzo
	,to_crncy VARCHAR(5)  		--//  ENCODE lzo
	,psr_code VARCHAR(100)  		--//  ENCODE lzo
	,psr_name VARCHAR(255)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,report_to VARCHAR(500)  		--//  ENCODE lzo
	,reportto_name VARCHAR(500)  		--//  ENCODE lzo
	,reverse VARCHAR(500)  		--//  ENCODE lzo
	,monthly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_target NUMERIC(38,4)  		--//  ENCODE az64
	,monthly_achievement NUMERIC(38,2)  		--//  ENCODE az64
	,monthly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_actual NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_target NUMERIC(38,4)  		--//  ENCODE az64
	,quarterly_achievement NUMERIC(38,2)  		--//  ENCODE az64
	,quarterly_incentive_amount NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_bounce_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_BOUNCE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_bounce_data
(
	oyb_account_id VARCHAR(20)  		--//  ENCODE lzo
	,job_id VARCHAR(20)  		--//  ENCODE lzo
	,list_id VARCHAR(10)  		--//  ENCODE lzo
	,batch_id VARCHAR(10)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(20)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,is_unique VARCHAR(10)  		--//  ENCODE lzo
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,bounce_category_id VARCHAR(10)  		--//  ENCODE lzo
	,bounce_category VARCHAR(30)  		--//  ENCODE lzo
	,bounce_subcategory_id VARCHAR(10)  		--//  ENCODE lzo
	,bounce_subcategory VARCHAR(30)  		--//  ENCODE lzo
	,bounce_type_id VARCHAR(10)  		--//  ENCODE lzo
	,bounce_type VARCHAR(30)  		--//  ENCODE lzo
	,smtp_bounce_reason VARCHAR(1000)  		--//  ENCODE lzo
	,smtp_message VARCHAR(200)  		--//  ENCODE lzo
	,smtp_code VARCHAR(10)  		--//  ENCODE lzo
	,triggerer_send_definition_object_id VARCHAR(50)  		--//  ENCODE lzo
	,triggered_send_customer_key VARCHAR(10)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,bcc_email VARCHAR(50)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_id VARCHAR(20)  		--//  ENCODE lzo
	,email_address VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_children_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_CHILDREN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_children_data
(
	parent_key VARCHAR(200)  		--//  ENCODE lzo
	,child_nm VARCHAR(50)  		--//  ENCODE lzo
	,child_birth_mnth VARCHAR(10)  		--//  ENCODE lzo
	,child_birth_year VARCHAR(10)  		--//  ENCODE lzo
	,child_gender VARCHAR(10)  		--//  ENCODE lzo
	,child_number VARCHAR(30)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_click_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_CLICK_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_click_data
(
	oyb_account_id VARCHAR(50)  		--//  ENCODE lzo
	,job_id VARCHAR(50)  		--//  ENCODE lzo
	,list_id VARCHAR(50)  		--//  ENCODE lzo
	,batch_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,url VARCHAR(1000)  		--//  ENCODE lzo
	,link_name VARCHAR(200)  		--//  ENCODE lzo
	,link_content VARCHAR(1000)  		--//  ENCODE lzo
	,is_unique VARCHAR(10)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_complaint_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_COMPLAINT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_complaint_data
(
	oyb_account_id VARCHAR(50)  		--//  ENCODE lzo
	,job_id VARCHAR(50)  		--//  ENCODE lzo
	,list_id VARCHAR(50)  		--//  ENCODE lzo
	,batch_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,is_unique VARCHAR(10)  		--//  ENCODE lzo
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_id VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_consumer_master;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_CONSUMER_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_consumer_master
(
	first_name VARCHAR(200)  		--//  ENCODE lzo
	,last_name VARCHAR(200)  		--//  ENCODE lzo
	,mobile_num VARCHAR(30)  		--//  ENCODE lzo
	,mobile_cntry_cd VARCHAR(10)  		--//  ENCODE lzo
	,birthday_mnth VARCHAR(10)  		--//  ENCODE lzo
	,birthday_year VARCHAR(10)  		--//  ENCODE lzo
	,address_1 VARCHAR(255)  		--//  ENCODE lzo
	,address_2 VARCHAR(255)  		--//  ENCODE lzo
	,address_city VARCHAR(100)  		--//  ENCODE lzo
	,address_zipcode VARCHAR(30)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,website_unique_id VARCHAR(150)  		--//  ENCODE lzo
	,source VARCHAR(100)  		--//  ENCODE lzo
	,medium VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(200)  		--//  ENCODE lzo
	,address_cntry VARCHAR(100)  		--//  ENCODE lzo
	,campaign_id VARCHAR(100)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,unsubscribe_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,email VARCHAR(100)  		--//  ENCODE lzo
	,full_name VARCHAR(200)  		--//  ENCODE lzo
	,last_logon_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,remaining_points NUMERIC(10,4)  		--//  ENCODE az64
	,redeemed_points NUMERIC(10,4)  		--//  ENCODE az64
	,total_points NUMERIC(10,4)  		--//  ENCODE az64
	,gender VARCHAR(20)  		--//  ENCODE lzo
	,line_id VARCHAR(50)  		--//  ENCODE lzo
	,line_name VARCHAR(200)  		--//  ENCODE lzo
	,line_email VARCHAR(100)  		--//  ENCODE lzo
	,line_channel_id VARCHAR(50)  		--//  ENCODE lzo
	,address_region VARCHAR(100)  		--//  ENCODE lzo
	,tier VARCHAR(100)  		--//  ENCODE lzo
	,opt_in_for_communication VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_consumer_master_temp1;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_CONSUMER_MASTER_TEMP1		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_consumer_master_temp1
(
	subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,md5_sdl VARCHAR(32)  		--//  ENCODE lzo
	,md5_itg VARCHAR(32)  		--//  ENCODE lzo
	,compare VARCHAR(9)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_invoice_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_INVOICE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_invoice_data
(
	purchase_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,product VARCHAR(200)  		--//  ENCODE lzo
	,status VARCHAR(50)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,completed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,points numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,show_record VARCHAR(20)  		--//  ENCODE lzo
	,qty NUMERIC(20,4)  		--//  ENCODE az64
	,invoice_type VARCHAR(200)  		--//  ENCODE lzo
	,seller_nm VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(200)  		--//  ENCODE lzo
	,website_unique_id VARCHAR(150)  		--//  ENCODE lzo
	,invoice_num VARCHAR(50)  		--//  ENCODE lzo
	,epsilon_price_per_unit NUMERIC(20,4)  		--//  ENCODE az64
	,epsilon_amount NUMERIC(20,4)  		--//  ENCODE az64
	,epsilon_total_amount NUMERIC(20,4)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_open_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_OPEN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_open_data
(
	oyb_account_id VARCHAR(50)  		--//  ENCODE lzo
	,job_id VARCHAR(50)  		--//  ENCODE lzo
	,list_id VARCHAR(30)  		--//  ENCODE lzo
	,batch_id VARCHAR(30)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,bcc_email VARCHAR(50)  		--//  ENCODE lzo
	,email_id VARCHAR(30)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,is_unique VARCHAR(10)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_redemption_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_REDEMPTION_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_redemption_data
(
	prod_nm VARCHAR(255)  		--//  ENCODE lzo
	,redeemed_points NUMERIC(20,4)  		--//  ENCODE az64
	,qty NUMERIC(20,4)  		--//  ENCODE az64
	,redeemed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,status VARCHAR(100)  		--//  ENCODE lzo
	,completed_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,order_num VARCHAR(50)  		--//  ENCODE lzo
	,website_unique_id VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_sent_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_SENT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_sent_data
(
	oyb_account_id VARCHAR(50)  		--//  ENCODE lzo
	,job_id VARCHAR(50)  		--//  ENCODE lzo
	,list_id VARCHAR(30)  		--//  ENCODE lzo
	,batch_id VARCHAR(30)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,triggerer_send_definition_object_id VARCHAR(50)  		--//  ENCODE lzo
	,triggered_send_customer_key VARCHAR(10)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,email_id VARCHAR(30)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_unsubscribe_data;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_SFMC_UNSUBSCRIBE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_sfmc_unsubscribe_data
(
	oyb_account_id VARCHAR(50)  		--//  ENCODE lzo
	,job_id VARCHAR(50)  		--//  ENCODE lzo
	,list_id VARCHAR(30)  		--//  ENCODE lzo
	,batch_id VARCHAR(30)  		--//  ENCODE lzo
	,subscriber_id VARCHAR(50)  		--//  ENCODE lzo
	,subscriber_key VARCHAR(100)  		--//  ENCODE lzo
	,event_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,domain VARCHAR(50)  		--//  ENCODE lzo
	,email_name VARCHAR(100)  		--//  ENCODE lzo
	,email_subject VARCHAR(200)  		--//  ENCODE lzo
	,email_id VARCHAR(30)  		--//  ENCODE lzo
	,is_unique VARCHAR(10)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_strategic_cust_hier;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_STRATEGIC_CUST_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_strategic_cust_hier
(
	strategy_customer_hierachy_code VARCHAR(10)  		--//  ENCODE lzo
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE lzo
	,cust_cd VARCHAR(10)  		--//  ENCODE lzo
	,cust_nm VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_tw_strategic_cust_hier_temp;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TW_STRATEGIC_CUST_HIER_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_tw_strategic_cust_hier_temp
(
	strategy_customer_hierachy_code VARCHAR(10)  		--//  ENCODE lzo
	,strategy_customer_hierachy_name VARCHAR(255)  		--//  ENCODE lzo
	,cust_cd VARCHAR(10)  		--//  ENCODE lzo
	,cust_nm VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HONG_KONG_REGIONAL_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(9)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HONG_KONG_REGIONAL_SELLOUT_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_base
(
	data_src VARCHAR(8)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(9)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_cd VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
	,so_sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_sls_value NUMERIC(22,5)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HONG_KONG_REGIONAL_SELLOUT_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(9)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
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
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_offtake_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_HONG_KONG_REGIONAL_SELLOUT_OFFTAKE_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_hong_kong_regional_sellout_offtake_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(9)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
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
	,selling_price NUMERIC(38,5)  		--//  ENCODE az64
	,cnt_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KOREA_REGIONAL_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_name VARCHAR(2000)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(500)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,msl_product_code VARCHAR(100)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,retail_env VARCHAR(150)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KOREA_REGIONAL_SELLOUT_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_base
(
	data_src VARCHAR(8)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(5)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day DATE  		--//  ENCODE az64
	,soldto_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_name VARCHAR(2000)  		--//  ENCODE lzo
	,store_cd VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(500)  		--//  ENCODE lzo
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
	,so_sls_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,so_sls_value DOUBLE PRECISION
	,msl_product_code VARCHAR(100)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,retail_env VARCHAR(150)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KOREA_REGIONAL_SELLOUT_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_name VARCHAR(2000)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(500)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,msl_product_code VARCHAR(100)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,retail_env VARCHAR(150)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
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
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_offtake_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_KOREA_REGIONAL_SELLOUT_OFFTAKE_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_korea_regional_sellout_offtake_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_code VARCHAR(2000)  		--//  ENCODE lzo
	,distributor_name VARCHAR(2000)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(100)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(40)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(500)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,sellout_value_list_price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellout_value_list_price_usd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_min_date DATE  		--//  ENCODE az64
	,customer_product_min_date DATE  		--//  ENCODE az64
	,market_min_date DATE  		--//  ENCODE az64
	,market_product_min_date DATE  		--//  ENCODE az64
	,rn_cus numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rn_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,msl_product_code VARCHAR(100)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,retail_env VARCHAR(150)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,first_scan_flag_parent_customer_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_parent_customer_level VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level VARCHAR(1)  		--//  ENCODE lzo
	,selling_price DOUBLE PRECISION
	,cnt_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_REGIONAL_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(6)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_base;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_REGIONAL_SELLOUT_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_base
(
	data_src VARCHAR(8)  		--//  ENCODE lzo
	,cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(6)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_cd VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,matl_num VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
	,so_sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_sls_value NUMERIC(22,5)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_REGIONAL_SELLOUT_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(6)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
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
--DROP TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_offtake_npd;
CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.WKS_TAIWAN_REGIONAL_SELLOUT_OFFTAKE_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SNAPNTAWKS_INTEGRATION.wks_taiwan_regional_sellout_offtake_npd
(
	year VARCHAR(10)  		--//  ENCODE lzo
	,qrtr_no VARCHAR(14)  		--//  ENCODE lzo
	,mnth_id VARCHAR(21)  		--//  ENCODE lzo
	,mnth_no VARCHAR(10)  		--//  ENCODE lzo
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(6)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,soldto_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(10)  		--//  ENCODE lzo
	,distributor_name VARCHAR(100)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(100)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
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
	,region VARCHAR(2)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(2)  		--//  ENCODE lzo
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
	,ean VARCHAR(100)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(3)  		--//  ENCODE lzo
	,to_currency VARCHAR(3)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sellout_sales_value NUMERIC(38,5)  		--//  ENCODE az64
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
	,selling_price NUMERIC(38,5)  		--//  ENCODE az64
	,cnt_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
