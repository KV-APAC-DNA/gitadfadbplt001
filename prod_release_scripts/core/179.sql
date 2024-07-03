CREATE OR REPLACE TABLE aspedw_integration.EDW_REG_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_SYNC
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_quarter VARCHAR(14)  		--//  ENCODE lzo
	,month_year VARCHAR(23)  		--//  ENCODE lzo
	,month_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_name VARCHAR(40)
	,distributor_id VARCHAR(100)  		--//  ENCODE lzo
	,distributor_id_name VARCHAR(250)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,prod_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,variant VARCHAR(100)  		--//  ENCODE lzo
	,segment VARCHAR(50)  		--//  ENCODE lzo
	,prod_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,prod_category VARCHAR(50)  		--//  ENCODE lzo
	,prod_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,sku_cd VARCHAR(255)  		--//  ENCODE lzo
	,sku_description VARCHAR(100)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(200)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(500)  		--//  ENCODE lzo
	,product_key VARCHAR(200)  		--//  ENCODE lzo
	,product_key_description VARCHAR(500)  		--//  ENCODE lzo
	,from_ccy VARCHAR(5)  		--//  ENCODE lzo
	,to_ccy VARCHAR(5)  		--//  ENCODE lzo
	,exch_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sap_prnt_cust_key VARCHAR(50)  		--//  ENCODE lzo
	,sap_prnt_cust_desc VARCHAR(382)  		--//  ENCODE lzo
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
	,region VARCHAR(255)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(255)  		--//  ENCODE lzo
	,si_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val NUMERIC(38,5)  		--//  ENCODE az64
	,si_gts_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_quantity NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val NUMERIC(38,5)  		--//  ENCODE az64
	,inventory_val_usd NUMERIC(38,5)  		--//  ENCODE az64
	,so_sls_qty NUMERIC(38,5)  		--//  ENCODE az64
	,so_grs_trd_sls NUMERIC(38,5)  		--//  ENCODE az64
	,so_grs_trd_sls_usd DOUBLE PRECISION
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
	,diff_weeks numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,l12m_weeks numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,l6m_weeks numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,l3m_weeks numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,l12m_weeks_avg_sales NUMERIC(38,17)  		--//  ENCODE az64
	,l6m_weeks_avg_sales NUMERIC(38,17)  		--//  ENCODE az64
	,l3m_weeks_avg_sales NUMERIC(38,17)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l6m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l3m_weeks_avg_sales_usd NUMERIC(38,5)  		--//  ENCODE az64
	,l12m_weeks_avg_sales_qty NUMERIC(38,5)  		--//  ENCODE az64
	,l6m_weeks_avg_sales_qty NUMERIC(38,5)  		--//  ENCODE az64
	,l3m_weeks_avg_sales_qty NUMERIC(38,5)  		--//  ENCODE az64
);
