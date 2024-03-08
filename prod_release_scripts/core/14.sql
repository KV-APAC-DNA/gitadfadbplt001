CREATE OR REPLACE VIEW ASPEDW_ACCESS.EDW_REG_INVENTORY_HEALTH_ANALYSIS_PROPAGATION AS (
SELECT
year AS "year"
,year_quarter AS "year_quarter"
,month_year AS "month_year"
,month_number AS "month_number"
,country_name AS "country_name"
,distributor_id AS "distributor_id"
,distributor_id_name AS "distributor_id_name"
,franchise AS "franchise"
,brand AS "brand"
,prod_sub_brand AS "prod_sub_brand"
,variant AS "variant"
,segment AS "segment"
,prod_subsegment AS "prod_subsegment"
,prod_category AS "prod_category"
,prod_subcategory AS "prod_subcategory"
,put_up_description AS "put_up_description"
,sku_cd AS "sku_cd"
,sku_description AS "sku_description"
,pka_product_key AS "pka_product_key"
,pka_product_key_description AS "pka_product_key_description"
,product_key AS "product_key"
,product_key_description AS "product_key_description"
,from_ccy AS "from_ccy"
,to_ccy AS "to_ccy"
,exch_rate AS "exch_rate"
,sap_prnt_cust_key AS "sap_prnt_cust_key"
,sap_prnt_cust_desc AS "sap_prnt_cust_desc"
,sap_cust_chnl_key AS "sap_cust_chnl_key"
,sap_cust_chnl_desc AS "sap_cust_chnl_desc"
,sap_cust_sub_chnl_key AS "sap_cust_sub_chnl_key"
,sap_sub_chnl_desc AS "sap_sub_chnl_desc"
,sap_go_to_mdl_key AS "sap_go_to_mdl_key"
,sap_go_to_mdl_desc AS "sap_go_to_mdl_desc"
,sap_bnr_key AS "sap_bnr_key"
,sap_bnr_desc AS "sap_bnr_desc"
,sap_bnr_frmt_key AS "sap_bnr_frmt_key"
,sap_bnr_frmt_desc AS "sap_bnr_frmt_desc"
,retail_env AS "retail_env"
,region AS "region"
,zone_or_area AS "zone_or_area"
,si_sls_qty AS "si_sls_qty"
,si_gts_val AS "si_gts_val"
,si_gts_val_usd AS "si_gts_val_usd"
,inventory_quantity AS "inventory_quantity"
,inventory_val AS "inventory_val"
,inventory_val_usd AS "inventory_val_usd"
,so_sls_qty AS "so_sls_qty"
,so_grs_trd_sls AS "so_grs_trd_sls"
,so_grs_trd_sls_usd AS "so_grs_trd_sls_usd"
,si_all_db_val AS "si_all_db_val"
,si_all_db_val_usd AS "si_all_db_val_usd"
,si_inv_db_val AS "si_inv_db_val"
,si_inv_db_val_usd AS "si_inv_db_val_usd"
,last_3months_so_qty AS "last_3months_so_qty"
,last_6months_so_qty AS "last_6months_so_qty"
,last_12months_so_qty AS "last_12months_so_qty"
,last_3months_so_val AS "last_3months_so_val"
,last_3months_so_val_usd AS "last_3months_so_val_usd"
,last_6months_so_val AS "last_6months_so_val"
,last_6months_so_val_usd AS "last_6months_so_val_usd"
,last_12months_so_val AS "last_12months_so_val"
,last_12months_so_val_usd AS "last_12months_so_val_usd"
,propagate_flag AS "propagate_flag"
,propagate_from AS "propagate_from"
,reason AS "reason"
,last_36months_so_val AS "last_36months_so_val"
,healthy_inventory AS "healthy_inventory"
,min_date AS "min_date"
,diff_weeks AS "diff_weeks"
,l12m_weeks AS "l12m_weeks"
,l6m_weeks AS "l6m_weeks"
,l3m_weeks AS "l3m_weeks"
,l12m_weeks_avg_sales AS "l12m_weeks_avg_sales"
,l6m_weeks_avg_sales AS "l6m_weeks_avg_sales"
,l3m_weeks_avg_sales AS "l3m_weeks_avg_sales"
,l12m_weeks_avg_sales_usd AS "l12m_weeks_avg_sales_usd"
,l6m_weeks_avg_sales_usd AS "l6m_weeks_avg_sales_usd"
,l3m_weeks_avg_sales_usd AS "l3m_weeks_avg_sales_usd"
,l12m_weeks_avg_sales_qty AS "l12m_weeks_avg_sales_qty"
,l6m_weeks_avg_sales_qty AS "l6m_weeks_avg_sales_qty"
,l3m_weeks_avg_sales_qty AS "l3m_weeks_avg_sales_qty"
FROM ASPEDW_INTEGRATION.EDW_REG_INVENTORY_HEALTH_ANALYSIS_PROPAGATION
);


create or replace VIEW ASPEDW_ACCESS.EDW_COMPANY_DIM AS 
(SELECT
	CLNT as "clnt" ,
	CO_CD as "co_cd",
	CTRY_KEY as "ctry_key",
	CTRY_NM as "ctry_nm",
	CRNCY_KEY as "crncy_key" ,
	CHRT_ACCT as "chrt_acct",
	CRDT_CNTRL_AREA as "crdt_cntrl" ,
	FISC_YR_VRNT as "fisc_yr_vrnt",
	COMPANY as "company",
	COMPANY_NM as "company_nm",
	CRT_DTTM as "crt_dttm",
	UPDT_DTTM as "updt_dttm",
	CTRY_GROUP as "ctry_group",
	"cluster" 
FROM ASPEDW_INTEGRATION.EDW_COMPANY_DIM
);

