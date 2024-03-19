CREATE TABLE ASPWKS_INTEGRATION.WKS_MY_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE ASPWKS_INTEGRATION.wks_my_inventory_health_analysis_propagation_final (
    year numeric(18,0),		--//  ENCODE az64 // integer
    year_quarter varchar(14),		--//  ENCODE lzo // character varying
    month_year varchar(21),		--//  ENCODE lzo // character varying
    month_number numeric(18,0),		--//  ENCODE az64 // integer
    country_name varchar(8),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(30),		--//  ENCODE lzo // character varying
    distributor_id_name varchar(40),		--//  ENCODE lzo // character varying
    franchise varchar(30),		--//  ENCODE lzo // character varying
    brand varchar(30),		--//  ENCODE lzo // character varying
    prod_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    variant varchar(100),		--//  ENCODE lzo // character varying
    segment varchar(50),		--//  ENCODE lzo // character varying
    prod_subsegment varchar(100),		--//  ENCODE lzo // character varying
    prod_category varchar(50),		--//  ENCODE lzo // character varying
    prod_subcategory varchar(50),		--//  ENCODE lzo // character varying
    put_up_description varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(100),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(3),		--//  ENCODE lzo // character varying
    to_ccy varchar(3),		--//  ENCODE lzo // character varying
    exch_rate numeric(15,5),		--//  ENCODE az64
    sap_prnt_cust_key varchar(12),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(61),		--//  ENCODE lzo // character varying
    zone_or_area varchar(80),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_grs_trd_sls numeric(38,5),		--//  ENCODE az64
    so_grs_trd_sls_usd numeric(17,0),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,6),		--//  ENCODE az64
    last_6months_so_qty numeric(38,6),		--//  ENCODE az64
    last_12months_so_qty numeric(38,6),		--//  ENCODE az64
    last_3months_so_val numeric(38,17),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,17),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,17),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,17),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,17),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,17),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,17),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,6),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,6),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,6)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE TABLE PCFWKS_INTEGRATION.PACIFIC_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE PCFWKS_INTEGRATION.pacific_inventory_health_analysis_propagation_final (
    year numeric(18,0),		--//  ENCODE az64 // integer
    qrtr_no varchar(11),		--//  ENCODE lzo // character varying
    mnth_id varchar(23),		--//  ENCODE lzo // character varying
    mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cntry_nm varchar(9),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(20),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd_nm varchar(2),		--//  ENCODE lzo // character varying
    global_prod_franchise varchar(30),		--//  ENCODE lzo // character varying
    global_prod_brand varchar(30),		--//  ENCODE lzo // character varying
    global_prod_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    global_prod_variant varchar(100),		--//  ENCODE lzo // character varying
    global_prod_segment varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subsegment varchar(100),		--//  ENCODE lzo // character varying
    global_prod_category varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subcategory varchar(50),		--//  ENCODE lzo // character varying
    global_put_up_desc varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(40),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(3),		--//  ENCODE lzo // character varying
    to_ccy varchar(3),		--//  ENCODE lzo // character varying
    exch_rate numeric(15,5),		--//  ENCODE az64
    sap_prnt_cust_key varchar(12),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(9),		--//  ENCODE lzo // character varying
    zone_or_area varchar(9),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(38,5),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,4),		--//  ENCODE az64
    last_6months_so_qty numeric(38,4),		--//  ENCODE az64
    last_12months_so_qty numeric(38,4),		--//  ENCODE az64
    last_3months_so_val numeric(38,4),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,4),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,4),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,4),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,4)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE TABLE ASPWKS_INTEGRATION.WKS_PH_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE ASPWKS_INTEGRATION.wks_ph_inventory_health_analysis_propagation_final (
    year numeric(18,0),		--//  ENCODE az64 // integer
    qrtr_no varchar(14),		--//  ENCODE lzo // character varying
    mnth_id varchar(21),		--//  ENCODE lzo // character varying
    mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    country_name varchar(11),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(50),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd_nm varchar(308),		--//  ENCODE lzo // character varying
    global_prod_franchise varchar(30),		--//  ENCODE lzo // character varying
    global_prod_brand varchar(30),		--//  ENCODE lzo // character varying
    global_prod_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    global_prod_variant varchar(100),		--//  ENCODE lzo // character varying
    global_prod_segment varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subsegment varchar(100),		--//  ENCODE lzo // character varying
    global_prod_category varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subcategory varchar(50),		--//  ENCODE lzo // character varying
    global_put_up_desc varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(255),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(5),		--//  ENCODE lzo // character varying
    to_ccy varchar(5),		--//  ENCODE lzo // character varying
    exch_rate numeric(15,5),		--//  ENCODE az64
    sap_prnt_cust_key varchar(12),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(255),		--//  ENCODE lzo // character varying
    zone_or_area varchar(255),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(22,0),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,6),		--//  ENCODE az64
    last_6months_so_qty numeric(38,6),		--//  ENCODE az64
    last_12months_so_qty numeric(38,6),		--//  ENCODE az64
    last_3months_so_val numeric(38,12),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,12),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,12),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,12),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date timestamp without time zone,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,12),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,12),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,12),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,6),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,6),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,6)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE TABLE ASPWKS_INTEGRATION.WKS_TW_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE ASPWKS_INTEGRATION.wks_tw_inventory_health_analysis_propagation_final (
    cal_year numeric(18,0),		--//  ENCODE az64 // integer
    cal_qrtr_no varchar(11),		--//  ENCODE lzo // character varying
    cal_mnth_id varchar(11),		--//  ENCODE lzo // character varying
    cal_mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cntry_nm varchar(6),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(12),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd_name varchar(2),		--//  ENCODE lzo // character varying
    global_prod_franchise varchar(500),		--//  ENCODE lzo // character varying
    global_prod_brand varchar(500),		--//  ENCODE lzo // character varying
    global_prod_sub_brand varchar(2),		--//  ENCODE lzo // character varying
    global_prod_variant varchar(500),		--//  ENCODE lzo // character varying
    global_prod_segment varchar(500),		--//  ENCODE lzo // character varying
    global_prod_subsegment varchar(2),		--//  ENCODE lzo // character varying
    global_prod_category varchar(500),		--//  ENCODE lzo // character varying
    global_prod_subcategory varchar(2),		--//  ENCODE lzo // character varying
    global_put_up_desc varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(100),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(3),		--//  ENCODE lzo // character varying
    to_ccy varchar(3),		--//  ENCODE lzo // character varying
    exch_rate numeric(15,5),		--//  ENCODE az64
    sap_prnt_cust_key varchar(13),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(6),		--//  ENCODE lzo // character varying
    zone_or_area varchar(6),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(38,5),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,4),		--//  ENCODE az64
    last_6months_so_qty numeric(38,4),		--//  ENCODE az64
    last_12months_so_qty numeric(38,4),		--//  ENCODE az64
    last_3months_so_val numeric(38,4),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,4),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,4),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,4),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,4),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,4)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE TABLE OSEWKS_INTEGRATION.WKS_THAILAND_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE OSEWKS_INTEGRATION.wks_thailand_inventory_health_analysis_propagation_final (
    year numeric(18,0),		--//  ENCODE az64 // integer
    year_quarter varchar(11),		--//  ENCODE lzo // character varying
    month_year varchar(11),		--//  ENCODE lzo // character varying
    month_number numeric(18,0),		--//  ENCODE az64 // integer
    country_name varchar(8),		--//  ENCODE lzo // character varying
    distributor_id varchar(12),		--//  ENCODE lzo // character varying
    distributor_id_name varchar(50),		--//  ENCODE lzo // character varying
    franchise varchar(30),		--//  ENCODE lzo // character varying
    brand varchar(30),		--//  ENCODE lzo // character varying
    prod_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    variant varchar(100),		--//  ENCODE lzo // character varying
    segment varchar(50),		--//  ENCODE lzo // character varying
    prod_subsegment varchar(100),		--//  ENCODE lzo // character varying
    prod_category varchar(50),		--//  ENCODE lzo // character varying
    prod_subcategory varchar(50),		--//  ENCODE lzo // character varying
    put_up_description varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(40),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(5),		--//  ENCODE lzo // character varying
    to_ccy varchar(5),		--//  ENCODE lzo // character varying
    exch_rate numeric(15,5),		--//  ENCODE az64
    sap_prnt_cust_key varchar(12),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(150),		--//  ENCODE lzo // character varying
    zone_or_area varchar(150),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_grs_trd_sls numeric(38,5),		--//  ENCODE az64
    so_grs_trd_sls_usd double precision,		--//  ENCODE raw
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty double precision,		--//  ENCODE raw
    last_6months_so_qty double precision,		--//  ENCODE raw
    last_12months_so_qty double precision,		--//  ENCODE raw
    last_3months_so_val double precision,		--//  ENCODE raw
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val double precision,		--//  ENCODE raw
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val double precision,		--//  ENCODE raw
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val double precision,		--//  ENCODE raw
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales double precision,		--//  ENCODE raw
    l6m_weeks_avg_sales double precision,		--//  ENCODE raw
    l3m_weeks_avg_sales double precision,		--//  ENCODE raw
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty double precision,		--//  ENCODE raw
    l6m_weeks_avg_sales_qty double precision,		--//  ENCODE raw
    l3m_weeks_avg_sales_qty double precision		--//  ENCODE raw
)
;		--// DISTSTYLE AUTO;

CREATE TABLE OSEWKS_INTEGRATION.WKS_VIETNAM_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE OSEWKS_INTEGRATION.wks_vietnam_inventory_health_analysis_propagation_final (
    year numeric(18,0),		--//  ENCODE az64 // integer
    qrtr_no varchar(11),		--//  ENCODE lzo // character varying
    mnth_id varchar(23),		--//  ENCODE lzo // character varying
    mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cntry_nm varchar(7),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(12),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd_name varchar(2),		--//  ENCODE lzo // character varying
    global_prod_franchise varchar(30),		--//  ENCODE lzo // character varying
    global_prod_brand varchar(30),		--//  ENCODE lzo // character varying
    global_prod_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    global_prod_variant varchar(100),		--//  ENCODE lzo // character varying
    global_prod_segment varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subsegment varchar(100),		--//  ENCODE lzo // character varying
    global_prod_category varchar(50),		--//  ENCODE lzo // character varying
    global_prod_subcategory varchar(50),		--//  ENCODE lzo // character varying
    global_put_up_desc varchar(30),		--//  ENCODE lzo // character varying
    sku_cd varchar(40),		--//  ENCODE lzo // character varying
    sku_description varchar(100),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    product_key varchar(68),		--//  ENCODE lzo // character varying
    product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_ccy varchar(3),		--//  ENCODE lzo // character varying
    to_ccy varchar(3),		--//  ENCODE lzo // character varying
    exch_rate numeric(20,10),		--//  ENCODE az64
    sap_prnt_cust_key varchar(12),		--//  ENCODE lzo // character varying
    sap_prnt_cust_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_cust_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_cust_sub_chnl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_chnl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_desc varchar(50),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_key varchar(12),		--//  ENCODE lzo // character varying
    sap_bnr_frmt_desc varchar(50),		--//  ENCODE lzo // character varying
    retail_env varchar(50),		--//  ENCODE lzo // character varying
    region varchar(20),		--//  ENCODE lzo // character varying
    zone_or_area varchar(100),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(38,5),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,4),		--//  ENCODE az64
    last_6months_so_qty numeric(38,4),		--//  ENCODE az64
    last_12months_so_qty numeric(38,4),		--//  ENCODE az64
    last_3months_so_val numeric(38,11),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,11),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,11),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,11),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,11),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,11),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,11),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,4),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,4)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;
