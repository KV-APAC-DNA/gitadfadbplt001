CREATE TABLE PHLWKS_INTEGRATION.WKS_PH_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_PRESTEP (		--// CREATE TABLE rg_wks.wks_ph_inventory_health_analysis_propagation_prestep (
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
    last_36months_so_val numeric(38,12)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;
