CREATE OR REPLACE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_POS_SALES_FACT (		--// CREATE OR REPLACE TABLE MYSITG_INTEGRATION.sdl_raw_my_pos_sales_fact (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    store_cd varchar(255),		--//  ENCODE lzo // character varying
    store_nm varchar(255),		--//  ENCODE lzo // character varying
    dept_cd varchar(255),		--//  ENCODE lzo // character varying
    dept_nm varchar(255),		--//  ENCODE lzo // character varying
    mt_item_cd varchar(255),		--//  ENCODE lzo // character varying
    mt_item_desc varchar(255),		--//  ENCODE lzo // character varying
    jj_mnth_id varchar(255),		--//  ENCODE lzo // character varying
    jj_yr_week_no varchar(255),		--//  ENCODE lzo // character varying
    qty varchar(255),		--//  ENCODE lzo // character varying
    so_val varchar(255),		--//  ENCODE lzo // character varying
    sap_matl_num varchar(255),		--//  ENCODE lzo // character varying
    file_nm varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt varchar(255)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE EVEN;

CREATE OR REPLACE TABLE MYSITG_INTEGRATION.sdl_my_raw_as_watsons_inventory (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_my_raw_as_watsons_inventory (
    cust_cd varchar(30),		--//  ENCODE lzo // character varying
    store_cd varchar(30),		--//  ENCODE lzo // character varying
    year varchar(10),		--//  ENCODE lzo // character varying
    mnth_id varchar(10),		--//  ENCODE lzo // character varying
    matl_num varchar(100),		--//  ENCODE lzo // character varying
    inv_qty_pc varchar(100),		--//  ENCODE lzo // character varying
    inv_value varchar(100),		--//  ENCODE lzo // character varying
    filename varchar(100),		--//  ENCODE lzo // character varying
    crtd_dttm timestamp without time zone		--//  ENCODE az64
)
;

CREATE TABLE MYSITG_INTEGRATION.ITG_MDS_MY_CUSTOMER_HIERARCHY (		--// CREATE TABLE MYSITG_INTEGRATION.itg_mds_my_customer_hierarchy (
    sold_to varchar(10),		--//  ENCODE lzo // character varying
    sold_to_desc varchar(200),		--//  ENCODE lzo // character varying
    cust_grp_code varchar(10),		--//  ENCODE lzo // character varying
    cust_grp varchar(50),		--//  ENCODE lzo // character varying
    customer varchar(200),		--//  ENCODE lzo // character varying
    channel varchar(50),		--//  ENCODE lzo // character varying
    channel_name varchar(100),		--//  ENCODE lzo // character varying
    territory varchar(50),		--//  ENCODE lzo // character varying
    territory_name varchar(100),		--//  ENCODE lzo // character varying
    reg_channel varchar(50),		--//  ENCODE lzo // character varying
    reg_channel_name varchar(100),		--//  ENCODE lzo // character varying
    reg_group varchar(50),		--//  ENCODE lzo // character varying
    reg_group_name varchar(100),		--//  ENCODE lzo // character varying
    crt_dttm timestamp without time zone DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone,		--//     crt_dttm timestamp without time zone DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64, //  ENCODE az64 // character varying
    customer_segmentation_level_2_code varchar(256)		--//  ENCODE lzo // character varying
)
;	

CREATE TABLE MYSITG_INTEGRATION.ITG_MY_LISTPRICE (		--// CREATE TABLE MYSITG_INTEGRATION.itg_my_listprice (
    plant varchar(4),		--//  ENCODE lzo // character varying
    cnty varchar(4),		--//  ENCODE lzo // character varying
    item_cd varchar(20),		--//  ENCODE lzo // character varying
    item_desc varchar(100),		--//  ENCODE lzo // character varying
    valid_from varchar(10),		--//  ENCODE lzo // character varying
    valid_to varchar(10),		--//  ENCODE lzo // character varying
    rate numeric(20,4),		--//  ENCODE delta
    currency varchar(4),		--//  ENCODE lzo // character varying
    field9 numeric(16,4),		--//  ENCODE delta
    uom varchar(6),		--//  ENCODE lzo // character varying
    yearmo varchar(6),		--//  ENCODE lzo // character varying
    jj_mnth_id varchar(6),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    crtd_dttm timestamp without time zone,		--//  ENCODE delta
    updt_dttm timestamp without time zone,		--//  ENCODE delta
    snapshot_dt varchar(20)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE AUTO;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_POS_CUST_MSTR (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_pos_cust_mstr (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    store_cd varchar(255),		--//  ENCODE lzo // character varying
    store_nm varchar(255),		--//  ENCODE lzo // character varying
    dept_cd varchar(255),		--//  ENCODE lzo // character varying
    dept_nm varchar(255),		--//  ENCODE lzo // character varying
    region varchar(255),		--//  ENCODE lzo // character varying
    store_frmt varchar(255),		--//  ENCODE lzo // character varying
    store_type varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;	
CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_CUSTOMER_DIM (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_customer_dim (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_grp_nm varchar(255),		--//  ENCODE lzo // character varying
    ullage varchar(255),		--//  ENCODE lzo // character varying
    chnl varchar(255),		--//  ENCODE lzo // character varying
    territory varchar(255),		--//  ENCODE lzo // character varying
    retail_env varchar(255),		--//  ENCODE lzo // character varying
    trdng_term_val varchar(255),		--//  ENCODE lzo // character varying
    rdd_ind varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_ACCRUALS (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_accruals (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    jj_year varchar(255),		--//  ENCODE lzo // character varying
    file_type varchar(255),		--//  ENCODE lzo // character varying
    jan_val varchar(255),		--//  ENCODE lzo // character varying
    feb_val varchar(255),		--//  ENCODE lzo // character varying
    mar_val varchar(255),		--//  ENCODE lzo // character varying
    apr_val varchar(255),		--//  ENCODE lzo // character varying
    may_val varchar(255),		--//  ENCODE lzo // character varying
    jun_val varchar(255),		--//  ENCODE lzo // character varying
    jul_val varchar(255),		--//  ENCODE lzo // character varying
    aug_val varchar(255),		--//  ENCODE lzo // character varying
    sep_val varchar(255),		--//  ENCODE lzo // character varying
    oct_val varchar(255),		--//  ENCODE lzo // character varying
    nov_val varchar(255),		--//  ENCODE lzo // character varying
    dec_val varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_AFGR (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_afgr (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    afgr_num varchar(255),		--//  ENCODE lzo // character varying
    cust_dn_num varchar(255),		--//  ENCODE lzo // character varying
    dn_amt_exc_gst_val varchar(255),		--//  ENCODE lzo // character varying
    afgr_amt varchar(255),		--//  ENCODE lzo // character varying
    dt_to_sc varchar(255),		--//  ENCODE lzo // character varying
    sc_validation varchar(255),		--//  ENCODE lzo // character varying
    rtn_ord_num varchar(255),		--//  ENCODE lzo // character varying
    rtn_ord_dt varchar(255),		--//  ENCODE lzo // character varying
    rtn_ord_amt varchar(255),		--//  ENCODE lzo // character varying
    cn_exp_issue_dt varchar(255),		--//  ENCODE lzo // character varying
    bill_num varchar(255),		--//  ENCODE lzo // character varying
    bill_dt varchar(255),		--//  ENCODE lzo // character varying
    cn_amt varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_CIW_MAP (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_ciw_map (
    ciw_ctgry varchar(255),		--//  ENCODE lzo // character varying
    ciw_buckt1 varchar(255),		--//  ENCODE lzo // character varying
    ciw_buckt2 varchar(255),		--//  ENCODE lzo // character varying
    bravo_cd1 varchar(255),		--//  ENCODE lzo // character varying
    bravo_desc1 varchar(255),		--//  ENCODE lzo // character varying
    bravo_cd2 varchar(255),		--//  ENCODE lzo // character varying
    bravo_desc2 varchar(255),		--//  ENCODE lzo // character varying
    acct_type varchar(255),		--//  ENCODE lzo // character varying
    acct_num varchar(255),		--//  ENCODE lzo // character varying
    acct_desc varchar(255),		--//  ENCODE lzo // character varying
    acct_type1 varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_TRGTS (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_trgts (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    brnd_desc varchar(255),		--//  ENCODE lzo // character varying
    sub_segment varchar(255),		--//  ENCODE lzo // character varying
    jj_year varchar(255),		--//  ENCODE lzo // character varying
    trgt_type varchar(255),		--//  ENCODE lzo // character varying
    trgt_val_type varchar(255),		--//  ENCODE lzo // character varying
    jan_val varchar(255),		--//  ENCODE lzo // character varying
    feb_val varchar(255),		--//  ENCODE lzo // character varying
    mar_val varchar(255),		--//  ENCODE lzo // character varying
    apr_val varchar(255),		--//  ENCODE lzo // character varying
    may_val varchar(255),		--//  ENCODE lzo // character varying
    jun_val varchar(255),		--//  ENCODE lzo // character varying
    jul_val varchar(255),		--//  ENCODE lzo // character varying
    aug_val varchar(255),		--//  ENCODE lzo // character varying
    sep_val varchar(255),		--//  ENCODE lzo // character varying
    oct_val varchar(255),		--//  ENCODE lzo // character varying
    nov_val varchar(255),		--//  ENCODE lzo // character varying
    dec_val varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_LE_TRGT (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_le_trgt (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    jj_year varchar(255),		--//  ENCODE lzo // character varying
    mnth_nm varchar(255),		--//  ENCODE lzo // character varying
    trgt_type varchar(255),		--//  ENCODE lzo // character varying
    trgt_val_type varchar(255),		--//  ENCODE lzo // character varying
    wk1 varchar(255),		--//  ENCODE lzo // character varying
    wk2 varchar(255),		--//  ENCODE lzo // character varying
    wk3 varchar(255),		--//  ENCODE lzo // character varying
    wk4 varchar(255),		--//  ENCODE lzo // character varying
    wk5 varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_DSTRBTRR_DIM (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_dstrbtrr_dim (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    lvl1 varchar(255),		--//  ENCODE lzo // character varying
    lvl2 varchar(255),		--//  ENCODE lzo // character varying
    lvl3 varchar(255),		--//  ENCODE lzo // character varying
    lvl4 varchar(255),		--//  ENCODE lzo // character varying
    lvl5 varchar(255),		--//  ENCODE lzo // character varying
    trdng_term_val varchar(255),		--//  ENCODE lzo // character varying
    abbrevation varchar(255),		--//  ENCODE lzo // character varying
    buyer_gln varchar(255),		--//  ENCODE lzo // character varying
    location_gln varchar(255),		--//  ENCODE lzo // character varying
    chnl_manager varchar(255),		--//  ENCODE lzo // character varying
    cdm varchar(255),		--//  ENCODE lzo // character varying
    region varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_DSTRBTR_DOC_TYPE (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_dstrbtr_doc_type (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    lvl1 varchar(255),		--//  ENCODE lzo // character varying
    lvl2 varchar(255),		--//  ENCODE lzo // character varying
    wh_id varchar(255),		--//  ENCODE lzo // character varying
    doc_type varchar(255),		--//  ENCODE lzo // character varying
    doc_type_desc varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_IN_TRANSIT (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_in_transit (
    bill_doc varchar(255),		--//  ENCODE lzo // character varying
    bill_dt varchar(255),		--//  ENCODE lzo // character varying
    gr_dt varchar(255),		--//  ENCODE lzo // character varying
    closing_dt varchar(255),		--//  ENCODE lzo // character varying
    remarks varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt varchar(255)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_IDS_RATE (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_ids_rate (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    exchng_rate varchar(255),		--//  ENCODE lzo // character varying
    yearmo varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_DAILY_SELLOUT_SALES_FACT (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_daily_sellout_sales_fact (
    dstrbtr_id varchar(255),		--//  ENCODE lzo // character varying
    sls_ord_num varchar(255),		--//  ENCODE lzo // character varying
    sls_ord_dt varchar(255),		--//  ENCODE lzo // character varying
    type varchar(255),		--//  ENCODE lzo // character varying
    cust_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_wh_id varchar(255),		--//  ENCODE lzo // character varying
    item_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_cd varchar(255),		--//  ENCODE lzo // character varying
    ean_num varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_desc varchar(255),		--//  ENCODE lzo // character varying
    grs_prc varchar(255),		--//  ENCODE lzo // character varying
    qty varchar(255),		--//  ENCODE lzo // character varying
    uom varchar(255),		--//  ENCODE lzo // character varying
    qty_pc varchar(255),		--//  ENCODE lzo // character varying
    qty_aft_conv varchar(255),		--//  ENCODE lzo // character varying
    subtotal_1 varchar(255),		--//  ENCODE lzo // character varying
    discount varchar(255),		--//  ENCODE lzo // character varying
    subtotal_2 varchar(255),		--//  ENCODE lzo // character varying
    bottom_line_dscnt varchar(255),		--//  ENCODE lzo // character varying
    total_amt_aft_tax varchar(255),		--//  ENCODE lzo // character varying
    total_amt_bfr_tax varchar(255),		--//  ENCODE lzo // character varying
    sls_emp varchar(255),		--//  ENCODE lzo // character varying
    custom_field1 varchar(255),		--//  ENCODE lzo // character varying
    custom_field2 varchar(255),		--//  ENCODE lzo // character varying
    custom_field3 varchar(255),		--//  ENCODE lzo // character varying
    filename varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSITG_INTEGRATION.SDL_RAW_MY_DAILY_SELLOUT_STOCK_FACT (		--// CREATE TABLE MYSITG_INTEGRATION.sdl_raw_my_daily_sellout_stock_fact (
    cust_id varchar(255),		--//  ENCODE lzo // character varying
    inv_dt varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_wh_id varchar(255),		--//  ENCODE lzo // character varying
    item_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_cd varchar(255),		--//  ENCODE lzo // character varying
    ean_num varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_desc varchar(255),		--//  ENCODE lzo // character varying
    qty varchar(255),		--//  ENCODE lzo // character varying
    uom varchar(255),		--//  ENCODE lzo // character varying
    qty_on_ord varchar(255),		--//  ENCODE lzo // character varying
    uom_on_ord varchar(255),		--//  ENCODE lzo // character varying
    qty_committed varchar(255),		--//  ENCODE lzo // character varying
    uom_committed varchar(255),		--//  ENCODE lzo // character varying
    available_qty_pc varchar(255),		--//  ENCODE lzo // character varying
    qty_on_ord_pc varchar(255),		--//  ENCODE lzo // character varying
    qty_committed_pc varchar(255),		--//  ENCODE lzo // character varying
    unit_prc varchar(255),		--//  ENCODE lzo // character varying
    total_val varchar(255),		--//  ENCODE lzo // character varying
    custom_field1 varchar(255),		--//  ENCODE lzo // character varying
    custom_field2 varchar(255),		--//  ENCODE lzo // character varying
    filename varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE EVEN;

CREATE TABLE MYSWKS_INTEGRATION.WKS_MY_SELLOUT_SALES_FACT (		--// CREATE TABLE os_wks.wks_my_sellout_sales_fact (
    dstrbtr_id varchar(255),		--//  ENCODE lzo // character varying
    sls_ord_num varchar(255),		--//  ENCODE lzo // character varying
    sls_ord_dt varchar(255),		--//  ENCODE lzo // character varying
    type varchar(255),		--//  ENCODE lzo // character varying
    cust_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_wh_id varchar(255),		--//  ENCODE lzo // character varying
    item_cd varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_cd varchar(255),		--//  ENCODE lzo // character varying
    ean_num varchar(255),		--//  ENCODE lzo // character varying
    dstrbtr_prod_desc varchar(255),		--//  ENCODE lzo // character varying
    grs_prc varchar(255),		--//  ENCODE lzo // character varying
    qty varchar(255),		--//  ENCODE lzo // character varying
    uom varchar(255),		--//  ENCODE lzo // character varying
    qty_pc varchar(255),		--//  ENCODE lzo // character varying
    qty_aft_conv varchar(255),		--//  ENCODE lzo // character varying
    subtotal_1 varchar(255),		--//  ENCODE lzo // character varying
    discount varchar(255),		--//  ENCODE lzo // character varying
    subtotal_2 varchar(255),		--//  ENCODE lzo // character varying
    bottom_line_dscnt varchar(255),		--//  ENCODE lzo // character varying
    total_amt_aft_tax varchar(255),		--//  ENCODE lzo // character varying
    total_amt_bfr_tax varchar(255),		--//  ENCODE lzo // character varying
    sls_emp varchar(255),		--//  ENCODE lzo // character varying
    custom_field1 varchar(255),		--//  ENCODE lzo // character varying
    custom_field2 varchar(255),		--//  ENCODE lzo // character varying
    custom_field3 varchar(255),		--//  ENCODE lzo // character varying
    filename varchar(255),		--//  ENCODE lzo // character varying
    cdl_dttm varchar(255),		--//  ENCODE lzo // character varying
    curr_dt timestamp without time zone		--//  ENCODE delta
)
;		--// DISTSTYLE AUTO;
