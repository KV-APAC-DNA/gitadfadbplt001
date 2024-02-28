USE DATABASE PROD_DNA_CORE;
USE schema SNAPOSEITG_INTEGRATION;

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

create or replace TABLE SGPITG_INTEGRATION.ITG_QUERY_PARAMETERS (
	COUNTRY_CODE VARCHAR(20),
	PARAMETER_NAME VARCHAR(300),
	PARAMETER_VALUE VARCHAR(300),
	PARAMETER_TYPE VARCHAR(100)
);

create or replace TABLE SGPITG_INTEGRATION.SDL_RAW_SG_CIW_MAPPING (
	CONDITION_TYPE VARCHAR(500),
	GL VARCHAR(10),
	GL_DESCRIPTION VARCHAR(500),
	POSTED_WHERE VARCHAR(500),
	PURPOSE VARCHAR(500),
	CIW_BUCKET VARCHAR(500),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_COPA17_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	FISC_YR_PER NUMBER(7,0),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(4,0),
	CAL_DAY DATE,
	PSTNG_PER NUMBER(3,0),
	CAL_YR_MO NUMBER(6,0),
	CAL_YR NUMBER(4,0),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(3,0),
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_EMP_HIST NUMBER(8,0),
	SLS_ORG VARCHAR(4),
	SLS_GRP VARCHAR(3),
	SLS_OFF VARCHAR(4),
	CUST_GRP VARCHAR(2),
	DSTN_CHNL VARCHAR(2),
	SLS_DSTRC VARCHAR(6),
	CUST VARCHAR(10),
	MATL VARCHAR(18),
	CUST_SLS_VIEW VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	MERCIA_REF VARCHAR(5),
	B3_BASE_PROD VARCHAR(3),
	B4_VRNT VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	RGN VARCHAR(3),
	CTRY VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	FRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	FISC_QTR NUMBER(1,0),
	MATL2 VARCHAR(18),
	BILL_TYPE VARCHAR(4),
	FISC_WK DATE,
	AMT_GRP_CRCY NUMBER(20,2),
	AMT_OBJ_CRCY NUMBER(20,2),
	CRNCY VARCHAR(5),
	OBJ_CRNCY VARCHAR(5),
	ACCT_NUM VARCHAR(10),
	CHRT_OF_ACCT VARCHAR(4),
	MGMT_ENTITY VARCHAR(6),
	SLS_PRSN_RESPONS VARCHAR(30),
	BUSN_AREA VARCHAR(4),
	GA NUMBER(20,2),
	TC VARCHAR(5),
	MATL_PLNT_VIEW VARCHAR(18),
	QTY NUMBER(20,3),
	UOM VARCHAR(10),
	SLS_VOL_IEU NUMBER(17,3),
	UN_SLS_VOL__IEU VARCHAR(10),
	BPT_DSTN_CHNL VARCHAR(2),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_COPA_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_ORG VARCHAR(4),
	MATL VARCHAR(18),
	CUST_NUM VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	CHRT_ACCT VARCHAR(4),
	ACCT_NUM VARCHAR(10),
	DSTR_CHNL VARCHAR(2),
	FISC_YR_VAR VARCHAR(2),
	VERS VARCHAR(3),
	BW_DELTA_UPD_MODE VARCHAR(1),
	BILL_TYP VARCHAR(4),
	SLS_OFF VARCHAR(4),
	CNTRY_KEY VARCHAR(3),
	SLS_DEAL VARCHAR(10),
	SLS_GRP VARCHAR(3),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_DIST VARCHAR(6),
	CUST_GRP VARCHAR(2),
	CUST_SLS VARCHAR(10),
	BUSS_AREA VARCHAR(4),
	VAL_TYPE_RPT NUMBER(18,0),
	MERCIA_REF VARCHAR(5),
	CALN_DAY VARCHAR(20),
	CALN_YR_MO NUMBER(18,0),
	FISC_YR NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	B3_BASE_PROD VARCHAR(3),
	B4_VAR VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	REG VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	GRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	SLS_PRSN_RESP VARCHAR(30),
	MATL_SLS VARCHAR(18),
	PROD_HIER VARCHAR(18),
	MGMT_ENTITY VARCHAR(6),
	FX_AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	CRNCY_KEY VARCHAR(5),
	AMT_OBJ_CRNCY NUMBER(20,5),
	OBJ_CRNCY_CO_OBJ VARCHAR(5),
	GRS_AMT_TRANS_CRNCY NUMBER(20,5),
	CRNCY_KEY_TRANS_CRNCY VARCHAR(5),
	QTY NUMBER(20,5),
	UOM VARCHAR(20),
	SLS_VOL NUMBER(20,5),
	UN_SLS_VOL VARCHAR(20),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_INVC_SLS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	ACT_DELV_DT DATE,
	ACT_GOOD_ISS_DT DATE,
	BILL_TO_PRTY VARCHAR(10),
	BILL_DT DATE,
	BILL_TY VARCHAR(4),
	BILL_DOC VARCHAR(10),
	CMPY_CD VARCHAR(4),
	CUST_NO VARCHAR(10),
	DELV_DOC_CRT_DT DATE,
	DSTR_CHNL VARCHAR(2),
	DIV VARCHAR(2),
	DOC_CRT_DT DATE,
	DOC_DT DATE,
	GOOD_ISS_DT DATE,
	MAT VARCHAR(18),
	MAT_AVAIL_DT DATE,
	ORD_RSN VARCHAR(3),
	OVRL_REJ_STS VARCHAR(1),
	OVRL_STS_CRD_CHK VARCHAR(1),
	PAYER VARCHAR(10),
	PLANT VARCHAR(4),
	PREC_DOC_ITM NUMBER(18,0),
	PREC_DOC_NUM VARCHAR(10),
	PROOF_DELV_DT DATE,
	RSN_CD_KEY VARCHAR(29),
	RSN_REJ VARCHAR(2),
	RLSE_DT_CR_MGMT DATE,
	RQST_DELV_DT DATE,
	ROUTE VARCHAR(6),
	SLS_DOC VARCHAR(10),
	SLS_DOC_CAT VARCHAR(2),
	SLS_DOC_ITM NUMBER(18,0),
	SLS_DOC_TYP VARCHAR(4),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_ORG VARCHAR(4),
	SLS_DOC_ITM_CAT VARCHAR(4),
	SHIP_TO_PRTY VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	BILL_QTY_CSE NUMBER(15,4),
	BILL_QTY_PC NUMBER(15,4),
	BILL_QTY_DIFOT NUMBER(15,4),
	BILL_QTY_OTIF NUMBER(15,4),
	BILL_QTY_SLS_UOM NUMBER(15,4),
	CNFRM_QTY_DIFOT NUMBER(15,4),
	CNFRM_QTY_PC NUMBER(15,4),
	DELV_QTY_CSE NUMBER(15,4),
	DELV_QTY_PC NUMBER(15,4),
	DELV_QTY_SLS_UOM NUMBER(15,4),
	EST_NTS NUMBER(15,4),
	NTS_BILL NUMBER(15,4),
	NET_INVC_SLS NUMBER(15,4),
	FUT_SLS_QTY NUMBER(15,4),
	GROS_TRD_SLS NUMBER(15,4),
	NET_AMT NUMBER(15,4),
	NET_PRC NUMBER(15,4),
	NET_BILL_VAL NUMBER(15,4),
	NET_ORD_VAL NUMBER(15,4),
	ORD_QTY_CSE NUMBER(15,4),
	ORD_QTY_PC NUMBER(15,4),
	ORD_PC_QTY NUMBER(15,4),
	ORD_SLS_QTY NUMBER(15,4),
	TRAN_LDTM NUMBER(15,4),
	UNSPP_QTY NUMBER(15,4),
	UNSPP_VAL NUMBER(15,4),
	VOL_DELV NUMBER(15,4),
	VOL_ORD NUMBER(20,4),
	CAL_DAY DATE,
	BASE_UOM VARCHAR(4),
	CURR_KEY VARCHAR(5),
	DOC_CURR VARCHAR(5),
	SLS_UNIT VARCHAR(4),
	FISC_YR VARCHAR(10),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_INVNT (
	REQUEST_NUMBER VARCHAR(100),
	DATA_PACKET VARCHAR(50),
	DATA_RECORD VARCHAR(100),
	LCL_CRNCY VARCHAR(5),
	BASE_UNIT VARCHAR(3),
	MATL_NO VARCHAR(18),
	STRG_LOC VARCHAR(4),
	STK_TYPE VARCHAR(1),
	STK_CAT VARCHAR(1),
	CO_CD VARCHAR(4),
	MATL_PLNT_VIEW VARCHAR(18),
	BTCH_NUM VARCHAR(10),
	PLNT VARCHAR(4),
	WHSE_NUM VARCHAR(3),
	STRG_BIN VARCHAR(10),
	STRG_TYPE VARCHAR(3),
	SPL_STCK_VAL VARCHAR(1),
	SPL_STCK_INDICA VARCHAR(1),
	DOC_DT DATE,
	VALUT_CLS VARCHAR(4),
	VALUT_AREA VARCHAR(4),
	VALUT_TYPE VARCHAR(10),
	PSTNG_DT DATE,
	CAL_DAY DATE,
	WH_MSTR VARCHAR(18),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(18,0),
	VEND VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	MVMT_IND_SEC VARCHAR(1),
	CRNCY VARCHAR(5),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(18,0),
	CAL_YR NUMBER(18,0),
	CAL_MO NUMBER(18,0),
	QTR NUMBER(18,0),
	CAL_YR_QTR NUMBER(18,0),
	CAL_YR_WK NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	HALF_YR NUMBER(18,0),
	WKDAY NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	CAL_YR_MON NUMBER(18,0),
	STCK_REC_VAL NUMBER(19,2),
	ISS_STCK_VAL NUMBER(19,2),
	ISS_BLOK_QTY NUMBER(20,3),
	CNG_STK_QTY_ISS NUMBER(20,3),
	ISS_QTY_QUAL NUMBER(20,3),
	ISS_QTY_TRST NUMBER(20,3),
	RCPT_QTY_BLOK NUMBER(20,3),
	CNS_STCK_RCPT NUMBER(20,3),
	RCPT_QTY_QUAL NUMBER(20,3),
	RCPT_QTY_TRST NUMBER(20,3),
	ISS_QTY_SCRAP NUMBER(20,3),
	ISS_VAL_SCRAP NUMBER(19,2),
	RCPT_TOT_STCK NUMBER(20,3),
	ISS_TOT_STCK NUMBER(20,3),
	ISS_QTY_STCK_VAL NUMBER(20,3),
	REC_QTY_VAL_STCK NUMBER(20,3),
	VDR_CNSGNMNT_STCK_CNVAL NUMBER(19,2),
	REC_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_BLOK NUMBER(19,2),
	ISS_VAL_BLOK NUMBER(19,2),
	ISS_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_TRST NUMBER(19,2),
	ISS_VAL_TRST NUMBER(19,2),
	ISS_VAL_CONS_STCK NUMBER(19,2),
	RCPT_VAL_CONS_STK NUMBER(19,2),
	BW_PRCH_VAL NUMBER(19,2),
	BW_AMT_BUNITM NUMBER(20,3),
	DELV_COST NUMBER(19,2),
	STD_MATL_COST NUMBER(19,2),
	PRC_UNIT NUMBER(20,3),
	LINE_CNT NUMBER(18,0),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS ITG_MDS_MY_CUSTOMER_HIERARCHY (
	SOLD_TO VARCHAR(10),
	SOLD_TO_DESC VARCHAR(200),
	CUST_GRP_CODE VARCHAR(10),
	CUST_GRP VARCHAR(50),
	CUSTOMER VARCHAR(200),
	CHANNEL VARCHAR(50),
	CHANNEL_NAME VARCHAR(100),
	TERRITORY VARCHAR(50),
	TERRITORY_NAME VARCHAR(100),
	REG_CHANNEL VARCHAR(50),
	REG_CHANNEL_NAME VARCHAR(100),
	REG_GROUP VARCHAR(50),
	REG_GROUP_NAME VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)),
	CUSTOMER_SEGMENTATION_LEVEL_2_CODE VARCHAR(256)
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_CUSTOMER_HIERARCHY (
	CUSTOMER_NUMBER VARCHAR(10),
	CUSTOMER_NAME VARCHAR(100),
	CHANNEL VARCHAR(50),
	CUSTOMER_GROUP_CODE VARCHAR(100),
	CUSTOMER_GROUP_NAME VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)),
	CUSTOMER_SEGMENTATION_CODE VARCHAR(256),
	CUSTOMER_SEGMENTATION_LEVEL_2_CODE VARCHAR(256)
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_PRODUCT_HIERARCHY (
	MATERIAL VARCHAR(10),
	MATERIAL_DESCRIPTION VARCHAR(100),
	BRAND VARCHAR(50),
	CATEGORY VARCHAR(50),
	PRODUCTTYPE VARCHAR(50),
	PRODUCTVARIENT VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_PRODUCT_MAPPING (
	CUSTOMER_NAME VARCHAR(100),
	CUSTOMER_BRAND VARCHAR(200),
	CUSTOMER_PRODUCT_CODE VARCHAR(300),
	CUSTOMER_PRODUCT_NAME VARCHAR(500),
	MASTER_CODE VARCHAR(300),
	MATERIAL_CODE VARCHAR(300),
	PRODUCT_NAME VARCHAR(500),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_PS_TARGETS (
	CHANNEL VARCHAR(100),
	RETAIL_ENV VARCHAR(100),
	KPI VARCHAR(100),
	ATTRIBUTE_1 VARCHAR(100),
	ATTRIBUTE_2 VARCHAR(100),
	TARGET NUMBER(20,4),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_PS_WEIGHTS (
	CHANNEL VARCHAR(100),
	RETAIL_ENV VARCHAR(100),
	KPI VARCHAR(100),
	WEIGHT NUMBER(20,4),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
CREATE TABLE IF NOT EXISTS ITG_MDS_SG_STORE_MASTER (
	CUSTOMER_NAME VARCHAR(200),
	CUSTOMER_STORE_CODE VARCHAR(200),
	CUSTOMER_STORE_NAME VARCHAR(200),
	CUSTOMER_STORE_LOCATION VARCHAR(200),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_ACCRUALS (
	CUST_ID VARCHAR(10),
	CUST_NM VARCHAR(50),
	JJ_YEAR VARCHAR(10),
	JJ_MNTH_ID VARCHAR(10),
	FILE_TYPE VARCHAR(20),
	ACCRUAL_VAL NUMBER(20,6),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_AFGR (
	CUST_ID VARCHAR(30),
	CUST_NM VARCHAR(100),
	AFGR_NUM VARCHAR(30),
	CUST_DN_NUM VARCHAR(100),
	DN_AMT_EXC_GST_VAL NUMBER(20,4),
	AFGR_AMT NUMBER(20,4),
	DT_TO_SC DATE,
	SC_VALIDATION VARCHAR(30),
	RTN_ORD_NUM VARCHAR(30),
	RTN_ORD_DT DATE,
	RTN_ORD_AMT NUMBER(20,4),
	CN_EXP_ISSUE_DT DATE,
	BILL_NUM VARCHAR(30),
	BILL_DT DATE,
	CN_AMT NUMBER(20,4),
	DOC_DT DATE,
	AFGR_STATUS VARCHAR(30),
	AFGR_VAL NUMBER(20,4),
	AFGR_CN_DIFF NUMBER(20,4),
	CHNL VARCHAR(30),
	CDL_DTTM VARCHAR(30),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_AS_WATSONS_INVENTORY (
	CUST_CD VARCHAR(30),
	STORE_CD VARCHAR(30),
	YEAR VARCHAR(10),
	MNTH_ID VARCHAR(10),
	MATL_NUM VARCHAR(100),
	INV_QTY_PC NUMBER(20,4),
	INV_VALUE NUMBER(20,4),
	FILENAME VARCHAR(100),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_CIW_MAP (
	CIW_CTGRY VARCHAR(100),
	CIW_BUCKT1 VARCHAR(100),
	CIW_BUCKT2 VARCHAR(100),
	BRAVO_CD1 VARCHAR(20),
	BRAVO_DESC1 VARCHAR(100),
	BRAVO_CD2 VARCHAR(20),
	BRAVO_DESC2 VARCHAR(100),
	ACCT_TYPE VARCHAR(20),
	ACCT_NUM NUMBER(18,0),
	ACCT_DESC VARCHAR(100),
	ACCT_TYPE1 VARCHAR(20),
	CDL_DTTM VARCHAR(100),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_CUSTOMER_DIM (
	CUST_ID VARCHAR(20),
	CUST_NM VARCHAR(100),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	ULLAGE NUMBER(20,4),
	CHNL VARCHAR(20),
	TERRITORY VARCHAR(20),
	RETAIL_ENV VARCHAR(20),
	TRDNG_TERM_VAL NUMBER(20,4),
	RDD_IND VARCHAR(10),
	CDL_DTTM VARCHAR(40),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_DSTRBTRR_DIM (
	CUST_ID VARCHAR(10),
	CUST_NM VARCHAR(100),
	LVL1 VARCHAR(40),
	LVL2 VARCHAR(40),
	LVL3 VARCHAR(40),
	LVL4 VARCHAR(40),
	LVL5 VARCHAR(40),
	TRDNG_TERM_VAL NUMBER(20,4),
	ABBREVATION VARCHAR(40),
	BUYER_GLN VARCHAR(40),
	LOCATION_GLN VARCHAR(40),
	CHNL_MANAGER VARCHAR(80),
	CDM VARCHAR(40),
	REGION VARCHAR(20),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_DSTRBTR_CUST_DIM (
	OUTLET_KEY VARCHAR(100),
	CUST_ID VARCHAR(20),
	CUST_NM VARCHAR(100),
	OUTLET_ID VARCHAR(50),
	OUTLET_DESC VARCHAR(100),
	OUTLET_TYPE1 VARCHAR(100),
	OUTLET_TYPE2 VARCHAR(100),
	OUTLET_TYPE3 VARCHAR(100),
	OUTLET_TYPE4 VARCHAR(100),
	TOWN VARCHAR(100),
	CUST_YEAR VARCHAR(50),
	SLSMN_CD VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_DSTRBTR_DOC_TYPE (
	CUST_ID VARCHAR(8),
	CUST_NM VARCHAR(80),
	LVL1 VARCHAR(40),
	LVL2 VARCHAR(40),
	WH_ID VARCHAR(30),
	DOC_TYPE VARCHAR(20),
	DOC_TYPE_DESC VARCHAR(20),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_GT_OUTLET_EXCLUSION (
	DSTRBTR_CD VARCHAR(20),
	OUTLET_CD VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_MY_IDS_EXCHG_RATE (
	CUST_ID VARCHAR(50),
	CUST_NM VARCHAR(100),
	EXCHNG_RATE NUMBER(20,4),
	YEARMO VARCHAR(30),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_IN_TRANSIT (
	BILL_DOC VARCHAR(50),
	BILL_DT DATE,
	GR_DT DATE,
	CLOSING_DT DATE,
	REMARKS VARCHAR(255),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_LE_TRGT (
	CUST_ID VARCHAR(20),
	CUST_NM VARCHAR(50),
	JJ_YEAR VARCHAR(10),
	MNTH_NM VARCHAR(10),
	JJ_MNTH_ID VARCHAR(10),
	TRGT_TYPE VARCHAR(10),
	TRGT_VAL_TYPE VARCHAR(10),
	WK1 NUMBER(20,6),
	WK2 NUMBER(20,6),
	WK3 NUMBER(20,6),
	WK4 NUMBER(20,6),
	WK5 NUMBER(20,6),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_LISTPRICE_DAILY (
	PLANT VARCHAR(4),
	CNTY VARCHAR(4),
	ITEM_CD VARCHAR(20),
	ITEM_DESC VARCHAR(100),
	VALID_FROM VARCHAR(10),
	VALID_TO VARCHAR(10),
	RATE NUMBER(20,4),
	CURRENCY VARCHAR(4),
	PRICE_UNIT NUMBER(16,4),
	UOM VARCHAR(6),
	YEARMO VARCHAR(6),
	MNTH_TYPE VARCHAR(6),
	SNAPSHOT_DT DATE,
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_MATERIAL_DIM (
	ITEM_CD VARCHAR(40),
	ITEM_DESC VARCHAR(200),
	ITEM_DESC2 VARCHAR(200),
	ITEM_TYPE VARCHAR(10),
	STATUS VARCHAR(40),
	COMM_STATUS VARCHAR(40),
	PIPO_FLAG VARCHAR(40),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	PUTUP_DESC VARCHAR(40),
	PACK_SIZE VARCHAR(40),
	PLATFRM VARCHAR(40),
	PROMO_REG_IND VARCHAR(40),
	SAP_VAL_CLASS VARCHAR(40),
	ITEM_BAR_CD VARCHAR(40),
	LAUNCH_DT DATE,
	LIST_PRCE NUMBER(20,4),
	CARTON_PRCE NUMBER(20,4),
	DZ_PRCE NUMBER(20,4),
	RSP NUMBER(20,4),
	RSP_WITH_GST NUMBER(20,4),
	TRD_MRGIN NUMBER(20,4),
	SHIPPER_QTY_PC NUMBER(20,4),
	ABC VARCHAR(40),
	REMRKS VARCHAR(255),
	PRFT_CNTR VARCHAR(40),
	SHELF_LIFE NUMBER(20,4),
	LIST_PRCE_WITH_GST NUMBER(20,4),
	NPI_IND VARCHAR(10),
	NPI_STRT_PERIOD VARCHAR(10),
	HERO_IND VARCHAR(10),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	SELLING_STATUS_NAME VARCHAR(500),
	SELLING_STATUS_ID NUMBER(18,0),
	STRONGHOLD VARCHAR(200),
	CATEGORY VARCHAR(200),
	PREDECESSOR_MATERIAL_DESC VARCHAR(200),
	PROJECT_NAME VARCHAR(200),
	SUPPLIER VARCHAR(200),
	PRIMARY_SOURCE VARCHAR(200),
	MATERIAL_CODE_DESC VARCHAR(200),
	"product_dimension-l_(mm)" NUMBER(31,2),
	"product_dimension-w_(mm)" NUMBER(31,2),
	"product_dimension-h_(mm)" NUMBER(31,3),
	"product_dimension-m3" NUMBER(31,2),
	"shipper_dimension-l_(mm)" NUMBER(31,2),
	"shipper_dimension-w_(mm)" NUMBER(31,2),
	"shipper_dimension-h_(mm)" NUMBER(31,2),
	"shipper_dimension-m3" NUMBER(31,2),
	PRODUCT_WEIGHT NUMBER(31,0),
	BUNDLE_BARCODE VARCHAR(200),
	SHIPPER_BARCODE VARCHAR(200),
	"qty-inner_shipper_(pc)" NUMBER(31,0),
	"qty-shrink_wrap_(pc)" NUMBER(31,0),
	"qty-shipper/layer" NUMBER(31,0),
	"qty-layer/pallet" NUMBER(31,0),
	"qty-shipper/pallet" NUMBER(31,0),
	"qty-dz/pallet" NUMBER(31,0),
	"apo-master_code" VARCHAR(200),
	"apo-product_classification" VARCHAR(200),
	"apo-dp_product_type" VARCHAR(200),
	"apo-copy_history" VARCHAR(200),
	"apo-forecast_ind" VARCHAR(200),
	"apo-npi_indicator" VARCHAR(200),
	"bnd-zcpe" NUMBER(31,2),
	"bn-list_price" NUMBER(31,2),
	"bnd-list_price" NUMBER(31,2),
	"bn-duty" NUMBER(31,2),
	"bnd-rcp" NUMBER(31,2),
	"bnd-rcp(duty)" NUMBER(31,2),
	"bn-trade_margin" NUMBER(31,2),
	COMPLIANCE_ASSET_ID VARCHAR(200),
	ATTACHMENTS NUMBER(31,0),
	PREDECESSOR_MATERIAL_CODE VARCHAR(200)
);
CREATE TABLE IF NOT EXISTS ITG_MY_MATERIAL_MAP (
	CUST_ID VARCHAR(50),
	CUST_NM VARCHAR(100),
	ITEM_CD VARCHAR(50),
	ITEM_DESC VARCHAR(200),
	EXT_ITEM_CD VARCHAR(50),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_OUTLET_ATTR (
	CUST_ID VARCHAR(20),
	CUST_NM VARCHAR(100),
	OUTLET_ID VARCHAR(25),
	OUTLET_DESC VARCHAR(100),
	OUTLET_TYPE1 VARCHAR(50),
	OUTLET_TYPE2 VARCHAR(50),
	OUTLET_TYPE3 VARCHAR(100),
	OUTLET_TYPE4 VARCHAR(100),
	TOWN VARCHAR(50),
	CUST_YEAR VARCHAR(50),
	SLSMN_CD VARCHAR(30),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_POS_CUST_MSTR (
	CUST_ID VARCHAR(10),
	CUST_NM VARCHAR(255),
	STORE_CD VARCHAR(10),
	STORE_NM VARCHAR(255),
	DEPT_CD VARCHAR(10),
	DEPT_NM VARCHAR(255),
	REGION VARCHAR(255),
	STORE_FRMT VARCHAR(255),
	STORE_TYPE VARCHAR(255),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_POS_SALES_FACT (
	CUST_ID VARCHAR(50),
	CUST_NM VARCHAR(255),
	STORE_CD VARCHAR(50),
	STORE_NM VARCHAR(255),
	DEPT_CD VARCHAR(50),
	DEPT_NM VARCHAR(255),
	MT_ITEM_CD VARCHAR(50),
	MT_ITEM_DESC VARCHAR(255),
	JJ_MNTH_ID VARCHAR(10),
	JJ_YR_WEEK_NO VARCHAR(10),
	QTY NUMBER(15,6),
	SO_VAL NUMBER(15,6),
	SAP_MATL_NUM VARCHAR(50),
	FILE_NM VARCHAR(50),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_SELLOUT_SALES_FACT (
	DSTRBTR_ID VARCHAR(50),
	SLS_ORD_NUM VARCHAR(50),
	SLS_ORD_DT DATE,
	TYPE VARCHAR(20),
	CUST_CD VARCHAR(50),
	DSTRBTR_WH_ID VARCHAR(50),
	ITEM_CD VARCHAR(50),
	DSTRBTR_PROD_CD VARCHAR(50),
	EAN_NUM VARCHAR(50),
	DSTRBTR_PROD_DESC VARCHAR(100),
	GRS_PRC NUMBER(20,4),
	QTY NUMBER(20,4),
	UOM VARCHAR(20),
	QTY_PC NUMBER(20,4),
	QTY_AFT_CONV NUMBER(20,4),
	SUBTOTAL_1 NUMBER(20,4),
	DISCOUNT NUMBER(20,4),
	SUBTOTAL_2 NUMBER(20,4),
	BOTTOM_LINE_DSCNT NUMBER(20,4),
	TOTAL_AMT_AFT_TAX NUMBER(20,4),
	TOTAL_AMT_BFR_TAX NUMBER(20,4),
	SLS_EMP VARCHAR(100),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	SAP_MATL_NUM VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_SELLOUT_STOCK_FACT (
	CUST_ID VARCHAR(50),
	INV_DT DATE,
	DSTRBTR_WH_ID VARCHAR(50),
	ITEM_CD VARCHAR(50),
	DSTRBTR_PROD_CD VARCHAR(50),
	EAN_NUM VARCHAR(50),
	DSTRBTR_PROD_DESC VARCHAR(100),
	QTY NUMBER(20,4),
	UOM VARCHAR(20),
	QTY_ON_ORD NUMBER(20,4),
	UOM_ON_ORD VARCHAR(100),
	QTY_COMMITTED NUMBER(20,4),
	UOM_COMMITTED VARCHAR(100),
	AVAILABLE_QTY_PC NUMBER(20,4),
	QTY_ON_ORD_PC NUMBER(20,4),
	QTY_COMMITTED_PC NUMBER(20,4),
	UNIT_PRC NUMBER(20,4),
	TOTAL_VAL NUMBER(20,4),
	CUSTOM_FIELD1 VARCHAR(255),
	SAP_MATL_NUM VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_MY_TRGTS (
	CUST_ID VARCHAR(20),
	CUST_NM VARCHAR(100),
	BRND_DESC VARCHAR(100),
	SUB_SEGMENT VARCHAR(100),
	JJ_YEAR VARCHAR(10),
	JJ_MNTH_ID VARCHAR(10),
	TRGT_TYPE VARCHAR(20),
	TRGT_VAL_TYPE VARCHAR(20),
	TRGT_VAL NUMBER(20,6),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_PH_POS_SALES_FACT (
	CUST_CD VARCHAR(30),
	JJ_MNTH_ID VARCHAR(30),
	ITEM_CD VARCHAR(30),
	BRNCH_CD VARCHAR(50),
	POS_QTY NUMBER(20,4),
	POS_GTS NUMBER(20,4),
	POS_ITEM_PRC NUMBER(20,4),
	POS_TAX NUMBER(20,4),
	POS_NTS NUMBER(20,4),
	CONV_FACTOR NUMBER(20,4),
	JJ_QTY_PC NUMBER(20,4),
	JJ_ITEM_PRC_PER_PC NUMBER(20,4),
	JJ_GTS NUMBER(20,4),
	JJ_VAT_AMT NUMBER(20,4),
	JJ_NTS NUMBER(20,4),
	FILE_NM VARCHAR(150),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_QUERY_PARAMETERS (
	COUNTRY_CODE VARCHAR(20),
	PARAMETER_NAME VARCHAR(300),
	PARAMETER_VALUE VARCHAR(300),
	PARAMETER_TYPE VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS ITG_SG_CIW_MAPPING (
	CONDITION_TYPE VARCHAR(500),
	GL VARCHAR(10),
	GL_DESCRIPTION VARCHAR(500),
	POSTED_WHERE VARCHAR(500),
	PURPOSE VARCHAR(500),
	CIW_BUCKET VARCHAR(500),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_CONSTANT_KEY_VALUE (
	DATA_CATEGORY_CD NUMBER(10,0),
	DATA_CATEGORY_TYPE VARCHAR(255),
	KEY VARCHAR(50),
	VALUE VARCHAR(50),
	EFFECTIVE_PERIOD_FROM VARCHAR(6),
	EFFECTIVE_PERIOD_TO VARCHAR(6)
);
CREATE TABLE IF NOT EXISTS ITG_SG_LISTPRICE (
	PLANT VARCHAR(4),
	CNTY VARCHAR(4),
	ITEM_CD VARCHAR(20),
	ITEM_DESC VARCHAR(100),
	VALID_FROM VARCHAR(10),
	VALID_TO VARCHAR(10),
	RATE NUMBER(20,4),
	CURRENCY VARCHAR(4),
	PRICE_UNIT NUMBER(16,4),
	UOM VARCHAR(6),
	YEARMO VARCHAR(6),
	MNTH_TYPE VARCHAR(6),
	CDL_DTTM VARCHAR(255),
	SNAPSHOT_DT DATE,
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS ITG_SG_POS_SALES_FACT (
	CUST_ID VARCHAR(10),
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(28),
	WEEK VARCHAR(20),
	MASTER_CODE VARCHAR(255),
	SAP_CODE VARCHAR(255),
	PRODUCT_BARCODE VARCHAR(255),
	BRAND VARCHAR(300),
	STORE VARCHAR(300),
	ITEM_CODE VARCHAR(300),
	ITEM_DESC VARCHAR(500),
	SALES_QTY NUMBER(10,0),
	NET_SALES NUMBER(10,6),
	BILL_DATE DATE,
	STORE_NAME VARCHAR(300),
	SOLD_TO_CODE VARCHAR(200),
	PRODUCT_KEY VARCHAR(300),
	PRODUCT_KEY_DESC VARCHAR(500),
	SOURCE VARCHAR(255),
	STORE_TYPE VARCHAR(200),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_AMAZON (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	RM VARCHAR(50),
	MERCHANT_CUSTOMER_ID VARCHAR(15),
	GL VARCHAR(50),
	CATEGORY VARCHAR(200),
	SUBCATEGORY VARCHAR(200),
	BRAND VARCHAR(255),
	ITEM_CODE VARCHAR(50),
	ITEM_DESC VARCHAR(500),
	NET_SALES NUMBER(10,4),
	PCOGS NUMBER(10,4),
	SALES_QTY NUMBER(10,0),
	PPMPERCENT NUMBER(10,5),
	PPMDOLLAR NUMBER(10,5),
	MONTH NUMBER(18,0),
	VENDOR_CODE VARCHAR(10),
	VENDOR_NAME VARCHAR(255),
	STORE VARCHAR(50),
	STORE_NAME VARCHAR(50),
	BARCODE VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_DFI (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	BUYER_CODE VARCHAR(50),
	VENDOR_CODE VARCHAR(100),
	STORE_CODE VARCHAR(50),
	STORE_SHORT_CODE VARCHAR(100),
	STORE_DESC VARCHAR(300),
	BRAND VARCHAR(300),
	ITEM_CODE VARCHAR(50),
	SUPPLIER_ITEM_CODE VARCHAR(50),
	ITEM_DESC VARCHAR(500),
	SIZE VARCHAR(100),
	UOM VARCHAR(20),
	PUF NUMBER(10,0),
	BARCODE VARCHAR(255),
	SALES_AMOUNT NUMBER(14,4),
	SALES_QTY NUMBER(10,0),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_GUARDIAN (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	BUYER_CODE VARCHAR(50),
	VENDOR_CODE VARCHAR(100),
	STORE_CODE VARCHAR(50),
	STORE_SHORT_CODE VARCHAR(50),
	STORE_POSTAL_CODE VARCHAR(50),
	STORE_ADDRESS_1 VARCHAR(200),
	STORE_ADDRESS_2 VARCHAR(200),
	STORE_ADDRESS_3 VARCHAR(100),
	STORE_COUNTRY VARCHAR(20),
	STORE_DESC VARCHAR(500),
	BRAND VARCHAR(300),
	ITEM_CODE VARCHAR(50),
	SUPPLIER_ITEM_CODE VARCHAR(50),
	ITEM_DESC VARCHAR(500),
	SIZE VARCHAR(100),
	UOM VARCHAR(20),
	PUF NUMBER(10,0),
	SALES_QTY NUMBER(10,0),
	SALES_AMOUNT NUMBER(14,4),
	INVENTORY_ON_HAND NUMBER(10,0),
	BARCODE VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_MARKETPLACE (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	STORE_NAME VARCHAR(20),
	MONTH VARCHAR(20),
	INVOICE_NUMBER VARCHAR(255),
	STATUS VARCHAR(100),
	ITEM_CODE VARCHAR(500),
	ITEM_DESC VARCHAR(500),
	BARCODE VARCHAR(255),
	SALES_QTY NUMBER(10,0),
	NET_SALES NUMBER(10,4),
	BRAND VARCHAR(300),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_NTUC (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	VENDOR_CODE VARCHAR(100),
	VENDOR_NAME VARCHAR(255),
	DEPT_CODE VARCHAR(50),
	DEPT_DESCRIPTION VARCHAR(255),
	CLASS_NO VARCHAR(50),
	CLASS_DESCRIPTION VARCHAR(255),
	SUB_CLASS_DESCRIPTION VARCHAR(255),
	MCH VARCHAR(255),
	ITEM_CODE VARCHAR(100),
	ITEM_DESC VARCHAR(500),
	BRAND VARCHAR(255),
	SALES_UOM VARCHAR(100),
	PACK_SIZE NUMBER(10,0),
	STORE_CODE VARCHAR(100),
	STORE_NAME VARCHAR(255),
	BARCODE VARCHAR(255),
	STORE_FORMAT VARCHAR(255),
	ATTRIBUTE2 VARCHAR(50),
	NET_SALES NUMBER(10,4),
	SALES_QTY NUMBER(10,0),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_REDMART (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	ITEM_CODE VARCHAR(100),
	PRODUCT_CODE VARCHAR(300),
	ITEM_DESC VARCHAR(500),
	PACK_SIZE VARCHAR(100),
	BRAND VARCHAR(300),
	SUPPLIER_ID VARCHAR(100),
	SUPPLIER_NAME VARCHAR(100),
	MANUFACTURER VARCHAR(200),
	BARCODE VARCHAR(255),
	CATEGORY_1 VARCHAR(150),
	CATEGORY_2 VARCHAR(150),
	CATEGORY_3 VARCHAR(150),
	CATEGORY_4 VARCHAR(150),
	NET_SALES NUMBER(10,4),
	SALES_QTY NUMBER(10,0),
	STORE VARCHAR(50),
	STORE_NAME VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_SCOMMERCE (
	TRX_DATE DATE,
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	ORDER_SN VARCHAR(50),
	ITEM_CODE VARCHAR(50),
	MODEL_ID VARCHAR(50),
	SKU_ID VARCHAR(100),
	ITEM_DESC VARCHAR(500),
	BARCODE VARCHAR(255),
	SALES_QTY NUMBER(10,0),
	NET_SALES NUMBER(10,6),
	STORE VARCHAR(50),
	STORE_NAME VARCHAR(50),
	BRAND VARCHAR(300),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_SCAN_DATA_WATSONS (
	YEAR VARCHAR(20),
	MNTH_ID VARCHAR(23),
	WEEK VARCHAR(20),
	STORE VARCHAR(255),
	DIV VARCHAR(255),
	PRDT_DEPT VARCHAR(255),
	PRDTCODE VARCHAR(255),
	PRDTDESC VARCHAR(500),
	BRAND VARCHAR(300),
	SUPCODE VARCHAR(255),
	SUP_NAME VARCHAR(300),
	BARCODE VARCHAR(255),
	SUP_CAT VARCHAR(255),
	DEPT_NAME VARCHAR(255),
	NET_SALES NUMBER(14,4),
	SALES_QTY NUMBER(10,0),
	BILL_DATE DATE,
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0),
	CUST_NAME VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS ITG_SG_TP_CLOSED_MONTH (
	FILE_NAME VARCHAR(255),
	SHEET_NAME VARCHAR(255),
	MONTH_NUMBER VARCHAR(255),
	SALES_REP VARCHAR(255),
	CUSTMER_L1 VARCHAR(255),
	CUSTOMER VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CHANNEL VARCHAR(255),
	FRANCHISE VARCHAR(255),
	BRAND VARCHAR(255),
	BRAND_PROFIT_CENTER VARCHAR(255),
	PROMO_TYPE VARCHAR(255),
	GL_ACCOUNT VARCHAR(255),
	DESCRIPTION VARCHAR(255),
	REQUESTED_DATE VARCHAR(255),
	MONTH_OF_ACTIVITY VARCHAR(255),
	PROMO_START_DATE VARCHAR(255),
	PROMO_END_DATE VARCHAR(255),
	COMMITTED_ACCRUAL_W_O_GST NUMBER(17,3),
	TP_NUMBER VARCHAR(255),
	ZP_CMM_INVOICE VARCHAR(255),
	RETAILERS_BILLING VARCHAR(255),
	JNJ_ACTUALS_W_O_GST NUMBER(17,3),
	CN_NUMBER VARCHAR(255),
	TP_DATE VARCHAR(255),
	JNJ_TOTAL_COMMITTED_W_O_GST NUMBER(17,3),
	JNJ_TOTAL_UNCLAIMED_W_O_GST NUMBER(17,3),
	REVERSED_ACCRUED_AMT NUMBER(17,3),
	MONTH_OF_REVERSAL VARCHAR(255),
	SUPPORTING VARCHAR(255),
	MONTH_OF_DOC_SCANNING VARCHAR(255),
	REMARKS VARCHAR(255),
	WORKING_SC VARCHAR(255),
	CONSO_PR VARCHAR(255),
	CREATE_PR VARCHAR(255),
	PAYMENT_REF VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM DATE,
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_TP_CLOSED_YEAR_BAL (
	FILE_NAME VARCHAR(255),
	SHEET_NAME VARCHAR(255),
	MONTH_NUMBER VARCHAR(255),
	SALES_REP VARCHAR(255),
	CUSTOMER_L1 VARCHAR(255),
	CUSTOMER VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CHANNEL VARCHAR(255),
	FRANCHISE VARCHAR(255),
	BRAND VARCHAR(255),
	BRAND_PROFIT_CENTER VARCHAR(255),
	PROMO_TYPE VARCHAR(255),
	GL_ACCOUNT VARCHAR(255),
	DESCRIPTION VARCHAR(255),
	REQUESTED_DATE VARCHAR(255),
	MONTH_OF_ACTIVITY VARCHAR(255),
	PROMO_START_DATE VARCHAR(255),
	PROMO_END_DATE VARCHAR(255),
	COMMITTED_OR_ACCRUAL_WO_GST NUMBER(17,3),
	TP_NUMBER VARCHAR(255),
	ZP_CMM_INVOICE VARCHAR(255),
	RETAILERS_BILLING VARCHAR(255),
	JNJ_ACTUALS_WO_GST NUMBER(17,3),
	MONTH_OF_ACTUAL VARCHAR(255),
	CN_NUMBER VARCHAR(255),
	CN_DATE VARCHAR(255),
	REVERSAL_AMOUNT NUMBER(17,3),
	OUTSTANDING_ACCRUAL NUMBER(17,3),
	JNJ_TOTAL_COMMITTED_WO_GST NUMBER(17,3),
	JNJ_TOTAL_UNCLAIMED_WO_GST NUMBER(17,3),
	COMMENTS_OR_REVERSED_ACCRUED_AMT NUMBER(17,3),
	MONTH_OF_REVERSAL VARCHAR(255),
	SUPPORTING VARCHAR(255),
	TP_IMPACT NUMBER(17,3),
	LEFT_ACCRUAL NUMBER(17,3),
	MONTH_OF_DOC_SCANNING VARCHAR(255),
	REMARKS VARCHAR(255),
	WORKING_SC VARCHAR(255),
	CONSO_PR VARCHAR(255),
	CREATE_PR VARCHAR(255),
	PAYMENT_REF VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM DATE,
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_ZUELLIG_CUSTOMER_MAPPING (
	ZUELLIG_CUSTOMER VARCHAR(255),
	REGIONAL_BANNER VARCHAR(255),
	MERCHANDIZING VARCHAR(255),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_ZUELLIG_PRODUCT_MAPPING (
	ZP_MATERIAL VARCHAR(20),
	ZP_ITEM_CODE VARCHAR(20),
	JJ_CODE VARCHAR(20),
	ITEM_NAME VARCHAR(255),
	BRAND VARCHAR(20),
	CDL_DTTM VARCHAR(20),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_SG_ZUELLIG_SELLOUT (
	PERIOD VARCHAR(255),
	SALES_DATE VARCHAR(255),
	WAREHOUSE_CODE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CUSTOMER_NAME VARCHAR(255),
	SG_BANNER VARCHAR(255),
	MERCHANDIZING_FLG VARCHAR(1),
	INVOICE VARCHAR(255),
	ITEM_CODE VARCHAR(255),
	ITEM_NAME VARCHAR(255),
	SAP_ITEM_CODE VARCHAR(20),
	SG_BRAND VARCHAR(20),
	TYPE VARCHAR(255),
	MATL_LP NUMBER(17,3),
	SALES_VALUE NUMBER(17,3),
	SALES_UNITS NUMBER(17,3),
	BONUS_UNITS NUMBER(17,3),
	RETURNS_UNITS NUMBER(17,3),
	RETURNS_VALUE NUMBER(17,3),
	RETURNS_BONUS_UNITS NUMBER(17,3),
	GTS NUMBER(17,3),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS ITG_TH_POS_SALES_INVENTORY_FACT (
	DATA_SET VARCHAR(20),
	CUSTOMER VARCHAR(20),
	SOLD_TO_CODE VARCHAR(6),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	SUPPLIER_CODE VARCHAR(20),
	TRANS_DT DATE,
	MATERIAL_NUMBER VARCHAR(20),
	MATERIAL_NAME VARCHAR(500),
	BAR_CODE VARCHAR(20),
	BRANCH_CODE VARCHAR(20),
	BRANCH_NAME VARCHAR(200),
	INVENTORY_QTY_SOURCE NUMBER(17,4),
	SALES_QTY_SOURCE NUMBER(17,4),
	RETAILER_UNIT_CONVERSION NUMBER(31,0),
	INVENTORY_QTY_CONVERTED NUMBER(17,4),
	SALES_QTY_CONVERTED NUMBER(17,4),
	LIST_PRICE NUMBER(20,4),
	INVENTORY_GTS NUMBER(20,4),
	SALES_GTS NUMBER(20,4),
	CUSTOMER_RSP NUMBER(20,4),
	INVENTORY_BAHT NUMBER(20,4),
	SALES_BAHT NUMBER(20,4),
	FOC_PRODUCT VARCHAR(1)
);

-------------------------------------------------------------
USE schema SNAPOSEWKS_INTEGRATION;

CREATE TABLE IF NOT EXISTS MY_POS_MT_CUST_SALES (
	CUSTOMER VARCHAR(255),
	CUSTOMER_NAME VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	DEPT_CODE VARCHAR(255),
	DEPT_NAME VARCHAR(255),
	ITEM_CODE VARCHAR(255),
	ITEM_DESC VARCHAR(255),
	YEAR_MTH VARCHAR(255),
	WEEK_NO VARCHAR(255),
	QTY VARCHAR(255),
	SELLOUT_VALUE VARCHAR(255),
	SAP_CODE VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS MY_PROPAGATE_FROM_TO (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	LATEST_MONTH NUMBER(18,0),
	PROPAGATE_TO NUMBER(18,0),
	PROPAGATE_FROM NUMBER(18,0),
	SO_QTY NUMBER(38,6),
	INV_QTY NUMBER(38,4),
	DIFF_MONTH NUMBER(38,0),
	REASON VARCHAR(29)
);
CREATE TABLE IF NOT EXISTS MY_PROPAGATE_TO (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MONTH NUMBER(18,0),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	PROPAGATE_FLAG VARCHAR(1),
	LATEST_MONTH NUMBER(18,0)
);
CREATE TABLE IF NOT EXISTS NOTIFY_MISSING_PRODUCTS (
	CUSTOMER_NAME VARCHAR(30),
	CUSTOMER_BRAND VARCHAR(500),
	CUSTOMER_PRODUCT_CODE VARCHAR(500),
	CUSTOMER_PRODUCT_NAME VARCHAR(500),
	SOURCE VARCHAR(500),
	RECORD_COUNT NUMBER(18,0)
);
CREATE TABLE IF NOT EXISTS WKS_CUST_MASTER (
	SOLD_TO VARCHAR(255),
	SOLD_TO_DESC VARCHAR(255),
	CUST_GRP_CODE VARCHAR(255),
	CUSTOMER VARCHAR(255),
	ULLAGE VARCHAR(255),
	CHANNEL VARCHAR(255),
	TERRITORY VARCHAR(255),
	RETAIL_ENVIR VARCHAR(255),
	TT VARCHAR(255),
	RDD_INDICATOR VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_ACCRUAL (
	SOLD_TO VARCHAR(255),
	CUSTOMER VARCHAR(255),
	YEAR VARCHAR(255),
	TYPE VARCHAR(255),
	JAN VARCHAR(255),
	FEB VARCHAR(255),
	MAR VARCHAR(255),
	APR VARCHAR(255),
	MAY VARCHAR(255),
	JUN VARCHAR(255),
	JUL VARCHAR(255),
	AUG VARCHAR(255),
	SEP VARCHAR(255),
	OCT VARCHAR(255),
	NOV VARCHAR(255),
	DEC VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_AFGR (
	CUST_CODE VARCHAR(255),
	SOLD_TO_DESC VARCHAR(255),
	AFGR_NUMBER VARCHAR(255),
	CUSTOMER_DN_NO VARCHAR(255),
	DN_AMOUNT_EXCL_GST VARCHAR(255),
	AFGR_AMOUNT VARCHAR(255),
	DATE_TO_SC VARCHAR(255),
	SC_VALIDATION VARCHAR(255),
	RETURN_ORDER_NO VARCHAR(255),
	RETURN_ORDER_DATE VARCHAR(255),
	RETURN_ORDER_AMOUNT VARCHAR(255),
	CN_EXPECTED_ISSUE_DATE VARCHAR(255),
	BILLING_NO VARCHAR(255),
	BILLING_DATE VARCHAR(255),
	CN_AMOUNT VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_ALLMONTHS_BASE (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	MATL_NUM VARCHAR(100),
	MONTH NUMBER(18,0),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13)
);
CREATE TABLE IF NOT EXISTS WKS_MY_BASE (
	YEAR NUMBER(18,0),
	QRTR_NO VARCHAR(14),
	MNTH_ID VARCHAR(21),
	MNTH_NO NUMBER(18,0),
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	MATL_NUM VARCHAR(100),
	SI_SLS_QTY NUMBER(38,4),
	SI_GTS_VAL NUMBER(38,13),
	INVENTORY_QUANTITY NUMBER(38,4),
	INVENTORY_VAL NUMBER(38,13),
	SO_SLS_QTY NUMBER(38,6),
	SO_TRD_SLS NUMBER(38,17)
);
CREATE TABLE IF NOT EXISTS WKS_MY_BASE_DETAIL (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MATL_NUM VARCHAR(100),
	MONTH NUMBER(18,0),
	BASE_MATL_NUM VARCHAR(100),
	REPLICATED_FLAG VARCHAR(1),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13),
	LAST_3MONTHS_SO NUMBER(38,6),
	LAST_6MONTHS_SO NUMBER(38,6),
	LAST_12MONTHS_SO NUMBER(38,6),
	LAST_3MONTHS_SO_VALUE NUMBER(38,17),
	LAST_6MONTHS_SO_VALUE NUMBER(38,17),
	LAST_12MONTHS_SO_VALUE NUMBER(38,17),
	LAST_36MONTHS_SO_VALUE NUMBER(38,17)
);
CREATE TABLE IF NOT EXISTS WKS_MY_CIW_MAP (
	CIW_CAT VARCHAR(255),
	CIW_BUCKET1 VARCHAR(255),
	CIW_BUCKET2 VARCHAR(255),
	BRAVO_CODE1 VARCHAR(255),
	BRAVO_DESC1 VARCHAR(255),
	BRAVO_CODE2 VARCHAR(255),
	BRAVO_DESC2 VARCHAR(255),
	TYPE VARCHAR(255),
	GL_ACCOUNT VARCHAR(255),
	GL_DESC VARCHAR(255),
	TYPE1 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_DISTRIBUTOR_DOC_TYPE (
	DIST_CODE VARCHAR(255),
	DIST_NAME VARCHAR(255),
	LEVEL1 VARCHAR(255),
	LEVEL2 VARCHAR(255),
	WH_ID VARCHAR(255),
	DOC_TYPE VARCHAR(255),
	DOC_TYPE_DES VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_GT_DISTRIBUTOR_HIERARCHY (
	DISTRIBUTOR_ID VARCHAR(255),
	DISTRIBUTOR_NAME VARCHAR(255),
	LEVEL_1 VARCHAR(255),
	LEVEL_2 VARCHAR(255),
	LEVEL_3 VARCHAR(255),
	LEVEL_4 VARCHAR(255),
	LEVEL_5 VARCHAR(255),
	TRADETERM VARCHAR(255),
	ABBREVIATION VARCHAR(255),
	BUYER_GLN VARCHAR(255),
	LOCATION_GLN VARCHAR(255),
	CHANNEL_MANAGER VARCHAR(255),
	CDM VARCHAR(255),
	REGION VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_INVENTORY_HEALTHY_UNHEALTHY_ANALYSIS (
	MONTH NUMBER(18,0),
	DSTRBTR_GRP_CD VARCHAR(30),
	DISTRIBUTOR_ID_NAME VARCHAR(40),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	PKA_SIZE_DESC VARCHAR(30),
	PKA_PRODUCT_KEY VARCHAR(68),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	LAST_3MONTHS_SO_VAL NUMBER(38,17),
	LAST_6MONTHS_SO_VAL NUMBER(38,17),
	LAST_12MONTHS_SO_VAL NUMBER(38,17),
	LAST_36MONTHS_SO_VAL NUMBER(38,17),
	HEALTHY_INVENTORY VARCHAR(1)
);
CREATE TABLE IF NOT EXISTS WKS_MY_INVENTORY_HEALTH_ANALYSIS_PROPAGATION (
	YEAR NUMBER(18,0),
	YEAR_QUARTER VARCHAR(14),
	MONTH_YEAR VARCHAR(21),
	MONTH_NUMBER NUMBER(18,0),
	COUNTRY_NAME VARCHAR(8),
	DSTRBTR_GRP_CD VARCHAR(30),
	DISTRIBUTOR_ID_NAME VARCHAR(40),
	FRANCHISE VARCHAR(30),
	BRAND VARCHAR(30),
	PROD_SUB_BRAND VARCHAR(100),
	VARIANT VARCHAR(100),
	SEGMENT VARCHAR(50),
	PROD_SUBSEGMENT VARCHAR(100),
	PROD_CATEGORY VARCHAR(50),
	PROD_SUBCATEGORY VARCHAR(50),
	PUT_UP_DESCRIPTION VARCHAR(30),
	SKU_CD VARCHAR(100),
	SKU_DESCRIPTION VARCHAR(100),
	PKA_PRODUCT_KEY VARCHAR(68),
	PKA_PRODUCT_KEY_DESCRIPTION VARCHAR(255),
	PRODUCT_KEY VARCHAR(68),
	PRODUCT_KEY_DESCRIPTION VARCHAR(255),
	FROM_CCY VARCHAR(3),
	TO_CCY VARCHAR(3),
	EXCH_RATE NUMBER(15,5),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	SAP_CUST_CHNL_KEY VARCHAR(12),
	SAP_CUST_CHNL_DESC VARCHAR(50),
	SAP_CUST_SUB_CHNL_KEY VARCHAR(12),
	SAP_SUB_CHNL_DESC VARCHAR(50),
	SAP_GO_TO_MDL_KEY VARCHAR(12),
	SAP_GO_TO_MDL_DESC VARCHAR(50),
	SAP_BNR_KEY VARCHAR(12),
	SAP_BNR_DESC VARCHAR(50),
	SAP_BNR_FRMT_KEY VARCHAR(12),
	SAP_BNR_FRMT_DESC VARCHAR(50),
	RETAIL_ENV VARCHAR(50),
	REGION VARCHAR(61),
	ZONE_OR_AREA VARCHAR(80),
	SI_SLS_QTY NUMBER(38,5),
	SI_GTS_VAL NUMBER(38,5),
	SI_GTS_VAL_USD NUMBER(38,5),
	INVENTORY_QUANTITY NUMBER(38,5),
	INVENTORY_VAL NUMBER(38,5),
	INVENTORY_VAL_USD NUMBER(38,5),
	SO_SLS_QTY NUMBER(38,5),
	SO_GRS_TRD_SLS NUMBER(38,5),
	SO_GRS_TRD_SLS_USD NUMBER(17,0),
	SI_ALL_DB_VAL NUMBER(38,5),
	SI_ALL_DB_VAL_USD NUMBER(38,5),
	SI_INV_DB_VAL NUMBER(38,5),
	SI_INV_DB_VAL_USD NUMBER(38,5),
	LAST_3MONTHS_SO_QTY NUMBER(38,6),
	LAST_6MONTHS_SO_QTY NUMBER(38,6),
	LAST_12MONTHS_SO_QTY NUMBER(38,6),
	LAST_3MONTHS_SO_VAL NUMBER(38,17),
	LAST_3MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_6MONTHS_SO_VAL NUMBER(38,17),
	LAST_6MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_12MONTHS_SO_VAL NUMBER(38,17),
	LAST_12MONTHS_SO_VAL_USD NUMBER(38,5),
	PROPAGATE_FLAG VARCHAR(1),
	PROPAGATE_FROM NUMBER(18,0),
	REASON VARCHAR(100),
	LAST_36MONTHS_SO_VAL NUMBER(38,17)
);
CREATE TABLE IF NOT EXISTS WKS_MY_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (
	YEAR NUMBER(18,0),
	YEAR_QUARTER VARCHAR(14),
	MONTH_YEAR VARCHAR(21),
	MONTH_NUMBER NUMBER(18,0),
	COUNTRY_NAME VARCHAR(8),
	DSTRBTR_GRP_CD VARCHAR(30),
	DISTRIBUTOR_ID_NAME VARCHAR(40),
	FRANCHISE VARCHAR(30),
	BRAND VARCHAR(30),
	PROD_SUB_BRAND VARCHAR(100),
	VARIANT VARCHAR(100),
	SEGMENT VARCHAR(50),
	PROD_SUBSEGMENT VARCHAR(100),
	PROD_CATEGORY VARCHAR(50),
	PROD_SUBCATEGORY VARCHAR(50),
	PUT_UP_DESCRIPTION VARCHAR(30),
	SKU_CD VARCHAR(100),
	SKU_DESCRIPTION VARCHAR(100),
	PKA_PRODUCT_KEY VARCHAR(68),
	PKA_PRODUCT_KEY_DESCRIPTION VARCHAR(255),
	PRODUCT_KEY VARCHAR(68),
	PRODUCT_KEY_DESCRIPTION VARCHAR(255),
	FROM_CCY VARCHAR(3),
	TO_CCY VARCHAR(3),
	EXCH_RATE NUMBER(15,5),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	SAP_CUST_CHNL_KEY VARCHAR(12),
	SAP_CUST_CHNL_DESC VARCHAR(50),
	SAP_CUST_SUB_CHNL_KEY VARCHAR(12),
	SAP_SUB_CHNL_DESC VARCHAR(50),
	SAP_GO_TO_MDL_KEY VARCHAR(12),
	SAP_GO_TO_MDL_DESC VARCHAR(50),
	SAP_BNR_KEY VARCHAR(12),
	SAP_BNR_DESC VARCHAR(50),
	SAP_BNR_FRMT_KEY VARCHAR(12),
	SAP_BNR_FRMT_DESC VARCHAR(50),
	RETAIL_ENV VARCHAR(50),
	REGION VARCHAR(61),
	ZONE_OR_AREA VARCHAR(80),
	SI_SLS_QTY NUMBER(38,5),
	SI_GTS_VAL NUMBER(38,5),
	SI_GTS_VAL_USD NUMBER(38,5),
	INVENTORY_QUANTITY NUMBER(38,5),
	INVENTORY_VAL NUMBER(38,5),
	INVENTORY_VAL_USD NUMBER(38,5),
	SO_SLS_QTY NUMBER(38,5),
	SO_GRS_TRD_SLS NUMBER(38,5),
	SO_GRS_TRD_SLS_USD NUMBER(17,0),
	SI_ALL_DB_VAL NUMBER(38,5),
	SI_ALL_DB_VAL_USD NUMBER(38,5),
	SI_INV_DB_VAL NUMBER(38,5),
	SI_INV_DB_VAL_USD NUMBER(38,5),
	LAST_3MONTHS_SO_QTY NUMBER(38,6),
	LAST_6MONTHS_SO_QTY NUMBER(38,6),
	LAST_12MONTHS_SO_QTY NUMBER(38,6),
	LAST_3MONTHS_SO_VAL NUMBER(38,17),
	LAST_3MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_6MONTHS_SO_VAL NUMBER(38,17),
	LAST_6MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_12MONTHS_SO_VAL NUMBER(38,17),
	LAST_12MONTHS_SO_VAL_USD NUMBER(38,5),
	PROPAGATE_FLAG VARCHAR(1),
	PROPAGATE_FROM NUMBER(18,0),
	REASON VARCHAR(100),
	LAST_36MONTHS_SO_VAL NUMBER(38,17),
	HEALTHY_INVENTORY VARCHAR(1),
	MIN_DATE DATE,
	DIFF_WEEKS NUMBER(38,0),
	L12M_WEEKS NUMBER(38,0),
	L6M_WEEKS NUMBER(38,0),
	L3M_WEEKS NUMBER(38,0),
	L12M_WEEKS_AVG_SALES NUMBER(38,17),
	L6M_WEEKS_AVG_SALES NUMBER(38,17),
	L3M_WEEKS_AVG_SALES NUMBER(38,17),
	L12M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L6M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L3M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L12M_WEEKS_AVG_SALES_QTY NUMBER(38,6),
	L6M_WEEKS_AVG_SALES_QTY NUMBER(38,6),
	L3M_WEEKS_AVG_SALES_QTY NUMBER(38,6)
);
CREATE TABLE IF NOT EXISTS WKS_MY_IN_TRANSIT (
	BILLING_DOC VARCHAR(255),
	BILLING_DATE VARCHAR(255),
	GR_DATE VARCHAR(255),
	CLOSING_DATE VARCHAR(255),
	REMARKS VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_LASTNMONTHS (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MATL_NUM VARCHAR(100),
	MONTH NUMBER(18,0),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13),
	LAST_3MONTHS_SO NUMBER(38,6),
	LAST_3MONTHS_SO_VALUE NUMBER(38,17),
	LAST_6MONTHS_SO NUMBER(38,6),
	LAST_6MONTHS_SO_VALUE NUMBER(38,17),
	LAST_12MONTHS_SO NUMBER(38,6),
	LAST_12MONTHS_SO_VALUE NUMBER(38,17),
	LAST_36MONTHS_SO NUMBER(38,6),
	LAST_36MONTHS_SO_VALUE NUMBER(38,17)
);
CREATE TABLE IF NOT EXISTS WKS_MY_LE_TARGET (
	SOLD_TO VARCHAR(255),
	CUSTOMER VARCHAR(255),
	YEAR VARCHAR(255),
	MONTH VARCHAR(255),
	TYPE VARCHAR(255),
	MEASURE_TYPE VARCHAR(255),
	WEEK_1 VARCHAR(255),
	WEEK_2 VARCHAR(255),
	WEEK_3 VARCHAR(255),
	WEEK_4 VARCHAR(255),
	WEEK_5 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_LISTPRICE (
	PLANT VARCHAR(4),
	CNTY VARCHAR(4),
	ITEM_CD VARCHAR(20),
	ITEM_DESC VARCHAR(100),
	VALID_FROM VARCHAR(10),
	VALID_TO VARCHAR(10),
	RATE NUMBER(20,4),
	CURRENCY VARCHAR(4),
	PRICE_UNIT NUMBER(16,4),
	UOM VARCHAR(6),
	CDL_DTTM VARCHAR(255),
	SNAPSHOT_DT DATE,
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS WKS_MY_MNTHLY_SELLOUT_SALES_FACT (
	DSTRBTR_ID VARCHAR(255),
	SLS_ORD_NUM VARCHAR(255),
	SLS_ORD_DT VARCHAR(255),
	TYPE VARCHAR(255),
	CUST_CD VARCHAR(255),
	DSTRBTR_WH_ID VARCHAR(255),
	ITEM_CD VARCHAR(255),
	DSTRBTR_PROD_CD VARCHAR(255),
	EAN_NUM VARCHAR(255),
	DSTRBTR_PROD_DESC VARCHAR(255),
	GRS_PRC VARCHAR(255),
	QTY VARCHAR(255),
	UOM VARCHAR(255),
	QTY_PC VARCHAR(255),
	QTY_AFT_CONV VARCHAR(255),
	SUBTOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUBTOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DSCNT VARCHAR(255),
	TOTAL_AMT_AFT_TAX VARCHAR(255),
	TOTAL_AMT_BFR_TAX VARCHAR(255),
	SLS_EMP VARCHAR(255),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	CUSTOM_FIELD3 VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS WKS_MY_MNTHLY_SELLOUT_STOCK_FACT (
	CUST_ID VARCHAR(255),
	INV_DT VARCHAR(255),
	DSTRBTR_WH_ID VARCHAR(255),
	ITEM_CD VARCHAR(255),
	DSTRBTR_PROD_CD VARCHAR(255),
	EAN_NUM VARCHAR(255),
	DSTRBTR_PROD_DESC VARCHAR(255),
	QTY VARCHAR(255),
	UOM VARCHAR(255),
	QTY_ON_ORD VARCHAR(255),
	UOM_ON_ORD VARCHAR(255),
	QTY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	AVAILABLE_QTY_PC VARCHAR(255),
	QTY_ON_ORD_PC VARCHAR(255),
	QTY_COMMITTED_PC VARCHAR(255),
	UNIT_PRC VARCHAR(255),
	TOTAL_VAL VARCHAR(255),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS WKS_MY_POS_MASTER (
	CUSTOMER_CODE VARCHAR(255),
	CUSTOMER VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	DEPT_CODE VARCHAR(255),
	DEPT_NAME VARCHAR(255),
	REGION VARCHAR(255),
	STORE_FORMAT VARCHAR(255),
	STORE_TYPE VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_MY_SELLOUT_FOR_INV_ANALYSIS (
	CNTRY_NM VARCHAR(8),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_LVL3 VARCHAR(40),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	PKA_SIZE_DESC VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	PKA_PRODUCT_KEY VARCHAR(68),
	TO_CCY VARCHAR(5),
	MIN_DATE DATE
);
CREATE TABLE IF NOT EXISTS WKS_MY_SISO_PROPAGATE_FINAL (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MONTH NUMBER(18,0),
	MATL_NUM VARCHAR(100),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13),
	LAST_3MONTHS_SO NUMBER(38,6),
	LAST_6MONTHS_SO NUMBER(38,6),
	LAST_12MONTHS_SO NUMBER(38,6),
	LAST_3MONTHS_SO_VALUE NUMBER(38,17),
	LAST_6MONTHS_SO_VALUE NUMBER(38,17),
	LAST_12MONTHS_SO_VALUE NUMBER(38,17),
	LAST_36MONTHS_SO_VALUE NUMBER(38,17),
	PROPAGATE_FLAG VARCHAR(1),
	PROPAGATE_FROM NUMBER(18,0),
	REASON VARCHAR(100),
	REPLICATED_FLAG VARCHAR(1),
	EXISTING_SO_QTY NUMBER(38,5),
	EXISTING_SO_VALUE NUMBER(38,5),
	EXISTING_INV_QTY NUMBER(38,5),
	EXISTING_INV_VALUE NUMBER(38,5),
	EXISTING_SELL_IN_QTY NUMBER(38,5),
	EXISTING_SELL_IN_VALUE NUMBER(38,5),
	EXISTING_LAST_3MONTHS_SO NUMBER(38,5),
	EXISTING_LAST_6MONTHS_SO NUMBER(38,5),
	EXISTING_LAST_12MONTHS_SO NUMBER(38,5),
	EXISTING_LAST_3MONTHS_SO_VALUE NUMBER(38,5),
	EXISTING_LAST_6MONTHS_SO_VALUE NUMBER(38,5),
	EXISTING_LAST_12MONTHS_SO_VALUE NUMBER(38,5)
);
CREATE TABLE IF NOT EXISTS WKS_MY_SISO_PROPAGATE_TO_DETAILS (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MONTH NUMBER(18,0),
	MATL_NUM VARCHAR(100),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13),
	LAST_3MONTHS_SO NUMBER(38,6),
	LAST_6MONTHS_SO NUMBER(38,6),
	LAST_12MONTHS_SO NUMBER(38,6),
	LAST_3MONTHS_SO_VALUE NUMBER(38,17),
	LAST_6MONTHS_SO_VALUE NUMBER(38,17),
	LAST_12MONTHS_SO_VALUE NUMBER(38,17),
	LAST_36MONTHS_SO_VALUE NUMBER(38,17),
	PROPAGATION_FLAG VARCHAR(1),
	PROPAGATE_FROM NUMBER(18,0),
	REASON VARCHAR(29),
	REPLICATED_FLAG VARCHAR(1)
);
CREATE TABLE IF NOT EXISTS WKS_MY_SISO_PROPAGATE_TO_EXISTING_DTLS (
	DISTRIBUTOR VARCHAR(40),
	DSTRBTR_GRP_CD VARCHAR(30),
	SAP_PARENT_CUSTOMER_KEY VARCHAR(12),
	SAP_PARENT_CUSTOMER_DESC VARCHAR(50),
	MONTH NUMBER(18,0),
	MATL_NUM VARCHAR(100),
	SO_QTY NUMBER(38,6),
	SO_VALUE NUMBER(38,17),
	INV_QTY NUMBER(38,4),
	INV_VALUE NUMBER(38,13),
	SELL_IN_QTY NUMBER(38,4),
	SELL_IN_VALUE NUMBER(38,13),
	LAST_3MONTHS_SO NUMBER(38,6),
	LAST_6MONTHS_SO NUMBER(38,6),
	LAST_12MONTHS_SO NUMBER(38,6),
	LAST_3MONTHS_SO_VALUE NUMBER(38,17),
	LAST_6MONTHS_SO_VALUE NUMBER(38,17),
	LAST_12MONTHS_SO_VALUE NUMBER(38,17),
	LAST_36MONTHS_SO_VALUE NUMBER(38,17),
	PROPAGATE_FROM NUMBER(18,0),
	REPLICATED_FLAG VARCHAR(1)
);
CREATE TABLE IF NOT EXISTS WKS_MY_TARGETS (
	SOLD_TO VARCHAR(255),
	CUSTOMER VARCHAR(255),
	BRAND VARCHAR(255),
	SUBSEGMENT VARCHAR(255),
	YEAR VARCHAR(255),
	TYPE VARCHAR(255),
	TARGET_MEASURE_NAME VARCHAR(255),
	JAN VARCHAR(255),
	FEB VARCHAR(255),
	MAR VARCHAR(255),
	APR VARCHAR(255),
	MAY VARCHAR(255),
	JUN VARCHAR(255),
	JUL VARCHAR(255),
	AUG VARCHAR(255),
	SEP VARCHAR(255),
	OCT VARCHAR(255),
	NOV VARCHAR(255),
	DEC VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SG_CUSTOMER_MAPPING (
	ZUELLIG_CUSTOMER VARCHAR(255),
	REGIONAL_BANNER VARCHAR(255),
	MERCHANDIZING VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_LISTPRICE (
	PLANT VARCHAR(4),
	CNTY VARCHAR(4),
	ITEM_CD VARCHAR(20),
	ITEM_DESC VARCHAR(100),
	VALID_FROM VARCHAR(10),
	VALID_TO VARCHAR(10),
	RATE NUMBER(20,4),
	CURRENCY VARCHAR(4),
	PRICE_UNIT NUMBER(16,4),
	UOM VARCHAR(6),
	CDL_DTTM VARCHAR(255),
	SNAPSHOT_DT DATE,
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS WKS_SG_PRODUCT_MAPPING (
	ZP_MATERIAL VARCHAR(255),
	ZP_ITEM_CODE VARCHAR(255),
	JJ_CODE VARCHAR(255),
	ITEM_NAME VARCHAR(255),
	BRAND VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_AMAZON (
	RM VARCHAR(50),
	MERCHANT_CUSTOMER_ID VARCHAR(15),
	GL VARCHAR(50),
	CATEGORY VARCHAR(200),
	SUBCATEGORY VARCHAR(200),
	BRAND VARCHAR(255),
	ASIN VARCHAR(50),
	ITEM_NAME VARCHAR(500),
	OPS NUMBER(10,4),
	PCOGS NUMBER(10,4),
	SHIPPED_UNITS NUMBER(10,0),
	PPMPERCENT NUMBER(10,5),
	PPMDOLLAR NUMBER(10,5),
	MONTH NUMBER(18,0),
	YEAR NUMBER(18,0),
	VENDOR_CODE VARCHAR(10),
	VENDOR_NAME VARCHAR(255),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_AMAZON_TEMP (
	RM VARCHAR(50),
	MERCHANT_CUSTOMER_ID VARCHAR(15),
	GL VARCHAR(50),
	CATEGORY VARCHAR(200),
	SUBCATEGORY VARCHAR(200),
	BRAND VARCHAR(255),
	ASIN VARCHAR(50),
	ITEM_NAME VARCHAR(500),
	OPS NUMBER(10,4),
	PCOGS NUMBER(10,4),
	SHIPPED_UNITS NUMBER(10,0),
	PPMPERCENT NUMBER(10,5),
	PPMDOLLAR NUMBER(10,5),
	MONTH NUMBER(18,0),
	YEAR NUMBER(18,0),
	VENDOR_CODE VARCHAR(10),
	VENDOR_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_MARKETPLACE (
	CHANNEL VARCHAR(20),
	MONTH VARCHAR(20),
	ORDER_CREATION_DATE DATE,
	INVOICE_NUMBER VARCHAR(255),
	STATUS VARCHAR(100),
	BARCODE VARCHAR(500),
	ITEM_NAME VARCHAR(500),
	SALES_UNIT NUMBER(10,0),
	NET_INVOICED_SALES NUMBER(10,4),
	BRAND VARCHAR(300),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_MARKETPLACE_TEMP (
	CHANNEL VARCHAR(20),
	MONTH VARCHAR(20),
	ORDER_CREATION_DATE DATE,
	INVOICE_NUMBER VARCHAR(255),
	STATUS VARCHAR(100),
	BARCODE VARCHAR(500),
	ITEM_NAME VARCHAR(500),
	SALES_UNIT NUMBER(10,0),
	NET_INVOICED_SALES NUMBER(10,4),
	BRAND VARCHAR(300)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_NTUC (
	VENDOR_CODE VARCHAR(100),
	VENDOR_NAME VARCHAR(255),
	DEPT_CODE VARCHAR(50),
	DEPT_DESCRIPTION VARCHAR(255),
	CLASS_NO VARCHAR(50),
	CLASS_DESCRIPTION VARCHAR(255),
	SUB_CLASS_DESCRIPTION VARCHAR(255),
	MCH VARCHAR(255),
	SKU_NO VARCHAR(100),
	ARTICLE_DESCRIPTION VARCHAR(500),
	BRAND VARCHAR(255),
	SALES_UOM VARCHAR(100),
	PACK_SIZE NUMBER(10,0),
	STORE_CODE VARCHAR(100),
	STORE_NAME VARCHAR(255),
	STORE_FORMAT VARCHAR(255),
	ATTRIBUTE1 VARCHAR(50),
	ATTRIBUTE2 VARCHAR(50),
	ATTRIBUTE3 DATE,
	VALUE NUMBER(10,4),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_NTUC_TEMP (
	VENDOR_CODE VARCHAR(100),
	VENDOR_NAME VARCHAR(255),
	DEPT_CODE VARCHAR(50),
	DEPT_DESCRIPTION VARCHAR(255),
	CLASS_NO VARCHAR(50),
	CLASS_DESCRIPTION VARCHAR(255),
	SUB_CLASS_DESCRIPTION VARCHAR(255),
	MCH VARCHAR(255),
	SKU_NO VARCHAR(100),
	ARTICLE_DESCRIPTION VARCHAR(500),
	BRAND VARCHAR(255),
	SALES_UOM VARCHAR(100),
	PACK_SIZE NUMBER(10,0),
	STORE_CODE VARCHAR(100),
	STORE_NAME VARCHAR(255),
	STORE_FORMAT VARCHAR(255),
	ATTRIBUTE1 VARCHAR(50),
	ATTRIBUTE2 VARCHAR(50),
	ATTRIBUTE3 DATE,
	VALUE NUMBER(10,4)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_REDMART (
	WEEK_START_DATE DATE,
	RPC_ITEM_ID VARCHAR(100),
	PRODUCT_CODE VARCHAR(300),
	ITEM_DESCRIPTION VARCHAR(500),
	PACKSIZE VARCHAR(100),
	BRAND VARCHAR(300),
	SUPPLIER_ID VARCHAR(100),
	SUPPLIER_NAME VARCHAR(100),
	MANUFACTURER VARCHAR(200),
	CATEGORY_1 VARCHAR(150),
	CATEGORY_2 VARCHAR(150),
	CATEGORY_3 VARCHAR(150),
	CATEGORY_4 VARCHAR(150),
	GMV NUMBER(10,4),
	UNITS_SOLD NUMBER(10,0),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_REDMART_TEMP (
	WEEK_START_DATE DATE,
	RPC_ITEM_ID VARCHAR(100),
	PRODUCT_CODE VARCHAR(300),
	ITEM_DESCRIPTION VARCHAR(500),
	PACKSIZE VARCHAR(100),
	BRAND VARCHAR(300),
	SUPPLIER_ID VARCHAR(100),
	SUPPLIER_NAME VARCHAR(100),
	MANUFACTURER VARCHAR(200),
	CATEGORY_1 VARCHAR(150),
	CATEGORY_2 VARCHAR(150),
	CATEGORY_3 VARCHAR(150),
	CATEGORY_4 VARCHAR(150),
	GMV NUMBER(10,4),
	UNITS_SOLD NUMBER(10,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_SCOMMERCE (
	DATE_ID DATE,
	ORDERSN VARCHAR(50),
	ITEMID VARCHAR(50),
	MODELID VARCHAR(50),
	SKU_ID VARCHAR(100),
	ITEM_NAME VARCHAR(500),
	MODEL_NAME VARCHAR(500),
	SUM_OF_AMOUNT NUMBER(10,0),
	SUM_OF_GMV_SGD NUMBER(10,6),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_SCOMMERCE_TEMP (
	DATE_ID DATE,
	ORDERSN VARCHAR(50),
	ITEMID VARCHAR(50),
	MODELID VARCHAR(50),
	SKU_ID VARCHAR(100),
	ITEM_NAME VARCHAR(500),
	MODEL_NAME VARCHAR(500),
	SUM_OF_AMOUNT NUMBER(10,0),
	SUM_OF_GMV_SGD NUMBER(10,6)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_WATSONS (
	YEAR VARCHAR(20),
	WEEK VARCHAR(20),
	STORE VARCHAR(255),
	DIV VARCHAR(255),
	PRDT_DEPT VARCHAR(255),
	PRDTCODE VARCHAR(255),
	PRDTDESC VARCHAR(500),
	BRAND VARCHAR(300),
	SUPCODE VARCHAR(255),
	SUP_NAME VARCHAR(300),
	BARCODE VARCHAR(255),
	SUP_CAT VARCHAR(255),
	DEPT_NAME VARCHAR(255),
	NET_SALES VARCHAR(100),
	SALES_QTY NUMBER(10,0),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_SCAN_DATA_WATSONS_TEMP (
	YEAR VARCHAR(20),
	WEEK VARCHAR(20),
	STORE VARCHAR(255),
	DIV VARCHAR(255),
	PRDT_DEPT VARCHAR(255),
	PRDTCODE VARCHAR(255),
	PRDTDESC VARCHAR(500),
	BRAND VARCHAR(300),
	SUPCODE VARCHAR(255),
	SUP_NAME VARCHAR(300),
	BARCODE VARCHAR(255),
	SUP_CAT VARCHAR(255),
	DEPT_NAME VARCHAR(255),
	NET_SALES VARCHAR(100),
	SALES_QTY NUMBER(10,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_TP_CLOSED_MONTH (
	FILE_NAME VARCHAR(255),
	SHEET_NAME VARCHAR(255),
	MONTH_NO VARCHAR(255),
	SALES_REP VARCHAR(255),
	CUSTOMER_L1 VARCHAR(255),
	CUSTOMER VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CHANNEL VARCHAR(255),
	FRANCHISE VARCHAR(255),
	BRAND VARCHAR(255),
	BRAND_PROFIT_CENTER VARCHAR(255),
	PROMO_TYPE VARCHAR(255),
	GL_ACCOUNT VARCHAR(255),
	DESCRIPTION VARCHAR(255),
	REQUESTED_DATE VARCHAR(255),
	MONTH_OF_ACTIVITY VARCHAR(255),
	PROMO_START_DATE VARCHAR(255),
	PROMO_END_DATE VARCHAR(255),
	COMMITTED_OR_ACCRUAL_WO_GST VARCHAR(255),
	TP_NUMBER VARCHAR(255),
	ZP_OR_CMM_INVOICE_NO VARCHAR(255),
	RETAILERS_BILLING_NO VARCHAR(255),
	JNJ_ACTUALS_WO_GST VARCHAR(255),
	CN_NUMBER VARCHAR(255),
	CN_DATE VARCHAR(255),
	JNJ_TOTAL_COMMITTED_WO_GST VARCHAR(255),
	JNJ_TOTAL_UNCLAIMED_WO_GST VARCHAR(255),
	COMMENTS_OR_REVERSED_ACCRUED_AMT VARCHAR(255),
	MONTH_OF_REVERSAL VARCHAR(255),
	SUPPORTING VARCHAR(255),
	MONTH_OF_DOC_SCANNING VARCHAR(255),
	REMARKS VARCHAR(255),
	WORKING_SC VARCHAR(255),
	CONSO_PR VARCHAR(255),
	CREATE_PR VARCHAR(255),
	PAYMENT_REF VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_TP_CLOSED_YEAR_BAL (
	FILE_NAME VARCHAR(255),
	SHEET_NAME VARCHAR(255),
	MONTH_NO VARCHAR(255),
	SALES_REP VARCHAR(255),
	CUSTOMER_L1 VARCHAR(255),
	CUSTOMER VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CHANNEL VARCHAR(255),
	FRANCHISE VARCHAR(255),
	BRAND VARCHAR(255),
	BRAND_PROFIT_CENTER VARCHAR(255),
	PROMO_TYPE VARCHAR(255),
	GL_ACCOUNT VARCHAR(255),
	DESCRIPTION VARCHAR(255),
	REQUESTED_DATE VARCHAR(255),
	MONTH_OF_ACTIVITY VARCHAR(255),
	PROMO_START_DATE VARCHAR(255),
	PROMO_END_DATE VARCHAR(255),
	COMMITTED_OR_ACCRUAL_WO_GST VARCHAR(255),
	TP_NUMBER VARCHAR(255),
	ZP_CMM_INVOICE VARCHAR(255),
	RETAILERS_BILLING VARCHAR(255),
	JNJ_ACTUALS_WO_GST VARCHAR(255),
	MONTH_OF_ACTUAL VARCHAR(255),
	CN_NUMBER VARCHAR(255),
	CN_DATE VARCHAR(255),
	REVERSAL_AMOUNT VARCHAR(255),
	OUTSTANDING_ACCRUAL VARCHAR(255),
	JNJ_TOTAL_COMMITTED_WO_GST VARCHAR(255),
	JNJ_TOTAL_UNCLAIMED_WO_GST VARCHAR(255),
	COMMENTS_OR_REVERSED_ACCRUED_AMT VARCHAR(255),
	MONTH_OF_REVERSAL VARCHAR(255),
	SUPPORTING VARCHAR(255),
	TP_IMPACT VARCHAR(255),
	LEFT_ACCRUAL VARCHAR(255),
	MONTH_OF_DOC_SCANNING VARCHAR(255),
	REMARKS VARCHAR(255),
	WORKING_SC VARCHAR(255),
	CONSO_PR VARCHAR(255),
	CREATE_PR VARCHAR(255),
	PAYMENT_REF VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SG_ZUELLIG_SELLOUT (
	MONTH_NO VARCHAR(255),
	SALES_DATE VARCHAR(255),
	WAREHOUSE_CODE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CUSTOMER_NAME VARCHAR(255),
	INVOICE VARCHAR(255),
	ITEM_NAME VARCHAR(255),
	ITEM_CODE VARCHAR(255),
	TYPE VARCHAR(255),
	SALES_VALUE NUMBER(17,3),
	SALES_UNITS NUMBER(17,3),
	BONUS_UNITS NUMBER(17,3),
	RETURNS_UNITS NUMBER(17,3),
	RETURNS_VALUE NUMBER(17,3),
	RETURNS_BONUS_UNITS NUMBER(17,3),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_108129 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_108273 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_108565 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_109772 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_118477 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_119024 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_119025 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130516 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130517 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130518 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130519 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130520 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_130521 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_131164 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_131165 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_131166 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_131167 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133980 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133981 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133982 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133983 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133984 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133985 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_133986 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_135976 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_INV_137021 (
	DISTRIBUTOR_ID VARCHAR(255),
	DATE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	QUANTITY_AVAILABLE VARCHAR(255),
	UOM_AVAILABLE VARCHAR(255),
	QUANTITY_ON_ORDER VARCHAR(255),
	UOM_ON_ORDER VARCHAR(255),
	QUANTITY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	QUANTITY_AVAILABLE_IN_PIECES VARCHAR(255),
	QUANTITY_ON_ORDER_IN_PIECES VARCHAR(255),
	QUANTITY_COMMITTED_IN_PIECES VARCHAR(255),
	UNIT_PRICE VARCHAR(255),
	TOTAL_VALUE_AVAILABLE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_108129 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_108273 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_108565 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_109772 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_118477 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_119024 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_119025 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130516 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130517 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130518 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130519 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130520 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_130521 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_131164 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_131165 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_131166 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_131167 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133980 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133981 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133982 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133983 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133984 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133985 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_133986 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_135976 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_137021 (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255),
	FILE_NAME VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS WKS_SO_SALES_TEMP (
	DISTRIBUTOR_ID VARCHAR(255),
	SALES_ORDER_NUMBER VARCHAR(255),
	SALES_ORDER_DATE VARCHAR(255),
	TYPE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	DISTRIBUTOR_WH_ID VARCHAR(255),
	SAP_MATERIAL_ID VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_EAN_CODE VARCHAR(255),
	PRODUCT_DESCRIPTION VARCHAR(255),
	GROSS_ITEM_PRICE VARCHAR(255),
	QUANTITY VARCHAR(255),
	UOM VARCHAR(255),
	QUANTITY_IN_PIECES VARCHAR(255),
	QUANTITY_AFTER_CONVERSION VARCHAR(255),
	SUB_TOTAL_1 VARCHAR(255),
	DISCOUNT VARCHAR(255),
	SUB_TOTAL_2 VARCHAR(255),
	BOTTOM_LINE_DISCOUNT VARCHAR(255),
	TOTAL_AMT_AFTER_TAX VARCHAR(255),
	TOTAL_AMT_BEFORE_TAX VARCHAR(255),
	SALES_EMPLOYEE VARCHAR(255),
	CUSTOM_FIELD_1 VARCHAR(255),
	CUSTOM_FIELD_2 VARCHAR(255),
	CUSTOM_FIELD_3 VARCHAR(255)
);

-------------------------------------------------------------------------------------------
USE schema SNAPOSEEDW_INTEGRATION;

CREATE TABLE IF NOT EXISTS EDW_MY_POS_ANALYSIS (
	DATA_SRC VARCHAR(3),
	JJ_YEAR NUMBER(18,0),
	JJ_QTR VARCHAR(33),
	JJ_MNTH_ID VARCHAR(30),
	JJ_MNTH_NO NUMBER(18,0),
	JJ_YEAR_WK_NO VARCHAR(20),
	CNTRY_NM VARCHAR(11),
	CUST_CD VARCHAR(255),
	CUST_BRNCH_CD VARCHAR(300),
	MT_CUST_BRNCH_NM VARCHAR(300),
	CUST_DEPT_CD VARCHAR(10),
	MT_CUST_DEPT_NM VARCHAR(255),
	REGION_CD VARCHAR(255),
	REGION_NM VARCHAR(255),
	ITEM_CD VARCHAR(300),
	MT_ITEM_NM VARCHAR(500),
	SOLD_TO VARCHAR(15),
	SOLD_TO_NM VARCHAR(100),
	TRADE_TYPE VARCHAR(12),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	SAP_STATE_CD VARCHAR(150),
	SAP_SLS_ORG VARCHAR(4),
	SAP_CMP_ID VARCHAR(6),
	SAP_CNTRY_CD VARCHAR(3),
	SAP_CNTRY_NM VARCHAR(40),
	SAP_ADDR VARCHAR(150),
	SAP_REGION VARCHAR(150),
	SAP_CITY VARCHAR(150),
	SAP_POST_CD VARCHAR(150),
	SAP_CHNL_CD VARCHAR(2),
	SAP_CHNL_DESC VARCHAR(20),
	SAP_SLS_OFFICE_CD VARCHAR(4),
	SAP_SLS_OFFICE_DESC VARCHAR(40),
	SAP_SLS_GRP_CD VARCHAR(3),
	SAP_SLS_GRP_DESC VARCHAR(40),
	SAP_CURR_CD VARCHAR(5),
	GCH_REGION VARCHAR(50),
	GCH_CLUSTER VARCHAR(50),
	GCH_SUBCLUSTER VARCHAR(50),
	GCH_MARKET VARCHAR(50),
	GCH_RETAIL_BANNER VARCHAR(50),
	SKU VARCHAR(40),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	PUTUP_DESC VARCHAR(40),
	ITEM_DESC2 VARCHAR(200),
	SKU_DESC VARCHAR(100),
	SAP_MAT_TYPE_CD VARCHAR(10),
	SAP_MAT_TYPE_DESC VARCHAR(40),
	SAP_BASE_UOM_CD VARCHAR(10),
	SAP_PRCHSE_UOM_CD VARCHAR(10),
	SAP_PROD_SGMT_CD VARCHAR(18),
	SAP_PROD_SGMT_DESC VARCHAR(100),
	SAP_BASE_PROD_CD VARCHAR(10),
	SAP_BASE_PROD_DESC VARCHAR(100),
	SAP_MEGA_BRND_CD VARCHAR(10),
	SAP_MEGA_BRND_DESC VARCHAR(100),
	SAP_BRND_CD VARCHAR(10),
	SAP_BRND_DESC VARCHAR(100),
	SAP_VRNT_CD VARCHAR(10),
	SAP_VRNT_DESC VARCHAR(100),
	SAP_PUT_UP_CD VARCHAR(10),
	SAP_PUT_UP_DESC VARCHAR(100),
	SAP_GRP_FRNCHSE_CD VARCHAR(18),
	SAP_GRP_FRNCHSE_DESC VARCHAR(100),
	SAP_FRNCHSE_CD VARCHAR(18),
	SAP_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_FRNCHSE_CD VARCHAR(18),
	SAP_PROD_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_MJR_CD VARCHAR(18),
	SAP_PROD_MJR_DESC VARCHAR(100),
	SAP_PROD_MNR_CD VARCHAR(18),
	SAP_PROD_MNR_DESC VARCHAR(100),
	SAP_PROD_HIER_CD VARCHAR(18),
	SAP_PROD_HIER_DESC VARCHAR(100),
	GLOBAL_MAT_REGION VARCHAR(50),
	GLOBAL_PROD_FRANCHISE VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_PUT_UP_CD VARCHAR(10),
	GLOBAL_PUT_UP_DESC VARCHAR(100),
	GLOBAL_PROD_SUB_BRAND VARCHAR(100),
	GLOBAL_PROD_NEED_STATE VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SUBCATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_SUBSEGMENT VARCHAR(100),
	GLOBAL_PROD_SIZE VARCHAR(20),
	GLOBAL_PROD_SIZE_UOM VARCHAR(20),
	FROM_CCY VARCHAR(5),
	TO_CCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	POS_QTY FLOAT,
	POS_GTS FLOAT,
	POS_ITEM_PRC FLOAT,
	POS_TAX FLOAT,
	POS_NTS FLOAT,
	CONV_FACTOR NUMBER(20,4),
	JJ_POS_QTY_PC NUMBER(22,6),
	JJ_POS_ITEM_PRC_PER_PC NUMBER(36,9),
	JJ_POS_GTS NUMBER(38,15),
	JJ_POS_VAT_AMT NUMBER(20,4),
	JJ_POS_NTS NUMBER(38,15),
	IS_NPI VARCHAR(10),
	NPI_STR_PERIOD VARCHAR(10),
	NPI_END_PERIOD VARCHAR(1),
	IS_REG VARCHAR(1),
	IS_PROMO VARCHAR(40),
	PROMO_STRT_PERIOD VARCHAR(1),
	PROMO_END_PERIOD VARCHAR(1),
	IS_MCL VARCHAR(1),
	IS_HERO VARCHAR(10)
);
CREATE TABLE IF NOT EXISTS EDW_MY_SELLIN_ANALYSIS (
	DATA_SRC VARCHAR(12),
	JJ_YEAR NUMBER(18,0),
	JJ_QTR VARCHAR(14),
	JJ_MNTH_ID VARCHAR(23),
	JJ_MNTH_NO NUMBER(18,0),
	SAP_CNTRY_CD VARCHAR(3),
	CNTRY_NM VARCHAR(8),
	ACCT_NO VARCHAR(10),
	ACCT_DESC VARCHAR(100),
	SAP_STATE_CD VARCHAR(150),
	SOLD_TO VARCHAR(10),
	SOLD_TO_NM VARCHAR(100),
	SAP_SLS_ORG VARCHAR(4),
	SAP_CMP_ID VARCHAR(6),
	SAP_ADDR VARCHAR(150),
	SAP_REGION VARCHAR(150),
	SAP_CITY VARCHAR(150),
	SAP_POST_CD VARCHAR(150),
	SAP_CHNL_CD VARCHAR(2),
	SAP_CHNL_DESC VARCHAR(20),
	SAP_SLS_OFFICE_CD VARCHAR(4),
	SAP_SLS_OFFICE_DESC VARCHAR(40),
	SAP_SLS_GRP_CD VARCHAR(3),
	SAP_SLS_GRP_DESC VARCHAR(40),
	SAP_CURR_CD VARCHAR(5),
	GCH_REGION VARCHAR(50),
	GCH_CLUSTER VARCHAR(50),
	GCH_SUBCLUSTER VARCHAR(50),
	GCH_MARKET VARCHAR(50),
	GCH_RETAIL_BANNER VARCHAR(50),
	LOC_CUST_NM VARCHAR(100),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	ULLAGE NUMBER(20,4),
	CHNL VARCHAR(20),
	TERRITORY VARCHAR(20),
	RETAIL_ENV VARCHAR(20),
	RDD_IND VARCHAR(10),
	MATL_NUM VARCHAR(40),
	MATL_DESC VARCHAR(100),
	MATL_DESC2 VARCHAR(200),
	MATL_DESC3 VARCHAR(200),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	SKU_EAN_NUM VARCHAR(100),
	GLOBAL_MAT_REGION VARCHAR(50),
	GLOBAL_PROD_FRANCHISE VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_PUT_UP_CD VARCHAR(10),
	GLOBAL_PUT_UP_DESC VARCHAR(100),
	PUT_UP_DESC VARCHAR(40),
	SAP_PROD_MNR_CD VARCHAR(18),
	SAP_PROD_MNR_DESC VARCHAR(100),
	SAP_PROD_HIER_CD VARCHAR(18),
	SAP_PROD_HIER_DESC VARCHAR(100),
	GLOBAL_PROD_SUB_BRAND VARCHAR(100),
	GLOBAL_PROD_NEED_STATE VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SUBCATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_SUBSEGMENT VARCHAR(100),
	GLOBAL_PROD_SIZE VARCHAR(20),
	GLOBAL_PROD_SIZE_UOM VARCHAR(20),
	SKU_LAUNCH_DT TIMESTAMP_NTZ(9),
	QTY_SHIPPER_PC VARCHAR(100),
	PRFT_CTR VARCHAR(100),
	SHELF_LIFE VARCHAR(100),
	NPI_IND VARCHAR(10),
	NPI_STRT_PERIOD VARCHAR(10),
	HERO_IND VARCHAR(10),
	CIW_CTGRY VARCHAR(100),
	CIW_BUCKT1 VARCHAR(100),
	CIW_BUCKT2 VARCHAR(100),
	BRAVO_DESC1 VARCHAR(100),
	BRAVO_DESC2 VARCHAR(100),
	ACCT_TYPE VARCHAR(20),
	ACCT_TYPE1 VARCHAR(20),
	FROM_CRNCY VARCHAR(5),
	TO_CRNCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	BASE_VAL NUMBER(20,4),
	SLS_QTY NUMBER(20,4),
	RET_QTY NUMBER(20,4),
	SLS_LESS_RTN_QTY NUMBER(20,4),
	GTS_VAL NUMBER(20,4),
	RET_VAL NUMBER(20,4),
	GTS_LESS_RTN_VAL NUMBER(20,4),
	TRDNG_TERM_VAL NUMBER(20,4),
	TP_VAL NUMBER(20,4),
	TRDE_PRMTN_VAL NUMBER(20,4),
	NTS_VAL NUMBER(20,4),
	NTS_QTY NUMBER(20,4),
	PO_NUM VARCHAR(30),
	SLS_DOC_NUM VARCHAR(50),
	SLS_DOC_ITEM VARCHAR(50),
	SLS_DOC_TYPE VARCHAR(30),
	BILL_NUM VARCHAR(50),
	BILL_ITEM VARCHAR(50),
	DOC_CREATION_DT DATE,
	ORDER_STATUS VARCHAR(19),
	ITEM_STATUS VARCHAR(24),
	REJECTN_ST VARCHAR(20),
	REJECTN_CD VARCHAR(30),
	REJECTN_DESC VARCHAR(1),
	ORD_QTY NUMBER(38,4),
	ORD_NET_PRICE NUMBER(38,13),
	ORD_GRS_TRD_SLS NUMBER(38,13),
	ORD_SUBTOTAL_2 NUMBER(38,13),
	ORD_SUBTOTAL_3 NUMBER(38,13),
	ORD_SUBTOTAL_4 NUMBER(38,13),
	ORD_NET_AMT NUMBER(38,13),
	ORD_EST_NTS NUMBER(38,13),
	MISSED_VAL NUMBER(38,13),
	BILL_QTY_PC NUMBER(38,4),
	BILL_GRS_TRD_SLS NUMBER(38,13),
	BILL_SUBTOTAL_2 NUMBER(38,13),
	BILL_SUBTOTAL_3 NUMBER(38,13),
	BILL_SUBTOTAL_4 NUMBER(38,13),
	BILL_NET_AMT NUMBER(38,13),
	BILL_EST_NTS NUMBER(38,13),
	BILL_NET_VAL NUMBER(38,13),
	BILL_GROSS_VAL NUMBER(38,13),
	TRGT_TYPE VARCHAR(20),
	TRGT_VAL_TYPE VARCHAR(20),
	TRGT_VAL NUMBER(36,11),
	ACCRUAL_VAL NUMBER(36,11),
	LE_TRGT_VAL_WK1 NUMBER(36,11),
	LE_TRGT_VAL_WK2 NUMBER(36,11),
	LE_TRGT_VAL_WK3 NUMBER(36,11),
	LE_TRGT_VAL_WK4 NUMBER(36,11),
	LE_TRGT_VAL_WK5 NUMBER(36,11),
	CURR_PRD_ELAPSED NUMBER(20,6),
	ELAPSED_FLAG VARCHAR(1),
	IS_CURR VARCHAR(1),
	AFGR_NUM VARCHAR(30),
	CUST_DN_NUM VARCHAR(100),
	RTN_ORD_NUM VARCHAR(30),
	AFGR_BILL_NUM VARCHAR(30),
	DN_AMT_EXC_GST_VAL NUMBER(38,9),
	AFGR_AMT NUMBER(38,9),
	RTN_ORD_AMT NUMBER(38,9),
	CN_AMT NUMBER(38,9),
	AFGR_STATUS VARCHAR(30),
	AFGR_VAL NUMBER(38,9),
	AFGR_CN_DIFF NUMBER(38,9),
	CUR_PERIOD_SGT VARCHAR(23)
);
CREATE TABLE IF NOT EXISTS EDW_MY_SELLIN_PREV_DT_SNPSHT (
	CO_CD VARCHAR(4),
	CNTRY_NM VARCHAR(3),
	PSTNG_DT VARCHAR(20),
	JJ_MNTH_ID VARCHAR(22),
	ITEM_CD VARCHAR(18),
	CUST_ID VARCHAR(10),
	SLS_ORG VARCHAR(4),
	PLNT VARCHAR(4),
	DSTR_CHNL VARCHAR(2),
	ACCT_NO VARCHAR(10),
	BILL_TYP VARCHAR(4),
	SLS_OFC VARCHAR(4),
	SLS_GRP VARCHAR(3),
	SLS_DIST VARCHAR(6),
	CUST_GRP VARCHAR(2),
	CUST_SLS VARCHAR(10),
	FISC_YR NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	BASE_VAL NUMBER(38,5),
	SLS_QTY NUMBER(38,5),
	RET_QTY NUMBER(38,5),
	SLS_LESS_RTN_QTY NUMBER(38,5),
	GTS_VAL NUMBER(38,5),
	RET_VAL NUMBER(38,5),
	GTS_LESS_RTN_VAL NUMBER(38,5),
	TRDNG_TERM_VAL NUMBER(38,5),
	TP_VAL NUMBER(38,5),
	TRDE_PRMTN_VAL NUMBER(38,5),
	NTS_VAL NUMBER(38,5),
	NTS_QTY NUMBER(38,5),
	SNAPSHOT_DT TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS EDW_MY_SELLOUT_ANALYSIS (
	DATA_SRC VARCHAR(12),
	YEAR NUMBER(18,0),
	QRTR_NO NUMBER(18,0),
	MNTH_ID NUMBER(18,0),
	MNTH_NO NUMBER(18,0),
	MNTH_NM VARCHAR(9),
	MAX_YEARMO VARCHAR(28),
	CNTRY_NM VARCHAR(8),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	DSTRBTR_CUST_CD VARCHAR(50),
	DSTRBTR_CUST_NM VARCHAR(500),
	SAP_SOLDTO_CODE VARCHAR(10),
	SAP_SOLDTO_NM VARCHAR(100),
	DSTRBTR_LVL1 VARCHAR(40),
	DSTRBTR_LVL2 VARCHAR(40),
	DSTRBTR_LVL3 VARCHAR(40),
	DSTRBTR_LVL4 VARCHAR(40),
	DSTRBTR_LVL5 VARCHAR(40),
	REGION_NM VARCHAR(100),
	TOWN_NM VARCHAR(100),
	SLSMN_CD VARCHAR(150),
	CHNL_DESC VARCHAR(100),
	SUB_CHNL_DESC VARCHAR(100),
	CHNL_ATTR1_DESC VARCHAR(100),
	CHNL_ATTR2_DESC VARCHAR(100),
	SAP_STATE_CD VARCHAR(150),
	SAP_SLS_ORG VARCHAR(4),
	SAP_CMP_ID VARCHAR(6),
	SAP_CNTRY_CD VARCHAR(3),
	SAP_CNTRY_NM VARCHAR(40),
	SAP_ADDR VARCHAR(150),
	SAP_REGION VARCHAR(150),
	SAP_CITY VARCHAR(150),
	SAP_POST_CD VARCHAR(150),
	SAP_CHNL_CD VARCHAR(2),
	SAP_CHNL_DESC VARCHAR(20),
	SAP_SLS_OFFICE_CD VARCHAR(4),
	SAP_SLS_OFFICE_DESC VARCHAR(40),
	SAP_SLS_GRP_CD VARCHAR(3),
	SAP_SLS_GRP_DESC VARCHAR(40),
	SAP_CURR_CD VARCHAR(5),
	GCH_REGION VARCHAR(50),
	GCH_CLUSTER VARCHAR(50),
	GCH_SUBCLUSTER VARCHAR(50),
	GCH_MARKET VARCHAR(50),
	GCH_RETAIL_BANNER VARCHAR(50),
	DSTRBTR_MATL_NUM VARCHAR(75),
	BAR_CD VARCHAR(50),
	SKU VARCHAR(40),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	PUTUP_DESC VARCHAR(40),
	ITEM_DESC2 VARCHAR(200),
	SKU_DESC VARCHAR(100),
	SAP_MAT_TYPE_CD VARCHAR(10),
	SAP_MAT_TYPE_DESC VARCHAR(40),
	SAP_BASE_UOM_CD VARCHAR(10),
	SAP_PRCHSE_UOM_CD VARCHAR(10),
	SAP_PROD_SGMT_CD VARCHAR(18),
	SAP_PROD_SGMT_DESC VARCHAR(100),
	SAP_BASE_PROD_CD VARCHAR(10),
	SAP_BASE_PROD_DESC VARCHAR(100),
	SAP_MEGA_BRND_CD VARCHAR(10),
	SAP_MEGA_BRND_DESC VARCHAR(100),
	SAP_BRND_CD VARCHAR(10),
	SAP_BRND_DESC VARCHAR(100),
	SAP_VRNT_CD VARCHAR(10),
	SAP_VRNT_DESC VARCHAR(100),
	SAP_PUT_UP_CD VARCHAR(10),
	SAP_PUT_UP_DESC VARCHAR(100),
	SAP_GRP_FRNCHSE_CD VARCHAR(18),
	SAP_GRP_FRNCHSE_DESC VARCHAR(100),
	SAP_FRNCHSE_CD VARCHAR(18),
	SAP_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_FRNCHSE_CD VARCHAR(18),
	SAP_PROD_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_MJR_CD VARCHAR(18),
	SAP_PROD_MJR_DESC VARCHAR(100),
	SAP_PROD_MNR_CD VARCHAR(18),
	SAP_PROD_MNR_DESC VARCHAR(100),
	SAP_PROD_HIER_CD VARCHAR(18),
	SAP_PROD_HIER_DESC VARCHAR(100),
	GLOBAL_MAT_REGION VARCHAR(50),
	GLOBAL_PROD_FRANCHISE VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_PUT_UP_CD VARCHAR(10),
	GLOBAL_PUT_UP_DESC VARCHAR(100),
	GLOBAL_PROD_SUB_BRAND VARCHAR(100),
	GLOBAL_PROD_NEED_STATE VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SUBCATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_SUBSEGMENT VARCHAR(100),
	GLOBAL_PROD_SIZE VARCHAR(20),
	GLOBAL_PROD_SIZE_UOM VARCHAR(20),
	FROM_CCY VARCHAR(5),
	TO_CCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	WH_ID VARCHAR(50),
	DOC_TYPE VARCHAR(20),
	DOC_TYPE_DESC VARCHAR(20),
	BILL_DATE TIMESTAMP_NTZ(9),
	BILL_DOC VARCHAR(255),
	BASE_SLS NUMBER(36,9),
	SLS_QTY NUMBER(22,6),
	RET_QTY NUMBER(24,6),
	UOM VARCHAR(20),
	SLS_QTY_PC NUMBER(22,6),
	RET_QTY_PC NUMBER(24,6),
	IN_TRANSIT_QTY NUMBER(38,4),
	MAT_LIST_PRICE NUMBER(20,4),
	GRS_TRD_SLS NUMBER(38,17),
	RET_VAL NUMBER(38,17),
	TRD_DISCNT NUMBER(38,11),
	TRD_SLS NUMBER(36,9),
	NET_TRD_SLS NUMBER(38,11),
	JJ_GRS_TRD_SLS NUMBER(38,17),
	JJ_RET_VAL NUMBER(38,17),
	JJ_TRD_SLS NUMBER(38,13),
	JJ_NET_TRD_SLS NUMBER(38,13),
	IN_TRANSIT_VAL NUMBER(38,13),
	TRGT_VAL NUMBER(36,11),
	INV_DT VARCHAR(52),
	WAREHSE_CD VARCHAR(50),
	END_STOCK_QTY NUMBER(20,4),
	END_STOCK_VAL_RAW NUMBER(36,9),
	END_STOCK_VAL NUMBER(38,13),
	IS_NPI VARCHAR(10),
	NPI_STR_PERIOD VARCHAR(10),
	NPI_END_PERIOD VARCHAR(1),
	IS_REG VARCHAR(1),
	IS_PROMO VARCHAR(40),
	PROMO_STRT_PERIOD VARCHAR(1),
	PROMO_END_PERIOD VARCHAR(1),
	IS_MCL VARCHAR(1),
	IS_HERO VARCHAR(10),
	CONTRIBUTION NUMBER(38,29)
);
CREATE TABLE IF NOT EXISTS EDW_MY_SIPOS_ANALYSIS (
	DATA_SRC VARCHAR(14),
	JJ_YEAR NUMBER(18,0),
	JJ_QTR VARCHAR(14),
	JJ_MNTH_ID VARCHAR(23),
	JJ_MNTH_NO NUMBER(18,0),
	JJ_YEAR_WK_NO VARCHAR(32),
	CNTRY_NM VARCHAR(11),
	CUST_CD VARCHAR(255),
	CUST_BRNCH_CD VARCHAR(300),
	MT_CUST_BRNCH_NM VARCHAR(300),
	CUST_DEPT_CD VARCHAR(10),
	MT_CUST_DEPT_NM VARCHAR(255),
	REGION_CD VARCHAR(255),
	REGION_NM VARCHAR(255),
	ITEM_CD VARCHAR(300),
	MT_ITEM_NM VARCHAR(500),
	SOLD_TO VARCHAR(15),
	SOLD_TO_NM VARCHAR(100),
	TRADE_TYPE VARCHAR(12),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	SAP_STATE_CD VARCHAR(150),
	SAP_SLS_ORG VARCHAR(4),
	SAP_CMP_ID VARCHAR(6),
	SAP_CNTRY_CD VARCHAR(3),
	SAP_CNTRY_NM VARCHAR(40),
	SAP_ADDR VARCHAR(150),
	SAP_REGION VARCHAR(150),
	SAP_CITY VARCHAR(150),
	SAP_POST_CD VARCHAR(150),
	SAP_CHNL_CD VARCHAR(2),
	SAP_CHNL_DESC VARCHAR(20),
	SAP_SLS_OFFICE_CD VARCHAR(4),
	SAP_SLS_OFFICE_DESC VARCHAR(40),
	SAP_SLS_GRP_CD VARCHAR(3),
	SAP_SLS_GRP_DESC VARCHAR(40),
	SAP_CURR_CD VARCHAR(5),
	GCH_REGION VARCHAR(50),
	GCH_CLUSTER VARCHAR(50),
	GCH_SUBCLUSTER VARCHAR(50),
	GCH_MARKET VARCHAR(50),
	GCH_RETAIL_BANNER VARCHAR(50),
	SKU VARCHAR(40),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	PUTUP_DESC VARCHAR(40),
	ITEM_DESC2 VARCHAR(200),
	SKU_DESC VARCHAR(100),
	SAP_MAT_TYPE_CD VARCHAR(10),
	SAP_MAT_TYPE_DESC VARCHAR(40),
	SAP_BASE_UOM_CD VARCHAR(10),
	SAP_PRCHSE_UOM_CD VARCHAR(10),
	SAP_PROD_SGMT_CD VARCHAR(18),
	SAP_PROD_SGMT_DESC VARCHAR(100),
	SAP_BASE_PROD_CD VARCHAR(10),
	SAP_BASE_PROD_DESC VARCHAR(100),
	SAP_MEGA_BRND_CD VARCHAR(10),
	SAP_MEGA_BRND_DESC VARCHAR(100),
	SAP_BRND_CD VARCHAR(10),
	SAP_BRND_DESC VARCHAR(100),
	SAP_VRNT_CD VARCHAR(10),
	SAP_VRNT_DESC VARCHAR(100),
	SAP_PUT_UP_CD VARCHAR(10),
	SAP_PUT_UP_DESC VARCHAR(100),
	SAP_GRP_FRNCHSE_CD VARCHAR(18),
	SAP_GRP_FRNCHSE_DESC VARCHAR(100),
	SAP_FRNCHSE_CD VARCHAR(18),
	SAP_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_FRNCHSE_CD VARCHAR(18),
	SAP_PROD_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_MJR_CD VARCHAR(18),
	SAP_PROD_MJR_DESC VARCHAR(100),
	SAP_PROD_MNR_CD VARCHAR(18),
	SAP_PROD_MNR_DESC VARCHAR(100),
	SAP_PROD_HIER_CD VARCHAR(18),
	SAP_PROD_HIER_DESC VARCHAR(100),
	GLOBAL_MAT_REGION VARCHAR(50),
	GLOBAL_PROD_FRANCHISE VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_PUT_UP_CD VARCHAR(10),
	GLOBAL_PUT_UP_DESC VARCHAR(100),
	GLOBAL_PROD_SUB_BRAND VARCHAR(100),
	GLOBAL_PROD_NEED_STATE VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SUBCATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_SUBSEGMENT VARCHAR(100),
	GLOBAL_PROD_SIZE VARCHAR(20),
	GLOBAL_PROD_SIZE_UOM VARCHAR(20),
	FROM_CCY VARCHAR(5),
	TO_CCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	BILL_TYPE VARCHAR(30),
	BILL_QTY_PC NUMBER(38,4),
	BILLING_GRS_TRD_SLS NUMBER(38,13),
	BILLING_SUBTOT2 NUMBER(38,13),
	BILLING_SUBTOT3 NUMBER(38,13),
	BILLING_SUBTOT4 NUMBER(38,13),
	BILLING_NET_AMT NUMBER(38,13),
	BILLING_EST_NTS NUMBER(38,13),
	BILLING_INVOICE_VAL NUMBER(38,13),
	BILLING_GROSS_VAL NUMBER(38,13),
	POS_QTY FLOAT,
	POS_GTS FLOAT,
	POS_ITEM_PRC FLOAT,
	POS_TAX FLOAT,
	POS_NTS FLOAT,
	CONV_FACTOR NUMBER(20,4),
	JJ_POS_QTY_PC NUMBER(22,6),
	JJ_POS_ITEM_PRC_PER_PC NUMBER(36,9),
	JJ_POS_GTS NUMBER(38,15),
	JJ_POS_VAT_AMT NUMBER(20,4),
	JJ_POS_NTS NUMBER(38,15),
	IS_NPI VARCHAR(10),
	NPI_STR_PERIOD VARCHAR(10),
	NPI_END_PERIOD VARCHAR(1),
	IS_REG VARCHAR(1),
	IS_PROMO VARCHAR(40),
	PROMO_STRT_PERIOD VARCHAR(1),
	PROMO_END_PERIOD VARCHAR(1),
	IS_MCL VARCHAR(1),
	IS_HERO VARCHAR(10)
);
CREATE TABLE IF NOT EXISTS EDW_MY_SISO_ANALYSIS (
	DATA_SRC VARCHAR(14),
	YEAR NUMBER(18,0),
	QRTR_NO NUMBER(18,0),
	MNTH_ID NUMBER(18,0),
	MNTH_NO NUMBER(18,0),
	MNTH_NM VARCHAR(9),
	MAX_YEARMO VARCHAR(28),
	CNTRY_NM VARCHAR(8),
	DSTRBTR_GRP_CD VARCHAR(20),
	DSTRBTR_GRP_NM VARCHAR(100),
	DSTRBTR_CUST_CD VARCHAR(50),
	DSTRBTR_CUST_NM VARCHAR(500),
	SAP_SOLDTO_CODE VARCHAR(10),
	SAP_SOLDTO_NM VARCHAR(100),
	DSTRBTR_LVL1 VARCHAR(40),
	DSTRBTR_LVL2 VARCHAR(40),
	DSTRBTR_LVL3 VARCHAR(40),
	DSTRBTR_LVL4 VARCHAR(40),
	DSTRBTR_LVL5 VARCHAR(40),
	REGION_NM VARCHAR(20),
	TOWN_NM VARCHAR(100),
	SLSMN_CD VARCHAR(150),
	CHNL_DESC VARCHAR(100),
	SUB_CHNL_DESC VARCHAR(100),
	CHNL_ATTR1_DESC VARCHAR(100),
	CHNL_ATTR2_DESC VARCHAR(100),
	SAP_STATE_CD VARCHAR(150),
	SAP_SLS_ORG VARCHAR(4),
	SAP_CMP_ID VARCHAR(6),
	SAP_CNTRY_CD VARCHAR(3),
	SAP_CNTRY_NM VARCHAR(40),
	SAP_ADDR VARCHAR(150),
	SAP_REGION VARCHAR(150),
	SAP_CITY VARCHAR(150),
	SAP_POST_CD VARCHAR(150),
	SAP_CHNL_CD VARCHAR(2),
	SAP_CHNL_DESC VARCHAR(20),
	SAP_SLS_OFFICE_CD VARCHAR(4),
	SAP_SLS_OFFICE_DESC VARCHAR(40),
	SAP_SLS_GRP_CD VARCHAR(3),
	SAP_SLS_GRP_DESC VARCHAR(40),
	SAP_CURR_CD VARCHAR(5),
	GCH_REGION VARCHAR(50),
	GCH_CLUSTER VARCHAR(50),
	GCH_SUBCLUSTER VARCHAR(50),
	GCH_MARKET VARCHAR(50),
	GCH_RETAIL_BANNER VARCHAR(50),
	SKU VARCHAR(40),
	FRNCHSE_DESC VARCHAR(100),
	BRND_DESC VARCHAR(100),
	VRNT_DESC VARCHAR(100),
	PUTUP_DESC VARCHAR(40),
	ITEM_DESC2 VARCHAR(200),
	SKU_DESC VARCHAR(100),
	SAP_MAT_TYPE_CD VARCHAR(10),
	SAP_MAT_TYPE_DESC VARCHAR(40),
	SAP_BASE_UOM_CD VARCHAR(10),
	SAP_PRCHSE_UOM_CD VARCHAR(10),
	SAP_PROD_SGMT_CD VARCHAR(18),
	SAP_PROD_SGMT_DESC VARCHAR(100),
	SAP_BASE_PROD_CD VARCHAR(10),
	SAP_BASE_PROD_DESC VARCHAR(100),
	SAP_MEGA_BRND_CD VARCHAR(10),
	SAP_MEGA_BRND_DESC VARCHAR(100),
	SAP_BRND_CD VARCHAR(10),
	SAP_BRND_DESC VARCHAR(100),
	SAP_VRNT_CD VARCHAR(10),
	SAP_VRNT_DESC VARCHAR(100),
	SAP_PUT_UP_CD VARCHAR(10),
	SAP_PUT_UP_DESC VARCHAR(100),
	SAP_GRP_FRNCHSE_CD VARCHAR(18),
	SAP_GRP_FRNCHSE_DESC VARCHAR(100),
	SAP_FRNCHSE_CD VARCHAR(18),
	SAP_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_FRNCHSE_CD VARCHAR(18),
	SAP_PROD_FRNCHSE_DESC VARCHAR(100),
	SAP_PROD_MJR_CD VARCHAR(18),
	SAP_PROD_MJR_DESC VARCHAR(100),
	SAP_PROD_MNR_CD VARCHAR(18),
	SAP_PROD_MNR_DESC VARCHAR(100),
	SAP_PROD_HIER_CD VARCHAR(18),
	SAP_PROD_HIER_DESC VARCHAR(100),
	GLOBAL_MAT_REGION VARCHAR(50),
	GLOBAL_PROD_FRANCHISE VARCHAR(30),
	GLOBAL_PROD_BRAND VARCHAR(30),
	GLOBAL_PROD_VARIANT VARCHAR(100),
	GLOBAL_PROD_PUT_UP_CD VARCHAR(10),
	GLOBAL_PUT_UP_DESC VARCHAR(100),
	GLOBAL_PROD_SUB_BRAND VARCHAR(100),
	GLOBAL_PROD_NEED_STATE VARCHAR(50),
	GLOBAL_PROD_CATEGORY VARCHAR(50),
	GLOBAL_PROD_SUBCATEGORY VARCHAR(50),
	GLOBAL_PROD_SEGMENT VARCHAR(50),
	GLOBAL_PROD_SUBSEGMENT VARCHAR(100),
	GLOBAL_PROD_SIZE VARCHAR(20),
	GLOBAL_PROD_SIZE_UOM VARCHAR(20),
	FROM_CCY VARCHAR(5),
	TO_CCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	WH_ID VARCHAR(50),
	DOC_TYPE VARCHAR(20),
	DOC_TYPE_DESC VARCHAR(20),
	BASE_SLS NUMBER(38,9),
	SLS_QTY NUMBER(38,6),
	RET_QTY NUMBER(38,6),
	UOM VARCHAR(20),
	SLS_QTY_PC NUMBER(38,6),
	RET_QTY_PC NUMBER(38,6),
	BILL_QTY_PC NUMBER(38,4),
	IN_TRANSIT_QTY NUMBER(38,4),
	MAT_LIST_PRICE NUMBER(38,4),
	GRS_TRD_SLS NUMBER(38,17),
	RET_VAL NUMBER(38,17),
	TRD_DISCNT NUMBER(38,11),
	TRD_SLS NUMBER(38,9),
	NET_TRD_SLS NUMBER(38,11),
	JJ_GRS_TRD_SLS NUMBER(38,17),
	JJ_RET_VAL NUMBER(38,17),
	JJ_TRD_SLS NUMBER(38,13),
	JJ_NET_TRD_SLS NUMBER(38,13),
	BILLING_GRS_TRD_SLS NUMBER(38,13),
	BILLING_SUBTOT2 NUMBER(38,13),
	BILLING_SUBTOT3 NUMBER(38,13),
	BILLING_SUBTOT4 NUMBER(38,13),
	BILLING_NET_AMT NUMBER(38,13),
	BILLING_EST_NTS NUMBER(38,13),
	BILLING_INVOICE_VAL NUMBER(38,13),
	BILLING_GROSS_VAL NUMBER(38,13),
	IN_TRANSIT_VAL NUMBER(38,13),
	TRGT_VAL NUMBER(36,11),
	WAREHSE_CD VARCHAR(50),
	END_STOCK_QTY NUMBER(38,4),
	END_STOCK_VAL_RAW NUMBER(38,9),
	END_STOCK_VAL NUMBER(38,13),
	IS_NPI VARCHAR(10),
	NPI_STR_PERIOD VARCHAR(10),
	NPI_END_PERIOD VARCHAR(1),
	IS_REG VARCHAR(1),
	IS_PROMO VARCHAR(40),
	PROMO_STRT_PERIOD VARCHAR(1),
	PROMO_END_PERIOD VARCHAR(1),
	IS_MCL VARCHAR(1),
	IS_HERO VARCHAR(10)
);
CREATE TABLE IF NOT EXISTS EDW_REG_INVENTORY_HEALTH_ANALYSIS_PROPAGATION (
	YEAR NUMBER(18,0),
	YEAR_QUARTER VARCHAR(14),
	MONTH_YEAR VARCHAR(23),
	MONTH_NUMBER NUMBER(18,0),
	COUNTRY_NAME VARCHAR(40),
	DISTRIBUTOR_ID VARCHAR(100),
	DISTRIBUTOR_ID_NAME VARCHAR(250),
	FRANCHISE VARCHAR(50),
	BRAND VARCHAR(50),
	PROD_SUB_BRAND VARCHAR(100),
	VARIANT VARCHAR(100),
	SEGMENT VARCHAR(50),
	PROD_SUBSEGMENT VARCHAR(100),
	PROD_CATEGORY VARCHAR(50),
	PROD_SUBCATEGORY VARCHAR(50),
	PUT_UP_DESCRIPTION VARCHAR(100),
	SKU_CD VARCHAR(255),
	SKU_DESCRIPTION VARCHAR(100),
	PKA_PRODUCT_KEY VARCHAR(200),
	PKA_PRODUCT_KEY_DESCRIPTION VARCHAR(500),
	PRODUCT_KEY VARCHAR(200),
	PRODUCT_KEY_DESCRIPTION VARCHAR(500),
	FROM_CCY VARCHAR(5),
	TO_CCY VARCHAR(5),
	EXCH_RATE NUMBER(15,5),
	SAP_PRNT_CUST_KEY VARCHAR(50),
	SAP_PRNT_CUST_DESC VARCHAR(382),
	SAP_CUST_CHNL_KEY VARCHAR(12),
	SAP_CUST_CHNL_DESC VARCHAR(50),
	SAP_CUST_SUB_CHNL_KEY VARCHAR(12),
	SAP_SUB_CHNL_DESC VARCHAR(50),
	SAP_GO_TO_MDL_KEY VARCHAR(12),
	SAP_GO_TO_MDL_DESC VARCHAR(50),
	SAP_BNR_KEY VARCHAR(12),
	SAP_BNR_DESC VARCHAR(50),
	SAP_BNR_FRMT_KEY VARCHAR(12),
	SAP_BNR_FRMT_DESC VARCHAR(50),
	RETAIL_ENV VARCHAR(50),
	REGION VARCHAR(255),
	ZONE_OR_AREA VARCHAR(255),
	SI_SLS_QTY NUMBER(38,5),
	SI_GTS_VAL NUMBER(38,5),
	SI_GTS_VAL_USD NUMBER(38,5),
	INVENTORY_QUANTITY NUMBER(38,5),
	INVENTORY_VAL NUMBER(38,5),
	INVENTORY_VAL_USD NUMBER(38,5),
	SO_SLS_QTY NUMBER(38,5),
	SO_GRS_TRD_SLS NUMBER(38,5),
	SO_GRS_TRD_SLS_USD FLOAT,
	SI_ALL_DB_VAL NUMBER(38,5),
	SI_ALL_DB_VAL_USD NUMBER(38,5),
	SI_INV_DB_VAL NUMBER(38,5),
	SI_INV_DB_VAL_USD NUMBER(38,5),
	LAST_3MONTHS_SO_QTY NUMBER(38,4),
	LAST_6MONTHS_SO_QTY NUMBER(38,4),
	LAST_12MONTHS_SO_QTY NUMBER(38,4),
	LAST_3MONTHS_SO_VAL NUMBER(38,4),
	LAST_3MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_6MONTHS_SO_VAL NUMBER(38,4),
	LAST_6MONTHS_SO_VAL_USD NUMBER(38,5),
	LAST_12MONTHS_SO_VAL NUMBER(38,4),
	LAST_12MONTHS_SO_VAL_USD NUMBER(38,5),
	PROPAGATE_FLAG VARCHAR(1),
	PROPAGATE_FROM NUMBER(18,0),
	REASON VARCHAR(100),
	LAST_36MONTHS_SO_VAL NUMBER(38,4),
	HEALTHY_INVENTORY VARCHAR(1),
	MIN_DATE DATE,
	DIFF_WEEKS NUMBER(18,0),
	L12M_WEEKS NUMBER(18,0),
	L6M_WEEKS NUMBER(18,0),
	L3M_WEEKS NUMBER(18,0),
	L12M_WEEKS_AVG_SALES NUMBER(38,17),
	L6M_WEEKS_AVG_SALES NUMBER(38,17),
	L3M_WEEKS_AVG_SALES NUMBER(38,17),
	L12M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L6M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L3M_WEEKS_AVG_SALES_USD NUMBER(38,5),
	L12M_WEEKS_AVG_SALES_QTY NUMBER(38,5),
	L6M_WEEKS_AVG_SALES_QTY NUMBER(38,5),
	L3M_WEEKS_AVG_SALES_QTY NUMBER(38,5)
);
CREATE TABLE IF NOT EXISTS EDW_SG_POS_ANALYSIS (
	SAP_CNTRY_CD VARCHAR(2),
	SAP_CNTRY_NM VARCHAR(9),
	JJ_YEAR NUMBER(18,0),
	JJ_QTR VARCHAR(14),
	JJ_MNTH_ID VARCHAR(23),
	JJ_MNTH_NO NUMBER(18,0),
	JJ_YR_WEEK_NO VARCHAR(20),
	CUST_CD VARCHAR(50),
	RETAIL_ENV VARCHAR(50),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	SAP_BNR_KEY VARCHAR(12),
	SAP_BNR_DESC VARCHAR(50),
	SAP_BNR_FRMT_KEY VARCHAR(12),
	SAP_BNR_FRMT_DESC VARCHAR(50),
	ITEM_CD VARCHAR(300),
	ITEM_DESC VARCHAR(500),
	SAP_MATL_NUM VARCHAR(255),
	SAP_MAT_DESC VARCHAR(100),
	BAR_CD VARCHAR(255),
	MASTER_CODE VARCHAR(255),
	CUST_BRNCH_CD VARCHAR(300),
	GPH_REGION VARCHAR(50),
	GPH_REG_FRNCHSE VARCHAR(100),
	GPH_REG_FRNCHSE_GRP VARCHAR(50),
	GPH_PROD_FRNCHSE VARCHAR(30),
	GPH_PROD_BRND VARCHAR(30),
	GPH_PROD_SUB_BRND VARCHAR(100),
	GPH_PROD_VRNT VARCHAR(100),
	GPH_PROD_NEEDSTATE VARCHAR(50),
	GPH_PROD_CTGRY VARCHAR(50),
	GPH_PROD_SUBCTGRY VARCHAR(50),
	GPH_PROD_SGMNT VARCHAR(50),
	GPH_PROD_SUBSGMNT VARCHAR(100),
	GPH_PROD_PUT_UP_CD VARCHAR(10),
	GPH_PROD_PUT_UP_DESC VARCHAR(100),
	GPH_PROD_SIZE VARCHAR(20),
	GPH_PROD_SIZE_UOM VARCHAR(20),
	CURRENCY VARCHAR(5),
	POS_QTY FLOAT,
	POS_GROSS_VAL FLOAT,
	POS_NET_VAL FLOAT,
	JJ_QTY_PC NUMBER(22,6),
	POS_GROSS_JJ_VAL NUMBER(38,15),
	POS_NET_JJ_VAL NUMBER(38,15)
);
CREATE TABLE IF NOT EXISTS EDW_SG_SELLIN_ANALYSIS (
	DATA_SOURCE_CD VARCHAR(5),
	DATA_SOURCE VARCHAR(18),
	SAP_CMP_ID VARCHAR(6),
	SAP_CNTRY_NM VARCHAR(40),
	YEAR NUMBER(18,0),
	QRTR VARCHAR(8),
	MNTH_ID VARCHAR(11),
	MNTH_NO NUMBER(18,0),
	GL_ACCT_NO NUMBER(18,0),
	GL_DESCRIPTION VARCHAR(500),
	MEASURE_BUCKET VARCHAR(500),
	CUST_L1 VARCHAR(7),
	SG_BANNER VARCHAR(255),
	SAP_BNR_FRMT_DESC VARCHAR(50),
	SAP_CUST_NM VARCHAR(100),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	RETAIL_ENV VARCHAR(50),
	SG_BRAND VARCHAR(255),
	GPH_REG_FRNCHSE_GRP VARCHAR(50),
	GPH_PROD_FRNCHSE VARCHAR(30),
	GPH_PROD_BRND VARCHAR(30),
	GPH_PROD_SUB_BRND VARCHAR(100),
	SAP_MATL_NUM VARCHAR(40),
	SAP_MAT_DESC VARCHAR(100),
	CURRENCY VARCHAR(5),
	BASE_VALUE FLOAT
);


---------------------CORE-----------------------------------
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_PS_TARGETS CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_PS_TARGETS;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_PS_WEIGHTS CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_PS_WEIGHTS;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_CUSTOMER_HIERARCHY CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_CUSTOMER_HIERARCHY;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_PRODUCT_HIERARCHY CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_PRODUCT_HIERARCHY;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_PRODUCT_MAPPING CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_PRODUCT_MAPPING;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_MDS_SG_STORE_MASTER CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_SG_STORE_MASTER;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_SG_TP_CLOSED_YEAR_BAL CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_SG_TP_CLOSED_YEAR_BAL;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_SG_CONSTANT_KEY_VALUE CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_SG_CONSTANT_KEY_VALUE;
CREATE TABLE IF NOT EXISTS PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_SG_POS_SALES_FACT CLONE PROD_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_SG_POS_SALES_FACT;

create or replace TABLE MYSITG_INTEGRATION.ITG_MY_SELLOUT_STOCK_FACT (
	CUST_ID VARCHAR(50),
	INV_DT DATE,
	DSTRBTR_WH_ID VARCHAR(50),
	ITEM_CD VARCHAR(50),
	DSTRBTR_PROD_CD VARCHAR(50),
	EAN_NUM VARCHAR(50),
	DSTRBTR_PROD_DESC VARCHAR(100),
	QTY NUMBER(20,4),
	UOM VARCHAR(20),
	QTY_ON_ORD NUMBER(20,4),
	UOM_ON_ORD VARCHAR(100),
	QTY_COMMITTED NUMBER(20,4),
	UOM_COMMITTED VARCHAR(100),
	AVAILABLE_QTY_PC NUMBER(20,4),
	QTY_ON_ORD_PC NUMBER(20,4),
	QTY_COMMITTED_PC NUMBER(20,4),
	UNIT_PRC NUMBER(20,4),
	TOTAL_VAL NUMBER(20,4),
	CUSTOM_FIELD1 VARCHAR(255),
	SAP_MATL_NUM VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE MYSITG_INTEGRATION.ITG_MY_SELLOUT_SALES_FACT (
	DSTRBTR_ID VARCHAR(50),
	SLS_ORD_NUM VARCHAR(50),
	SLS_ORD_DT DATE,
	TYPE VARCHAR(20),
	CUST_CD VARCHAR(50),
	DSTRBTR_WH_ID VARCHAR(50),
	ITEM_CD VARCHAR(50),
	DSTRBTR_PROD_CD VARCHAR(50),
	EAN_NUM VARCHAR(50),
	DSTRBTR_PROD_DESC VARCHAR(100),
	GRS_PRC NUMBER(20,4),
	QTY NUMBER(20,4),
	UOM VARCHAR(20),
	QTY_PC NUMBER(20,4),
	QTY_AFT_CONV NUMBER(20,4),
	SUBTOTAL_1 NUMBER(20,4),
	DISCOUNT NUMBER(20,4),
	SUBTOTAL_2 NUMBER(20,4),
	BOTTOM_LINE_DSCNT NUMBER(20,4),
	TOTAL_AMT_AFT_TAX NUMBER(20,4),
	TOTAL_AMT_BFR_TAX NUMBER(20,4),
	SLS_EMP VARCHAR(100),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	SAP_MATL_NUM VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE MYSITG_INTEGRATION.ITG_MY_SELLOUT_SALES_FACT (
	DSTRBTR_ID VARCHAR(50),
	SLS_ORD_NUM VARCHAR(50),
	SLS_ORD_DT DATE,
	TYPE VARCHAR(20),
	CUST_CD VARCHAR(50),
	DSTRBTR_WH_ID VARCHAR(50),
	ITEM_CD VARCHAR(50),
	DSTRBTR_PROD_CD VARCHAR(50),
	EAN_NUM VARCHAR(50),
	DSTRBTR_PROD_DESC VARCHAR(100),
	GRS_PRC NUMBER(20,4),
	QTY NUMBER(20,4),
	UOM VARCHAR(20),
	QTY_PC NUMBER(20,4),
	QTY_AFT_CONV NUMBER(20,4),
	SUBTOTAL_1 NUMBER(20,4),
	DISCOUNT NUMBER(20,4),
	SUBTOTAL_2 NUMBER(20,4),
	BOTTOM_LINE_DSCNT NUMBER(20,4),
	TOTAL_AMT_AFT_TAX NUMBER(20,4),
	TOTAL_AMT_BFR_TAX NUMBER(20,4),
	SLS_EMP VARCHAR(100),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	SAP_MATL_NUM VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE MYSWKS_INTEGRATION.WKS_MY_SELLOUT_STOCK_FACT (
	CUST_ID VARCHAR(255),
	INV_DT VARCHAR(255),
	DSTRBTR_WH_ID VARCHAR(255),
	ITEM_CD VARCHAR(255),
	DSTRBTR_PROD_CD VARCHAR(255),
	EAN_NUM VARCHAR(255),
	DSTRBTR_PROD_DESC VARCHAR(255),
	QTY VARCHAR(255),
	UOM VARCHAR(255),
	QTY_ON_ORD VARCHAR(255),
	UOM_ON_ORD VARCHAR(255),
	QTY_COMMITTED VARCHAR(255),
	UOM_COMMITTED VARCHAR(255),
	AVAILABLE_QTY_PC VARCHAR(255),
	QTY_ON_ORD_PC VARCHAR(255),
	QTY_COMMITTED_PC VARCHAR(255),
	UNIT_PRC VARCHAR(255),
	TOTAL_VAL VARCHAR(255),
	CUSTOM_FIELD1 VARCHAR(255),
	CUSTOM_FIELD2 VARCHAR(255),
	FILENAME VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9)
);

create or replace TABLE ASPITG_INTEGRATION.ITG_RG_TRAVEL_RETAIL_SELLOUT (
	CTRY_CD VARCHAR(2),
	CRNCY_CD VARCHAR(3),
	RETAILER_NAME VARCHAR(50),
	YEAR NUMBER(18,0),
	MONTH NUMBER(18,0),
	YEAR_MONTH VARCHAR(10),
	BRAND VARCHAR(50),
	SKU VARCHAR(50),
	PRODUCT_DESCRIPTION VARCHAR(100),
	DCL_CODE VARCHAR(50),
	EAN VARCHAR(50),
	RSP NUMBER(18,0),
	DOOR_NAME VARCHAR(50),
	SLS_QTY NUMBER(18,0),
	SLS_AMT NUMBER(21,5),
	STOCK_QTY NUMBER(18,0),
	STOCK_AMT NUMBER(21,5),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	STORE_SLS_QTY NUMBER(18,0),
	STORE_SLS_AMT NUMBER(38,18),
	ECOMMERCE_SLS_QTY NUMBER(18,0),
	ECOMMERCE_SLS_AMT NUMBER(38,18),
	MEMBERSHIP_SLS_QTY NUMBER(18,0),
	MEMBERSHIP_SLS_AMT NUMBER(38,18)
);

CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SAP_BILLING_CONDITION (		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sap_billing_condition (
    bill_num varchar(10),		--//  ENCODE lzo // character varying
    bill_item varchar(6),		--//  ENCODE lzo // character varying
    zstepnum varchar(3),		--//  ENCODE lzo // character varying
    kncounter varchar(2),		--//  ENCODE lzo // character varying
    doc_number varchar(10),		--//  ENCODE lzo // character varying
    s_ord_item varchar(6),		--//  ENCODE lzo // character varying
    knart varchar(4),		--//  ENCODE lzo // character varying
    ch_on varchar(8),		--//  ENCODE lzo // character varying
    comp_code varchar(4),		--//  ENCODE lzo // character varying
    sales_dist varchar(6),		--//  ENCODE lzo // character varying
    bill_type varchar(4),		--//  ENCODE lzo // character varying
    bill_date varchar(8),		--//  ENCODE lzo // character varying
    bill_cat varchar(1),		--//  ENCODE lzo // character varying
    loc_currcy varchar(5),		--//  ENCODE lzo // character varying
    cust_group varchar(2),		--//  ENCODE lzo // character varying
    sold_to varchar(10),		--//  ENCODE lzo // character varying
    payer varchar(10),		--//  ENCODE lzo // character varying
    exrate_acc varchar(17),		--//  ENCODE lzo // character varying
    stat_curr varchar(5),		--//  ENCODE lzo // character varying
    doc_categ varchar(2),		--//  ENCODE lzo // character varying
    salesorg varchar(4),		--//  ENCODE lzo // character varying
    distr_chan varchar(2),		--//  ENCODE lzo // character varying
    doc_currcy varchar(5),		--//  ENCODE lzo // character varying
    createdon varchar(8),		--//  ENCODE lzo // character varying
    co_area varchar(4),		--//  ENCODE lzo // character varying
    costcenter varchar(10),		--//  ENCODE lzo // character varying
    trans_date varchar(8),		--//  ENCODE lzo // character varying
    exchg_rate varchar(25),		--//  ENCODE lzo // character varying
    cust_grp1 varchar(3),		--//  ENCODE lzo // character varying
    cust_grp2 varchar(3),		--//  ENCODE lzo // character varying
    cust_grp3 varchar(3),		--//  ENCODE lzo // character varying
    cust_grp4 varchar(3),		--//  ENCODE lzo // character varying
    cust_grp5 varchar(3),		--//  ENCODE lzo // character varying
    matl_group varchar(9),		--//  ENCODE lzo // character varying
    material varchar(18),		--//  ENCODE lzo // character varying
    mat_entrd varchar(18),		--//  ENCODE lzo // character varying
    matl_grp_1 varchar(3),		--//  ENCODE lzo // character varying
    matl_grp_2 varchar(3),		--//  ENCODE lzo // character varying
    matl_grp_3 varchar(3),		--//  ENCODE lzo // character varying
    matl_grp_4 varchar(3),		--//  ENCODE lzo // character varying
    matl_grp_5 varchar(3),		--//  ENCODE lzo // character varying
    billtoprty varchar(10),		--//  ENCODE lzo // character varying
    ship_to varchar(10),		--//  ENCODE lzo // character varying
    itm_type varchar(1),		--//  ENCODE lzo // character varying
    prod_hier varchar(18),		--//  ENCODE lzo // character varying
    prov_group varchar(2),		--//  ENCODE lzo // character varying
    price_date varchar(8),		--//  ENCODE lzo // character varying
    item_categ varchar(4),		--//  ENCODE lzo // character varying
    div_head varchar(2),		--//  ENCODE lzo // character varying
    division varchar(2),		--//  ENCODE lzo // character varying
    stat_date varchar(8),		--//  ENCODE lzo // character varying
    refer_doc varchar(10),		--//  ENCODE lzo // character varying
    refer_itm varchar(6),		--//  ENCODE lzo // character varying
    sales_off varchar(4),		--//  ENCODE lzo // character varying
    sales_grp varchar(3),		--//  ENCODE lzo // character varying
    wbs_elemt varchar(24),		--//  ENCODE lzo // character varying
    calday varchar(8),		--//  ENCODE lzo // character varying
    calmonth varchar(6),		--//  ENCODE lzo // character varying
    calweek varchar(6),		--//  ENCODE lzo // character varying
    fiscper varchar(7),		--//  ENCODE lzo // character varying
    fiscvarnt varchar(2),		--//  ENCODE lzo // character varying
    knclass varchar(4),		--//  ENCODE lzo // character varying
    knorigin varchar(4),		--//  ENCODE lzo // character varying
    kntyp varchar(4),		--//  ENCODE lzo // character varying
    knval numeric(17,3),		--//  ENCODE lzo
    kprice numeric(17,3),		--//  ENCODE lzo
    kinak varchar(1),		--//  ENCODE lzo // character varying
    kstat varchar(1),		--//  ENCODE lzo // character varying
    storno varchar(1),		--//  ENCODE lzo // character varying
    rt_promo varchar(10),		--//  ENCODE lzo // character varying
    rebate_grp varchar(2),		--//  ENCODE lzo // character varying
    bwapplnm varchar(30),		--//  ENCODE lzo // character varying
    processkey varchar(3),		--//  ENCODE lzo // character varying
    eanupc varchar(18),		--//  ENCODE lzo // character varying
    createdby varchar(12),		--//  ENCODE lzo // character varying
    serv_date varchar(8),		--//  ENCODE lzo // character varying
    inv_qty numeric(17,3),		--//  ENCODE lzo
    actual_quantity_pc numeric(17,3),		--//  ENCODE lzo
    forwagent varchar(10),		--//  ENCODE lzo // character varying
    salesemply varchar(8),		--//  ENCODE lzo // character varying
    sales_unit varchar(3),		--//  ENCODE lzo // character varying
    kappl varchar(2),		--//  ENCODE lzo // character varying
    acrn_id varchar(2),		--//  ENCODE lzo // character varying
    recordmode varchar(1),		--//  ENCODE lzo // character varying
    crt_dttm timestamp without time zone,		--//  ENCODE lzo
    source_file_name varchar(255)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE EVEN;

--DROP TABLE ASPEDW_INTEGRATION.edw_calendar_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_CALENDAR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_calendar_dim
(
	cal_day DATE
	,fisc_yr_vrnt VARCHAR(2)
	,wkday numeric(18,0)		--// INTEGER
	,cal_wk numeric(18,0)		--// INTEGER
	,cal_mo_1 numeric(18,0)		--// INTEGER
	,cal_mo_2 numeric(18,0)		--// INTEGER
	,cal_qtr_1 numeric(18,0)		--// INTEGER
	,cal_qtr_2 numeric(18,0)		--// INTEGER
	,half_yr numeric(18,0)		--// INTEGER
	,cal_yr numeric(18,0)		--// INTEGER
	,fisc_per numeric(18,0)		--// INTEGER
	,pstng_per numeric(18,0)		--// INTEGER
	,fisc_yr numeric(18,0)		--// INTEGER
	,rec_mode VARCHAR(1)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_code_descriptions;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_CODE_DESCRIPTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_code_descriptions
(
	source_type VARCHAR(10) NOT NULL 		--//  ENCODE lzo
	,code_type VARCHAR(50)  		--//  ENCODE lzo
	,code VARCHAR(15)  		--//  ENCODE lzo
	,code_desc VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_company_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_COMPANY_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_company_dim
(
	clnt VARCHAR(3)
	,co_cd VARCHAR(4) NOT NULL
	,ctry_key VARCHAR(3)
	,ctry_nm VARCHAR(40)
	,crncy_key VARCHAR(5)
	,chrt_acct VARCHAR(4)
	,crdt_cntrl_area VARCHAR(4)
	,fisc_yr_vrnt VARCHAR(2)
	,company VARCHAR(6)
	,company_nm VARCHAR(100)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,ctry_group VARCHAR(40)  		--//  ENCODE lzo
	,cluster VARCHAR(100)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_crncy_exch;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_CRNCY_EXCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_crncy_exch
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,ex_rt_typ VARCHAR(4) NOT NULL
	,from_crncy VARCHAR(5) NOT NULL 		--//  ENCODE bytedict
	,to_crncy VARCHAR(5) NOT NULL 		--//  ENCODE bytedict
	,vld_from VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,ex_rt NUMERIC(9,5)  		--//  ENCODE zstd
	,from_ratio NUMERIC(9,0)  		--//  ENCODE zstd
	,to_ratio NUMERIC(9,0)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,PRIMARY KEY (clnt, ex_rt_typ, from_crncy, to_crncy, vld_from)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPEDW_INTEGRATION.edw_customer_base_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_CUSTOMER_BASE_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_customer_base_dim
(
	clnt VARCHAR(150)  		--//  ENCODE zstd
	,cust_num VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,cust_nm VARCHAR(100)  		--//  ENCODE zstd
	,addr VARCHAR(150)  		--//  ENCODE zstd
	,title VARCHAR(150)  		--//  ENCODE zstd
	,cntrl_ordr_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,exp_train_stn VARCHAR(150)  		--//  ENCODE zstd
	,train_stn VARCHAR(150)  		--//  ENCODE zstd
	,intnl_loc_num1 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,intnl_loc_num2 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,auth_grp VARCHAR(150)  		--//  ENCODE zstd
	,indstr_key VARCHAR(150)  		--//  ENCODE zstd
	,chk_dig_intnl_loc_num numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,data_comm_line_no VARCHAR(150)  		--//  ENCODE zstd
	,dt_on_rec_crt DATE  		--//  ENCODE zstd
	,nm_prsn_crt_obj VARCHAR(150)  		--//  ENCODE zstd
	,unld_pt_exist_ind VARCHAR(150)  		--//  ENCODE zstd
	,cent_bill_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_mstr_rec_fisc_addr VARCHAR(150)  		--//  ENCODE zstd
	,wrk_num_cal VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_alt_pyr VARCHAR(150)  		--//  ENCODE zstd
	,grp_key VARCHAR(150)  		--//  ENCODE zstd
	,cust_acct_grp VARCHAR(150)  		--//  ENCODE zstd
	,cust_clsn VARCHAR(150)  		--//  ENCODE zstd
	,ctry_key VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_vend VARCHAR(150)  		--//  ENCODE zstd
	,cent_delv_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,city_coordnt VARCHAR(150)  		--//  ENCODE zstd
	,cent_del_fl_mstr_rec VARCHAR(150)  		--//  ENCODE zstd
	,nm_1 VARCHAR(150)  		--//  ENCODE zstd
	,nm_2 VARCHAR(150)  		--//  ENCODE zstd
	,nm_3 VARCHAR(150)  		--//  ENCODE zstd
	,nm_4 VARCHAR(150)  		--//  ENCODE zstd
	,nlsn_id VARCHAR(150)  		--//  ENCODE zstd
	,city VARCHAR(150)  		--//  ENCODE zstd
	,dstrc VARCHAR(150)  		--//  ENCODE zstd
	,po_box VARCHAR(150)  		--//  ENCODE zstd
	,po_box_pstl_cd VARCHAR(150)  		--//  ENCODE zstd
	,pstl_cd VARCHAR(150)  		--//  ENCODE zstd
	,rgn VARCHAR(150)  		--//  ENCODE zstd
	,cnty_cd VARCHAR(150)  		--//  ENCODE zstd
	,city_cd VARCHAR(150)  		--//  ENCODE zstd
	,fcst_chnl VARCHAR(150)  		--//  ENCODE zstd
	,srt_fld VARCHAR(150)  		--//  ENCODE zstd
	,cent_pstng_blk VARCHAR(150)  		--//  ENCODE zstd
	,lang_key VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_1 VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_2 VARCHAR(150)  		--//  ENCODE zstd
	,busn_ptnr_subj_eqlztn_tax_ind VARCHAR(150)  		--//  ENCODE zstd
	,liab_for_vat VARCHAR(150)  		--//  ENCODE zstd
	,hs_num_and_street VARCHAR(150)  		--//  ENCODE zstd
	,telebox_num VARCHAR(150)  		--//  ENCODE zstd
	,fst_phn_num VARCHAR(150)  		--//  ENCODE zstd
	,sec_phn_num VARCHAR(150)  		--//  ENCODE zstd
	,fax_num VARCHAR(150)  		--//  ENCODE zstd
	,teletex_num VARCHAR(150)  		--//  ENCODE zstd
	,telex_num VARCHAR(150)  		--//  ENCODE zstd
	,trspn_zn_goods_delv VARCHAR(150)  		--//  ENCODE zstd
	,is_acct_one_time_acct_ind VARCHAR(150)  		--//  ENCODE zstd
	,alt_payee_doc_allw_ind VARCHAR(150)  		--//  ENCODE zstd
	,co_id_trad_ptnr VARCHAR(150)  		--//  ENCODE zstd
	,vat_regs_num VARCHAR(150)  		--//  ENCODE zstd
	,cmpt_ind VARCHAR(150)  		--//  ENCODE zstd
	,sls_ptnr_ind VARCHAR(150)  		--//  ENCODE zstd
	,sls_prosp_ind VARCHAR(150)  		--//  ENCODE zstd
	,for_cust_type4_ind VARCHAR(150)  		--//  ENCODE zstd
	,id_dflt_sold_to_prty VARCHAR(150)  		--//  ENCODE zstd
	,in_cnsmr VARCHAR(150)  		--//  ENCODE zstd
	,legal_sts VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_1 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_2 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_3 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_4 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_5 VARCHAR(150)  		--//  ENCODE zstd
	,init_cntct VARCHAR(150)  		--//  ENCODE zstd
	,annl_sls_1 VARCHAR(150)  		--//  ENCODE zstd
	,yr_for_sls_gvn numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,crncy_sls_fig VARCHAR(150)  		--//  ENCODE zstd
	,yr_num_emp numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,yr_for_num_emp_gvn numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,attr_1 VARCHAR(150)  		--//  ENCODE zstd
	,attr_2 VARCHAR(150)  		--//  ENCODE zstd
	,attr_3 VARCHAR(150)  		--//  ENCODE zstd
	,attr_4 VARCHAR(150)  		--//  ENCODE zstd
	,attr_5 VARCHAR(150)  		--//  ENCODE zstd
	,attr_6 VARCHAR(150)  		--//  ENCODE zstd
	,attr_7 VARCHAR(150)  		--//  ENCODE zstd
	,attr_8 VARCHAR(150)  		--//  ENCODE zstd
	,attr_9 VARCHAR(150)  		--//  ENCODE zstd
	,attr_10 VARCHAR(150)  		--//  ENCODE zstd
	,ntrl_prsn VARCHAR(150)  		--//  ENCODE zstd
	,annl_sls_2 VARCHAR(150)  		--//  ENCODE zstd
	,tax_juris VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch1 VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch2 VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch3 VARCHAR(150)  		--//  ENCODE zstd
	,fisc_yr_vrnt VARCHAR(150)  		--//  ENCODE zstd
	,usg_ind VARCHAR(150)  		--//  ENCODE zstd
	,insp_carr_out_by_cust VARCHAR(150)  		--//  ENCODE zstd
	,insp_delv_note_after_outb_delv VARCHAR(150)  		--//  ENCODE zstd
	,ref_acct_grp_one_time_acct VARCHAR(150)  		--//  ENCODE zstd
	,po_box_city VARCHAR(150)  		--//  ENCODE zstd
	,plnt VARCHAR(150)  		--//  ENCODE zstd
	,data_med_exch_ind VARCHAR(150)  		--//  ENCODE zstd
	,instr_key_data_med_exch VARCHAR(150)  		--//  ENCODE zstd
	,sts_data_tfr_subsq_rlse VARCHAR(150)  		--//  ENCODE zstd
	,asgnmt_hier numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,pmt_blk VARCHAR(150)  		--//  ENCODE zstd
	,cust_grp VARCHAR(150)  		--//  ENCODE zstd
	,id_mn_non_mil_use VARCHAR(150)  		--//  ENCODE zstd
	,id_mn_mil_use VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_1 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_2 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_3 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_4 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_5 VARCHAR(150)  		--//  ENCODE zstd
	,alt_pyr_acct_num_ind VARCHAR(150)  		--//  ENCODE zstd
	,tax_type VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_type VARCHAR(150)  		--//  ENCODE zstd
	,tax_num3 VARCHAR(150)  		--//  ENCODE zstd
	,tax_num4 VARCHAR(150)  		--//  ENCODE zstd
	,cust_is_icms_expt VARCHAR(150)  		--//  ENCODE zstd
	,cust_is_ipi_expt VARCHAR(150)  		--//  ENCODE zstd
	,cust_grp_tax_subst VARCHAR(150)  		--//  ENCODE zstd
	,cust_cfop_cat VARCHAR(150)  		--//  ENCODE zstd
	,tax_law_icms VARCHAR(150)  		--//  ENCODE zstd
	,tax_law_ipi VARCHAR(150)  		--//  ENCODE zstd
	,biochem_wf_ind VARCHAR(150)  		--//  ENCODE zstd
	,nuclr_np_ind VARCHAR(150)  		--//  ENCODE zstd
	,natl_scty_ind VARCHAR(150)  		--//  ENCODE zstd
	,missile_tech_ind VARCHAR(150)  		--//  ENCODE zstd
	,cent_sls_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,unifm_rsrs_lctr VARCHAR(132)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,PRIMARY KEY (cust_num)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPEDW_INTEGRATION.edw_customer_sales_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_CUSTOMER_SALES_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_customer_sales_dim
(
	clnt VARCHAR(3)  		--//  ENCODE zstd
	,cust_num VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sls_org VARCHAR(4) NOT NULL 		--//  ENCODE bytedict
	,dstr_chnl VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,div VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,obj_crt_prsn VARCHAR(12)  		--//  ENCODE bytedict
	,rec_crt_dt DATE  		--//  ENCODE zstd
	,auth_grp VARCHAR(4)  		--//  ENCODE zstd
	,cust_del_flag VARCHAR(1)  		--//  ENCODE zstd
	,cust_stat_grp VARCHAR(1)  		--//  ENCODE zstd
	,cust_ord_blk VARCHAR(2)  		--//  ENCODE zstd
	,prc_pcdr_asgn VARCHAR(1)  		--//  ENCODE zstd
	,cust_grp VARCHAR(2)  		--//  ENCODE zstd
	,sls_dstrc VARCHAR(6)  		--//  ENCODE zstd
	,prc_grp VARCHAR(2)  		--//  ENCODE zstd
	,prc_list_typ VARCHAR(2)  		--//  ENCODE zstd
	,ord_prob_itm numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,incoterm1 VARCHAR(3)  		--//  ENCODE zstd
	,incoterm2 VARCHAR(28)  		--//  ENCODE zstd
	,cust_delv_blk VARCHAR(2)  		--//  ENCODE zstd
	,cmplt_delv_sls_ord VARCHAR(1)  		--//  ENCODE zstd
	,max_no_prtl_delv_allw_itm NUMERIC(1,0)  		--//  ENCODE zstd
	,prtl_delv_itm_lvl VARCHAR(1)  		--//  ENCODE zstd
	,ord_comb_in VARCHAR(1)  		--//  ENCODE zstd
	,btch_splt_allw VARCHAR(1)  		--//  ENCODE zstd
	,delv_prir numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,vend_acct_num VARCHAR(12)  		--//  ENCODE zstd
	,shipping_cond VARCHAR(2)  		--//  ENCODE zstd
	,bill_blk_cust VARCHAR(2)  		--//  ENCODE zstd
	,man_invc_maint VARCHAR(1)  		--//  ENCODE zstd
	,invc_dt VARCHAR(2)  		--//  ENCODE zstd
	,invc_list_sched VARCHAR(2)  		--//  ENCODE zstd
	,cost_est_in VARCHAR(1)  		--//  ENCODE zstd
	,val_lmt_cost_est NUMERIC(13,2)  		--//  ENCODE zstd
	,crncy_key VARCHAR(5)  		--//  ENCODE zstd
	,cust_clas VARCHAR(2)  		--//  ENCODE zstd
	,acct_asgnmt_grp VARCHAR(2)  		--//  ENCODE zstd
	,delv_plnt VARCHAR(4)  		--//  ENCODE bytedict
	,sls_grp VARCHAR(3)  		--//  ENCODE zstd
	,sls_grp_desc VARCHAR(40)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(4)  		--//  ENCODE bytedict
	,sls_ofc_desc VARCHAR(40)  		--//  ENCODE zstd
	,itm_props VARCHAR(10)  		--//  ENCODE zstd
	,cust_grp1 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp2 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp3 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp4 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp5 VARCHAR(3)  		--//  ENCODE zstd
	,cust_rebt_in VARCHAR(1)  		--//  ENCODE zstd
	,rebt_indx_cust_strt_prd DATE  		--//  ENCODE zstd
	,exch_rt_typ VARCHAR(4)  		--//  ENCODE zstd
	,prc_dtrmn_id VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id1 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id2 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id3 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id4 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id5 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id6 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id7 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id8 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id9 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id10 VARCHAR(1)  		--//  ENCODE zstd
	,pymt_key_term VARCHAR(4)  		--//  ENCODE bytedict
	,persnl_num NUMERIC(8,0)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,cur_sls_emp NUMERIC(8,0)  		--//  ENCODE zstd
	,lcl_cust_grp_1 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_2 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_3 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_4 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_5 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_6 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_7 VARCHAR(10)  		--//  ENCODE zstd
	,lcl_cust_grp_8 VARCHAR(10)  		--//  ENCODE zstd
	,prc_proc VARCHAR(1)  		--//  ENCODE zstd
	,par_del VARCHAR(1)  		--//  ENCODE zstd
	,max_num_pa VARCHAR(1)  		--//  ENCODE zstd
	,prnt_cust_key VARCHAR(12)  		--//  ENCODE zstd
	,bnr_key VARCHAR(12)  		--//  ENCODE zstd
	,bnr_frmt_key VARCHAR(12)  		--//  ENCODE zstd
	,go_to_mdl_key VARCHAR(12)  		--//  ENCODE zstd
	,chnl_key VARCHAR(12)  		--//  ENCODE zstd
	,sub_chnl_key VARCHAR(12)  		--//  ENCODE zstd
	,segmt_key VARCHAR(12)  		--//  ENCODE zstd
	,cust_set_1 VARCHAR(12)  		--//  ENCODE zstd
	,cust_set_2 VARCHAR(12)  		--//  ENCODE zstd
	,cust_set_3 VARCHAR(12)  		--//  ENCODE zstd
	,cust_set_4 VARCHAR(12)  		--//  ENCODE zstd
	,cust_set_5 VARCHAR(12)  		--//  ENCODE zstd
	,PRIMARY KEY (cust_num, sls_org, dstr_chnl, div)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPEDW_INTEGRATION.edw_dstrbtn_chnl;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_DSTRBTN_CHNL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_dstrbtn_chnl
(
	distr_chan VARCHAR(2) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtsh VARCHAR(20)  		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (distr_chan, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_ecc_marm;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_ECC_MARM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_ecc_marm
(
	matl_no VARCHAR(18) NOT NULL 		--//  ENCODE bytedict
	,alt_unt VARCHAR(3) NOT NULL 		--//  ENCODE bytedict
	,numrtr NUMERIC(5,0)  		--//  ENCODE lzo
	,denomtr NUMERIC(5,0)  		--//  ENCODE lzo
	,ean_nr VARCHAR(13)  		--//  ENCODE bytedict
	,ean_11 VARCHAR(18)  		--//  ENCODE bytedict
	,ctgry VARCHAR(2)  		--//  ENCODE lzo
	,length NUMERIC(13,3)  		--//  ENCODE lzo
	,width NUMERIC(13,3)  		--//  ENCODE lzo
	,height NUMERIC(13,3)  		--//  ENCODE lzo
	,unit VARCHAR(3)  		--//  ENCODE lzo
	,volum VARCHAR(14)  		--//  ENCODE bytedict
	,vol_unt VARCHAR(3)  		--//  ENCODE bytedict
	,gross_wt NUMERIC(13,3)  		--//  ENCODE bytedict
	,wt_unt VARCHAR(3)  		--//  ENCODE lzo
	,lowrlvl_unt VARCHAR(3)  		--//  ENCODE lzo
	,intrnl_char NUMERIC(10,0)  		--//  ENCODE lzo
	,uom_srtno NUMERIC(2,0)  		--//  ENCODE lzo
	,leadng_unt VARCHAR(1)  		--//  ENCODE lzo
	,valutn_unt VARCHAR(1)  		--//  ENCODE lzo
	,unts_use VARCHAR(1)  		--//  ENCODE lzo
	,uom VARCHAR(3)  		--//  ENCODE bytedict
	,lg_vrnt VARCHAR(1)  		--//  ENCODE lzo
	,ean_vrnt VARCHAR(2)  		--//  ENCODE lzo
	,remng_vol NUMERIC(3,0)  		--//  ENCODE bytedict
	,max_stack NUMERIC(3,0)  		--//  ENCODE lzo
	,capause NUMERIC(15,0)  		--//  ENCODE lzo
	,uom_ctgry VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE runlength
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (matl_no, alt_unt)
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPEDW_INTEGRATION.edw_ecc_standard_cost;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_ECC_STANDARD_COST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_ecc_standard_cost
(
	mandt VARCHAR(3) NOT NULL 		--//  ENCODE lzo
	,matnr VARCHAR(18) NOT NULL 		--//  ENCODE lzo
	,bwkey VARCHAR(4) NOT NULL 		--//  ENCODE lzo
	,bwtar VARCHAR(10) NOT NULL 		--//  ENCODE lzo
	,lvorm VARCHAR(1)  		--//  ENCODE lzo
	,lbkum NUMERIC(13,3)  		--//  ENCODE delta
	,salk3 NUMERIC(13,2)  		--//  ENCODE delta
	,vprsv VARCHAR(1)  		--//  ENCODE lzo
	,verpr NUMERIC(11,2)  		--//  ENCODE delta
	,stprs NUMERIC(11,2)  		--//  ENCODE delta
	,peinh NUMERIC(5,0)  		--//  ENCODE delta
	,bklas VARCHAR(4)  		--//  ENCODE lzo
	,salkv NUMERIC(13,2)  		--//  ENCODE delta
	,vmkum NUMERIC(13,3)  		--//  ENCODE delta
	,vmsal NUMERIC(13,2)  		--//  ENCODE delta
	,vmvpr VARCHAR(1)  		--//  ENCODE lzo
	,vmver NUMERIC(11,2)  		--//  ENCODE delta
	,vmstp NUMERIC(11,2)  		--//  ENCODE delta
	,vmpei NUMERIC(5,0)  		--//  ENCODE lzo
	,vmbkl VARCHAR(4)  		--//  ENCODE lzo
	,vmsav NUMERIC(13,2)  		--//  ENCODE delta
	,vjkum NUMERIC(13,3)  		--//  ENCODE delta
	,vjsal NUMERIC(13,2)  		--//  ENCODE delta
	,vjvpr VARCHAR(1)  		--//  ENCODE lzo
	,vjver NUMERIC(11,2)  		--//  ENCODE delta
	,vjstp NUMERIC(11,2)  		--//  ENCODE delta
	,vjpei NUMERIC(5,0)  		--//  ENCODE delta
	,vjbkl VARCHAR(4)  		--//  ENCODE lzo
	,vjsav NUMERIC(13,2)  		--//  ENCODE delta
	,lfgja NUMERIC(4,0)  		--//  ENCODE delta
	,lfmon NUMERIC(2,0)  		--//  ENCODE delta
	,bwtty VARCHAR(1)  		--//  ENCODE lzo
	,stprv NUMERIC(11,2)  		--//  ENCODE delta
	,laepr DATE  		--//  ENCODE delta
	,zkprs NUMERIC(11,2)  		--//  ENCODE delta
	,zkdat DATE  		--//  ENCODE delta
	,timestamps NUMERIC(15,0)  		--//  ENCODE delta
	,bwprs NUMERIC(11,2)  		--//  ENCODE delta
	,bwprh NUMERIC(11,2)  		--//  ENCODE delta
	,vjbws NUMERIC(11,2)  		--//  ENCODE delta
	,vjbwh NUMERIC(11,2)  		--//  ENCODE delta
	,vvjsl NUMERIC(13,2)  		--//  ENCODE delta
	,vvjlb NUMERIC(13,3)  		--//  ENCODE delta
	,vvmlb NUMERIC(13,3)  		--//  ENCODE delta
	,vvsal NUMERIC(13,2)  		--//  ENCODE delta
	,zplpr NUMERIC(11,2)  		--//  ENCODE delta
	,zplp1 NUMERIC(11,2)  		--//  ENCODE delta
	,zplp2 NUMERIC(11,2)  		--//  ENCODE delta
	,zplp3 NUMERIC(11,2)  		--//  ENCODE delta
	,zpld1 DATE  		--//  ENCODE delta
	,zpld2 DATE  		--//  ENCODE delta
	,zpld3 DATE  		--//  ENCODE delta
	,pperz NUMERIC(6,0)  		--//  ENCODE delta
	,pperl NUMERIC(6,0)  		--//  ENCODE delta
	,pperv NUMERIC(6,0)  		--//  ENCODE delta
	,kalkz VARCHAR(1)  		--//  ENCODE lzo
	,kalkl VARCHAR(1)  		--//  ENCODE lzo
	,kalkv VARCHAR(1)  		--//  ENCODE lzo
	,kalsc VARCHAR(6)  		--//  ENCODE lzo
	,xlifo VARCHAR(1)  		--//  ENCODE lzo
	,mypol VARCHAR(4)  		--//  ENCODE lzo
	,bwph1 NUMERIC(11,2)  		--//  ENCODE delta
	,bwps1 NUMERIC(11,2)  		--//  ENCODE delta
	,abwkz NUMERIC(2,0)  		--//  ENCODE delta
	,pstat VARCHAR(15)  		--//  ENCODE lzo
	,kaln1 NUMERIC(12,0)  		--//  ENCODE delta
	,kalnr NUMERIC(12,0)  		--//  ENCODE delta
	,bwva1 VARCHAR(3)  		--//  ENCODE lzo
	,bwva2 VARCHAR(3)  		--//  ENCODE lzo
	,bwva3 VARCHAR(3)  		--//  ENCODE lzo
	,vers1 NUMERIC(2,0)  		--//  ENCODE delta
	,vers2 NUMERIC(2,0)  		--//  ENCODE delta
	,vers3 NUMERIC(2,0)  		--//  ENCODE delta
	,hrkft VARCHAR(4)  		--//  ENCODE lzo
	,kosgr VARCHAR(10)  		--//  ENCODE lzo
	,pprdz NUMERIC(3,0)  		--//  ENCODE delta
	,pprdl NUMERIC(3,0)  		--//  ENCODE delta
	,pprdv NUMERIC(3,0)  		--//  ENCODE delta
	,pdatz NUMERIC(4,0)  		--//  ENCODE delta
	,pdatl NUMERIC(4,0)  		--//  ENCODE delta
	,pdatv NUMERIC(4,0)  		--//  ENCODE delta
	,ekalr VARCHAR(1)  		--//  ENCODE lzo
	,vplpr NUMERIC(11,2)  		--//  ENCODE delta
	,mlmaa VARCHAR(1)  		--//  ENCODE lzo
	,mlast VARCHAR(1)  		--//  ENCODE lzo
	,lplpr NUMERIC(11,2)  		--//  ENCODE delta
	,vksal NUMERIC(13,2)  		--//  ENCODE delta
	,hkmat VARCHAR(1)  		--//  ENCODE lzo
	,sperw VARCHAR(1)  		--//  ENCODE lzo
	,kziwl VARCHAR(3)  		--//  ENCODE lzo
	,wlinl DATE  		--//  ENCODE delta
	,abciw VARCHAR(1)  		--//  ENCODE lzo
	,bwspa NUMERIC(6,2)  		--//  ENCODE delta
	,lplpx NUMERIC(11,2)  		--//  ENCODE delta
	,vplpx NUMERIC(11,2)  		--//  ENCODE delta
	,fplpx NUMERIC(11,2)  		--//  ENCODE delta
	,lbwst VARCHAR(1)  		--//  ENCODE lzo
	,vbwst VARCHAR(1)  		--//  ENCODE lzo
	,fbwst VARCHAR(1)  		--//  ENCODE lzo
	,eklas VARCHAR(4)  		--//  ENCODE lzo
	,qklas VARCHAR(4)  		--//  ENCODE lzo
	,mtuse VARCHAR(1)  		--//  ENCODE lzo
	,mtorg VARCHAR(1)  		--//  ENCODE lzo
	,ownpr VARCHAR(1)  		--//  ENCODE lzo
	,xbewm VARCHAR(1)  		--//  ENCODE lzo
	,bwpei NUMERIC(5,0)  		--//  ENCODE delta
	,mbrue VARCHAR(1)  		--//  ENCODE lzo
	,oklas VARCHAR(4)  		--//  ENCODE lzo
	,oippinv VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (mandt, matnr, bwkey, bwtar)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_material_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_MATERIAL_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_material_dim
(
	matl_num VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,matl_desc VARCHAR(100)  		--//  ENCODE zstd
	,crt_on DATE  		--//  ENCODE zstd
	,crt_by_nm VARCHAR(12)  		--//  ENCODE zstd
	,chg_dttm DATE  		--//  ENCODE zstd
	,chg_by_nm VARCHAR(12)  		--//  ENCODE bytedict
	,maint_sts_cmplt_matl VARCHAR(15)  		--//  ENCODE zstd
	,maint_sts VARCHAR(15)  		--//  ENCODE zstd
	,fl_matl_del_clnt_lvl VARCHAR(10)  		--//  ENCODE zstd
	,matl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,indstr_sectr VARCHAR(10)  		--//  ENCODE zstd
	,matl_grp_cd VARCHAR(20)  		--//  ENCODE bytedict
	,old_matl_num VARCHAR(40)  		--//  ENCODE zstd
	,base_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,prch_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,doc_num VARCHAR(22)  		--//  ENCODE zstd
	,doc_type VARCHAR(10)  		--//  ENCODE zstd
	,doc_vers VARCHAR(10)  		--//  ENCODE zstd
	,pg_fmt__doc VARCHAR(10)  		--//  ENCODE zstd
	,doc_chg_num VARCHAR(20)  		--//  ENCODE zstd
	,pg_num_doc VARCHAR(10)  		--//  ENCODE zstd
	,num_sht numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdtn_memo_txt VARCHAR(40)  		--//  ENCODE zstd
	,pg_fmt_prdtn_memo VARCHAR(10)  		--//  ENCODE zstd
	,size_dims_txt VARCHAR(32)  		--//  ENCODE zstd
	,bsc_matl VARCHAR(100)  		--//  ENCODE zstd
	,indstr_std_desc VARCHAR(40)  		--//  ENCODE zstd
	,mercia_plan VARCHAR(10)  		--//  ENCODE zstd
	,prchsng_val_key VARCHAR(10)  		--//  ENCODE zstd
	,grs_wt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,net_wt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,wt_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,vol_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,vol_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,cntnr_rqr VARCHAR(10)  		--//  ENCODE zstd
	,strg_cond VARCHAR(10)  		--//  ENCODE zstd
	,temp_cond_ind VARCHAR(10)  		--//  ENCODE zstd
	,low_lvl_cd VARCHAR(10)  		--//  ENCODE zstd
	,trspn_grp VARCHAR(10)  		--//  ENCODE zstd
	,haz_matl_num VARCHAR(40)  		--//  ENCODE zstd
	,div VARCHAR(10)  		--//  ENCODE zstd
	,cmpt VARCHAR(10)  		--//  ENCODE zstd
	,ean_obsol VARCHAR(13)  		--//  ENCODE zstd
	,gr_prtd_qty NUMERIC(13,3)  		--//  ENCODE zstd
	,prcmt_rule VARCHAR(10)  		--//  ENCODE zstd
	,src_supl VARCHAR(10)  		--//  ENCODE zstd
	,seasn_cat VARCHAR(10)  		--//  ENCODE zstd
	,lbl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,lbl_form VARCHAR(10)  		--//  ENCODE zstd
	,deact VARCHAR(10)  		--//  ENCODE zstd
	,prmry_upc_cd VARCHAR(40)  		--//  ENCODE zstd
	,ean_cat VARCHAR(10)  		--//  ENCODE zstd
	,lgth_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,wdth_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,hght_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,dim_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,prod_hier_cd VARCHAR(40)  		--//  ENCODE zstd
	,stk_tfr_chg_cost VARCHAR(10)  		--//  ENCODE zstd
	,cad_ind VARCHAR(10)  		--//  ENCODE zstd
	,qm_prcmt_act VARCHAR(10)  		--//  ENCODE zstd
	,allw_pkgng_wt NUMERIC(13,3)  		--//  ENCODE zstd
	,wt_unit VARCHAR(10)  		--//  ENCODE zstd
	,allw_pkgng_vol NUMERIC(13,3)  		--//  ENCODE zstd
	,vol_unit VARCHAR(10)  		--//  ENCODE zstd
	,exces_wt_tlrnc NUMERIC(3,1)  		--//  ENCODE zstd
	,exces_vol_tlrnc NUMERIC(3,1)  		--//  ENCODE zstd
	,var_prch_ord_unit VARCHAR(10)  		--//  ENCODE zstd
	,rvsn_lvl_asgn_matl VARCHAR(10)  		--//  ENCODE zstd
	,configurable_matl_ind VARCHAR(10)  		--//  ENCODE zstd
	,btch_mgmt_reqt_ind VARCHAR(10)  		--//  ENCODE zstd
	,pkgng_matl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,max_lvl_vol NUMERIC(3,0)  		--//  ENCODE zstd
	,stack_fact numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,pkgng_matl_grp VARCHAR(10)  		--//  ENCODE zstd
	,auth_grp VARCHAR(10)  		--//  ENCODE zstd
	,vld_from_dt DATE  		--//  ENCODE zstd
	,del_dt DATE  		--//  ENCODE zstd
	,seasn_yr VARCHAR(10)  		--//  ENCODE zstd
	,prc_bnd_cat VARCHAR(10)  		--//  ENCODE zstd
	,bill_of_matl VARCHAR(10)  		--//  ENCODE zstd
	,extrnl_matl_grp_txt VARCHAR(40)  		--//  ENCODE zstd
	,cross_plnt_cnfg_matl VARCHAR(40)  		--//  ENCODE zstd
	,matl_cat VARCHAR(10)  		--//  ENCODE zstd
	,matl_coprod_ind VARCHAR(10)  		--//  ENCODE zstd
	,fllp_matl_ind VARCHAR(10)  		--//  ENCODE zstd
	,prc_ref_matl VARCHAR(40)  		--//  ENCODE zstd
	,cros_plnt_matl_sts VARCHAR(10)  		--//  ENCODE zstd
	,cros_dstn_chn_matl_sts VARCHAR(10)  		--//  ENCODE zstd
	,cros_plnt_matl_sts_vld_dt DATE  		--//  ENCODE zstd
	,chn_matl_vld_from_dt DATE  		--//  ENCODE zstd
	,tax_clsn_matl VARCHAR(10)  		--//  ENCODE zstd
	,catlg_prfl VARCHAR(20)  		--//  ENCODE zstd
	,min_rmn_shlf_lif NUMERIC(4,0)  		--//  ENCODE zstd
	,tot_shlf_lif NUMERIC(4,0)  		--//  ENCODE zstd
	,strg_pct NUMERIC(3,0)  		--//  ENCODE zstd
	,cntnt_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,net_cntnt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,cmpr_prc_unit NUMERIC(5,0)  		--//  ENCODE zstd
	,isr_matl_grp VARCHAR(40)  		--//  ENCODE zstd
	,grs_cntnt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,qty_conv_meth VARCHAR(10)  		--//  ENCODE zstd
	,intrnl_obj_num numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,envmt_rlvnt VARCHAR(10)  		--//  ENCODE zstd
	,prod_allc_dtrmn_proc VARCHAR(40)  		--//  ENCODE zstd
	,prc_prfl_vrnt VARCHAR(10)  		--//  ENCODE zstd
	,matl_qual_disc VARCHAR(10)  		--//  ENCODE zstd
	,mfr_part_num VARCHAR(40)  		--//  ENCODE zstd
	,mfr_num VARCHAR(10)  		--//  ENCODE zstd
	,intrnl_inv_mgmt VARCHAR(40)  		--//  ENCODE zstd
	,mfr_part_prfl VARCHAR(10)  		--//  ENCODE zstd
	,meas_usg_unit VARCHAR(10)  		--//  ENCODE zstd
	,rollout_seasn VARCHAR(10)  		--//  ENCODE zstd
	,dngrs_goods_ind_prof VARCHAR(10)  		--//  ENCODE zstd
	,hi_viscous_ind VARCHAR(10)  		--//  ENCODE zstd
	,in_bulk_lqd_ind VARCHAR(10)  		--//  ENCODE zstd
	,lvl_explc_ser_num VARCHAR(10)  		--//  ENCODE zstd
	,pkgng_matl_clse_pkgng VARCHAR(10)  		--//  ENCODE zstd
	,appr_btch_rec_ind VARCHAR(10)  		--//  ENCODE zstd
	,ovrd_chg_num VARCHAR(10)  		--//  ENCODE zstd
	,matl_cmplt_lvl numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,per_ind_shlf_lif_expn_dt VARCHAR(10)  		--//  ENCODE zstd
	,rd_rule_sled VARCHAR(10)  		--//  ENCODE zstd
	,prod_cmpos_prtd_pkgng VARCHAR(10)  		--//  ENCODE zstd
	,genl_item_cat_grp VARCHAR(10)  		--//  ENCODE zstd
	,gn_matl_logl_vrnt VARCHAR(10)  		--//  ENCODE zstd
	,prod_base VARCHAR(10)  		--//  ENCODE zstd
	,vrnt VARCHAR(10)  		--//  ENCODE zstd
	,put_up VARCHAR(10)  		--//  ENCODE zstd
	,mega_brnd_cd VARCHAR(10)  		--//  ENCODE zstd
	,brnd_cd VARCHAR(10)  		--//  ENCODE zstd
	,tech VARCHAR(10)  		--//  ENCODE zstd
	,color VARCHAR(10)  		--//  ENCODE zstd
	,seasonality VARCHAR(10)  		--//  ENCODE zstd
	,mfg_src_cd VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,mega_brnd_desc VARCHAR(100)  		--//  ENCODE zstd
	,brnd_desc VARCHAR(100)  		--//  ENCODE zstd
	,varnt_desc VARCHAR(100)  		--//  ENCODE zstd
	,base_prod_desc VARCHAR(100)  		--//  ENCODE zstd
	,put_up_desc VARCHAR(100)  		--//  ENCODE zstd
	,prodh1 VARCHAR(18)  		--//  ENCODE zstd
	,prodh1_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,prodh2 VARCHAR(18)  		--//  ENCODE zstd
	,prodh2_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,prodh3 VARCHAR(18)  		--//  ENCODE zstd
	,prodh3_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,prodh4 VARCHAR(18)  		--//  ENCODE bytedict
	,prodh4_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,prodh5 VARCHAR(18)  		--//  ENCODE bytedict
	,prodh5_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,prodh6 VARCHAR(18)  		--//  ENCODE bytedict
	,prodh6_txtmd VARCHAR(100)  		--//  ENCODE zstd
	,matl_type_desc VARCHAR(40)  		--//  ENCODE zstd
	,mfr_part_num_new VARCHAR(256)  		--//  ENCODE lzo
	,formulation_num VARCHAR(50)  		--//  ENCODE lzo
	,pka_franchise_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_franchise_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_brand_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_brand_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sub_brand_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_sub_brand_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_variant_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_variant_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sub_variant_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_sub_variant_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_flavor_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_flavor_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_ingredient_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_ingredient_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_application_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_application_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_length_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_length_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_shape_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_shape_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_spf_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_spf_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_cover_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_cover_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_form_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_form_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_size_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_character_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_character_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_package_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_package_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_attribute_13_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_attribute_13_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_attribute_14_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_attribute_14_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sku_identification_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_sku_identification_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_one_time_relabeling_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_one_time_relabeling_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,pka_product_key_description_2 VARCHAR(255)  		--//  ENCODE lzo
	,pka_root_code VARCHAR(68)  		--//  ENCODE lzo
	,pka_root_code_desc_1 VARCHAR(255)  		--//  ENCODE lzo
	,pka_root_code_desc_2 VARCHAR(255)  		--//  ENCODE lzo
	,PRIMARY KEY (matl_num)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPEDW_INTEGRATION.edw_material_typ;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_MATERIAL_TYP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_material_typ
(
	matl_type VARCHAR(4) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (matl_type, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_prod_hier;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_prod_hier
(
	prod_hier VARCHAR(18) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (prod_hier, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPEDW_INTEGRATION.edw_profit_center_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_PROFIT_CENTER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_profit_center_dim
(
	lang_key VARCHAR(4)  		--//  ENCODE zstd
	,cntl_area VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,prft_ctr VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,vld_to_dt DATE NOT NULL 		--//  ENCODE zstd
	,vld_from_dt DATE  		--//  ENCODE zstd
	,shrt_desc VARCHAR(20)  		--//  ENCODE zstd
	,med_desc VARCHAR(40)  		--//  ENCODE zstd
	,prsn_resp VARCHAR(20)  		--//  ENCODE zstd
	,crncy_key VARCHAR(5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,need_stat_shrt_desc VARCHAR(100)
	,strng_hold_shrt_desc VARCHAR(100)
	,rflt VARCHAR(100)
	,PRIMARY KEY (cntl_area, prft_ctr, vld_to_dt)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPEDW_INTEGRATION.edw_sales_org_dim;
CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.EDW_SALES_ORG_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPEDW_INTEGRATION.edw_sales_org_dim
(
	clnt numeric(18,0)		--// INTEGER
	,sls_org VARCHAR(4) NOT NULL
	,sls_org_nm VARCHAR(20)
	,stats_crncy VARCHAR(5)
	,sls_org_co_cd VARCHAR(4)
	,cust_num_intco_bill VARCHAR(10)
	,ctry_key VARCHAR(3)
	,crncy_key VARCHAR(5)
	,fisc_yr_vrnt VARCHAR(2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (sls_org)
)
		--// DISTSTYLE EVEN
;

--DROP TABLE ASPITG_INTEGRATION.itg_base_prod_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_BASE_PROD_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_base_prod_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,base_prod VARCHAR(3) NOT NULL
	,base_prod_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, base_prod)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_brnd_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_BRND_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_brnd_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,brnd VARCHAR(3) NOT NULL
	,brnd_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, brnd)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_code_descriptions;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CODE_DESCRIPTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_code_descriptions
(
	source_type VARCHAR(10) NOT NULL 		--//  ENCODE lzo
	,code_type VARCHAR(50)  		--//  ENCODE lzo
	,code VARCHAR(15)  		--//  ENCODE lzo
	,code_desc VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_comp;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_COMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_comp
(
	clnt VARCHAR(3)
	,co_cd VARCHAR(4) NOT NULL
	,ctry_key VARCHAR(3)
	,crncy_key VARCHAR(5)
	,chrt_acct VARCHAR(4)
	,crdt_cntrl_area VARCHAR(4)
	,fisc_yr_vrnt VARCHAR(2)
	,company VARCHAR(6)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (co_cd)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_comp_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_COMP_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_comp_text
(
	clnt VARCHAR(3)
	,co_cd VARCHAR(4) NOT NULL
	,com_cd_nm VARCHAR(25)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (co_cd)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_crncy_exch;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CRNCY_EXCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_crncy_exch
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,ex_rt_typ VARCHAR(4) NOT NULL
	,from_crncy VARCHAR(5) NOT NULL
	,to_crncy VARCHAR(5) NOT NULL
	,vld_from VARCHAR(8) NOT NULL
	,ex_rt NUMERIC(9,5)
	,from_ratio NUMERIC(9,0)
	,to_ratio NUMERIC(9,0)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, ex_rt_typ, from_crncy, to_crncy, vld_from)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_ctry_cd_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CTRY_CD_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_ctry_cd_text
(
	ctry_key VARCHAR(3) NOT NULL
	,lang_key VARCHAR(1) NOT NULL
	,shrt_desc VARCHAR(20)
	,med_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (ctry_key, lang_key)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_cust_base;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CUST_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_cust_base
(
	clnt VARCHAR(150)  		--//  ENCODE zstd
	,cust_num_1 VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,addr VARCHAR(150)  		--//  ENCODE zstd
	,title VARCHAR(150)  		--//  ENCODE zstd
	,cntrl_ordr_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,exp_train_stn VARCHAR(150)  		--//  ENCODE zstd
	,train_stn VARCHAR(150)  		--//  ENCODE zstd
	,intnl_loc_num1 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,intnl_loc_num2 numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,auth_grp VARCHAR(150)  		--//  ENCODE zstd
	,indstr_key VARCHAR(150)  		--//  ENCODE zstd
	,chk_dig_intnl_loc_num numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,data_comm_line_no VARCHAR(150)  		--//  ENCODE zstd
	,dt_on_rec_crt DATE  		--//  ENCODE zstd
	,nm_prsn_crt_obj VARCHAR(150)  		--//  ENCODE zstd
	,unld_pt_exist_ind VARCHAR(150)  		--//  ENCODE zstd
	,cent_bill_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_mstr_rec_fisc_addr VARCHAR(150)  		--//  ENCODE zstd
	,wrk_num_cal VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_alt_pyr VARCHAR(150)  		--//  ENCODE zstd
	,grp_key VARCHAR(150)  		--//  ENCODE zstd
	,cust_acct_grp VARCHAR(150)  		--//  ENCODE zstd
	,cust_clsn VARCHAR(150)  		--//  ENCODE zstd
	,ctry_key VARCHAR(150)  		--//  ENCODE zstd
	,acct_num_vend VARCHAR(150)  		--//  ENCODE zstd
	,cent_delv_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,city_coordnt VARCHAR(150)  		--//  ENCODE zstd
	,cent_del_fl_mstr_rec VARCHAR(150)  		--//  ENCODE zstd
	,nm_1 VARCHAR(150)  		--//  ENCODE zstd
	,nm_2 VARCHAR(150)  		--//  ENCODE zstd
	,nm_3 VARCHAR(150)  		--//  ENCODE zstd
	,nm_4 VARCHAR(150)  		--//  ENCODE zstd
	,nlsn_id VARCHAR(150)  		--//  ENCODE zstd
	,city VARCHAR(150)  		--//  ENCODE zstd
	,dstrc VARCHAR(150)  		--//  ENCODE zstd
	,po_box VARCHAR(150)  		--//  ENCODE zstd
	,po_box_pstl_cd VARCHAR(150)  		--//  ENCODE zstd
	,pstl_cd VARCHAR(150)  		--//  ENCODE zstd
	,rgn VARCHAR(150)  		--//  ENCODE zstd
	,cnty_cd VARCHAR(150)  		--//  ENCODE zstd
	,city_cd VARCHAR(150)  		--//  ENCODE zstd
	,fcst_chnl VARCHAR(150)  		--//  ENCODE zstd
	,srt_fld VARCHAR(150)  		--//  ENCODE zstd
	,cent_pstng_blk VARCHAR(150)  		--//  ENCODE zstd
	,lang_key VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_1 VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_2 VARCHAR(150)  		--//  ENCODE zstd
	,busn_ptnr_subj_eqlztn_tax_ind VARCHAR(150)  		--//  ENCODE zstd
	,liab_for_vat VARCHAR(150)  		--//  ENCODE zstd
	,hs_num_and_street VARCHAR(150)  		--//  ENCODE zstd
	,telebox_num VARCHAR(150)  		--//  ENCODE zstd
	,fst_phn_num VARCHAR(150)  		--//  ENCODE zstd
	,sec_phn_num VARCHAR(150)  		--//  ENCODE zstd
	,fax_num VARCHAR(150)  		--//  ENCODE zstd
	,teletex_num VARCHAR(150)  		--//  ENCODE zstd
	,telex_num VARCHAR(150)  		--//  ENCODE zstd
	,trspn_zn_goods_delv VARCHAR(150)  		--//  ENCODE zstd
	,is_acct_one_time_acct_ind VARCHAR(150)  		--//  ENCODE zstd
	,alt_payee_doc_allw_ind VARCHAR(150)  		--//  ENCODE zstd
	,co_id_trad_ptnr VARCHAR(150)  		--//  ENCODE zstd
	,vat_regs_num VARCHAR(150)  		--//  ENCODE zstd
	,cmpt_ind VARCHAR(150)  		--//  ENCODE zstd
	,sls_ptnr_ind VARCHAR(150)  		--//  ENCODE zstd
	,sls_prosp_ind VARCHAR(150)  		--//  ENCODE zstd
	,for_cust_type4_ind VARCHAR(150)  		--//  ENCODE zstd
	,id_dflt_sold_to_prty VARCHAR(150)  		--//  ENCODE zstd
	,in_cnsmr VARCHAR(150)  		--//  ENCODE zstd
	,legal_sts VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_1 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_2 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_3 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_4 VARCHAR(150)  		--//  ENCODE zstd
	,indstr_cd_5 VARCHAR(150)  		--//  ENCODE zstd
	,init_cntct VARCHAR(150)  		--//  ENCODE zstd
	,annl_sls_1 VARCHAR(150)  		--//  ENCODE zstd
	,yr_for_sls_gvn numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,crncy_sls_fig VARCHAR(150)  		--//  ENCODE zstd
	,yr_num_emp numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,yr_for_num_emp_gvn numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,attr_1 VARCHAR(150)  		--//  ENCODE zstd
	,attr_2 VARCHAR(150)  		--//  ENCODE zstd
	,attr_3 VARCHAR(150)  		--//  ENCODE zstd
	,attr_4 VARCHAR(150)  		--//  ENCODE zstd
	,attr_5 VARCHAR(150)  		--//  ENCODE zstd
	,attr_6 VARCHAR(150)  		--//  ENCODE zstd
	,attr_7 VARCHAR(150)  		--//  ENCODE zstd
	,attr_8 VARCHAR(150)  		--//  ENCODE zstd
	,attr_9 VARCHAR(150)  		--//  ENCODE zstd
	,attr_10 VARCHAR(150)  		--//  ENCODE zstd
	,ntrl_prsn VARCHAR(150)  		--//  ENCODE zstd
	,annl_sls_2 VARCHAR(150)  		--//  ENCODE zstd
	,tax_juris VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch1 VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch2 VARCHAR(150)  		--//  ENCODE zstd
	,srch_term_match_cd_srch3 VARCHAR(150)  		--//  ENCODE zstd
	,fisc_yr_vrnt VARCHAR(150)  		--//  ENCODE zstd
	,usg_ind VARCHAR(150)  		--//  ENCODE zstd
	,insp_carr_out_by_cust VARCHAR(150)  		--//  ENCODE zstd
	,insp_delv_note_after_outb_delv VARCHAR(150)  		--//  ENCODE zstd
	,ref_acct_grp_one_time_acct VARCHAR(150)  		--//  ENCODE zstd
	,po_box_city VARCHAR(150)  		--//  ENCODE zstd
	,plnt VARCHAR(150)  		--//  ENCODE zstd
	,data_med_exch_ind VARCHAR(150)  		--//  ENCODE zstd
	,instr_key_data_med_exch VARCHAR(150)  		--//  ENCODE zstd
	,sts_data_tfr_subsq_rlse VARCHAR(150)  		--//  ENCODE zstd
	,asgnmt_hier numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,pmt_blk VARCHAR(150)  		--//  ENCODE zstd
	,cust_grp VARCHAR(150)  		--//  ENCODE zstd
	,id_mn_non_mil_use VARCHAR(150)  		--//  ENCODE zstd
	,id_mn_mil_use VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_1 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_2 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_3 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_4 VARCHAR(150)  		--//  ENCODE zstd
	,cust_cond_grp_5 VARCHAR(150)  		--//  ENCODE zstd
	,alt_pyr_acct_num_ind VARCHAR(150)  		--//  ENCODE zstd
	,tax_type VARCHAR(150)  		--//  ENCODE zstd
	,tax_num_type VARCHAR(150)  		--//  ENCODE zstd
	,tax_num3 VARCHAR(150)  		--//  ENCODE zstd
	,tax_num4 VARCHAR(150)  		--//  ENCODE zstd
	,cust_is_icms_expt VARCHAR(150)  		--//  ENCODE zstd
	,cust_is_ipi_expt VARCHAR(150)  		--//  ENCODE zstd
	,cust_grp_tax_subst VARCHAR(150)  		--//  ENCODE zstd
	,cust_cfop_cat VARCHAR(150)  		--//  ENCODE zstd
	,tax_law_icms VARCHAR(150)  		--//  ENCODE zstd
	,tax_law_ipi VARCHAR(150)  		--//  ENCODE zstd
	,biochem_wf_ind VARCHAR(150)  		--//  ENCODE zstd
	,nuclr_np_ind VARCHAR(150)  		--//  ENCODE zstd
	,natl_scty_ind VARCHAR(150)  		--//  ENCODE zstd
	,missile_tech_ind VARCHAR(150)  		--//  ENCODE zstd
	,cent_sls_blk_cust VARCHAR(150)  		--//  ENCODE zstd
	,unifm_rsrs_lctr VARCHAR(132)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,PRIMARY KEY (cust_num_1)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPITG_INTEGRATION.itg_cust_sls;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CUST_SLS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_cust_sls
(
	clnt VARCHAR(3)  		--//  ENCODE zstd
	,cust_no VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sls_org VARCHAR(4) NOT NULL 		--//  ENCODE zstd
	,dstr_chnl VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,div VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,obj_crt_prsn VARCHAR(12)  		--//  ENCODE zstd
	,rec_crt_dt DATE  		--//  ENCODE zstd
	,auth_grp VARCHAR(4)  		--//  ENCODE zstd
	,cust_del_flag VARCHAR(1)  		--//  ENCODE zstd
	,cust_stat_grp VARCHAR(1)  		--//  ENCODE zstd
	,cust_ord_blk VARCHAR(2)  		--//  ENCODE zstd
	,prc_pcdr_asgn VARCHAR(1)  		--//  ENCODE zstd
	,cust_grp VARCHAR(2)  		--//  ENCODE zstd
	,sls_dstrc VARCHAR(6)  		--//  ENCODE zstd
	,prc_grp VARCHAR(2)  		--//  ENCODE zstd
	,prc_list_typ VARCHAR(2)  		--//  ENCODE zstd
	,ord_prob_itm numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,incoterm1 VARCHAR(3)  		--//  ENCODE zstd
	,incoterm2 VARCHAR(28)  		--//  ENCODE zstd
	,cust_delv_blk VARCHAR(2)  		--//  ENCODE zstd
	,cmplt_delv_sls_ord VARCHAR(1)  		--//  ENCODE zstd
	,max_no_prtl_delv_allw_itm NUMERIC(1,0)  		--//  ENCODE zstd
	,prtl_delv_itm_lvl VARCHAR(1)  		--//  ENCODE zstd
	,ord_comb_in VARCHAR(1)  		--//  ENCODE zstd
	,btch_splt_allw VARCHAR(1)  		--//  ENCODE zstd
	,delv_prir numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,vend_acct_no VARCHAR(12)  		--//  ENCODE zstd
	,shipping_cond VARCHAR(2)  		--//  ENCODE zstd
	,bill_blk_cust VARCHAR(2)  		--//  ENCODE zstd
	,man_invc_maint VARCHAR(1)  		--//  ENCODE zstd
	,invc_dt VARCHAR(2)  		--//  ENCODE zstd
	,invc_list_sched VARCHAR(2)  		--//  ENCODE zstd
	,cost_est_in VARCHAR(1)  		--//  ENCODE zstd
	,val_lmt_cost_est NUMERIC(13,2)  		--//  ENCODE zstd
	,crncy VARCHAR(5)  		--//  ENCODE zstd
	,cust_clas VARCHAR(2)  		--//  ENCODE zstd
	,acct_asgnmt_grp VARCHAR(2)  		--//  ENCODE zstd
	,delv_plnt VARCHAR(4)  		--//  ENCODE zstd
	,sls_grp VARCHAR(3)  		--//  ENCODE zstd
	,sls_ofc VARCHAR(4)  		--//  ENCODE zstd
	,itm_props VARCHAR(10)  		--//  ENCODE zstd
	,cust_grp1 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp2 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp3 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp4 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp5 VARCHAR(3)  		--//  ENCODE zstd
	,cust_rebt_in VARCHAR(1)  		--//  ENCODE zstd
	,rebt_indx_cust_strt_prd DATE  		--//  ENCODE zstd
	,exch_rt_typ VARCHAR(4)  		--//  ENCODE zstd
	,prc_dtrmn_id VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id1 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id2 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id3 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id4 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id5 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id6 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id7 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id8 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id9 VARCHAR(1)  		--//  ENCODE zstd
	,prod_attr_id10 VARCHAR(1)  		--//  ENCODE zstd
	,pymt_key_term VARCHAR(4)  		--//  ENCODE zstd
	,persnl_num NUMERIC(8,0)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,PRIMARY KEY (cust_no, sls_org, dstr_chnl, div)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPITG_INTEGRATION.itg_cust_sls_attr;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CUST_SLS_ATTR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_cust_sls_attr
(
	division VARCHAR(2) NOT NULL 		--//  ENCODE lzo
	,distr_chan VARCHAR(2) NOT NULL 		--//  ENCODE lzo
	,salesorg VARCHAR(4) NOT NULL 		--//  ENCODE lzo
	,cust_sales VARCHAR(10) NOT NULL 		--//  ENCODE lzo
	,accnt_asgn VARCHAR(2)  		--//  ENCODE lzo
	,cust_cla VARCHAR(2)  		--//  ENCODE lzo
	,cust_group VARCHAR(2)  		--//  ENCODE lzo
	,cust_grp1 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp2 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp3 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp4 VARCHAR(3)  		--//  ENCODE lzo
	,cust_grp5 VARCHAR(3)  		--//  ENCODE lzo
	,c_ctr_area VARCHAR(4)  		--//  ENCODE lzo
	,incoterms VARCHAR(3)  		--//  ENCODE lzo
	,incoterms2 VARCHAR(32)  		--//  ENCODE lzo
	,plant VARCHAR(4)  		--//  ENCODE lzo
	,pmnttrms VARCHAR(4)  		--//  ENCODE lzo
	,sales_dist VARCHAR(6)  		--//  ENCODE lzo
	,sales_grp VARCHAR(3)  		--//  ENCODE lzo
	,sales_off VARCHAR(4)  		--//  ENCODE lzo
	,cur_sls_emp NUMERIC(8,0)  		--//  ENCODE delta
	,lcl_cust_grp_1 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_2 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_3 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_4 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_5 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_6 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_7 VARCHAR(10)  		--//  ENCODE lzo
	,lcl_cust_grp_8 VARCHAR(10)  		--//  ENCODE lzo
	,customer VARCHAR(10)  		--//  ENCODE lzo
	,prc_proc VARCHAR(1)  		--//  ENCODE lzo
	,prc_grp VARCHAR(2)  		--//  ENCODE lzo
	,prc_lst_type VARCHAR(2)  		--//  ENCODE lzo
	,shpg_con VARCHAR(2)  		--//  ENCODE lzo
	,par_del VARCHAR(1)  		--//  ENCODE lzo
	,max_num_pa VARCHAR(1)  		--//  ENCODE lzo
	,prnt_cust_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_key VARCHAR(12)  		--//  ENCODE lzo
	,bnr_frmt_key VARCHAR(12)  		--//  ENCODE lzo
	,go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,sub_chnl_key VARCHAR(12)  		--//  ENCODE lzo
	,segmt_key VARCHAR(12)  		--//  ENCODE lzo
	,cust_set_1 VARCHAR(12)  		--//  ENCODE lzo
	,cust_set_2 VARCHAR(12)  		--//  ENCODE lzo
	,cust_set_3 VARCHAR(12)  		--//  ENCODE lzo
	,cust_set_4 VARCHAR(12)  		--//  ENCODE lzo
	,cust_set_5 VARCHAR(12)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (division, distr_chan, salesorg, cust_sales)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_cust_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_CUST_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_cust_text
(
	clnt VARCHAR(3)
	,cust_num1 VARCHAR(10)
	,nm VARCHAR(100)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_dstrbtn_chnl;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_DSTRBTN_CHNL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_dstrbtn_chnl
(
	distr_chan VARCHAR(2) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtsh VARCHAR(20)  		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (distr_chan, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_ecc_marm;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_ECC_MARM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_ecc_marm
(
	matl_no VARCHAR(18) NOT NULL 		--//  ENCODE bytedict
	,alt_unt VARCHAR(3) NOT NULL 		--//  ENCODE bytedict
	,numrtr NUMERIC(5,0)  		--//  ENCODE lzo
	,denomtr NUMERIC(5,0)  		--//  ENCODE lzo
	,ean_nr VARCHAR(13)  		--//  ENCODE bytedict
	,ean_11 VARCHAR(18)  		--//  ENCODE bytedict
	,ctgry VARCHAR(2)  		--//  ENCODE lzo
	,length NUMERIC(13,3)  		--//  ENCODE lzo
	,width NUMERIC(13,3)  		--//  ENCODE lzo
	,height NUMERIC(13,3)  		--//  ENCODE lzo
	,unit VARCHAR(3)  		--//  ENCODE lzo
	,volum VARCHAR(14)  		--//  ENCODE bytedict
	,vol_unt VARCHAR(3)  		--//  ENCODE bytedict
	,gross_wt NUMERIC(13,3)  		--//  ENCODE bytedict
	,wt_unt VARCHAR(3)  		--//  ENCODE lzo
	,lowrlvl_unt VARCHAR(3)  		--//  ENCODE lzo
	,intrnl_char NUMERIC(10,0)  		--//  ENCODE lzo
	,uom_srtno NUMERIC(2,0)  		--//  ENCODE lzo
	,leadng_unt VARCHAR(1)  		--//  ENCODE lzo
	,valutn_unt VARCHAR(1)  		--//  ENCODE lzo
	,unts_use VARCHAR(1)  		--//  ENCODE lzo
	,uom VARCHAR(3)  		--//  ENCODE bytedict
	,lg_vrnt VARCHAR(1)  		--//  ENCODE lzo
	,ean_vrnt VARCHAR(2)  		--//  ENCODE lzo
	,remng_vol NUMERIC(3,0)  		--//  ENCODE bytedict
	,max_stack NUMERIC(3,0)  		--//  ENCODE lzo
	,capause NUMERIC(15,0)  		--//  ENCODE lzo
	,uom_ctgry VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE runlength
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (matl_no, alt_unt)
)
--<<Error - UNKNOWN DISTSTYLE>>
		--// SORTKEY ( 
		--// 	matl_no
		--// 	)
;		--// ;
--DROP TABLE ASPITG_INTEGRATION.itg_ecc_standard_cost;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_ECC_STANDARD_COST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_ecc_standard_cost
(
	mandt VARCHAR(3) NOT NULL 		--//  ENCODE lzo
	,matnr VARCHAR(18) NOT NULL 		--//  ENCODE lzo
	,bwkey VARCHAR(4) NOT NULL 		--//  ENCODE lzo
	,bwtar VARCHAR(10) NOT NULL 		--//  ENCODE lzo
	,lvorm VARCHAR(1)  		--//  ENCODE lzo
	,lbkum NUMERIC(13,3)  		--//  ENCODE delta
	,salk3 NUMERIC(13,2)  		--//  ENCODE delta
	,vprsv VARCHAR(1)  		--//  ENCODE lzo
	,verpr NUMERIC(11,2)  		--//  ENCODE delta
	,stprs NUMERIC(11,2)  		--//  ENCODE delta
	,peinh NUMERIC(5,0)  		--//  ENCODE delta
	,bklas VARCHAR(4)  		--//  ENCODE lzo
	,salkv NUMERIC(13,2)  		--//  ENCODE delta
	,vmkum NUMERIC(13,3)  		--//  ENCODE delta
	,vmsal NUMERIC(13,2)  		--//  ENCODE delta
	,vmvpr VARCHAR(1)  		--//  ENCODE lzo
	,vmver NUMERIC(11,2)  		--//  ENCODE delta
	,vmstp NUMERIC(11,2)  		--//  ENCODE delta
	,vmpei NUMERIC(5,0)  		--//  ENCODE lzo
	,vmbkl VARCHAR(4)  		--//  ENCODE lzo
	,vmsav NUMERIC(13,2)  		--//  ENCODE delta
	,vjkum NUMERIC(13,3)  		--//  ENCODE delta
	,vjsal NUMERIC(13,2)  		--//  ENCODE delta
	,vjvpr VARCHAR(1)  		--//  ENCODE lzo
	,vjver NUMERIC(11,2)  		--//  ENCODE delta
	,vjstp NUMERIC(11,2)  		--//  ENCODE delta
	,vjpei NUMERIC(5,0)  		--//  ENCODE delta
	,vjbkl VARCHAR(4)  		--//  ENCODE lzo
	,vjsav NUMERIC(13,2)  		--//  ENCODE delta
	,lfgja NUMERIC(4,0)  		--//  ENCODE delta
	,lfmon NUMERIC(2,0)  		--//  ENCODE delta
	,bwtty VARCHAR(1)  		--//  ENCODE lzo
	,stprv NUMERIC(11,2)  		--//  ENCODE delta
	,laepr DATE  		--//  ENCODE delta
	,zkprs NUMERIC(11,2)  		--//  ENCODE delta
	,zkdat DATE  		--//  ENCODE delta
	,timestamps NUMERIC(15,0)  		--//  ENCODE delta
	,bwprs NUMERIC(11,2)  		--//  ENCODE delta
	,bwprh NUMERIC(11,2)  		--//  ENCODE delta
	,vjbws NUMERIC(11,2)  		--//  ENCODE delta
	,vjbwh NUMERIC(11,2)  		--//  ENCODE delta
	,vvjsl NUMERIC(13,2)  		--//  ENCODE delta
	,vvjlb NUMERIC(13,3)  		--//  ENCODE delta
	,vvmlb NUMERIC(13,3)  		--//  ENCODE delta
	,vvsal NUMERIC(13,2)  		--//  ENCODE delta
	,zplpr NUMERIC(11,2)  		--//  ENCODE delta
	,zplp1 NUMERIC(11,2)  		--//  ENCODE delta
	,zplp2 NUMERIC(11,2)  		--//  ENCODE delta
	,zplp3 NUMERIC(11,2)  		--//  ENCODE delta
	,zpld1 DATE  		--//  ENCODE delta
	,zpld2 DATE  		--//  ENCODE delta
	,zpld3 DATE  		--//  ENCODE delta
	,pperz NUMERIC(6,0)  		--//  ENCODE delta
	,pperl NUMERIC(6,0)  		--//  ENCODE delta
	,pperv NUMERIC(6,0)  		--//  ENCODE delta
	,kalkz VARCHAR(1)  		--//  ENCODE lzo
	,kalkl VARCHAR(1)  		--//  ENCODE lzo
	,kalkv VARCHAR(1)  		--//  ENCODE lzo
	,kalsc VARCHAR(6)  		--//  ENCODE lzo
	,xlifo VARCHAR(1)  		--//  ENCODE lzo
	,mypol VARCHAR(4)  		--//  ENCODE lzo
	,bwph1 NUMERIC(11,2)  		--//  ENCODE delta
	,bwps1 NUMERIC(11,2)  		--//  ENCODE delta
	,abwkz NUMERIC(2,0)  		--//  ENCODE delta
	,pstat VARCHAR(15)  		--//  ENCODE lzo
	,kaln1 NUMERIC(12,0)  		--//  ENCODE delta
	,kalnr NUMERIC(12,0)  		--//  ENCODE delta
	,bwva1 VARCHAR(3)  		--//  ENCODE lzo
	,bwva2 VARCHAR(3)  		--//  ENCODE lzo
	,bwva3 VARCHAR(3)  		--//  ENCODE lzo
	,vers1 NUMERIC(2,0)  		--//  ENCODE delta
	,vers2 NUMERIC(2,0)  		--//  ENCODE delta
	,vers3 NUMERIC(2,0)  		--//  ENCODE delta
	,hrkft VARCHAR(4)  		--//  ENCODE lzo
	,kosgr VARCHAR(10)  		--//  ENCODE lzo
	,pprdz NUMERIC(3,0)  		--//  ENCODE delta
	,pprdl NUMERIC(3,0)  		--//  ENCODE delta
	,pprdv NUMERIC(3,0)  		--//  ENCODE delta
	,pdatz NUMERIC(4,0)  		--//  ENCODE delta
	,pdatl NUMERIC(4,0)  		--//  ENCODE delta
	,pdatv NUMERIC(4,0)  		--//  ENCODE delta
	,ekalr VARCHAR(1)  		--//  ENCODE lzo
	,vplpr NUMERIC(11,2)  		--//  ENCODE delta
	,mlmaa VARCHAR(1)  		--//  ENCODE lzo
	,mlast VARCHAR(1)  		--//  ENCODE lzo
	,lplpr NUMERIC(11,2)  		--//  ENCODE delta
	,vksal NUMERIC(13,2)  		--//  ENCODE delta
	,hkmat VARCHAR(1)  		--//  ENCODE lzo
	,sperw VARCHAR(1)  		--//  ENCODE lzo
	,kziwl VARCHAR(3)  		--//  ENCODE lzo
	,wlinl DATE  		--//  ENCODE delta
	,abciw VARCHAR(1)  		--//  ENCODE lzo
	,bwspa NUMERIC(6,2)  		--//  ENCODE delta
	,lplpx NUMERIC(11,2)  		--//  ENCODE delta
	,vplpx NUMERIC(11,2)  		--//  ENCODE delta
	,fplpx NUMERIC(11,2)  		--//  ENCODE delta
	,lbwst VARCHAR(1)  		--//  ENCODE lzo
	,vbwst VARCHAR(1)  		--//  ENCODE lzo
	,fbwst VARCHAR(1)  		--//  ENCODE lzo
	,eklas VARCHAR(4)  		--//  ENCODE lzo
	,qklas VARCHAR(4)  		--//  ENCODE lzo
	,mtuse VARCHAR(1)  		--//  ENCODE lzo
	,mtorg VARCHAR(1)  		--//  ENCODE lzo
	,ownpr VARCHAR(1)  		--//  ENCODE lzo
	,xbewm VARCHAR(1)  		--//  ENCODE lzo
	,bwpei NUMERIC(5,0)  		--//  ENCODE delta
	,mbrue VARCHAR(1)  		--//  ENCODE lzo
	,oklas VARCHAR(4)  		--//  ENCODE lzo
	,oippinv VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (mandt, matnr, bwkey, bwtar)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_edw_material_dim_updt;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_EDW_MATERIAL_DIM_UPDT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_edw_material_dim_updt
(
	matl_num VARCHAR(18)  		--//  ENCODE lzo
	,base_uom VARCHAR(3)  		--//  ENCODE lzo
	,basic_matl VARCHAR(14)  		--//  ENCODE lzo
	,createdon DATE  		--//  ENCODE lzo
	,division VARCHAR(2)  		--//  ENCODE lzo
	,eanupc VARCHAR(18)  		--//  ENCODE lzo
	,gross_wt NUMERIC(17,0)  		--//  ENCODE lzo
	,logsys VARCHAR(10)  		--//  ENCODE lzo
	,manufactor VARCHAR(10)  		--//  ENCODE lzo
	,manu_matnr VARCHAR(40)  		--//  ENCODE lzo
	,matl_cat VARCHAR(2)  		--//  ENCODE lzo
	,matl_group VARCHAR(9)  		--//  ENCODE lzo
	,matl_type VARCHAR(256)  		--//  ENCODE lzo
	,net_weight NUMERIC(17,0)  		--//  ENCODE lzo
	,po_unit VARCHAR(3)  		--//  ENCODE lzo
	,prod_hier VARCHAR(18)  		--//  ENCODE lzo
	,rt_sups VARCHAR(1)  		--//  ENCODE lzo
	,size_dim VARCHAR(32)  		--//  ENCODE lzo
	,unit_dim VARCHAR(3)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(10)  		--//  ENCODE lzo
	,volume NUMERIC(17,0)  		--//  ENCODE lzo
	,volumeunit VARCHAR(10)  		--//  ENCODE lzo
	,height NUMERIC(17,0)  		--//  ENCODE lzo
	,lenght NUMERIC(17,0)  		--//  ENCODE lzo
	,width NUMERIC(17,0)  		--//  ENCODE lzo
	,bic_zz_mvgr1 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr2 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr3 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr4 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr5 VARCHAR(3)  		--//  ENCODE lzo
	,prodh1 VARCHAR(18)  		--//  ENCODE lzo
	,prodh2 VARCHAR(18)  		--//  ENCODE lzo
	,prodh3 VARCHAR(18)  		--//  ENCODE lzo
	,prodh4 VARCHAR(18)  		--//  ENCODE lzo
	,prodh5 VARCHAR(18)  		--//  ENCODE lzo
	,prodh6 VARCHAR(18)  		--//  ENCODE lzo
	,bic_zmerciapl VARCHAR(10)  		--//  ENCODE lzo
	,ch_on DATE  		--//  ENCODE lzo
	,createdby VARCHAR(100)  		--//  ENCODE lzo
	,datefrom DATE  		--//  ENCODE lzo
	,del_flag VARCHAR(1)  		--//  ENCODE lzo
	,ean_numtyp VARCHAR(10)  		--//  ENCODE lzo
	,bic_yuomcnvf NUMERIC(17,0)  		--//  ENCODE lzo
	,bic_ztragr VARCHAR(4)  		--//  ENCODE lzo
	,bic_zxchpf VARCHAR(1)  		--//  ENCODE lzo
	,bic_zpur_key VARCHAR(4)  		--//  ENCODE lzo
	,bic_zhaz_mat VARCHAR(18)  		--//  ENCODE lzo
	,bic_zstr_cond VARCHAR(2)  		--//  ENCODE lzo
	,bic_zmhdrz VARCHAR(4)  		--//  ENCODE lzo
	,bic_zmhdhb VARCHAR(4)  		--//  ENCODE lzo
	,bic_zqmpur VARCHAR(1)  		--//  ENCODE lzo
	,bic_zmstav VARCHAR(2)  		--//  ENCODE lzo
	,bic_zskutec VARCHAR(3)  		--//  ENCODE lzo
	,bic_zmjrtec VARCHAR(35)  		--//  ENCODE lzo
	,bic_zz_color NUMERIC(2,0)  		--//  ENCODE lzo
	,bic_zz_stack NUMERIC(2,0)  		--//  ENCODE lzo
	,bic_ztaklv VARCHAR(1)  		--//  ENCODE lzo
	,extmatlgrp VARCHAR(18)  		--//  ENCODE lzo
	,bic_zvpsta VARCHAR(15)  		--//  ENCODE lzo
	,bic_zpstat VARCHAR(15)  		--//  ENCODE lzo
	,bic_zoldmatl VARCHAR(18)  		--//  ENCODE lzo
	,std_descr VARCHAR(20)  		--//  ENCODE lzo
	,bic_zferth VARCHAR(18)  		--//  ENCODE lzo
	,bic_zmsource VARCHAR(10)  		--//  ENCODE lzo
	,bic_zbramin VARCHAR(6)  		--//  ENCODE lzo
	,bic_zprodseg VARCHAR(10)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_material_base;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MATERIAL_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_material_base
(
	matl_num VARCHAR(40) NOT NULL 		--//  ENCODE lzo
	,crt_on DATE  		--//  ENCODE az64
	,crt_by_nm VARCHAR(12)  		--//  ENCODE lzo
	,chg_dttm DATE  		--//  ENCODE az64
	,chg_by_nm VARCHAR(12)  		--//  ENCODE lzo
	,maint_sts_cmplt_matl VARCHAR(15)  		--//  ENCODE lzo
	,maint_sts VARCHAR(15)  		--//  ENCODE lzo
	,fl_matl_del_clnt_lvl VARCHAR(10)  		--//  ENCODE lzo
	,matl_type_cd VARCHAR(10)  		--//  ENCODE lzo
	,indstr_sectr VARCHAR(10)  		--//  ENCODE lzo
	,matl_grp_cd VARCHAR(20)  		--//  ENCODE lzo
	,old_matl_num VARCHAR(40)  		--//  ENCODE lzo
	,base_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,prch_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,doc_num VARCHAR(22)  		--//  ENCODE lzo
	,doc_type VARCHAR(10)  		--//  ENCODE lzo
	,doc_vers VARCHAR(10)  		--//  ENCODE lzo
	,pg_fmt__doc VARCHAR(10)  		--//  ENCODE lzo
	,doc_chg_num VARCHAR(20)  		--//  ENCODE lzo
	,pg_num_doc VARCHAR(10)  		--//  ENCODE lzo
	,num_sht numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdtn_memo_txt VARCHAR(40)  		--//  ENCODE lzo
	,pg_fmt_prdtn_memo VARCHAR(10)  		--//  ENCODE lzo
	,size_dims_txt VARCHAR(32)  		--//  ENCODE lzo
	,bsc_matl VARCHAR(100)  		--//  ENCODE lzo
	,indstr_std_desc VARCHAR(40)  		--//  ENCODE lzo
	,mercia_plan VARCHAR(10)  		--//  ENCODE lzo
	,prchsng_val_key VARCHAR(10)  		--//  ENCODE lzo
	,grs_wt_meas NUMERIC(13,3)  		--//  ENCODE az64
	,net_wt_meas NUMERIC(13,3)  		--//  ENCODE az64
	,wt_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,vol_meas NUMERIC(13,3)  		--//  ENCODE az64
	,vol_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,cntnr_rqr VARCHAR(10)  		--//  ENCODE lzo
	,strg_cond VARCHAR(10)  		--//  ENCODE lzo
	,temp_cond_ind VARCHAR(10)  		--//  ENCODE lzo
	,low_lvl_cd VARCHAR(10)  		--//  ENCODE lzo
	,trspn_grp VARCHAR(10)  		--//  ENCODE lzo
	,haz_matl_num VARCHAR(40)  		--//  ENCODE lzo
	,div VARCHAR(10)  		--//  ENCODE lzo
	,cmpt VARCHAR(10)  		--//  ENCODE lzo
	,ean_obsol VARCHAR(13)  		--//  ENCODE lzo
	,gr_prtd_qty NUMERIC(13,3)  		--//  ENCODE az64
	,prcmt_rule VARCHAR(10)  		--//  ENCODE lzo
	,src_supl VARCHAR(10)  		--//  ENCODE lzo
	,seasn_cat VARCHAR(10)  		--//  ENCODE lzo
	,lbl_type_cd VARCHAR(10)  		--//  ENCODE lzo
	,lbl_form VARCHAR(10)  		--//  ENCODE lzo
	,deact VARCHAR(10)  		--//  ENCODE lzo
	,prmry_upc_cd VARCHAR(40)  		--//  ENCODE lzo
	,ean_cat VARCHAR(10)  		--//  ENCODE lzo
	,lgth_meas NUMERIC(13,3)  		--//  ENCODE az64
	,wdth_meas NUMERIC(13,3)  		--//  ENCODE az64
	,hght_meas NUMERIC(13,3)  		--//  ENCODE az64
	,dim_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,prod_hier_cd VARCHAR(40)  		--//  ENCODE lzo
	,stk_tfr_chg_cost VARCHAR(10)  		--//  ENCODE lzo
	,cad_ind VARCHAR(10)  		--//  ENCODE lzo
	,qm_prcmt_act VARCHAR(10)  		--//  ENCODE lzo
	,allw_pkgng_wt NUMERIC(13,3)  		--//  ENCODE az64
	,wt_unit VARCHAR(10)  		--//  ENCODE lzo
	,allw_pkgng_vol NUMERIC(13,3)  		--//  ENCODE az64
	,vol_unit VARCHAR(10)  		--//  ENCODE lzo
	,exces_wt_tlrnc NUMERIC(3,1)  		--//  ENCODE az64
	,exces_vol_tlrnc NUMERIC(3,1)  		--//  ENCODE az64
	,var_prch_ord_unit VARCHAR(10)  		--//  ENCODE lzo
	,rvsn_lvl_asgn_matl VARCHAR(10)  		--//  ENCODE lzo
	,configurable_matl_ind VARCHAR(10)  		--//  ENCODE lzo
	,btch_mgmt_reqt_ind VARCHAR(10)  		--//  ENCODE lzo
	,pkgng_matl_type_cd VARCHAR(10)  		--//  ENCODE lzo
	,max_lvl_vol NUMERIC(3,0)  		--//  ENCODE az64
	,stack_fact numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pkgng_matl_grp VARCHAR(10)  		--//  ENCODE lzo
	,auth_grp VARCHAR(10)  		--//  ENCODE lzo
	,vld_from_dt DATE  		--//  ENCODE az64
	,del_dt DATE  		--//  ENCODE az64
	,seasn_yr VARCHAR(10)  		--//  ENCODE lzo
	,prc_bnd_cat VARCHAR(10)  		--//  ENCODE lzo
	,bill_of_matl VARCHAR(10)  		--//  ENCODE lzo
	,extrnl_matl_grp_txt VARCHAR(40)  		--//  ENCODE lzo
	,cross_plnt_cnfg_matl VARCHAR(40)  		--//  ENCODE lzo
	,matl_cat VARCHAR(10)  		--//  ENCODE lzo
	,matl_coprod_ind VARCHAR(10)  		--//  ENCODE lzo
	,fllp_matl_ind VARCHAR(10)  		--//  ENCODE lzo
	,prc_ref_matl VARCHAR(40)  		--//  ENCODE lzo
	,cros_plnt_matl_sts VARCHAR(10)  		--//  ENCODE lzo
	,cros_dstn_chn_matl_sts VARCHAR(10)  		--//  ENCODE lzo
	,cros_plnt_matl_sts_vld_dt DATE  		--//  ENCODE az64
	,chn_matl_vld_from_dt DATE  		--//  ENCODE az64
	,tax_clsn_matl VARCHAR(10)  		--//  ENCODE lzo
	,catlg_prfl VARCHAR(20)  		--//  ENCODE lzo
	,min_rmn_shlf_lif NUMERIC(4,0)  		--//  ENCODE az64
	,tot_shlf_lif NUMERIC(4,0)  		--//  ENCODE az64
	,strg_pct NUMERIC(3,0)  		--//  ENCODE az64
	,cntnt_uom_cd VARCHAR(10)  		--//  ENCODE lzo
	,net_cntnt_meas NUMERIC(13,3)  		--//  ENCODE az64
	,cmpr_prc_unit NUMERIC(5,0)  		--//  ENCODE az64
	,isr_matl_grp VARCHAR(40)  		--//  ENCODE lzo
	,grs_cntnt_meas NUMERIC(13,3)  		--//  ENCODE az64
	,qty_conv_meth VARCHAR(10)  		--//  ENCODE lzo
	,intrnl_obj_num numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,envmt_rlvnt VARCHAR(10)  		--//  ENCODE lzo
	,prod_allc_dtrmn_proc VARCHAR(40)  		--//  ENCODE lzo
	,prc_prfl_vrnt VARCHAR(10)  		--//  ENCODE lzo
	,matl_qual_disc VARCHAR(10)  		--//  ENCODE lzo
	,mfr_part_num VARCHAR(40)  		--//  ENCODE lzo
	,mfr_num VARCHAR(10)  		--//  ENCODE lzo
	,intrnl_inv_mgmt VARCHAR(40)  		--//  ENCODE lzo
	,mfr_part_prfl VARCHAR(10)  		--//  ENCODE lzo
	,meas_usg_unit VARCHAR(10)  		--//  ENCODE lzo
	,rollout_seasn VARCHAR(10)  		--//  ENCODE lzo
	,dngrs_goods_ind_prof VARCHAR(10)  		--//  ENCODE lzo
	,hi_viscous_ind VARCHAR(10)  		--//  ENCODE lzo
	,in_bulk_lqd_ind VARCHAR(10)  		--//  ENCODE lzo
	,lvl_explc_ser_num VARCHAR(10)  		--//  ENCODE lzo
	,pkgng_matl_clse_pkgng VARCHAR(10)  		--//  ENCODE lzo
	,appr_btch_rec_ind VARCHAR(10)  		--//  ENCODE lzo
	,ovrd_chg_num VARCHAR(10)  		--//  ENCODE lzo
	,matl_cmplt_lvl numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,per_ind_shlf_lif_expn_dt VARCHAR(10)  		--//  ENCODE lzo
	,rd_rule_sled VARCHAR(10)  		--//  ENCODE lzo
	,prod_cmpos_prtd_pkgng VARCHAR(10)  		--//  ENCODE lzo
	,genl_item_cat_grp VARCHAR(10)  		--//  ENCODE lzo
	,gn_matl_logl_vrnt VARCHAR(10)  		--//  ENCODE lzo
	,prod_base VARCHAR(10)  		--//  ENCODE lzo
	,vrnt VARCHAR(10)  		--//  ENCODE lzo
	,put_up VARCHAR(10)  		--//  ENCODE lzo
	,mega_brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,brnd_cd VARCHAR(10)  		--//  ENCODE lzo
	,tech VARCHAR(10)  		--//  ENCODE lzo
	,color VARCHAR(10)  		--//  ENCODE lzo
	,seasonality VARCHAR(10)  		--//  ENCODE lzo
	,mfg_src_cd VARCHAR(10)  		--//  ENCODE lzo
	,mfr_part_num_new VARCHAR(256)  		--//  ENCODE lzo
	,formulation_num VARCHAR(50)  		--//  ENCODE lzo
	,pka_franchise_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_franchise_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_brand_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_brand_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sub_brand_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_sub_brand_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_variant_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_variant_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sub_variant_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_sub_variant_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_flavor_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_flavor_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_ingredient_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_ingredient_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_application_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_application_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_length_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_length_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_shape_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_shape_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_spf_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_spf_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_cover_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_cover_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_form_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_form_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_size_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_size_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_character_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_character_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_package_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_package_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_attribute_13_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_attribute_13_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_attribute_14_cd VARCHAR(4)  		--//  ENCODE lzo
	,pka_attribute_14_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_sku_identification_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_sku_identification_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_one_time_relabeling_cd VARCHAR(2)  		--//  ENCODE lzo
	,pka_one_time_relabeling_desc VARCHAR(30)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,pka_product_key_description_2 VARCHAR(255)  		--//  ENCODE lzo
	,pka_root_code VARCHAR(68)  		--//  ENCODE lzo
	,pka_root_code_desc_1 VARCHAR(255)  		--//  ENCODE lzo
	,pka_root_code_desc_2 VARCHAR(255)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.itg_material_dim;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MATERIAL_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_material_dim
(
	matl_num VARCHAR(18) NOT NULL 		--//  ENCODE lzo
	,base_uom VARCHAR(3)  		--//  ENCODE lzo
	,basic_matl VARCHAR(14)  		--//  ENCODE lzo
	,createdon DATE  		--//  ENCODE delta
	,division VARCHAR(2)  		--//  ENCODE lzo
	,eanupc VARCHAR(18)  		--//  ENCODE lzo
	,gross_wt NUMERIC(17,0)  		--//  ENCODE delta
	,logsys VARCHAR(10)  		--//  ENCODE lzo
	,manufactor VARCHAR(10)  		--//  ENCODE lzo
	,manu_matnr VARCHAR(40)  		--//  ENCODE lzo
	,matl_cat VARCHAR(2)  		--//  ENCODE lzo
	,matl_group VARCHAR(9)  		--//  ENCODE lzo
	,matl_type VARCHAR(256)  		--//  ENCODE lzo
	,net_weight NUMERIC(17,0)  		--//  ENCODE delta
	,po_unit VARCHAR(3)  		--//  ENCODE lzo
	,prod_hier VARCHAR(18)  		--//  ENCODE lzo
	,rt_sups VARCHAR(1)  		--//  ENCODE lzo
	,size_dim VARCHAR(32)  		--//  ENCODE lzo
	,unit_dim VARCHAR(3)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(10)  		--//  ENCODE lzo
	,volume NUMERIC(17,0)  		--//  ENCODE delta
	,volumeunit VARCHAR(10)  		--//  ENCODE lzo
	,height NUMERIC(17,0)  		--//  ENCODE delta
	,lenght NUMERIC(17,0)  		--//  ENCODE delta
	,width NUMERIC(17,0)  		--//  ENCODE delta
	,bic_zz_mvgr1 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr2 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr3 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr4 VARCHAR(3)  		--//  ENCODE lzo
	,bic_zz_mvgr5 VARCHAR(3)  		--//  ENCODE lzo
	,prodh1 VARCHAR(18)  		--//  ENCODE lzo
	,prodh2 VARCHAR(18)  		--//  ENCODE lzo
	,prodh3 VARCHAR(18)  		--//  ENCODE lzo
	,prodh4 VARCHAR(18)  		--//  ENCODE lzo
	,prodh5 VARCHAR(18)  		--//  ENCODE lzo
	,prodh6 VARCHAR(18)  		--//  ENCODE lzo
	,bic_zmerciapl VARCHAR(10)  		--//  ENCODE lzo
	,ch_on DATE  		--//  ENCODE delta
	,createdby VARCHAR(100)  		--//  ENCODE lzo
	,datefrom DATE  		--//  ENCODE delta
	,del_flag VARCHAR(1)  		--//  ENCODE lzo
	,ean_numtyp VARCHAR(10)  		--//  ENCODE lzo
	,bic_yuomcnvf NUMERIC(17,0)  		--//  ENCODE delta
	,bic_ztragr VARCHAR(4)  		--//  ENCODE lzo
	,bic_zxchpf VARCHAR(1)  		--//  ENCODE lzo
	,bic_zpur_key VARCHAR(4)  		--//  ENCODE lzo
	,bic_zhaz_mat VARCHAR(18)  		--//  ENCODE lzo
	,bic_zstr_cond VARCHAR(2)  		--//  ENCODE lzo
	,bic_zmhdrz VARCHAR(4)  		--//  ENCODE lzo
	,bic_zmhdhb VARCHAR(4)  		--//  ENCODE lzo
	,bic_zqmpur VARCHAR(1)  		--//  ENCODE lzo
	,bic_zmstav VARCHAR(2)  		--//  ENCODE lzo
	,bic_zskutec VARCHAR(3)  		--//  ENCODE lzo
	,bic_zmjrtec VARCHAR(35)  		--//  ENCODE lzo
	,bic_zz_color NUMERIC(2,0)  		--//  ENCODE delta
	,bic_zz_stack NUMERIC(2,0)  		--//  ENCODE delta
	,bic_ztaklv VARCHAR(1)  		--//  ENCODE lzo
	,extmatlgrp VARCHAR(18)  		--//  ENCODE lzo
	,bic_zvpsta VARCHAR(15)  		--//  ENCODE lzo
	,bic_zpstat VARCHAR(15)  		--//  ENCODE lzo
	,bic_zoldmatl VARCHAR(18)  		--//  ENCODE lzo
	,std_descr VARCHAR(20)  		--//  ENCODE lzo
	,bic_zferth VARCHAR(18)  		--//  ENCODE lzo
	,bic_zmsource VARCHAR(10)  		--//  ENCODE lzo
	,bic_zbramin VARCHAR(6)  		--//  ENCODE lzo
	,bic_zprodseg VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (matl_num)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_material_typ;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MATERIAL_TYP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_material_typ
(
	matl_type VARCHAR(4) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (matl_type, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_matl_base;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MATL_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_matl_base
(
	clnt VARCHAR(10)  		--//  ENCODE zstd
	,matl_num VARCHAR(40) NOT NULL 		--//  ENCODE zstd
	,crt_on DATE  		--//  ENCODE zstd
	,crt_by_nm VARCHAR(12)  		--//  ENCODE zstd
	,chg_dttm DATE  		--//  ENCODE zstd
	,chg_by_nm VARCHAR(12)  		--//  ENCODE bytedict
	,maint_sts_cmplt_matl VARCHAR(15)  		--//  ENCODE zstd
	,maint_sts VARCHAR(15)  		--//  ENCODE zstd
	,fl_matl_del_clnt_lvl VARCHAR(10)  		--//  ENCODE zstd
	,matl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,indstr_sectr VARCHAR(10)  		--//  ENCODE zstd
	,matl_grp_cd VARCHAR(20)  		--//  ENCODE zstd
	,old_matl_num VARCHAR(40)  		--//  ENCODE zstd
	,base_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,prch_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,doc_num VARCHAR(22)  		--//  ENCODE zstd
	,doc_type VARCHAR(10)  		--//  ENCODE zstd
	,doc_vers VARCHAR(10)  		--//  ENCODE zstd
	,pg_fmt__doc VARCHAR(10)  		--//  ENCODE zstd
	,doc_chg_num VARCHAR(20)  		--//  ENCODE zstd
	,pg_num_doc VARCHAR(10)  		--//  ENCODE zstd
	,num_sht numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdtn_memo_txt VARCHAR(40)  		--//  ENCODE zstd
	,pg_fmt_prdtn_memo VARCHAR(10)  		--//  ENCODE zstd
	,size_dims_txt VARCHAR(32)  		--//  ENCODE zstd
	,bsc_matl VARCHAR(100)  		--//  ENCODE zstd
	,indstr_std_desc VARCHAR(40)  		--//  ENCODE zstd
	,mercia_plan VARCHAR(10)  		--//  ENCODE zstd
	,prchsng_val_key VARCHAR(10)  		--//  ENCODE zstd
	,grs_wt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,net_wt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,wt_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,vol_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,vol_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,cntnr_rqr VARCHAR(10)  		--//  ENCODE zstd
	,strg_cond VARCHAR(10)  		--//  ENCODE zstd
	,temp_cond_ind VARCHAR(10)  		--//  ENCODE zstd
	,low_lvl_cd VARCHAR(10)  		--//  ENCODE zstd
	,trspn_grp VARCHAR(10)  		--//  ENCODE zstd
	,haz_matl_num VARCHAR(40)  		--//  ENCODE zstd
	,div VARCHAR(10)  		--//  ENCODE zstd
	,cmpt VARCHAR(10)  		--//  ENCODE zstd
	,ean_obsol VARCHAR(13)  		--//  ENCODE zstd
	,gr_prtd_qty NUMERIC(13,3)  		--//  ENCODE zstd
	,prcmt_rule VARCHAR(10)  		--//  ENCODE zstd
	,src_supl VARCHAR(10)  		--//  ENCODE zstd
	,seasn_cat VARCHAR(10)  		--//  ENCODE zstd
	,lbl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,lbl_form VARCHAR(10)  		--//  ENCODE zstd
	,deact VARCHAR(10)  		--//  ENCODE zstd
	,prmry_upc_cd VARCHAR(40)  		--//  ENCODE zstd
	,ean_cat VARCHAR(10)  		--//  ENCODE zstd
	,lgth_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,wdth_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,hght_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,dim_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,prod_hier_cd VARCHAR(40)  		--//  ENCODE zstd
	,stk_tfr_chg_cost VARCHAR(10)  		--//  ENCODE zstd
	,cad_ind VARCHAR(10)  		--//  ENCODE zstd
	,qm_prcmt_act VARCHAR(10)  		--//  ENCODE zstd
	,allw_pkgng_wt NUMERIC(13,3)  		--//  ENCODE zstd
	,wt_unit VARCHAR(10)  		--//  ENCODE zstd
	,allw_pkgng_vol NUMERIC(13,3)  		--//  ENCODE zstd
	,vol_unit VARCHAR(10)  		--//  ENCODE zstd
	,exces_wt_tlrnc NUMERIC(3,1)  		--//  ENCODE zstd
	,exces_vol_tlrnc NUMERIC(3,1)  		--//  ENCODE zstd
	,var_prch_ord_unit VARCHAR(10)  		--//  ENCODE zstd
	,rvsn_lvl_asgn_matl VARCHAR(10)  		--//  ENCODE zstd
	,configurable_matl_ind VARCHAR(10)  		--//  ENCODE zstd
	,btch_mgmt_reqt_ind VARCHAR(10)  		--//  ENCODE zstd
	,pkgng_matl_type_cd VARCHAR(10)  		--//  ENCODE zstd
	,max_lvl_vol NUMERIC(3,0)  		--//  ENCODE zstd
	,stack_fact numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,pkgng_matl_grp VARCHAR(10)  		--//  ENCODE zstd
	,auth_grp VARCHAR(10)  		--//  ENCODE zstd
	,vld_from_dt DATE  		--//  ENCODE zstd
	,del_dt DATE  		--//  ENCODE zstd
	,seasn_yr VARCHAR(10)  		--//  ENCODE zstd
	,prc_bnd_cat VARCHAR(10)  		--//  ENCODE zstd
	,bill_of_matl VARCHAR(10)  		--//  ENCODE zstd
	,extrnl_matl_grp_txt VARCHAR(40)  		--//  ENCODE zstd
	,cross_plnt_cnfg_matl VARCHAR(40)  		--//  ENCODE zstd
	,matl_cat VARCHAR(10)  		--//  ENCODE zstd
	,matl_coprod_ind VARCHAR(10)  		--//  ENCODE zstd
	,fllp_matl_ind VARCHAR(10)  		--//  ENCODE zstd
	,prc_ref_matl VARCHAR(40)  		--//  ENCODE zstd
	,cros_plnt_matl_sts VARCHAR(10)  		--//  ENCODE zstd
	,cros_dstn_chn_matl_sts VARCHAR(10)  		--//  ENCODE zstd
	,cros_plnt_matl_sts_vld_dt DATE  		--//  ENCODE zstd
	,chn_matl_vld_from_dt DATE  		--//  ENCODE zstd
	,tax_clsn_matl VARCHAR(10)  		--//  ENCODE zstd
	,catlg_prfl VARCHAR(20)  		--//  ENCODE zstd
	,min_rmn_shlf_lif NUMERIC(4,0)  		--//  ENCODE zstd
	,tot_shlf_lif NUMERIC(4,0)  		--//  ENCODE zstd
	,strg_pct NUMERIC(3,0)  		--//  ENCODE zstd
	,cntnt_uom_cd VARCHAR(10)  		--//  ENCODE zstd
	,net_cntnt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,cmpr_prc_unit NUMERIC(5,0)  		--//  ENCODE zstd
	,isr_matl_grp VARCHAR(40)  		--//  ENCODE zstd
	,grs_cntnt_meas NUMERIC(13,3)  		--//  ENCODE zstd
	,qty_conv_meth VARCHAR(10)  		--//  ENCODE zstd
	,intrnl_obj_num numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,envmt_rlvnt VARCHAR(10)  		--//  ENCODE zstd
	,prod_allc_dtrmn_proc VARCHAR(40)  		--//  ENCODE zstd
	,prc_prfl_vrnt VARCHAR(10)  		--//  ENCODE zstd
	,matl_qual_disc VARCHAR(10)  		--//  ENCODE zstd
	,mfr_part_num VARCHAR(40)  		--//  ENCODE zstd
	,mfr_num VARCHAR(10)  		--//  ENCODE zstd
	,intrnl_inv_mgmt VARCHAR(40)  		--//  ENCODE zstd
	,mfr_part_prfl VARCHAR(10)  		--//  ENCODE zstd
	,meas_usg_unit VARCHAR(10)  		--//  ENCODE zstd
	,rollout_seasn VARCHAR(10)  		--//  ENCODE zstd
	,dngrs_goods_ind_prof VARCHAR(10)  		--//  ENCODE zstd
	,hi_viscous_ind VARCHAR(10)  		--//  ENCODE zstd
	,in_bulk_lqd_ind VARCHAR(10)  		--//  ENCODE zstd
	,lvl_explc_ser_num VARCHAR(10)  		--//  ENCODE zstd
	,pkgng_matl_clse_pkgng VARCHAR(10)  		--//  ENCODE zstd
	,appr_btch_rec_ind VARCHAR(10)  		--//  ENCODE zstd
	,ovrd_chg_num VARCHAR(10)  		--//  ENCODE zstd
	,matl_cmplt_lvl numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,per_ind_shlf_lif_expn_dt VARCHAR(10)  		--//  ENCODE zstd
	,rd_rule_sled VARCHAR(10)  		--//  ENCODE zstd
	,prod_cmpos_prtd_pkgng VARCHAR(10)  		--//  ENCODE zstd
	,genl_item_cat_grp VARCHAR(10)  		--//  ENCODE zstd
	,gn_matl_logl_vrnt VARCHAR(10)  		--//  ENCODE zstd
	,prod_base VARCHAR(10)  		--//  ENCODE zstd
	,vrnt VARCHAR(10)  		--//  ENCODE zstd
	,put_up VARCHAR(10)  		--//  ENCODE zstd
	,mega_brand_cd VARCHAR(10)  		--//  ENCODE zstd
	,brand_cd VARCHAR(10)  		--//  ENCODE zstd
	,tech VARCHAR(10)  		--//  ENCODE zstd
	,color VARCHAR(10)  		--//  ENCODE zstd
	,seasonality VARCHAR(10)  		--//  ENCODE zstd
	,mfg_src_cd VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,PRIMARY KEY (matl_num)
)
		--// DISTSTYLE ALL
;
--DROP TABLE ASPITG_INTEGRATION.itg_matl_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MATL_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_matl_text
(
	matl_no VARCHAR(18) NOT NULL
	,lang_key VARCHAR(1)
	,matl_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (matl_no)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_mds_ap_company_dim;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MDS_AP_COMPANY_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_mds_ap_company_dim
(
	name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,ctry_key VARCHAR(200)  		--//  ENCODE lzo
	,ctry_group VARCHAR(200)  		--//  ENCODE lzo
	,cluster VARCHAR(200)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.itg_mega_brnd_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_MEGA_BRND_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_mega_brnd_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,mega_brnd VARCHAR(3) NOT NULL
	,de VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, mega_brnd)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_needstates_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_NEEDSTATES_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_needstates_text
(
	need_states numeric(18,0)		--// INTEGER
	,language_key VARCHAR(1)
	,short_desc VARCHAR(40)
	,medium_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_prft_ctr;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_PRFT_CTR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_prft_ctr
(
	cntl_area VARCHAR(4) NOT NULL
	,prft_ctr VARCHAR(10) NOT NULL
	,vld_to_dt DATE
	,vld_from_dt DATE
	,prsn_resp VARCHAR(20)
	,crncy_key VARCHAR(5)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,strng_hold numeric(18,0)		--// INTEGER
	,need_stat numeric(18,0)		--// INTEGER
	,PRIMARY KEY (cntl_area, prft_ctr)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_prft_ctr_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_PRFT_CTR_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_prft_ctr_text
(
	lang_key VARCHAR(4)
	,cntl_area VARCHAR(10)
	,prft_ctr VARCHAR(40)
	,vld_to_dt DATE
	,vld_from_dt DATE
	,shrt_desc VARCHAR(20)
	,med_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_prod_hier;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_prod_hier
(
	prod_hier VARCHAR(18) NOT NULL 		--//  ENCODE lzo
	,langu VARCHAR(1) NOT NULL 		--//  ENCODE lzo
	,txtmd VARCHAR(40)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,PRIMARY KEY (prod_hier, langu)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_put_up_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_PUT_UP_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_put_up_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,put_up VARCHAR(3) NOT NULL
	,put_up_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, put_up)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_sls_grp_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SLS_GRP_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sls_grp_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,sls_grp VARCHAR(3) NOT NULL
	,de VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, sls_grp)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_sls_off_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SLS_OFF_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sls_off_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,sls_off VARCHAR(4) NOT NULL
	,de VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, sls_off)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_sls_org;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SLS_ORG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sls_org
(
	clnt numeric(18,0)		--// INTEGER
	,sls_org VARCHAR(4) NOT NULL
	,stats_crncy VARCHAR(5)
	,sls_org_co_cd VARCHAR(4)
	,cust_no_intco_bill VARCHAR(10)
	,ctry_key VARCHAR(3)
	,crncy_key VARCHAR(5)
	,fisc_yr_vrnt VARCHAR(2)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (sls_org)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_sls_org_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SLS_ORG_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sls_org_text
(
	clnt VARCHAR(3)
	,lang_key VARCHAR(1)
	,sls_org VARCHAR(4)
	,sls_org_nm VARCHAR(20)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_strongholds_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_STRONGHOLDS_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_strongholds_text
(
	strongholds numeric(18,0)		--// INTEGER
	,language_key VARCHAR(1)
	,short_desc VARCHAR(40)
	,medium_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_time;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_TIME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_time
(
	cal_day DATE NOT NULL
	,fisc_yr_vrnt VARCHAR(2) NOT NULL
	,wkday numeric(18,0)		--// INTEGER
	,cal_wk numeric(18,0)		--// INTEGER
	,cal_mo_1 numeric(18,0)		--// INTEGER
	,cal_mo_2 numeric(18,0)		--// INTEGER
	,cal_qtr_1 numeric(18,0)		--// INTEGER
	,cal_qtr_2 numeric(18,0)		--// INTEGER
	,half_yr numeric(18,0)		--// INTEGER
	,cal_yr numeric(18,0)		--// INTEGER
	,fisc_per numeric(18,0)		--// INTEGER
	,pstng_per numeric(18,0)		--// INTEGER
	,fisc_yr numeric(18,0)		--// INTEGER
	,rec_mode VARCHAR(1)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (cal_day, fisc_yr_vrnt)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.itg_varnt_text;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_VARNT_TEXT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_varnt_text
(
	clnt numeric(18,0) NOT NULL		--// INTEGER 
	,lang_key VARCHAR(1) NOT NULL
	,varnt VARCHAR(3) NOT NULL
	,varnt_desc VARCHAR(40)
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE
	,PRIMARY KEY (clnt, lang_key, varnt)
)
		--// DISTSTYLE EVEN
;


------------------------------------------------------------
create or replace TABLE OSEITG_INTEGRATION.ITG_TH_POS_SALES_INVENTORY_FACT (
	DATA_SET VARCHAR(20),
	CUSTOMER VARCHAR(20),
	SOLD_TO_CODE VARCHAR(6),
	SAP_PRNT_CUST_KEY VARCHAR(12),
	SAP_PRNT_CUST_DESC VARCHAR(50),
	SUPPLIER_CODE VARCHAR(20),
	TRANS_DT DATE,
	MATERIAL_NUMBER VARCHAR(20),
	MATERIAL_NAME VARCHAR(500),
	BAR_CODE VARCHAR(20),
	BRANCH_CODE VARCHAR(20),
	BRANCH_NAME VARCHAR(200),
	INVENTORY_QTY_SOURCE NUMBER(17,4),
	SALES_QTY_SOURCE NUMBER(17,4),
	RETAILER_UNIT_CONVERSION NUMBER(31,0),
	INVENTORY_QTY_CONVERTED NUMBER(17,4),
	SALES_QTY_CONVERTED NUMBER(17,4),
	LIST_PRICE NUMBER(20,4),
	INVENTORY_GTS NUMBER(20,4),
	SALES_GTS NUMBER(20,4),
	CUSTOMER_RSP NUMBER(20,4),
	INVENTORY_BAHT NUMBER(20,4),
	SALES_BAHT NUMBER(20,4),
	FOC_PRODUCT VARCHAR(1)
);

create or replace TABLE OSEITG_INTEGRATION.ITG_PH_POS_SALES_FACT (
	CUST_CD VARCHAR(30),
	JJ_MNTH_ID VARCHAR(30),
	ITEM_CD VARCHAR(30),
	BRNCH_CD VARCHAR(50),
	POS_QTY NUMBER(20,4),
	POS_GTS NUMBER(20,4),
	POS_ITEM_PRC NUMBER(20,4),
	POS_TAX NUMBER(20,4),
	POS_NTS NUMBER(20,4),
	CONV_FACTOR NUMBER(20,4),
	JJ_QTY_PC NUMBER(20,4),
	JJ_ITEM_PRC_PER_PC NUMBER(20,4),
	JJ_GTS NUMBER(20,4),
	JJ_VAT_AMT NUMBER(20,4),
	JJ_NTS NUMBER(20,4),
	FILE_NM VARCHAR(150),
	CDL_DTTM VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE SGPITG_INTEGRATION.SDL_RAW_SG_ZUELLIG_SELLOUT (
	MONTH_NO VARCHAR(255),
	SALES_DATE VARCHAR(255),
	WAREHOUSE_CODE VARCHAR(255),
	CUSTOMER_CODE VARCHAR(255),
	CUSTOMER_NAME VARCHAR(255),
	INVOICE VARCHAR(255),
	ITEM_NAME VARCHAR(255),
	ITEM_CODE VARCHAR(255),
	TYPE VARCHAR(255),
	SALES_VALUE NUMBER(17,3),
	SALES_UNITS NUMBER(17,3),
	BONUS_UNITS NUMBER(17,3),
	RETURNS_UNITS NUMBER(17,3),
	RETURNS_VALUE NUMBER(17,3),
	RETURNS_BONUS_UNITS NUMBER(17,3),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);

create or replace TABLE SGPITG_INTEGRATION.SDL_RAW_SG_ZUELLIG_CUSTOMER_MAPPING (
	ZUELLIG_CUSTOMER VARCHAR(255),
	REGIONAL_BANNER VARCHAR(255),
	MERCHANDIZING VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);

create or replace TABLE SGPITG_INTEGRATION.SDL_RAW_SG_ZUELLIG_PRODUCT_MAPPING (
	ZP_MATERIAL VARCHAR(255),
	ZP_ITEM_CODE VARCHAR(255),
	JJ_CODE VARCHAR(255),
	ITEM_NAME VARCHAR(255),
	BRAND VARCHAR(255),
	CDL_DTTM VARCHAR(255),
	CURR_DT TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);

--DROP TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_cnsc;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_ACCOUNT_ATTR_CIW (		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_account_attr_ciw (
    chrt_accts varchar(10),		--//  ENCODE lzo // character varying
    account varchar(10),		--//  ENCODE lzo // character varying
    objvers varchar(1),		--//  ENCODE lzo // character varying
    changed varchar(1),		--//  ENCODE lzo // character varying
    bal_flag varchar(1),		--//  ENCODE lzo // character varying
    cstel_flag varchar(1),		--//  ENCODE lzo // character varying
    glacc_flag varchar(1),		--//  ENCODE lzo // character varying
    logsys varchar(10),		--//  ENCODE lzo // character varying
    sem_posit varchar(16),		--//  ENCODE lzo // character varying
    zbravol1 varchar(30),		--//  ENCODE lzo // character varying
    zbravol2 varchar(30),		--//  ENCODE lzo // character varying
    zbravol3 varchar(30),		--//  ENCODE lzo // character varying
    zbravol4 varchar(30),		--//  ENCODE lzo // character varying
    zbravol5 varchar(30),		--//  ENCODE lzo // character varying
    zbravol6 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl1 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl2 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl3 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl4 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl5 varchar(30),		--//  ENCODE lzo // character varying
    zciwhl6 varchar(30),		--//  ENCODE lzo // character varying
    zpb_gl_ty varchar(40),		--//  ENCODE lzo // character varying
    filename varchar(100),		--//  ENCODE lzo // character varying
    run_id varchar(100),		--//  ENCODE lzo // character varying
    crtd_dttm timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_billing_condition;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BILLING_CONDITION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_billing_condition
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
	,knval NUMERIC(17,3)  		--//  ENCODE az64
	,kprice NUMERIC(17,3)  		--//  ENCODE az64
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
	,inv_qty NUMERIC(17,3)  		--//  ENCODE az64
	,forwagent VARCHAR(10)  		--//  ENCODE lzo
	,salesemply VARCHAR(8)  		--//  ENCODE lzo
	,sales_unit VARCHAR(3)  		--//  ENCODE lzo
	,kappl VARCHAR(2)  		--//  ENCODE lzo
	,acrn_id VARCHAR(2)  		--//  ENCODE lzo
	,recordmode VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,source_file_name VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_billing;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BW_BILLING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_billing
(
	bill_num VARCHAR(255)  		--//  ENCODE lzo
	,bill_item VARCHAR(255)  		--//  ENCODE lzo
	,bill_dt VARCHAR(255)  		--//  ENCODE lzo
	,bill_type VARCHAR(255)  		--//  ENCODE lzo
	,sold_to VARCHAR(255)  		--//  ENCODE lzo
	,rt_promo VARCHAR(255)  		--//  ENCODE lzo
	,s_ord_item VARCHAR(255)  		--//  ENCODE lzo
	,doc_num VARCHAR(255)  		--//  ENCODE lzo
	,grs_wgt_dl VARCHAR(255)  		--//  ENCODE lzo
	,inv_qty VARCHAR(255)  		--//  ENCODE lzo
	,bill_qty VARCHAR(255)  		--//  ENCODE lzo
	,base_uom VARCHAR(255)  		--//  ENCODE lzo
	,exchg_rate VARCHAR(255)  		--//  ENCODE lzo
	,req_qty VARCHAR(255)  		--//  ENCODE lzo
	,sls_unit VARCHAR(255)  		--//  ENCODE lzo
	,payer VARCHAR(255)  		--//  ENCODE lzo
	,rebate_bas VARCHAR(255)  		--//  ENCODE lzo
	,no_inv_it VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_1 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_3 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_4 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_2 VARCHAR(255)  		--//  ENCODE lzo
	,netval_inv VARCHAR(255)  		--//  ENCODE lzo
	,exchg_stat VARCHAR(255)  		--//  ENCODE lzo
	,zblqtycse VARCHAR(255)  		--//  ENCODE lzo
	,exratexacc VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_6 VARCHAR(255)  		--//  ENCODE lzo
	,gross_val VARCHAR(255)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_5 VARCHAR(255)  		--//  ENCODE lzo
	,numerator VARCHAR(255)  		--//  ENCODE lzo
	,cost VARCHAR(255)  		--//  ENCODE lzo
	,plant VARCHAR(255)  		--//  ENCODE lzo
	,volume_dl VARCHAR(255)  		--//  ENCODE lzo
	,loc_currcy VARCHAR(255)  		--//  ENCODE lzo
	,denomintr VARCHAR(255)  		--//  ENCODE lzo
	,volume_unit VARCHAR(255)  		--//  ENCODE lzo
	,scale_qty VARCHAR(255)  		--//  ENCODE lzo
	,cshdsc_bas VARCHAR(255)  		--//  ENCODE lzo
	,net_wgt_dl VARCHAR(255)  		--//  ENCODE lzo
	,tax_amt VARCHAR(255)  		--//  ENCODE lzo
	,rate_type VARCHAR(255)  		--//  ENCODE lzo
	,sls_org VARCHAR(255)  		--//  ENCODE lzo
	,exrate_acc VARCHAR(255)  		--//  ENCODE lzo
	,distr_chnl VARCHAR(255)  		--//  ENCODE lzo
	,doc_currcy VARCHAR(255)  		--//  ENCODE lzo
	,co_area VARCHAR(255)  		--//  ENCODE lzo
	,doc_categ VARCHAR(255)  		--//  ENCODE lzo
	,fisc_varnt VARCHAR(255)  		--//  ENCODE lzo
	,cost_center VARCHAR(255)  		--//  ENCODE lzo
	,matl_group VARCHAR(255)  		--//  ENCODE lzo
	,division VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,sls_grp VARCHAR(255)  		--//  ENCODE lzo
	,div_head VARCHAR(255)  		--//  ENCODE lzo
	,ship_point VARCHAR(255)  		--//  ENCODE lzo
	,wbs_elemt VARCHAR(255)  		--//  ENCODE lzo
	,bill_rule VARCHAR(255)  		--//  ENCODE lzo
	,bwapplnm VARCHAR(255)  		--//  ENCODE lzo
	,process_key VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp VARCHAR(255)  		--//  ENCODE lzo
	,sls_off VARCHAR(255)  		--//  ENCODE lzo
	,refer_itm VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_3 VARCHAR(255)  		--//  ENCODE lzo
	,price_dt VARCHAR(255)  		--//  ENCODE lzo
	,sls_emply VARCHAR(255)  		--//  ENCODE lzo
	,refer_doc VARCHAR(255)  		--//  ENCODE lzo
	,st_up_dte VARCHAR(255)  		--//  ENCODE lzo
	,stat_date VARCHAR(255)  		--//  ENCODE lzo
	,item_categ VARCHAR(255)  		--//  ENCODE lzo
	,prov_grp VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_5 VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier VARCHAR(255)  		--//  ENCODE lzo
	,itm_type VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_4 VARCHAR(255)  		--//  ENCODE lzo
	,ship_to VARCHAR(255)  		--//  ENCODE lzo
	,bill_to_prty VARCHAR(255)  		--//  ENCODE lzo
	,rebate_grp VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_2 VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_1 VARCHAR(255)  		--//  ENCODE lzo
	,eanupc VARCHAR(255)  		--//  ENCODE lzo
	,mat_entrd VARCHAR(255)  		--//  ENCODE lzo
	,batch VARCHAR(255)  		--//  ENCODE lzo
	,stor_loc VARCHAR(255)  		--//  ENCODE lzo
	,created_on VARCHAR(255)  		--//  ENCODE lzo
	,serv_date VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp5 VARCHAR(255)  		--//  ENCODE lzo
	,sls_deal VARCHAR(255)  		--//  ENCODE lzo
	,bill_cat VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp1 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp3 VARCHAR(255)  		--//  ENCODE lzo
	,trans_dt VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp4 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp2 VARCHAR(255)  		--//  ENCODE lzo
	,stat_curr VARCHAR(255)  		--//  ENCODE lzo
	,ch_on VARCHAR(255)  		--//  ENCODE lzo
	,comp_cd VARCHAR(255)  		--//  ENCODE lzo
	,sls_dist VARCHAR(255)  		--//  ENCODE lzo
	,stor_no VARCHAR(255)  		--//  ENCODE lzo
	,record_mode VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,cust_sls VARCHAR(255)  		--//  ENCODE lzo
	,oi_ebeln VARCHAR(255)  		--//  ENCODE lzo
	,oi_ebelp VARCHAR(255)  		--//  ENCODE lzo
	,zsd_pod VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
		--// SORTKEY ( 
		--// 	customer
		--// 	)
;		--// ;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_delivery;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BW_DELIVERY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_delivery
(
	deliv_numb VARCHAR(255)  		--//  ENCODE lzo
	,deliv_item VARCHAR(255)  		--//  ENCODE lzo
	,incoterms VARCHAR(255)  		--//  ENCODE lzo
	,incoterms2 VARCHAR(255)  		--//  ENCODE lzo
	,unload_pt VARCHAR(255)  		--//  ENCODE lzo
	,grs_wgt_dl VARCHAR(255)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(255)  		--//  ENCODE lzo
	,dlv_qty VARCHAR(255)  		--//  ENCODE lzo
	,denomintr VARCHAR(255)  		--//  ENCODE lzo
	,numerator VARCHAR(255)  		--//  ENCODE lzo
	,shp_pr_tmf VARCHAR(255)  		--//  ENCODE lzo
	,shp_pr_tmv VARCHAR(255)  		--//  ENCODE lzo
	,sales_unit VARCHAR(255)  		--//  ENCODE lzo
	,volumeunit VARCHAR(255)  		--//  ENCODE lzo
	,no_del_it VARCHAR(255)  		--//  ENCODE lzo
	,volume_dl VARCHAR(255)  		--//  ENCODE lzo
	,act_dl_qty VARCHAR(255)  		--//  ENCODE lzo
	,net_wgt_dl VARCHAR(255)  		--//  ENCODE lzo
	,base_uom VARCHAR(255)  		--//  ENCODE lzo
	,refer_doc VARCHAR(255)  		--//  ENCODE lzo
	,refer_itm VARCHAR(255)  		--//  ENCODE lzo
	,fiscvarnt VARCHAR(255)  		--//  ENCODE lzo
	,sold_to VARCHAR(255)  		--//  ENCODE lzo
	,cust_group VARCHAR(255)  		--//  ENCODE lzo
	,bill_block VARCHAR(255)  		--//  ENCODE lzo
	,ship_to VARCHAR(255)  		--//  ENCODE lzo
	,load_pt VARCHAR(255)  		--//  ENCODE lzo
	,salesorg VARCHAR(255)  		--//  ENCODE lzo
	,createdby VARCHAR(255)  		--//  ENCODE lzo
	,createdon VARCHAR(255)  		--//  ENCODE lzo
	,whse_num VARCHAR(255)  		--//  ENCODE lzo
	,strge_bin VARCHAR(255)  		--//  ENCODE lzo
	,strge_type VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_3 VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_4 VARCHAR(255)  		--//  ENCODE lzo
	,sales_off VARCHAR(255)  		--//  ENCODE lzo
	,del_wa_dh VARCHAR(255)  		--//  ENCODE lzo
	,wbs_elemt VARCHAR(255)  		--//  ENCODE lzo
	,prvdoc_ctg VARCHAR(255)  		--//  ENCODE lzo
	,st_up_dte VARCHAR(255)  		--//  ENCODE lzo
	,distr_chan VARCHAR(255)  		--//  ENCODE lzo
	,zactdldte VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_5 VARCHAR(255)  		--//  ENCODE lzo
	,division VARCHAR(255)  		--//  ENCODE lzo
	,itm_type VARCHAR(255)  		--//  ENCODE lzo
	,sales_grp VARCHAR(255)  		--//  ENCODE lzo
	,stat_date VARCHAR(255)  		--//  ENCODE lzo
	,crea_time VARCHAR(255)  		--//  ENCODE lzo
	,item_categ VARCHAR(255)  		--//  ENCODE lzo
	,plant VARCHAR(255)  		--//  ENCODE lzo
	,bwapplnm VARCHAR(255)  		--//  ENCODE lzo
	,creditor VARCHAR(255)  		--//  ENCODE lzo
	,doc_categ VARCHAR(255)  		--//  ENCODE lzo
	,forwagent VARCHAR(255)  		--//  ENCODE lzo
	,matl_group VARCHAR(255)  		--//  ENCODE lzo
	,salesemply VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier VARCHAR(255)  		--//  ENCODE lzo
	,payer VARCHAR(255)  		--//  ENCODE lzo
	,billtoprty VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_2 VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_1 VARCHAR(255)  		--//  ENCODE lzo
	,mat_entrd VARCHAR(255)  		--//  ENCODE lzo
	,stor_loc VARCHAR(255)  		--//  ENCODE lzo
	,eanupc VARCHAR(255)  		--//  ENCODE lzo
	,pick_indc VARCHAR(255)  		--//  ENCODE lzo
	,consu_flag VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp3 VARCHAR(255)  		--//  ENCODE lzo
	,bus_area VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp5 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp4 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp2 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp1 VARCHAR(255)  		--//  ENCODE lzo
	,bilblk_dl VARCHAR(255)  		--//  ENCODE lzo
	,batch VARCHAR(255)  		--//  ENCODE lzo
	,route VARCHAR(255)  		--//  ENCODE lzo
	,ship_point VARCHAR(255)  		--//  ENCODE lzo
	,del_block VARCHAR(255)  		--//  ENCODE lzo
	,del_type VARCHAR(255)  		--//  ENCODE lzo
	,ship_date VARCHAR(255)  		--//  ENCODE lzo
	,goodsmv_st VARCHAR(255)  		--//  ENCODE lzo
	,sales_dist VARCHAR(255)  		--//  ENCODE lzo
	,gi_date VARCHAR(255)  		--//  ENCODE lzo
	,act_gi_dte VARCHAR(255)  		--//  ENCODE lzo
	,comp_code VARCHAR(255)  		--//  ENCODE lzo
	,rt_promo VARCHAR(255)  		--//  ENCODE lzo
	,processkey VARCHAR(255)  		--//  ENCODE lzo
	,ch_on VARCHAR(255)  		--//  ENCODE lzo
	,pick_conf VARCHAR(255)  		--//  ENCODE lzo
	,sts_pick VARCHAR(255)  		--//  ENCODE lzo
	,storno VARCHAR(255)  		--//  ENCODE lzo
	,recordmode VARCHAR(255)  		--//  ENCODE lzo
	,zdelqtybu VARCHAR(255)  		--//  ENCODE lzo
	,zdlqtycse VARCHAR(255)  		--//  ENCODE lzo
	,cdl_datetime VARCHAR(255)  		--//  ENCODE lzo
	,curr_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_material_uom;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BW_MATERIAL_UOM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_material_uom
(
	material VARCHAR(255)  		--//  ENCODE lzo
	,unit VARCHAR(255)  		--//  ENCODE lzo
	,base_uom VARCHAR(255)  		--//  ENCODE lzo
	,record_mode VARCHAR(255)  		--//  ENCODE lzo
	,uomz1d VARCHAR(255)  		--//  ENCODE lzo
	,uomn1d VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_price_list;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BW_PRICE_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_price_list
(
	sls_org VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,cond_rec_no VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp VARCHAR(255)  		--//  ENCODE lzo
	,valid_to VARCHAR(255)  		--//  ENCODE lzo
	,knart VARCHAR(255)  		--//  ENCODE lzo
	,dt_from VARCHAR(255)  		--//  ENCODE lzo
	,amount VARCHAR(255)  		--//  ENCODE lzo
	,currency VARCHAR(255)  		--//  ENCODE lzo
	,unit VARCHAR(255)  		--//  ENCODE lzo
	,record_mode VARCHAR(255)  		--//  ENCODE lzo
	,comp_cd VARCHAR(255)  		--//  ENCODE lzo
	,price_unit VARCHAR(255)  		--//  ENCODE lzo
	,zcurrfpa VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
		--// SORTKEY ( 
		--// 	cdl_dttm
		--// 	)
;		--// ;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_sales;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_SAP_BW_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_sap_bw_sales
(
	doc_num VARCHAR(255)  		--//  ENCODE lzo
	,s_ord_item VARCHAR(255)  		--//  ENCODE lzo
	,rejectn_st VARCHAR(255)  		--//  ENCODE lzo
	,process_key VARCHAR(255)  		--//  ENCODE lzo
	,batch VARCHAR(255)  		--//  ENCODE lzo
	,created_on VARCHAR(255)  		--//  ENCODE lzo
	,sls_deal VARCHAR(255)  		--//  ENCODE lzo
	,stor_loc VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp VARCHAR(255)  		--//  ENCODE lzo
	,incoterms2 VARCHAR(255)  		--//  ENCODE lzo
	,accnt_asgn VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp VARCHAR(255)  		--//  ENCODE lzo
	,cond_pr_un VARCHAR(255)  		--//  ENCODE lzo
	,incoterms VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_4 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_5 VARCHAR(255)  		--//  ENCODE lzo
	,net_price VARCHAR(255)  		--//  ENCODE lzo
	,net_value VARCHAR(255)  		--//  ENCODE lzo
	,net_wt_ap VARCHAR(255)  		--//  ENCODE lzo
	,cond_unit VARCHAR(255)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(255)  		--//  ENCODE lzo
	,order_curr VARCHAR(255)  		--//  ENCODE lzo
	,stat_curr VARCHAR(255)  		--//  ENCODE lzo
	,loc_currcy VARCHAR(255)  		--//  ENCODE lzo
	,target_qty VARCHAR(255)  		--//  ENCODE lzo
	,targ_value VARCHAR(255)  		--//  ENCODE lzo
	,ord_items VARCHAR(255)  		--//  ENCODE lzo
	,zorqtycse VARCHAR(255)  		--//  ENCODE lzo
	,tax_value VARCHAR(255)  		--//  ENCODE lzo
	,exchg_rate VARCHAR(255)  		--//  ENCODE lzo
	,target_qu VARCHAR(255)  		--//  ENCODE lzo
	,cost VARCHAR(255)  		--//  ENCODE lzo
	,volume_ap VARCHAR(255)  		--//  ENCODE lzo
	,volume_unit VARCHAR(255)  		--//  ENCODE lzo
	,exchg_stat VARCHAR(255)  		--//  ENCODE lzo
	,reqdel_qty VARCHAR(255)  		--//  ENCODE lzo
	,lowr_bnd VARCHAR(255)  		--//  ENCODE lzo
	,uppr_bnd VARCHAR(255)  		--//  ENCODE lzo
	,numeratorz VARCHAR(255)  		--//  ENCODE lzo
	,denomintrz VARCHAR(255)  		--//  ENCODE lzo
	,numerator VARCHAR(255)  		--//  ENCODE lzo
	,denomintr VARCHAR(255)  		--//  ENCODE lzo
	,min_dl_qty VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_6 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_3 VARCHAR(255)  		--//  ENCODE lzo
	,wbs_elemt VARCHAR(255)  		--//  ENCODE lzo
	,refer_doc VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_2 VARCHAR(255)  		--//  ENCODE lzo
	,subtotal_1 VARCHAR(255)  		--//  ENCODE lzo
	,doc_currcy VARCHAR(255)  		--//  ENCODE lzo
	,cml_or_qty VARCHAR(255)  		--//  ENCODE lzo
	,cml_cd_qty VARCHAR(255)  		--//  ENCODE lzo
	,base_uom VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,refer_itm VARCHAR(255)  		--//  ENCODE lzo
	,fisc_varnt VARCHAR(255)  		--//  ENCODE lzo
	,apo_planned VARCHAR(255)  		--//  ENCODE lzo
	,log_sys VARCHAR(255)  		--//  ENCODE lzo
	,cml_cf_qty VARCHAR(255)  		--//  ENCODE lzo
	,sls_unit VARCHAR(255)  		--//  ENCODE lzo
	,doc_dt VARCHAR(255)  		--//  ENCODE lzo
	,zreq_dt VARCHAR(255)  		--//  ENCODE lzo
	,zord_mth VARCHAR(255)  		--//  ENCODE lzo
	,sub_reason VARCHAR(255)  		--//  ENCODE lzo
	,doc_categr VARCHAR(255)  		--//  ENCODE lzo
	,div_head VARCHAR(255)  		--//  ENCODE lzo
	,trans_dt VARCHAR(255)  		--//  ENCODE lzo
	,bill_dt VARCHAR(255)  		--//  ENCODE lzo
	,prod_cat VARCHAR(255)  		--//  ENCODE lzo
	,exchg_crd VARCHAR(255)  		--//  ENCODE lzo
	,sales_dist VARCHAR(255)  		--//  ENCODE lzo
	,serv_dt VARCHAR(255)  		--//  ENCODE lzo
	,plant VARCHAR(255)  		--//  ENCODE lzo
	,rt_promo VARCHAR(255)  		--//  ENCODE lzo
	,mat_entrd VARCHAR(255)  		--//  ENCODE lzo
	,ship_point VARCHAR(255)  		--//  ENCODE lzo
	,prvdoc_ctg VARCHAR(255)  		--//  ENCODE lzo
	,crea_time VARCHAR(255)  		--//  ENCODE lzo
	,bilblk_itm VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_1 VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_3 VARCHAR(255)  		--//  ENCODE lzo
	,price_dt VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_4 VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_5 VARCHAR(255)  		--//  ENCODE lzo
	,route VARCHAR(255)  		--//  ENCODE lzo
	,sls_emply VARCHAR(255)  		--//  ENCODE lzo
	,bnd_ind VARCHAR(255)  		--//  ENCODE lzo
	,gross_wgt VARCHAR(255)  		--//  ENCODE lzo
	,stat_dt VARCHAR(255)  		--//  ENCODE lzo
	,division VARCHAR(255)  		--//  ENCODE lzo
	,st_up_dt VARCHAR(255)  		--//  ENCODE lzo
	,ship_stck VARCHAR(255)  		--//  ENCODE lzo
	,forwagent VARCHAR(255)  		--//  ENCODE lzo
	,bill_to_prty VARCHAR(255)  		--//  ENCODE lzo
	,prod_hier VARCHAR(255)  		--//  ENCODE lzo
	,item_categ VARCHAR(255)  		--//  ENCODE lzo
	,ship_to VARCHAR(255)  		--//  ENCODE lzo
	,payer VARCHAR(255)  		--//  ENCODE lzo
	,unld_pt_we VARCHAR(255)  		--//  ENCODE lzo
	,matl_grp_2 VARCHAR(255)  		--//  ENCODE lzo
	,eanupc VARCHAR(255)  		--//  ENCODE lzo
	,created_by VARCHAR(255)  		--//  ENCODE lzo
	,sts_bill VARCHAR(255)  		--//  ENCODE lzo
	,order_prob VARCHAR(255)  		--//  ENCODE lzo
	,bwapplnm VARCHAR(255)  		--//  ENCODE lzo
	,quot_from VARCHAR(255)  		--//  ENCODE lzo
	,ord_reason VARCHAR(255)  		--//  ENCODE lzo
	,ch_on VARCHAR(255)  		--//  ENCODE lzo
	,reason_rej VARCHAR(255)  		--//  ENCODE lzo
	,comp_cd VARCHAR(255)  		--//  ENCODE lzo
	,distr_chan VARCHAR(255)  		--//  ENCODE lzo
	,sls_org VARCHAR(255)  		--//  ENCODE lzo
	,sls_grp VARCHAR(255)  		--//  ENCODE lzo
	,sls_off VARCHAR(255)  		--//  ENCODE lzo
	,doc_categ VARCHAR(255)  		--//  ENCODE lzo
	,rate_type VARCHAR(255)  		--//  ENCODE lzo
	,bill_block VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp1 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp2 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp4 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp3 VARCHAR(255)  		--//  ENCODE lzo
	,cust_grp5 VARCHAR(255)  		--//  ENCODE lzo
	,del_block VARCHAR(255)  		--//  ENCODE lzo
	,sold_to VARCHAR(255)  		--//  ENCODE lzo
	,quot_to VARCHAR(255)  		--//  ENCODE lzo
	,doc_type VARCHAR(255)  		--//  ENCODE lzo
	,sts_prc VARCHAR(255)  		--//  ENCODE lzo
	,sts_del VARCHAR(255)  		--//  ENCODE lzo
	,sts_itm VARCHAR(255)  		--//  ENCODE lzo
	,stor_no VARCHAR(255)  		--//  ENCODE lzo
	,record_mode VARCHAR(255)  		--//  ENCODE lzo
	,zordqtybu VARCHAR(255)  		--//  ENCODE lzo
	,zzp_num VARCHAR(255)  		--//  ENCODE lzo
	,zzp_itm VARCHAR(255)  		--//  ENCODE lzo
	,ztarqtycu VARCHAR(255)  		--//  ENCODE lzo
	,ztarqtybu VARCHAR(255)  		--//  ENCODE lzo
	,zhighritm VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;


--DROP TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_cnsc;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_RG_TRAVEL_RETAIL_CNSC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_cnsc
(
	door_name VARCHAR(25)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,retailer_name VARCHAR(35)  		--//  ENCODE zstd
	,supplier_product_code VARCHAR(15)  		--//  ENCODE zstd
	,product_description VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,pack_size VARCHAR(20)  		--//  ENCODE zstd
	,serial_number VARCHAR(30)  		--//  ENCODE zstd
	,inventory_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_amount NUMERIC(38,8)  		--//  ENCODE az64
	,material_code VARCHAR(25)  		--//  ENCODE zstd
	,ean VARCHAR(25)  		--//  ENCODE zstd
	,dcl_code VARCHAR(25)  		--//  ENCODE zstd
	,filename VARCHAR(80)  		--//  ENCODE zstd
	,store_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,total_store_sales NUMERIC(38,18)  		--//  ENCODE az64
	,no_of_ecommerce_sales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,total_ecommerce_sales NUMERIC(38,18)  		--//  ENCODE az64
	,membership_sls_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,membership_sls_amt NUMERIC(38,18)  		--//  ENCODE az64
	,crttd VARCHAR(100)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_dfs_hainan;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_RG_TRAVEL_RETAIL_DFS_HAINAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_dfs_hainan
(
	year VARCHAR(255)  		--//  ENCODE zstd
	,mon VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,retailer_name VARCHAR(15)  		--//  ENCODE zstd
	,product_department_desc VARCHAR(255)  		--//  ENCODE zstd
	,product_department_code VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(255)  		--//  ENCODE zstd
	,product_class_desc VARCHAR(255)  		--//  ENCODE zstd
	,product_class_code VARCHAR(255)  		--//  ENCODE zstd
	,product_subclass_desc VARCHAR(255)  		--//  ENCODE zstd
	,product_subclass_code VARCHAR(255)  		--//  ENCODE zstd
	,brand_collection VARCHAR(255)  		--//  ENCODE zstd
	,reatiler_product_code VARCHAR(255)  		--//  ENCODE zstd
	,reatiler_product_description VARCHAR(255)  		--//  ENCODE zstd
	,dcl_code VARCHAR(50)  		--//  ENCODE zstd
	,ean VARCHAR(50)  		--//  ENCODE zstd
	,style_type_code VARCHAR(255)  		--//  ENCODE zstd
	,month VARCHAR(255)  		--//  ENCODE zstd
	,door_name VARCHAR(255)  		--//  ENCODE zstd
	,sls_mtd_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_mtd_amt NUMERIC(38,18)  		--//  ENCODE az64
	,sls_ytd_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_ytd_amt NUMERIC(38,18)  		--//  ENCODE az64
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crttd VARCHAR(100)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_dufry_hainan;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_RG_TRAVEL_RETAIL_DUFRY_HAINAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_dufry_hainan
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yearmo VARCHAR(15)  		--//  ENCODE zstd
	,retailer_name VARCHAR(15)  		--//  ENCODE zstd
	,ean VARCHAR(50)  		--//  ENCODE zstd
	,dcl_code VARCHAR(50)  		--//  ENCODE zstd
	,product_desc VARCHAR(255)  		--//  ENCODE zstd
	,online_qty NUMERIC(38,8)  		--//  ENCODE az64
	,online_gmv NUMERIC(38,8)  		--//  ENCODE az64
	,online_sales_split_per VARCHAR(50)  		--//  ENCODE zstd
	,offline_qty NUMERIC(38,8)  		--//  ENCODE az64
	,offline_gmv NUMERIC(38,8)  		--//  ENCODE az64
	,offline_sales_split_per VARCHAR(50)  		--//  ENCODE zstd
	,total_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,total_gmv NUMERIC(38,8)  		--//  ENCODE az64
	,filename VARCHAR(35)  		--//  ENCODE zstd
	,brand VARCHAR(35)  		--//  ENCODE zstd
	,crtddt VARCHAR(100)  		--//  ENCODE zstd
	,stock numeric(18,0)		--//  ENCODE az64 // INTEGER  
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_lstr;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RAW_RG_TRAVEL_RETAIL_LSTR		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_raw_rg_travel_retail_lstr
(
	year VARCHAR(255)  		--//  ENCODE zstd
	,month VARCHAR(255)  		--//  ENCODE zstd
	,yearmo VARCHAR(255)  		--//  ENCODE zstd
	,retailer_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,ean VARCHAR(255)  		--//  ENCODE zstd
	,dcl_code VARCHAR(255)  		--//  ENCODE zstd
	,english_desc VARCHAR(255)  		--//  ENCODE zstd
	,chinese_desc VARCHAR(255)  		--//  ENCODE zstd
	,category VARCHAR(255)  		--//  ENCODE zstd
	,srp_usd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_qty_total numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sls_amt_total NUMERIC(38,18)  		--//  ENCODE az64
	,offline_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offline_amt NUMERIC(38,18)  		--//  ENCODE az64
	,online_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,online_amt NUMERIC(38,18)  		--//  ENCODE az64
	,stock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crttd VARCHAR(100)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_cdfg_raw;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RG_TRAVEL_RETAIL_CDFG_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_cdfg_raw
(
	location_name VARCHAR(50)  		--//  ENCODE zstd
	,retailer_name VARCHAR(50)  		--//  ENCODE zstd
	,year_month VARCHAR(10)  		--//  ENCODE zstd
	,dcl_code VARCHAR(50)  		--//  ENCODE zstd
	,barcode VARCHAR(50)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,sls_qty VARCHAR(30)  		--//  ENCODE zstd
	,stock_qty VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_dfs_raw;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RG_TRAVEL_RETAIL_DFS_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_dfs_raw
(
	retailer_name VARCHAR(50)  		--//  ENCODE zstd
	,year_month VARCHAR(10)  		--//  ENCODE zstd
	,product_department VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,product_class VARCHAR(50)  		--//  ENCODE zstd
	,common_sku VARCHAR(50)  		--//  ENCODE zstd
	,common_sku_status VARCHAR(50)  		--//  ENCODE zstd
	,common_sku_type VARCHAR(50)  		--//  ENCODE zstd
	,style VARCHAR(30)  		--//  ENCODE zstd
	,region_sku VARCHAR(10)  		--//  ENCODE zstd
	,vendor_style VARCHAR(30)  		--//  ENCODE zstd
	,metrics VARCHAR(30)  		--//  ENCODE zstd
	,location_name VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty VARCHAR(30)  		--//  ENCODE zstd
	,sls_amt VARCHAR(30)  		--//  ENCODE zstd
	,soh_qty VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_sales_stock_raw;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RG_TRAVEL_RETAIL_SALES_STOCK_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_sales_stock_raw
(
	location_name VARCHAR(50)  		--//  ENCODE zstd
	,retailer_name VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(4)  		--//  ENCODE zstd
	,month VARCHAR(2)  		--//  ENCODE zstd
	,dcl_code VARCHAR(50)  		--//  ENCODE zstd
	,sap_code VARCHAR(50)  		--//  ENCODE zstd
	,reference VARCHAR(50)  		--//  ENCODE zstd
	,product_desc VARCHAR(100)  		--//  ENCODE zstd
	,size VARCHAR(20)  		--//  ENCODE zstd
	,rsp VARCHAR(20)  		--//  ENCODE zstd
	,c_sls_qty VARCHAR(30)  		--//  ENCODE zstd
	,c_sls_amt VARCHAR(30)  		--//  ENCODE zstd
	,c_stock_qty VARCHAR(30)  		--//  ENCODE zstd
	,c_stock_amt VARCHAR(30)  		--//  ENCODE zstd
	,buffer VARCHAR(30)  		--//  ENCODE zstd
	,mix VARCHAR(30)  		--//  ENCODE zstd
	,r_3m VARCHAR(30)  		--//  ENCODE zstd
	,comparison VARCHAR(30)  		--//  ENCODE zstd
	,sls_qty VARCHAR(30)  		--//  ENCODE zstd
	,stock_qty VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_shilla_raw;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.SDL_RG_TRAVEL_RETAIL_SHILLA_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.sdl_rg_travel_retail_shilla_raw
(
	retailer_name VARCHAR(50)  		--//  ENCODE zstd
	,year_month VARCHAR(10)  		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,sku VARCHAR(50)  		--//  ENCODE zstd
	,description VARCHAR(100)  		--//  ENCODE zstd
	,dcl_code VARCHAR(50)  		--//  ENCODE zstd
	,ean VARCHAR(50)  		--//  ENCODE zstd
	,color VARCHAR(30)  		--//  ENCODE zstd
	,location_name VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty VARCHAR(30)  		--//  ENCODE zstd
	,sls_amt VARCHAR(30)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;

create or replace view PROD_DNA_CORE.ASPEDW_ACCESS.EDW_PERFECT_STORE_REBASE_WT(
	"hashkey",
	"hash_row",
	"dataset",
	"customerid",
	"salespersonid",
	"visitid",
	"questiontext",
	"productid",
	"kpi",
	"scheduleddate",
	"latestdate",
	"fisc_yr",
	"fisc_per",
	"merchandiser_name",
	"customername",
	"country",
	"state",
	"parent_customer",
	"retail_environment",
	"channel",
	"retailer",
	"business_unit",
	"eannumber",
	"matl_num",
	"prod_hier_l1",
	"prod_hier_l2",
	"prod_hier_l3",
	"prod_hier_l4",
	"prod_hier_l5",
	"prod_hier_l6",
	"prod_hier_l7",
	"prod_hier_l8",
	"prod_hier_l9",
	"ques_type",
	"y/n_flag",
	"priority_store_flag",
	"add_info",
	"response",
	"response_score",
	"kpi_chnl_wt",
	"channel_weightage",
	"weight_msl",
	"weight_oos",
	"weight_soa",
	"weight_sos",
	"weight_promo",
	"weight_planogram",
	"weight_display",
	"mkt_share",
	"salience_val",
	"actual_value",
	"ref_value",
	"kpi_actual_wt_val",
	"kpi_ref_val",
	"kpi_ref_wt_val",
	"photo_url",
	"compliance",
	"gap_to_target",
	"compliance_propogated",
	"gap_propagated",
	"full_opportunity_lcy",
	"weighted_opportunity_lcy",
	"full_opportunity_usd",
	"weighted_opportunity_usd",
	"sotp_lcy",
	"sotp_usd",
	"store_grade"
) as
SELECT
    hashkey AS "hashkey",
    hash_row AS "hash_row",
    dataset AS "dataset",
    customerid AS "customerid",
    salespersonid AS "salespersonid",
    visitid AS "visitid",
    questiontext AS "questiontext",
    productid AS "productid",
    kpi AS "kpi",
    scheduleddate AS "scheduleddate",
    latestdate AS "latestdate",
    fisc_yr AS "fisc_yr",
    fisc_per AS "fisc_per",
    merchandiser_name AS "merchandiser_name",
    customername AS "customername",
    country AS "country",
    state AS "state",
    parent_customer AS "parent_customer",
    retail_environment AS "retail_environment",
    channel AS "channel",
    retailer AS "retailer",
    business_unit AS "business_unit",
    eannumber AS "eannumber",
    matl_num AS "matl_num",
    prod_hier_l1 AS "prod_hier_l1",
    prod_hier_l2 AS "prod_hier_l2",
    prod_hier_l3 AS "prod_hier_l3",
    prod_hier_l4 AS "prod_hier_l4",
    prod_hier_l5 AS "prod_hier_l5",
    prod_hier_l6 AS "prod_hier_l6",
    prod_hier_l7 AS "prod_hier_l7",
    prod_hier_l8 AS "prod_hier_l8",
    prod_hier_l9 AS "prod_hier_l9",
    ques_type AS "ques_type",
    "y/n_flag" AS "y/n_flag",
    priority_store_flag AS "priority_store_flag",
    add_info AS "add_info",
    response AS "response",
    response_score AS "response_score",
    kpi_chnl_wt AS "kpi_chnl_wt",
    channel_weightage AS "channel_weightage",
    weight_msl AS "weight_msl",
    weight_oos AS "weight_oos",
    weight_soa AS "weight_soa",
    weight_sos AS "weight_sos",
    weight_promo AS "weight_promo",
    weight_planogram AS "weight_planogram",
    weight_display AS "weight_display",
    mkt_share AS "mkt_share",
    salience_val AS "salience_val",
    actual_value AS "actual_value",
    ref_value AS "ref_value",
    kpi_actual_wt_val AS "kpi_actual_wt_val",
    kpi_ref_val AS "kpi_ref_val",
    kpi_ref_wt_val AS "kpi_ref_wt_val",
    photo_url AS "photo_url",
    compliance AS "compliance",
    gap_to_target AS "gap_to_target",
    compliance_propogated AS "compliance_propogated",
    gap_propagated AS "gap_propagated",
    full_opportunity_lcy AS "full_opportunity_lcy",
    weighted_opportunity_lcy AS "weighted_opportunity_lcy",
    full_opportunity_usd AS "full_opportunity_usd",
    weighted_opportunity_usd AS "weighted_opportunity_usd",
    sotp_lcy AS "sotp_lcy",
    sotp_usd AS "sotp_usd",
    store_grade AS "store_grade"
FROM PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_PERFECT_STORE_REBASE_WT;

--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_amazon;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_AMAZON		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_amazon
(
	trx_date DATE  		--//  ENCODE az64
	,rm VARCHAR(50)  		--//  ENCODE lzo
	,merchant_customer_id VARCHAR(15)  		--//  ENCODE lzo
	,gl VARCHAR(50)  		--//  ENCODE lzo
	,category VARCHAR(200)  		--//  ENCODE lzo
	,subcategory VARCHAR(200)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,item_code VARCHAR(50)  		--//  ENCODE lzo
	,item_desc VARCHAR(500)  		--//  ENCODE lzo
	,net_sales NUMERIC(10,4)  		--//  ENCODE az64
	,pcogs NUMERIC(10,4)  		--//  ENCODE az64
	,sales_qty NUMERIC(10,0)  		--//  ENCODE az64
	,ppmpercent NUMERIC(10,5)  		--//  ENCODE az64
	,ppmdollar NUMERIC(10,5)  		--//  ENCODE az64
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vendor_code VARCHAR(10)  		--//  ENCODE lzo
	,vendor_name VARCHAR(255)  		--//  ENCODE lzo
	,store VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(50)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_dfi;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_DFI		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_dfi
(
	trxdate DATE  		--//  ENCODE az64
	,buyercode VARCHAR(50)  		--//  ENCODE lzo
	,vendorcode VARCHAR(100)  		--//  ENCODE lzo
	,storecode VARCHAR(50)  		--//  ENCODE lzo
	,storeshortcode VARCHAR(100)  		--//  ENCODE lzo
	,storedesc VARCHAR(300)  		--//  ENCODE lzo
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,itemcode VARCHAR(50)  		--//  ENCODE lzo
	,supplieritemcode VARCHAR(50)  		--//  ENCODE lzo
	,itemdesc VARCHAR(500)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,puf NUMERIC(10,0)  		--//  ENCODE az64
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,salesamount VARCHAR(100)  		--//  ENCODE lzo
	,salesqty NUMERIC(10,0)  		--//  ENCODE az64
	,cust_name VARCHAR(20)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_guardian;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_GUARDIAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_guardian
(
	trxdate DATE  		--//  ENCODE az64
	,buyercode VARCHAR(50)  		--//  ENCODE lzo
	,vendorcode VARCHAR(100)  		--//  ENCODE lzo
	,storecode VARCHAR(50)  		--//  ENCODE lzo
	,storeshortcode VARCHAR(50)  		--//  ENCODE lzo
	,storepostalcode VARCHAR(50)  		--//  ENCODE lzo
	,storeaddress1 VARCHAR(200)  		--//  ENCODE lzo
	,storeaddress2 VARCHAR(200)  		--//  ENCODE lzo
	,storeaddress3 VARCHAR(100)  		--//  ENCODE lzo
	,storecountry VARCHAR(20)  		--//  ENCODE lzo
	,storedesc VARCHAR(500)  		--//  ENCODE lzo
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,itemcode VARCHAR(50)  		--//  ENCODE lzo
	,supplieritemcode VARCHAR(50)  		--//  ENCODE lzo
	,itemdesc VARCHAR(500)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,uom VARCHAR(20)  		--//  ENCODE lzo
	,puf NUMERIC(10,0)  		--//  ENCODE az64
	,salesqty NUMERIC(10,0)  		--//  ENCODE az64
	,salesamount VARCHAR(100)  		--//  ENCODE lzo
	,inventoryonhand NUMERIC(10,0)  		--//  ENCODE az64
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,cust_name VARCHAR(20)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_marketplace;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_MARKETPLACE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_marketplace
(
	channel VARCHAR(20)  		--//  ENCODE lzo
	,month VARCHAR(20)  		--//  ENCODE lzo
	,order_creation_date DATE  		--//  ENCODE az64
	,invoice_number VARCHAR(255)  		--//  ENCODE lzo
	,status VARCHAR(100)  		--//  ENCODE lzo
	,item_code VARCHAR(500)  		--//  ENCODE lzo
	,item_name VARCHAR(500)  		--//  ENCODE lzo
	,sales_unit NUMERIC(10,0)  		--//  ENCODE az64
	,net_invoiced_sales NUMERIC(10,4)  		--//  ENCODE az64
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,cust_name VARCHAR(20)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_ntuc;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_NTUC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_ntuc
(
	vendor_code VARCHAR(100)  		--//  ENCODE lzo
	,vendor_name VARCHAR(255)  		--//  ENCODE lzo
	,dept_code VARCHAR(50)  		--//  ENCODE lzo
	,dept_description VARCHAR(255)  		--//  ENCODE lzo
	,class_no VARCHAR(50)  		--//  ENCODE lzo
	,class_description VARCHAR(255)  		--//  ENCODE lzo
	,sub_class_description VARCHAR(255)  		--//  ENCODE lzo
	,mch VARCHAR(255)  		--//  ENCODE lzo
	,item_code VARCHAR(100)  		--//  ENCODE lzo
	,item_desc VARCHAR(500)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,sales_uom VARCHAR(100)  		--//  ENCODE lzo
	,pack_size NUMERIC(10,0)  		--//  ENCODE az64
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_format VARCHAR(255)  		--//  ENCODE lzo
	,attribute1 VARCHAR(50)  		--//  ENCODE lzo
	,attribute2 VARCHAR(50)  		--//  ENCODE lzo
	,trx_date DATE  		--//  ENCODE az64
	,net_sales NUMERIC(10,4)  		--//  ENCODE az64
	,sales_qty NUMERIC(10,0)  		--//  ENCODE az64
	,cust_name VARCHAR(20)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_redmart;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_REDMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_redmart
(
	trx_date DATE  		--//  ENCODE az64
	,item_code VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(300)  		--//  ENCODE lzo
	,item_desc VARCHAR(500)  		--//  ENCODE lzo
	,packsize VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,supplier_id VARCHAR(100)  		--//  ENCODE lzo
	,supplier_name VARCHAR(100)  		--//  ENCODE lzo
	,manufacturer VARCHAR(200)  		--//  ENCODE lzo
	,category_1 VARCHAR(150)  		--//  ENCODE lzo
	,category_2 VARCHAR(150)  		--//  ENCODE lzo
	,category_3 VARCHAR(150)  		--//  ENCODE lzo
	,category_4 VARCHAR(150)  		--//  ENCODE lzo
	,gmv NUMERIC(10,4)  		--//  ENCODE az64
	,units_sold NUMERIC(10,0)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(50)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_scommerce;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_SCOMMERCE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_scommerce
(
	date_id DATE  		--//  ENCODE az64
	,ordersn VARCHAR(50)  		--//  ENCODE lzo
	,itemid VARCHAR(50)  		--//  ENCODE lzo
	,modelid VARCHAR(50)  		--//  ENCODE lzo
	,sku_id VARCHAR(100)  		--//  ENCODE lzo
	,item_name VARCHAR(500)  		--//  ENCODE lzo
	,model_name VARCHAR(500)  		--//  ENCODE lzo
	,sales_qty NUMERIC(10,0)  		--//  ENCODE az64
	,net_sales NUMERIC(10,6)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(50)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_watsons;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_SCAN_DATA_WATSONS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_scan_data_watsons
(
	year VARCHAR(20)  		--//  ENCODE lzo
	,week VARCHAR(20)  		--//  ENCODE lzo
	,store VARCHAR(255)  		--//  ENCODE lzo
	,div VARCHAR(255)  		--//  ENCODE lzo
	,prdt_dept VARCHAR(255)  		--//  ENCODE lzo
	,prdtcode VARCHAR(255)  		--//  ENCODE lzo
	,prdtdesc VARCHAR(500)  		--//  ENCODE lzo
	,brand VARCHAR(300)  		--//  ENCODE lzo
	,supcode VARCHAR(255)  		--//  ENCODE lzo
	,sup_name VARCHAR(300)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,sup_cat VARCHAR(255)  		--//  ENCODE lzo
	,dept_name VARCHAR(255)  		--//  ENCODE lzo
	,net_sales VARCHAR(100)  		--//  ENCODE lzo
	,sales_qty NUMERIC(10,0)  		--//  ENCODE delta
	,cust_name VARCHAR(20)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_tp_closed_month;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_TP_CLOSED_MONTH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_tp_closed_month
(
	file_name VARCHAR(255)  		--//  ENCODE lzo
	,sheet_name VARCHAR(255)  		--//  ENCODE lzo
	,month_number VARCHAR(255)  		--//  ENCODE lzo
	,sales_rep VARCHAR(255)  		--//  ENCODE lzo
	,custmer_l1 VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,customer_code VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,franchise VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,brand_profit_center VARCHAR(255)  		--//  ENCODE lzo
	,promo_type VARCHAR(255)  		--//  ENCODE lzo
	,gl_account VARCHAR(255)  		--//  ENCODE lzo
	,description VARCHAR(255)  		--//  ENCODE lzo
	,requested_date VARCHAR(255)  		--//  ENCODE lzo
	,month_of_activity VARCHAR(255)  		--//  ENCODE lzo
	,promo_start_date VARCHAR(255)  		--//  ENCODE lzo
	,promo_end_date VARCHAR(255)  		--//  ENCODE lzo
	,committed_accrual_w_o_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,tp_number VARCHAR(255)  		--//  ENCODE lzo
	,zp_cmm_invoice VARCHAR(255)  		--//  ENCODE lzo
	,retailers_billing VARCHAR(255)  		--//  ENCODE lzo
	,jnj_actuals_w_o_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,cn_number VARCHAR(255)  		--//  ENCODE lzo
	,tp_date VARCHAR(255)  		--//  ENCODE lzo
	,jnj_total_committed_w_o_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,jnj_total_unclaimed_w_o_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,reversed_accrued_amt NUMERIC(17,3)  		--//  ENCODE lzo
	,month_of_reversal VARCHAR(255)  		--//  ENCODE lzo
	,supporting VARCHAR(255)  		--//  ENCODE lzo
	,month_of_doc_scanning VARCHAR(255)  		--//  ENCODE lzo
	,remarks VARCHAR(255)  		--//  ENCODE lzo
	,working_sc VARCHAR(255)  		--//  ENCODE lzo
	,conso_pr VARCHAR(255)  		--//  ENCODE lzo
	,create_pr VARCHAR(255)  		--//  ENCODE lzo
	,payment_ref VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_tp_closed_year_bal;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_TP_CLOSED_YEAR_BAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_tp_closed_year_bal
(
	file_name VARCHAR(255)  		--//  ENCODE lzo
	,sheet_name VARCHAR(255)  		--//  ENCODE lzo
	,month_number VARCHAR(255)  		--//  ENCODE lzo
	,sales_rep VARCHAR(255)  		--//  ENCODE lzo
	,customer_l1 VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,customer_code VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,franchise VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,brand_profit_center VARCHAR(255)  		--//  ENCODE lzo
	,promo_type VARCHAR(255)  		--//  ENCODE lzo
	,gl_account VARCHAR(255)  		--//  ENCODE lzo
	,description VARCHAR(255)  		--//  ENCODE lzo
	,requested_date VARCHAR(255)  		--//  ENCODE lzo
	,month_of_activity VARCHAR(255)  		--//  ENCODE lzo
	,promo_start_date VARCHAR(255)  		--//  ENCODE lzo
	,promo_end_date VARCHAR(255)  		--//  ENCODE lzo
	,committed_or_accrual_wo_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,tp_number VARCHAR(255)  		--//  ENCODE lzo
	,zp_cmm_invoice VARCHAR(255)  		--//  ENCODE lzo
	,retailers_billing VARCHAR(255)  		--//  ENCODE lzo
	,jnj_actuals_wo_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,month_of_actual VARCHAR(255)  		--//  ENCODE lzo
	,cn_number VARCHAR(255)  		--//  ENCODE lzo
	,cn_date VARCHAR(255)  		--//  ENCODE lzo
	,reversal_amount NUMERIC(17,3)  		--//  ENCODE lzo
	,outstanding_accrual NUMERIC(17,3)  		--//  ENCODE lzo
	,jnj_total_committed_wo_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,jnj_total_unclaimed_wo_gst NUMERIC(17,3)  		--//  ENCODE lzo
	,comments_or_reversed_accrued_amt NUMERIC(17,3)  		--//  ENCODE lzo
	,month_of_reversal VARCHAR(255)  		--//  ENCODE lzo
	,supporting VARCHAR(255)  		--//  ENCODE lzo
	,tp_impact NUMERIC(17,3)  		--//  ENCODE lzo
	,left_accrual NUMERIC(17,3)  		--//  ENCODE lzo
	,month_of_doc_scanning VARCHAR(255)  		--//  ENCODE lzo
	,remarks VARCHAR(255)  		--//  ENCODE lzo
	,working_sc VARCHAR(255)  		--//  ENCODE lzo
	,conso_pr VARCHAR(255)  		--//  ENCODE lzo
	,create_pr VARCHAR(255)  		--//  ENCODE lzo
	,payment_ref VARCHAR(255)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE SGPITG_INTEGRATION.sdl_raw_sg_zuellig_sellout;
CREATE OR REPLACE TABLE SGPITG_INTEGRATION.SDL_RAW_SG_ZUELLIG_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE SGPITG_INTEGRATION.sdl_raw_sg_zuellig_sellout
(
	month_no VARCHAR(255)  		--//  ENCODE lzo
	,sales_date VARCHAR(255)  		--//  ENCODE lzo
	,warehouse_code VARCHAR(255)  		--//  ENCODE lzo
	,customer_code VARCHAR(255)  		--//  ENCODE lzo
	,customer_name VARCHAR(255)  		--//  ENCODE lzo
	,invoice VARCHAR(255)  		--//  ENCODE lzo
	,item_name VARCHAR(255)  		--//  ENCODE lzo
	,item_code VARCHAR(255)  		--//  ENCODE lzo
	,type VARCHAR(255)  		--//  ENCODE lzo
	,sales_value NUMERIC(17,3)  		--//  ENCODE lzo
	,sales_units NUMERIC(17,3)  		--//  ENCODE lzo
	,bonus_units NUMERIC(17,3)  		--//  ENCODE lzo
	,returns_units NUMERIC(17,3)  		--//  ENCODE lzo
	,returns_value NUMERIC(17,3)  		--//  ENCODE lzo
	,returns_bonus_units NUMERIC(17,3)  		--//  ENCODE lzo
	,cdl_dttm VARCHAR(255)  		--//  ENCODE lzo
	,curr_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;

create or replace TABLE ASPITG_INTEGRATION.ITG_COPA_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_ORG VARCHAR(4),
	MATL VARCHAR(18),
	CUST_NUM VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	CHRT_ACCT VARCHAR(4),
	ACCT_NUM VARCHAR(10),
	DSTR_CHNL VARCHAR(2),
	FISC_YR_VAR VARCHAR(2),
	VERS VARCHAR(3),
	BW_DELTA_UPD_MODE VARCHAR(1),
	BILL_TYP VARCHAR(4),
	SLS_OFF VARCHAR(4),
	CNTRY_KEY VARCHAR(3),
	SLS_DEAL VARCHAR(10),
	SLS_GRP VARCHAR(3),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_DIST VARCHAR(6),
	CUST_GRP VARCHAR(2),
	CUST_SLS VARCHAR(10),
	BUSS_AREA VARCHAR(4),
	VAL_TYPE_RPT NUMBER(18,0),
	MERCIA_REF VARCHAR(5),
	CALN_DAY VARCHAR(20),
	CALN_YR_MO NUMBER(18,0),
	FISC_YR NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	B3_BASE_PROD VARCHAR(3),
	B4_VAR VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	REG VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	GRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	SLS_PRSN_RESP VARCHAR(30),
	MATL_SLS VARCHAR(18),
	PROD_HIER VARCHAR(18),
	MGMT_ENTITY VARCHAR(6),
	FX_AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	CRNCY_KEY VARCHAR(5),
	AMT_OBJ_CRNCY NUMBER(20,5),
	OBJ_CRNCY_CO_OBJ VARCHAR(5),
	GRS_AMT_TRANS_CRNCY NUMBER(20,5),
	CRNCY_KEY_TRANS_CRNCY VARCHAR(5),
	QTY NUMBER(20,5),
	UOM VARCHAR(20),
	SLS_VOL NUMBER(20,5),
	UN_SLS_VOL VARCHAR(20),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE ASPITG_INTEGRATION.ITG_INVNT (
	REQUEST_NUMBER VARCHAR(100),
	DATA_PACKET VARCHAR(50),
	DATA_RECORD VARCHAR(100),
	LCL_CRNCY VARCHAR(5),
	BASE_UNIT VARCHAR(3),
	MATL_NO VARCHAR(18),
	STRG_LOC VARCHAR(4),
	STK_TYPE VARCHAR(1),
	STK_CAT VARCHAR(1),
	CO_CD VARCHAR(4),
	MATL_PLNT_VIEW VARCHAR(18),
	BTCH_NUM VARCHAR(10),
	PLNT VARCHAR(4),
	WHSE_NUM VARCHAR(3),
	STRG_BIN VARCHAR(10),
	STRG_TYPE VARCHAR(3),
	SPL_STCK_VAL VARCHAR(1),
	SPL_STCK_INDICA VARCHAR(1),
	DOC_DT DATE,
	VALUT_CLS VARCHAR(4),
	VALUT_AREA VARCHAR(4),
	VALUT_TYPE VARCHAR(10),
	PSTNG_DT DATE,
	CAL_DAY DATE,
	WH_MSTR VARCHAR(18),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(18,0),
	VEND VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	MVMT_IND_SEC VARCHAR(1),
	CRNCY VARCHAR(5),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(18,0),
	CAL_YR NUMBER(18,0),
	CAL_MO NUMBER(18,0),
	QTR NUMBER(18,0),
	CAL_YR_QTR NUMBER(18,0),
	CAL_YR_WK NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	HALF_YR NUMBER(18,0),
	WKDAY NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	CAL_YR_MON NUMBER(18,0),
	STCK_REC_VAL NUMBER(19,2),
	ISS_STCK_VAL NUMBER(19,2),
	ISS_BLOK_QTY NUMBER(20,3),
	CNG_STK_QTY_ISS NUMBER(20,3),
	ISS_QTY_QUAL NUMBER(20,3),
	ISS_QTY_TRST NUMBER(20,3),
	RCPT_QTY_BLOK NUMBER(20,3),
	CNS_STCK_RCPT NUMBER(20,3),
	RCPT_QTY_QUAL NUMBER(20,3),
	RCPT_QTY_TRST NUMBER(20,3),
	ISS_QTY_SCRAP NUMBER(20,3),
	ISS_VAL_SCRAP NUMBER(19,2),
	RCPT_TOT_STCK NUMBER(20,3),
	ISS_TOT_STCK NUMBER(20,3),
	ISS_QTY_STCK_VAL NUMBER(20,3),
	REC_QTY_VAL_STCK NUMBER(20,3),
	VDR_CNSGNMNT_STCK_CNVAL NUMBER(19,2),
	REC_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_BLOK NUMBER(19,2),
	ISS_VAL_BLOK NUMBER(19,2),
	ISS_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_TRST NUMBER(19,2),
	ISS_VAL_TRST NUMBER(19,2),
	ISS_VAL_CONS_STCK NUMBER(19,2),
	RCPT_VAL_CONS_STK NUMBER(19,2),
	BW_PRCH_VAL NUMBER(19,2),
	BW_AMT_BUNITM NUMBER(20,3),
	DELV_COST NUMBER(19,2),
	STD_MATL_COST NUMBER(19,2),
	PRC_UNIT NUMBER(20,3),
	LINE_CNT NUMBER(18,0),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE ASPITG_INTEGRATION.ITG_COPA17_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	FISC_YR_PER NUMBER(7,0),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(4,0),
	CAL_DAY DATE,
	PSTNG_PER NUMBER(3,0),
	CAL_YR_MO NUMBER(6,0),
	CAL_YR NUMBER(4,0),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(3,0),
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_EMP_HIST NUMBER(8,0),
	SLS_ORG VARCHAR(4),
	SLS_GRP VARCHAR(3),
	SLS_OFF VARCHAR(4),
	CUST_GRP VARCHAR(2),
	DSTN_CHNL VARCHAR(2),
	SLS_DSTRC VARCHAR(6),
	CUST VARCHAR(10),
	MATL VARCHAR(18),
	CUST_SLS_VIEW VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	MERCIA_REF VARCHAR(5),
	B3_BASE_PROD VARCHAR(3),
	B4_VRNT VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	RGN VARCHAR(3),
	CTRY VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	FRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	FISC_QTR NUMBER(1,0),
	MATL2 VARCHAR(18),
	BILL_TYPE VARCHAR(4),
	FISC_WK DATE,
	AMT_GRP_CRCY NUMBER(20,2),
	AMT_OBJ_CRCY NUMBER(20,2),
	CRNCY VARCHAR(5),
	OBJ_CRNCY VARCHAR(5),
	ACCT_NUM VARCHAR(10),
	CHRT_OF_ACCT VARCHAR(4),
	MGMT_ENTITY VARCHAR(6),
	SLS_PRSN_RESPONS VARCHAR(30),
	BUSN_AREA VARCHAR(4),
	GA NUMBER(20,2),
	TC VARCHAR(5),
	MATL_PLNT_VIEW VARCHAR(18),
	QTY NUMBER(20,3),
	UOM VARCHAR(10),
	SLS_VOL_IEU NUMBER(17,3),
	UN_SLS_VOL__IEU VARCHAR(10),
	BPT_DSTN_CHNL VARCHAR(2),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE ASPITG_INTEGRATION.ITG_INVC_SLS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	ACT_DELV_DT DATE,
	ACT_GOOD_ISS_DT DATE,
	BILL_TO_PRTY VARCHAR(10),
	BILL_DT DATE,
	BILL_TY VARCHAR(4),
	BILL_DOC VARCHAR(10),
	CMPY_CD VARCHAR(4),
	CUST_NO VARCHAR(10),
	DELV_DOC_CRT_DT DATE,
	DSTR_CHNL VARCHAR(2),
	DIV VARCHAR(2),
	DOC_CRT_DT DATE,
	DOC_DT DATE,
	GOOD_ISS_DT DATE,
	MAT VARCHAR(18),
	MAT_AVAIL_DT DATE,
	ORD_RSN VARCHAR(3),
	OVRL_REJ_STS VARCHAR(1),
	OVRL_STS_CRD_CHK VARCHAR(1),
	PAYER VARCHAR(10),
	PLANT VARCHAR(4),
	PREC_DOC_ITM NUMBER(18,0),
	PREC_DOC_NUM VARCHAR(10),
	PROOF_DELV_DT DATE,
	RSN_CD_KEY VARCHAR(29),
	RSN_REJ VARCHAR(2),
	RLSE_DT_CR_MGMT DATE,
	RQST_DELV_DT DATE,
	ROUTE VARCHAR(6),
	SLS_DOC VARCHAR(10),
	SLS_DOC_CAT VARCHAR(2),
	SLS_DOC_ITM NUMBER(18,0),
	SLS_DOC_TYP VARCHAR(4),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_ORG VARCHAR(4),
	SLS_DOC_ITM_CAT VARCHAR(4),
	SHIP_TO_PRTY VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	BILL_QTY_CSE NUMBER(15,4),
	BILL_QTY_PC NUMBER(15,4),
	BILL_QTY_DIFOT NUMBER(15,4),
	BILL_QTY_OTIF NUMBER(15,4),
	BILL_QTY_SLS_UOM NUMBER(15,4),
	CNFRM_QTY_DIFOT NUMBER(15,4),
	CNFRM_QTY_PC NUMBER(15,4),
	DELV_QTY_CSE NUMBER(15,4),
	DELV_QTY_PC NUMBER(15,4),
	DELV_QTY_SLS_UOM NUMBER(15,4),
	EST_NTS NUMBER(15,4),
	NTS_BILL NUMBER(15,4),
	NET_INVC_SLS NUMBER(15,4),
	FUT_SLS_QTY NUMBER(15,4),
	GROS_TRD_SLS NUMBER(15,4),
	NET_AMT NUMBER(15,4),
	NET_PRC NUMBER(15,4),
	NET_BILL_VAL NUMBER(15,4),
	NET_ORD_VAL NUMBER(15,4),
	ORD_QTY_CSE NUMBER(15,4),
	ORD_QTY_PC NUMBER(15,4),
	ORD_PC_QTY NUMBER(15,4),
	ORD_SLS_QTY NUMBER(15,4),
	TRAN_LDTM NUMBER(15,4),
	UNSPP_QTY NUMBER(15,4),
	UNSPP_VAL NUMBER(15,4),
	VOL_DELV NUMBER(15,4),
	VOL_ORD NUMBER(20,4),
	CAL_DAY DATE,
	BASE_UOM VARCHAR(4),
	CURR_KEY VARCHAR(5),
	DOC_CURR VARCHAR(5),
	SLS_UNIT VARCHAR(4),
	FISC_YR VARCHAR(10),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE ASPITG_INTEGRATION.ITG_RG_TRAVEL_RETAIL_SELLOUT (
	CTRY_CD VARCHAR(2),
	CRNCY_CD VARCHAR(3),
	RETAILER_NAME VARCHAR(50),
	YEAR NUMBER(18,0),
	MONTH NUMBER(18,0),
	YEAR_MONTH VARCHAR(10),
	BRAND VARCHAR(50),
	SKU VARCHAR(50),
	PRODUCT_DESCRIPTION VARCHAR(100),
	DCL_CODE VARCHAR(50),
	EAN VARCHAR(50),
	RSP NUMBER(18,0),
	DOOR_NAME VARCHAR(50),
	SLS_QTY NUMBER(18,0),
	SLS_AMT NUMBER(21,5),
	STOCK_QTY NUMBER(18,0),
	STOCK_AMT NUMBER(21,5),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	STORE_SLS_QTY NUMBER(18,0),
	STORE_SLS_AMT NUMBER(38,18),
	ECOMMERCE_SLS_QTY NUMBER(18,0),
	ECOMMERCE_SLS_AMT NUMBER(38,18),
	MEMBERSHIP_SLS_QTY NUMBER(18,0),
	MEMBERSHIP_SLS_AMT NUMBER(38,18)
);

create or replace TABLE SGPITG_INTEGRATION.ITG_QUERY_PARAMETERS (
	COUNTRY_CODE VARCHAR(20),
	PARAMETER_NAME VARCHAR(300),
	PARAMETER_VALUE VARCHAR(300),
	PARAMETER_TYPE VARCHAR(100)
);

create or replace TABLE SGPITG_INTEGRATION.SDL_RAW_SG_CIW_MAPPING (
	CONDITION_TYPE VARCHAR(500),
	GL VARCHAR(10),
	GL_DESCRIPTION VARCHAR(500),
	POSTED_WHERE VARCHAR(500),
	PURPOSE VARCHAR(500),
	CIW_BUCKET VARCHAR(500),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(255),
	RUN_ID NUMBER(14,0)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_COPA17_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	FISC_YR_PER NUMBER(7,0),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(4,0),
	CAL_DAY DATE,
	PSTNG_PER NUMBER(3,0),
	CAL_YR_MO NUMBER(6,0),
	CAL_YR NUMBER(4,0),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(3,0),
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_EMP_HIST NUMBER(8,0),
	SLS_ORG VARCHAR(4),
	SLS_GRP VARCHAR(3),
	SLS_OFF VARCHAR(4),
	CUST_GRP VARCHAR(2),
	DSTN_CHNL VARCHAR(2),
	SLS_DSTRC VARCHAR(6),
	CUST VARCHAR(10),
	MATL VARCHAR(18),
	CUST_SLS_VIEW VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	MERCIA_REF VARCHAR(5),
	B3_BASE_PROD VARCHAR(3),
	B4_VRNT VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	RGN VARCHAR(3),
	CTRY VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	FRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	FISC_QTR NUMBER(1,0),
	MATL2 VARCHAR(18),
	BILL_TYPE VARCHAR(4),
	FISC_WK DATE,
	AMT_GRP_CRCY NUMBER(20,2),
	AMT_OBJ_CRCY NUMBER(20,2),
	CRNCY VARCHAR(5),
	OBJ_CRNCY VARCHAR(5),
	ACCT_NUM VARCHAR(10),
	CHRT_OF_ACCT VARCHAR(4),
	MGMT_ENTITY VARCHAR(6),
	SLS_PRSN_RESPONS VARCHAR(30),
	BUSN_AREA VARCHAR(4),
	GA NUMBER(20,2),
	TC VARCHAR(5),
	MATL_PLNT_VIEW VARCHAR(18),
	QTY NUMBER(20,3),
	UOM VARCHAR(10),
	SLS_VOL_IEU NUMBER(17,3),
	UN_SLS_VOL__IEU VARCHAR(10),
	BPT_DSTN_CHNL VARCHAR(2),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_COPA_TRANS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	CO_CD VARCHAR(4),
	CNTL_AREA VARCHAR(4),
	PRFT_CTR VARCHAR(10),
	SLS_ORG VARCHAR(4),
	MATL VARCHAR(18),
	CUST_NUM VARCHAR(10),
	DIV VARCHAR(2),
	PLNT VARCHAR(4),
	CHRT_ACCT VARCHAR(4),
	ACCT_NUM VARCHAR(10),
	DSTR_CHNL VARCHAR(2),
	FISC_YR_VAR VARCHAR(2),
	VERS VARCHAR(3),
	BW_DELTA_UPD_MODE VARCHAR(1),
	BILL_TYP VARCHAR(4),
	SLS_OFF VARCHAR(4),
	CNTRY_KEY VARCHAR(3),
	SLS_DEAL VARCHAR(10),
	SLS_GRP VARCHAR(3),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_DIST VARCHAR(6),
	CUST_GRP VARCHAR(2),
	CUST_SLS VARCHAR(10),
	BUSS_AREA VARCHAR(4),
	VAL_TYPE_RPT NUMBER(18,0),
	MERCIA_REF VARCHAR(5),
	CALN_DAY VARCHAR(20),
	CALN_YR_MO NUMBER(18,0),
	FISC_YR NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	B3_BASE_PROD VARCHAR(3),
	B4_VAR VARCHAR(3),
	B5_PUT_UP VARCHAR(3),
	B1_MEGA_BRND VARCHAR(3),
	B2_BRND VARCHAR(3),
	REG VARCHAR(3),
	PROD_MINOR VARCHAR(18),
	PROD_MAJ VARCHAR(18),
	PROD_FRAN VARCHAR(18),
	FRAN VARCHAR(18),
	GRAN_GRP VARCHAR(18),
	OPER_GRP VARCHAR(18),
	SLS_PRSN_RESP VARCHAR(30),
	MATL_SLS VARCHAR(18),
	PROD_HIER VARCHAR(18),
	MGMT_ENTITY VARCHAR(6),
	FX_AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	AMT_CNTL_AREA_CRNCY NUMBER(20,5),
	CRNCY_KEY VARCHAR(5),
	AMT_OBJ_CRNCY NUMBER(20,5),
	OBJ_CRNCY_CO_OBJ VARCHAR(5),
	GRS_AMT_TRANS_CRNCY NUMBER(20,5),
	CRNCY_KEY_TRANS_CRNCY VARCHAR(5),
	QTY NUMBER(20,5),
	UOM VARCHAR(20),
	SLS_VOL NUMBER(20,5),
	UN_SLS_VOL VARCHAR(20),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_INVC_SLS (
	REQUEST_NUMBER VARCHAR(100) NOT NULL,
	DATA_PACKET VARCHAR(50) NOT NULL,
	DATA_RECORD VARCHAR(100) NOT NULL,
	ACT_DELV_DT DATE,
	ACT_GOOD_ISS_DT DATE,
	BILL_TO_PRTY VARCHAR(10),
	BILL_DT DATE,
	BILL_TY VARCHAR(4),
	BILL_DOC VARCHAR(10),
	CMPY_CD VARCHAR(4),
	CUST_NO VARCHAR(10),
	DELV_DOC_CRT_DT DATE,
	DSTR_CHNL VARCHAR(2),
	DIV VARCHAR(2),
	DOC_CRT_DT DATE,
	DOC_DT DATE,
	GOOD_ISS_DT DATE,
	MAT VARCHAR(18),
	MAT_AVAIL_DT DATE,
	ORD_RSN VARCHAR(3),
	OVRL_REJ_STS VARCHAR(1),
	OVRL_STS_CRD_CHK VARCHAR(1),
	PAYER VARCHAR(10),
	PLANT VARCHAR(4),
	PREC_DOC_ITM NUMBER(18,0),
	PREC_DOC_NUM VARCHAR(10),
	PROOF_DELV_DT DATE,
	RSN_CD_KEY VARCHAR(29),
	RSN_REJ VARCHAR(2),
	RLSE_DT_CR_MGMT DATE,
	RQST_DELV_DT DATE,
	ROUTE VARCHAR(6),
	SLS_DOC VARCHAR(10),
	SLS_DOC_CAT VARCHAR(2),
	SLS_DOC_ITM NUMBER(18,0),
	SLS_DOC_TYP VARCHAR(4),
	SLS_EMP_HIST NUMBER(18,0),
	SLS_ORG VARCHAR(4),
	SLS_DOC_ITM_CAT VARCHAR(4),
	SHIP_TO_PRTY VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	BILL_QTY_CSE NUMBER(15,4),
	BILL_QTY_PC NUMBER(15,4),
	BILL_QTY_DIFOT NUMBER(15,4),
	BILL_QTY_OTIF NUMBER(15,4),
	BILL_QTY_SLS_UOM NUMBER(15,4),
	CNFRM_QTY_DIFOT NUMBER(15,4),
	CNFRM_QTY_PC NUMBER(15,4),
	DELV_QTY_CSE NUMBER(15,4),
	DELV_QTY_PC NUMBER(15,4),
	DELV_QTY_SLS_UOM NUMBER(15,4),
	EST_NTS NUMBER(15,4),
	NTS_BILL NUMBER(15,4),
	NET_INVC_SLS NUMBER(15,4),
	FUT_SLS_QTY NUMBER(15,4),
	GROS_TRD_SLS NUMBER(15,4),
	NET_AMT NUMBER(15,4),
	NET_PRC NUMBER(15,4),
	NET_BILL_VAL NUMBER(15,4),
	NET_ORD_VAL NUMBER(15,4),
	ORD_QTY_CSE NUMBER(15,4),
	ORD_QTY_PC NUMBER(15,4),
	ORD_PC_QTY NUMBER(15,4),
	ORD_SLS_QTY NUMBER(15,4),
	TRAN_LDTM NUMBER(15,4),
	UNSPP_QTY NUMBER(15,4),
	UNSPP_VAL NUMBER(15,4),
	VOL_DELV NUMBER(15,4),
	VOL_ORD NUMBER(20,4),
	CAL_DAY DATE,
	BASE_UOM VARCHAR(4),
	CURR_KEY VARCHAR(5),
	DOC_CURR VARCHAR(5),
	SLS_UNIT VARCHAR(4),
	FISC_YR VARCHAR(10),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9),
	primary key (REQUEST_NUMBER, DATA_PACKET, DATA_RECORD)
);

create or replace TABLE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_INVNT (
	REQUEST_NUMBER VARCHAR(100),
	DATA_PACKET VARCHAR(50),
	DATA_RECORD VARCHAR(100),
	LCL_CRNCY VARCHAR(5),
	BASE_UNIT VARCHAR(3),
	MATL_NO VARCHAR(18),
	STRG_LOC VARCHAR(4),
	STK_TYPE VARCHAR(1),
	STK_CAT VARCHAR(1),
	CO_CD VARCHAR(4),
	MATL_PLNT_VIEW VARCHAR(18),
	BTCH_NUM VARCHAR(10),
	PLNT VARCHAR(4),
	WHSE_NUM VARCHAR(3),
	STRG_BIN VARCHAR(10),
	STRG_TYPE VARCHAR(3),
	SPL_STCK_VAL VARCHAR(1),
	SPL_STCK_INDICA VARCHAR(1),
	DOC_DT DATE,
	VALUT_CLS VARCHAR(4),
	VALUT_AREA VARCHAR(4),
	VALUT_TYPE VARCHAR(10),
	PSTNG_DT DATE,
	CAL_DAY DATE,
	WH_MSTR VARCHAR(18),
	VERS VARCHAR(3),
	VAL_TYPE NUMBER(18,0),
	VEND VARCHAR(10),
	SOLD_TO_PRTY VARCHAR(10),
	MVMT_IND_SEC VARCHAR(1),
	CRNCY VARCHAR(5),
	FISC_YR_VRNT VARCHAR(2),
	FISC_YR NUMBER(18,0),
	CAL_YR NUMBER(18,0),
	CAL_MO NUMBER(18,0),
	QTR NUMBER(18,0),
	CAL_YR_QTR NUMBER(18,0),
	CAL_YR_WK NUMBER(18,0),
	PSTNG_PER NUMBER(18,0),
	HALF_YR NUMBER(18,0),
	WKDAY NUMBER(18,0),
	FISC_YR_PER NUMBER(18,0),
	CAL_YR_MON NUMBER(18,0),
	STCK_REC_VAL NUMBER(19,2),
	ISS_STCK_VAL NUMBER(19,2),
	ISS_BLOK_QTY NUMBER(20,3),
	CNG_STK_QTY_ISS NUMBER(20,3),
	ISS_QTY_QUAL NUMBER(20,3),
	ISS_QTY_TRST NUMBER(20,3),
	RCPT_QTY_BLOK NUMBER(20,3),
	CNS_STCK_RCPT NUMBER(20,3),
	RCPT_QTY_QUAL NUMBER(20,3),
	RCPT_QTY_TRST NUMBER(20,3),
	ISS_QTY_SCRAP NUMBER(20,3),
	ISS_VAL_SCRAP NUMBER(19,2),
	RCPT_TOT_STCK NUMBER(20,3),
	ISS_TOT_STCK NUMBER(20,3),
	ISS_QTY_STCK_VAL NUMBER(20,3),
	REC_QTY_VAL_STCK NUMBER(20,3),
	VDR_CNSGNMNT_STCK_CNVAL NUMBER(19,2),
	REC_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_BLOK NUMBER(19,2),
	ISS_VAL_BLOK NUMBER(19,2),
	ISS_VAL_Q_STCK NUMBER(19,2),
	REC_VAL_TRST NUMBER(19,2),
	ISS_VAL_TRST NUMBER(19,2),
	ISS_VAL_CONS_STCK NUMBER(19,2),
	RCPT_VAL_CONS_STK NUMBER(19,2),
	BW_PRCH_VAL NUMBER(19,2),
	BW_AMT_BUNITM NUMBER(20,3),
	DELV_COST NUMBER(19,2),
	STD_MATL_COST NUMBER(19,2),
	PRC_UNIT NUMBER(20,3),
	LINE_CNT NUMBER(18,0),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
