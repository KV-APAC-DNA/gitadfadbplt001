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


