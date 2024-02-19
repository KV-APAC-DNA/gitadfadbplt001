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

