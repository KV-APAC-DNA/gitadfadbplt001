CREATE TABLE ASPEDW_INTEGRATION.EDW_CRNCY_EXCH_RATES (		--// CREATE TABLE ASPEDW_INTEGRATION.edw_crncy_exch_rates (
    valid_from date NOT NULL,		--//  ENCODE raw
    fisc_yr_per numeric(18,0) NOT NULL,		--//  ENCODE raw // integer 
    fisc_yr_per_rep date NOT NULL,		--//  ENCODE raw
    fisc_yr_per_fday date NOT NULL,		--//  ENCODE raw
    fisc_yr_per_lday date NOT NULL,		--//  ENCODE raw
    ex_rt_typ varchar(4) NOT NULL,		--//  ENCODE raw // character varying
    from_crncy varchar(5) NOT NULL,		--//  ENCODE raw // character varying
    to_crncy varchar(5) NOT NULL,		--//  ENCODE raw // character varying
    act_valid_from date NOT NULL,		--//  ENCODE raw
    ex_rt numeric(9,5),		--//  ENCODE raw
    from_ratio numeric(9,0),		--//  ENCODE raw
    to_ratio numeric(9,0),		--//  ENCODE raw
    PRIMARY KEY (valid_from, ex_rt_typ, from_crncy, to_crncy)
)
;		--// DISTSTYLE EVEN;
-----------------------------------------------------------------------------
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_bounce_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_BOUNCE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_bounce_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_children_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_CHILDREN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_children_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_click_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_CLICK_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_click_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_complaint_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_COMPLAINT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_complaint_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_consumer_master;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_CONSUMER_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_consumer_master
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_consumer_master_additional;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_CONSUMER_MASTER_ADDITIONAL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_consumer_master_additional
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_open_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_OPEN_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_open_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_sent_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_SENT_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_sent_data
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
--DROP TABLE ASPITG_INTEGRATION.itg_sfmc_unsubscribe_data;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_UNSUBSCRIBE_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_sfmc_unsubscribe_data
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

--DROP TABLE ASPITG_INTEGRATION.itg_parameter_reg_inventory;
CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_PARAMETER_REG_INVENTORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE ASPITG_INTEGRATION.itg_parameter_reg_inventory
(
	country_name VARCHAR(30)  		--//  ENCODE lzo
	,parameter_name VARCHAR(200)  		--//  ENCODE lzo
	,parameter_value VARCHAR(300)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPITG_INTEGRATION.itg_query_parameters;

------------------------------------------------------------------------------------

CREATE TABLE ASPWKS_INTEGRATION.WKS_KR_INVENTORY_HEALTH_ANALYSIS_PROPAGATION (		--// CREATE TABLE ASPWKS_INTEGRATION.wks_kr_inventory_health_analysis_propagation (
    cal_year numeric(18,0),		--//  ENCODE az64 // integer
    cal_qrtr_no varchar(11),		--//  ENCODE lzo // character varying
    cal_mnth_id varchar(23),		--//  ENCODE lzo // character varying
    cal_mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cntry_nm varchar(5),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(2),		--//  ENCODE lzo // character varying
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
    sku_cd varchar(50),		--//  ENCODE lzo // character varying
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
    region varchar(5),		--//  ENCODE lzo // character varying
    zone_or_area varchar(5),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(25,0),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,5),		--//  ENCODE az64
    last_6months_so_qty numeric(38,5),		--//  ENCODE az64
    last_12months_so_qty numeric(38,5),		--//  ENCODE az64
    last_3months_so_val numeric(38,9),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,9),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,9),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,9)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE TABLE ASPWKS_INTEGRATION.WKS_KR_INVENTORY_HEALTH_ANALYSIS_PROPAGATION_FINAL (		--// CREATE TABLE ASPWKS_INTEGRATION.wks_kr_inventory_health_analysis_propagation_final (
    cal_year numeric(18,0),		--//  ENCODE az64 // integer
    cal_qrtr_no varchar(11),		--//  ENCODE lzo // character varying
    cal_mnth_id varchar(23),		--//  ENCODE lzo // character varying
    cal_mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cntry_nm varchar(5),		--//  ENCODE lzo // character varying
    dstrbtr_grp_cd varchar(2),		--//  ENCODE lzo // character varying
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
    sku_cd varchar(50),		--//  ENCODE lzo // character varying
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
    region varchar(5),		--//  ENCODE lzo // character varying
    zone_or_area varchar(5),		--//  ENCODE lzo // character varying
    si_sls_qty numeric(38,5),		--//  ENCODE az64
    si_gts_val numeric(38,5),		--//  ENCODE az64
    si_gts_val_usd numeric(38,5),		--//  ENCODE az64
    inventory_quantity numeric(38,5),		--//  ENCODE az64
    inventory_val numeric(38,5),		--//  ENCODE az64
    inventory_val_usd numeric(38,5),		--//  ENCODE az64
    so_sls_qty numeric(38,5),		--//  ENCODE az64
    so_trd_sls numeric(38,5),		--//  ENCODE az64
    so_trd_sls_usd numeric(25,0),		--//  ENCODE az64
    si_all_db_val numeric(38,5),		--//  ENCODE az64
    si_all_db_val_usd numeric(38,5),		--//  ENCODE az64
    si_inv_db_val numeric(38,5),		--//  ENCODE az64
    si_inv_db_val_usd numeric(38,5),		--//  ENCODE az64
    last_3months_so_qty numeric(38,5),		--//  ENCODE az64
    last_6months_so_qty numeric(38,5),		--//  ENCODE az64
    last_12months_so_qty numeric(38,5),		--//  ENCODE az64
    last_3months_so_val numeric(38,9),		--//  ENCODE az64
    last_3months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_6months_so_val numeric(38,9),		--//  ENCODE az64
    last_6months_so_val_usd numeric(38,5),		--//  ENCODE az64
    last_12months_so_val numeric(38,9),		--//  ENCODE az64
    last_12months_so_val_usd numeric(38,5),		--//  ENCODE az64
    propagate_flag varchar(1),		--//  ENCODE lzo // character varying
    propagate_from numeric(18,0),		--//  ENCODE az64 // integer
    reason varchar(100),		--//  ENCODE lzo // character varying
    last_36months_so_val numeric(38,9),		--//  ENCODE az64
    healthy_inventory varchar(1),		--//  ENCODE lzo // character varying
    min_date date,		--//  ENCODE az64
    diff_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l6m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l3m_weeks numeric(38,0),		--//  ENCODE az64 // bigint
    l12m_weeks_avg_sales numeric(38,9),		--//  ENCODE az64
    l6m_weeks_avg_sales numeric(38,9),		--//  ENCODE az64
    l3m_weeks_avg_sales numeric(38,9),		--//  ENCODE az64
    l12m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_usd numeric(38,5),		--//  ENCODE az64
    l12m_weeks_avg_sales_qty numeric(38,5),		--//  ENCODE az64
    l6m_weeks_avg_sales_qty numeric(38,5),		--//  ENCODE az64
    l3m_weeks_avg_sales_qty numeric(38,5)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;
