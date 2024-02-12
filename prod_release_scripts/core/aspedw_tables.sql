USE DATABASE PROD_DNA_CORE;
USE schema ASPEDW_INTEGRATION;

CREATE OR REPLACE TABLE EDW_RPT_REGIONAL_SELLOUT_OFFTAKE (		--// CREATE TABLE rg_edw.edw_rpt_regional_sellout_offtake (
    year numeric(18,0),		--//  ENCODE az64 // integer
    qrtr_no varchar(14),		--//  ENCODE lzo // character varying
    mnth_id varchar(23),		--//  ENCODE lzo // character varying
    mnth_no numeric(18,0),		--//  ENCODE az64 // integer
    cal_date date,		--//  ENCODE az64
    week_start_date date,		--//  ENCODE az64
    univ_year numeric(18,0),		--//  ENCODE az64 // integer
    univ_month numeric(18,0),		--//  ENCODE az64 // integer
    country_code varchar(2),		--//  ENCODE lzo // character varying
    country_name varchar(50),		--//  ENCODE lzo // character varying
    data_source varchar(14),		--//  ENCODE lzo // character varying
    soldto_code varchar(255),		--//  ENCODE lzo // character varying
    distributor_code varchar(100),		--//  ENCODE lzo // character varying
    distributor_name varchar(255),		--//  ENCODE lzo // character varying
    store_code varchar(100),		--//  ENCODE lzo // character varying
    store_name varchar(500),		--//  ENCODE lzo // character varying
    store_type varchar(255),		--//  ENCODE lzo // character varying
    distributor_additional_attribute1 varchar(150),		--//  ENCODE lzo // character varying
    distributor_additional_attribute2 varchar(150),		--//  ENCODE lzo // character varying
    distributor_additional_attribute3 varchar(150),		--//  ENCODE lzo // character varying
    sap_parent_customer_key varchar(12),		--//  ENCODE lzo // character varying
    sap_parent_customer_description varchar(75),		--//  ENCODE lzo // character varying
    sap_customer_channel_key varchar(12),		--//  ENCODE lzo // character varying
    sap_customer_channel_description varchar(75),		--//  ENCODE lzo // character varying
    sap_customer_sub_channel_key varchar(12),		--//  ENCODE lzo // character varying
    sap_sub_channel_description varchar(75),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_key varchar(12),		--//  ENCODE lzo // character varying
    sap_go_to_mdl_description varchar(75),		--//  ENCODE lzo // character varying
    sap_banner_key varchar(12),		--//  ENCODE lzo // character varying
    sap_banner_description varchar(75),		--//  ENCODE lzo // character varying
    sap_banner_format_key varchar(12),		--//  ENCODE lzo // character varying
    sap_banner_format_description varchar(75),		--//  ENCODE lzo // character varying
    retail_environment varchar(50),		--//  ENCODE lzo // character varying
    region varchar(150),		--//  ENCODE lzo // character varying
    zone_or_area varchar(150),		--//  ENCODE lzo // character varying
    customer_segment_key varchar(12),		--//  ENCODE lzo // character varying
    customer_segment_description varchar(50),		--//  ENCODE lzo // character varying
    global_product_franchise varchar(30),		--//  ENCODE lzo // character varying
    global_product_brand varchar(30),		--//  ENCODE lzo // character varying
    global_product_sub_brand varchar(100),		--//  ENCODE lzo // character varying
    global_product_variant varchar(100),		--//  ENCODE lzo // character varying
    global_product_segment varchar(50),		--//  ENCODE lzo // character varying
    global_product_subsegment varchar(100),		--//  ENCODE lzo // character varying
    global_product_category varchar(50),		--//  ENCODE lzo // character varying
    global_product_subcategory varchar(50),		--//  ENCODE lzo // character varying
    global_put_up_description varchar(100),		--//  ENCODE lzo // character varying
    ean varchar(50),		--//  ENCODE lzo // character varying
    sku_code varchar(40),		--//  ENCODE lzo // character varying
    sku_description varchar(150),		--//  ENCODE lzo // character varying
    pka_product_key varchar(68),		--//  ENCODE lzo // character varying
    pka_product_key_description varchar(255),		--//  ENCODE lzo // character varying
    from_currency varchar(5),		--//  ENCODE lzo // character varying
    to_currency varchar(5),		--//  ENCODE lzo // character varying
    exchange_rate numeric(15,5),		--//  ENCODE az64
    sellout_sales_quantity numeric(38,6),		--//  ENCODE az64
    sellout_sales_value numeric(38,6),		--//  ENCODE az64
    sellout_sales_value_usd numeric(38,11),		--//  ENCODE az64
    list_price numeric(38,6),		--//  ENCODE az64
    sellout_value_list_price numeric(38,12),		--//  ENCODE az64
    sellout_value_list_price_usd numeric(38,17),		--//  ENCODE az64
    selling_price numeric(38,4),		--//  ENCODE az64
    first_scan_flag_parent_customer_level varchar(1),		--//  ENCODE lzo // character varying
    first_scan_flag_market_level varchar(1),		--//  ENCODE lzo // character varying
    npd_flag_market_level varchar(1),		--//  ENCODE lzo // character varying
    npd_flag_parent_customer_level varchar(1),		--//  ENCODE lzo // character varying
    customer_product_desc varchar(300),		--//  ENCODE lzo // character varying
    msl_product_code varchar(150),		--//  ENCODE lzo // character varying
    msl_product_desc varchar(300),		--//  ENCODE lzo // character varying
    store_grade varchar(150),		--//  ENCODE lzo // character varying
    retail_env varchar(150),		--//  ENCODE lzo // character varying
    channel varchar(150),		--//  ENCODE lzo // character varying
    crtd_dttm timestamp without time zone,		--//  ENCODE az64
    updt_dttm timestamp without time zone		--//  ENCODE az64
)
		--// DISTSTYLE AUTO
;		--// SORTKEY ( country_code );



