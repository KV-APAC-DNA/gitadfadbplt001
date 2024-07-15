CREATE or replace TABLE prod_dna_core.aspitg_integration.ITG_RE_MSL_INPUT_DEFINITION (		--// CREATE TABLE rg_itg.itg_re_msl_input_definition (
    start_date varchar(50),		--//  ENCODE lzo // character varying
    end_date varchar(50),		--//  ENCODE lzo // character varying
    market varchar(50),		--//  ENCODE lzo // character varying
    region varchar(20),		--//  ENCODE lzo // character varying
    zone varchar(20),		--//  ENCODE lzo // character varying
    retail_environment varchar(50),		--//  ENCODE lzo // character varying
    channel varchar(50),		--//  ENCODE lzo // character varying
    sub_channel varchar(50),		--//  ENCODE lzo // character varying
    customer varchar(50),		--//  ENCODE lzo // character varying
    store_grade varchar(20),		--//  ENCODE lzo // character varying
    unique_identifier_mapping varchar(50),		--//  ENCODE lzo // character varying
    sku_unique_identifier varchar(100) ,		--// distkey //  ENCODE lzo // character varying
    sku_description varchar(500),		--//  ENCODE lzo // character varying
    sku_code varchar(50),		--//  ENCODE lzo // character varying
    product_key varchar(500),		--//  ENCODE lzo // character varying
    msl_final varchar(200),		--//  ENCODE lzo // character varying
    active_status_code varchar(20),		--//  ENCODE lzo // character varying
    sourceexistenceflag_code varchar(20)		--//  ENCODE lzo // character varying
)
;	
