CREATE OR REPLACE TABLE INDEDW_INTEGRATION.edw_rpt_sales_details_increment (		--// CREATE TABLE in_edw.edw_rpt_sales_details_increment (
    data_increment_key numeric(18,0),		--//  ENCODE az64 // integer
    customer_code varchar(50),		--//  ENCODE lzo // character varying
    invoice_no varchar(50),		--//  ENCODE lzo // character varying
    invoice_date numeric(18,0),		--//  ENCODE az64 // integer
    retailer_code varchar(100),		--//  ENCODE lzo // character varying
    product_code varchar(50),		--//  ENCODE lzo // character varying
    quantity numeric(38,0),		--//  ENCODE az64 // bigint
    nr_value numeric(38,6),		--//  ENCODE az64
    achievement_nr numeric(38,6),		--//  ENCODE az64
    invoice_status varchar(1),		--//  ENCODE lzo // character varying
    customer_name varchar(150),		--//  ENCODE lzo // character varying
    region_name varchar(50),		--//  ENCODE lzo // character varying
    zone_name varchar(50),		--//  ENCODE lzo // character varying
    territory_name varchar(50),		--//  ENCODE lzo // character varying
    territory_classification varchar(50),		--//  ENCODE lzo // character varying
    class_desc varchar(50),		--//  ENCODE lzo // character varying
    outlet varchar(50),		--//  ENCODE lzo // character varying
    channel_name varchar(150),		--//  ENCODE lzo // character varying
    business_channel varchar(50),		--//  ENCODE lzo // character varying
    loyalty_desc varchar(50),		--//  ENCODE lzo // character varying
    status_desc varchar(10),		--//  ENCODE lzo // character varying
    product_name varchar(50),		--//  ENCODE lzo // character varying
    product_desc varchar(100),		--//  ENCODE lzo // character varying
    franchise_name varchar(50),		--//  ENCODE lzo // character varying
    brand_name varchar(50),		--//  ENCODE lzo // character varying
    product_category_name varchar(150),		--//  ENCODE lzo // character varying
    variant_name varchar(150),		--//  ENCODE lzo // character varying
    mothersku_name varchar(150),		--//  ENCODE lzo // character varying
    day numeric(18,0),		--//  ENCODE az64 // integer
    mth_mm numeric(18,0),		--//  ENCODE az64 // integer
    mth_yyyymm numeric(18,0),		--//  ENCODE az64 // integer
    qtr numeric(18,0),		--//  ENCODE az64 // integer
    yyyyqtr numeric(18,0),		--//  ENCODE az64 // integer
    cal_yr numeric(18,0),		--//  ENCODE az64 // integer
    fisc_yr numeric(18,0),		--//  ENCODE az64 // integer
    week numeric(18,0),		--//  ENCODE az64 // integer
    month varchar(3),		--//  ENCODE lzo // character varying
    retailer_name varchar(100),		--//  ENCODE lzo // character varying
    salesman_name varchar(200),		--//  ENCODE lzo // character varying
    salesman_code varchar(100),		--//  ENCODE lzo // character varying
    route_name varchar(200),		--//  ENCODE lzo // character varying
    route_code varchar(100),		--//  ENCODE lzo // character varying
    active_flag varchar(8),		--//  ENCODE lzo // character varying
    rtrlatitude varchar(40),		--//  ENCODE lzo // character varying
    rtrlongitude varchar(40),		--//  ENCODE lzo // character varying
    rtruniquecode varchar(100),		--//  ENCODE lzo // character varying
    unique_sales_code varchar(15),		--//  ENCODE lzo // character varying
    createddate timestamp without time zone,		--//  ENCODE az64
    retailer_address1 varchar(250),		--//  ENCODE lzo // character varying
    retailer_address2 varchar(250),		--//  ENCODE lzo // character varying
    retailer_address3 varchar(250),		--//  ENCODE lzo // character varying
    zone_classification varchar(50),		--//  ENCODE lzo // character varying
    state_name varchar(50),		--//  ENCODE lzo // character varying
    town_name varchar(50),		--//  ENCODE lzo // character varying
    town_classification varchar(100),		--//  ENCODE lzo // character varying
    achievement_amt numeric(38,6),		--//  ENCODE az64
    gross_amt numeric(38,6),		--//  ENCODE az64
    net_amt numeric(38,6),		--//  ENCODE az64
    num_lines numeric(38,0),		--//  ENCODE az64 // bigint
    prd_disc_sch_amt numeric(38,6),		--//  ENCODE az64
    tax_amt numeric(38,6),		--//  ENCODE az64
    qps_amt numeric(38,6),		--//  ENCODE az64
    qps_qty numeric(38,0),		--//  ENCODE az64 // bigint
    sku_rec_amt numeric(38,6),		--//  ENCODE az64
    sku_rec_qty numeric(38,0),		--//  ENCODE az64 // bigint
    num_recsku_lines numeric(38,0),		--//  ENCODE az64 // bigint
    num_buying_retailers varchar(151),		--//  ENCODE lzo // character varying
    num_bills varchar(202),		--//  ENCODE lzo // character varying
    num_packs varchar(50),		--//  ENCODE lzo // character varying
    udc_hsacounter varchar(150),		--//  ENCODE lzo // character varying
    udc_keyaccountname varchar(150),		--//  ENCODE lzo // character varying
    udc_pharmacychain varchar(150),		--//  ENCODE lzo // character varying
    udc_onedetailingbaby varchar(150),		--//  ENCODE lzo // character varying
    udc_sanprovisibilitysss varchar(150),		--//  ENCODE lzo // character varying
    udc_sssconsoffer varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumclub2018 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssendcaps varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumclub2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_signature2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_premium2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_gstn varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq22019new varchar(150),		--//  ENCODE lzo // character varying
    udc_sssprogram2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_umangq32019 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssscheme2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_ssspromoter2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_rtrtypeattr varchar(150),		--//  ENCODE lzo // character varying
    udc_bhagidariq32019 varchar(150),		--//  ENCODE lzo // character varying
    udc_directorclubq32019 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq32019new varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq22019 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq32019 varchar(150),		--//  ENCODE lzo // character varying
    udc_bbastore varchar(150),		--//  ENCODE lzo // character varying
    udc_avbabybodydocq42019 varchar(150),		--//  ENCODE lzo // character varying
    udc_orslcac2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_babyprofesionalcac2019 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq42019 varchar(150),		--//  ENCODE lzo // character varying
    udc_schemesss2020 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq12020 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_bhagidariq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_directorclubq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_umangq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_daudq32020 varchar(150),		--//  ENCODE lzo // character varying
    udc_daudq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_directorclubq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssprogramq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_bhagidariq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_umangq42020 varchar(150),		--//  ENCODE lzo // character varying
    udc_samriddhi varchar(150),		--//  ENCODE lzo // character varying
    udc_sssprogramq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_orslcac2021 varchar(150),		--//  ENCODE lzo // character varying
    udc_bhagidariq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_daudq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_directorclubq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_umangq12021 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssprogramq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_bhagidariq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_daudq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_directorclubq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_umangq22021 varchar(150),		--//  ENCODE lzo // character varying
    udc_platinumq32021 varchar(150),		--//  ENCODE lzo // character varying
    udc_sssprogramq32021 varchar(150),		--//  ENCODE lzo // character varying
    retailer_category_cd varchar(25),		--//  ENCODE lzo // character varying
    retailer_category_name varchar(50),		--//  ENCODE lzo // character varying
    csrtrcode varchar(50),		--//  ENCODE lzo // character varying
    abi_ntid varchar(50),		--//  ENCODE lzo // character varying
    flm_ntid varchar(50),		--//  ENCODE lzo // character varying
    bdm_ntid varchar(50),		--//  ENCODE lzo // character varying
    rsm_ntid varchar(50),		--//  ENCODE lzo // character varying
    crt_dttm timestamp without time zone,		--//  ENCODE az64
    updt_dttm timestamp without time zone,		--//  ENCODE az64
    type_name varchar(50),		--//  ENCODE lzo // character varying
    customer_type varchar(255),		--//  ENCODE lzo // character varying
    salesman_status varchar(20),		--//  ENCODE lzo // character varying
    total_retailers numeric(18,0),		--//  ENCODE az64 // integer
    prd_free_qty numeric(18,0),		--//  ENCODE az64 // integer
    retailer_channel_1 varchar(200),		--//  ENCODE lzo // character varying
    retailer_channel_2 varchar(200),		--//  ENCODE lzo // character varying
    retailer_channel_3 varchar(200),		--//  ENCODE lzo // character varying
    report_channel varchar(200),		--//  ENCODE lzo // character varying
    udc_newgtm varchar(150),		--//  ENCODE zstd // character varying
    udc_sssprogramq12022 varchar(150),		--//  ENCODE zstd // character varying
    udc_ssspharmacystore varchar(150),		--//  ENCODE zstd // character varying
    udc_ssstotstores varchar(150),		--//  ENCODE zstd // character varying
    udc_platinumq12022 varchar(150),		--//  ENCODE zstd // character varying
    udc_hsacounterq12022 varchar(150),		--//  ENCODE zstd // character varying
    udc_bssaveenoudc2022 varchar(150),		--//  ENCODE zstd // character varying
    mothersku_code varchar(50),		--//  ENCODE zstd // character varying
    udc_sssprogramq22022 varchar(150),		--//  ENCODE zstd // character varying
    udc_platinumq22022 varchar(150),		--//  ENCODE zstd // character varying
    udc_hsacounterq22022 varchar(150)		--//  ENCODE zstd // character varying
)
;		--// DISTSTYLE EVEN;

create or replace view INDEDW_ACCESS.edw_rpt_sales_details_increment as(
select 
data_increment_key as "data_increment_key"
,customer_code as "customer_code"
,invoice_no as "invoice_no"
,invoice_date as "invoice_date"
,retailer_code as "retailer_code"
,product_code as "product_code"
,quantity as "quantity"
,nr_value as "nr_value"
,achievement_nr as "achievement_nr"
,invoice_status as "invoice_status"
,customer_name as "customer_name"
,region_name as "region_name"
,zone_name as "zone_name"
,territory_name as "territory_name"
,territory_classification as "territory_classification"
,class_desc as "class_desc"
,outlet as "outlet"
,channel_name as "channel_name"
,business_channel as "business_channel"
,loyalty_desc as "loyalty_desc"
,status_desc as "status_desc"
,product_name as "product_name"
,product_desc as "product_desc"
,franchise_name as "franchise_name"
,brand_name as "brand_name"
,product_category_name as "product_category_name"
,variant_name as "variant_name"
,mothersku_name as "mothersku_name"
,day as "day"
,mth_mm as "mth_mm"
,mth_yyyymm as "mth_yyyymm"
,qtr as "qtr"
,yyyyqtr as "yyyyqtr"
,cal_yr as "cal_yr"
,fisc_yr as "fisc_yr"
,week as "week"
,month as "month"
,retailer_name as "retailer_name"
,salesman_name as "salesman_name"
,salesman_code as "salesman_code"
,route_name as "route_name"
,route_code as "route_code"
,active_flag as "active_flag"
,rtrlatitude as "rtrlatitude"
,rtrlongitude as "rtrlongitude"
,rtruniquecode as "rtruniquecode"
,unique_sales_code as "unique_sales_code"
,createddate as "createddate"
,retailer_address1 as "retailer_address1"
,retailer_address2 as "retailer_address2"
,retailer_address3 as "retailer_address3"
,zone_classification as "zone_classification"
,state_name as "state_name"
,town_name as "town_name"
,town_classification as "town_classification"
,achievement_amt as "achievement_amt"
,gross_amt as "gross_amt"
,net_amt as "net_amt"
,num_lines as "num_lines"
,prd_disc_sch_amt as "prd_disc_sch_amt"
,tax_amt as "tax_amt"
,qps_amt as "qps_amt"
,qps_qty as "qps_qty"
,sku_rec_amt as "sku_rec_amt"
,sku_rec_qty as "sku_rec_qty"
,num_recsku_lines as "num_recsku_lines"
,num_buying_retailers as "num_buying_retailers"
,num_bills as "num_bills"
,num_packs as "num_packs"
,udc_hsacounter as "udc_hsacounter"
,udc_keyaccountname as "udc_keyaccountname"
,udc_pharmacychain as "udc_pharmacychain"
,udc_onedetailingbaby as "udc_onedetailingbaby"
,udc_sanprovisibilitysss as "udc_sanprovisibilitysss"
,udc_sssconsoffer as "udc_sssconsoffer"
,udc_platinumclub2018 as "udc_platinumclub2018"
,udc_sssendcaps as "udc_sssendcaps"
,udc_platinumclub2019 as "udc_platinumclub2019"
,udc_signature2019 as "udc_signature2019"
,udc_premium2019 as "udc_premium2019"
,udc_gstn as "udc_gstn"
,udc_platinumq22019new as "udc_platinumq22019new"
,udc_sssprogram2019 as "udc_sssprogram2019"
,udc_umangq32019 as "udc_umangq32019"
,udc_sssscheme2019 as "udc_sssscheme2019"
,udc_ssspromoter2019 as "udc_ssspromoter2019"
,udc_rtrtypeattr as "udc_rtrtypeattr"
,udc_bhagidariq32019 as "udc_bhagidariq32019"
,udc_directorclubq32019 as "udc_directorclubq32019"
,udc_platinumq32019new as "udc_platinumq32019new"
,udc_platinumq22019 as "udc_platinumq22019"
,udc_platinumq32019 as "udc_platinumq32019"
,udc_bbastore as "udc_bbastore"
,udc_avbabybodydocq42019 as "udc_avbabybodydocq42019"
,udc_orslcac2019 as "udc_orslcac2019"
,udc_babyprofesionalcac2019 as "udc_babyprofesionalcac2019"
,udc_platinumq42019 as "udc_platinumq42019"
,udc_schemesss2020 as "udc_schemesss2020"
,udc_platinumq12020 as "udc_platinumq12020"
,udc_platinumq32020 as "udc_platinumq32020"
,udc_sssq32020 as "udc_sssq32020"
,udc_bhagidariq32020 as "udc_bhagidariq32020"
,udc_directorclubq32020 as "udc_directorclubq32020"
,udc_umangq32020 as "udc_umangq32020"
,udc_daudq32020 as "udc_daudq32020"
,udc_daudq42020 as "udc_daudq42020"
,udc_platinumq42020 as "udc_platinumq42020"
,udc_directorclubq42020 as "udc_directorclubq42020"
,udc_sssprogramq42020 as "udc_sssprogramq42020"
,udc_bhagidariq42020 as "udc_bhagidariq42020"
,udc_umangq42020 as "udc_umangq42020"
,udc_samriddhi as "udc_samriddhi"
,udc_sssprogramq12021 as "udc_sssprogramq12021"
,udc_platinumq12021 as "udc_platinumq12021"
,udc_orslcac2021 as "udc_orslcac2021"
,udc_bhagidariq12021 as "udc_bhagidariq12021"
,udc_daudq12021 as "udc_daudq12021"
,udc_directorclubq12021 as "udc_directorclubq12021"
,udc_umangq12021 as "udc_umangq12021"
,udc_platinumq22021 as "udc_platinumq22021"
,udc_sssprogramq22021 as "udc_sssprogramq22021"
,udc_bhagidariq22021 as "udc_bhagidariq22021"
,udc_daudq22021 as "udc_daudq22021"
,udc_directorclubq22021 as "udc_directorclubq22021"
,udc_umangq22021 as "udc_umangq22021"
,udc_platinumq32021 as "udc_platinumq32021"
,udc_sssprogramq32021 as "udc_sssprogramq32021"
,retailer_category_cd as "retailer_category_cd"
,retailer_category_name as "retailer_category_name"
,csrtrcode as "csrtrcode"
,abi_ntid as "abi_ntid"
,flm_ntid as "flm_ntid"
,bdm_ntid as "bdm_ntid"
,rsm_ntid as "rsm_ntid"
,crt_dttm as "crt_dttm"
,updt_dttm as "updt_dttm"
,type_name as "type_name"
,customer_type as "customer_type"
,salesman_status as "salesman_status"
,total_retailers as "total_retailers"
,prd_free_qty as "prd_free_qty"
,retailer_channel_1 as "retailer_channel_1"
,retailer_channel_2 as "retailer_channel_2"
,retailer_channel_3 as "retailer_channel_3"
,report_channel as "report_channel"
,udc_newgtm as "udc_newgtm"
,udc_sssprogramq12022 as "udc_sssprogramq12022"
,udc_ssspharmacystore as "udc_ssspharmacystore"
,udc_ssstotstores as "udc_ssstotstores"
,udc_platinumq12022 as "udc_platinumq12022"
,udc_hsacounterq12022 as "udc_hsacounterq12022"
,udc_bssaveenoudc2022 as "udc_bssaveenoudc2022"
,mothersku_code as "mothersku_code"
,udc_sssprogramq22022 as "udc_sssprogramq22022"
,udc_platinumq22022 as "udc_platinumq22022"
,udc_hsacounterq22022 as "udc_hsacounterq22022"
from INDEDW_INTEGRATION.edw_rpt_sales_details_increment
);
