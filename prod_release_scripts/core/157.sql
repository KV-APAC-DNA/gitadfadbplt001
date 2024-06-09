USE SCHEMA INDITG_INTEGRATION;
--DROP TABLE itg_billing_conditions;
CREATE TABLE IF NOT EXISTS ITG_BILLING_CONDITIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_billing_conditions
(
	bill_num VARCHAR(10)  		--//  ENCODE zstd
	,bill_item numeric(18,0)		--//  ENCODE bytedict // INTEGER  
	,bic_zstepnum numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,kncounter numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,doc_number VARCHAR(10)  		--//  ENCODE zstd
	,s_ord_item numeric(18,0)		--//  ENCODE bytedict // INTEGER  
	,knart VARCHAR(4)  		--//  ENCODE zstd
	,ch_on numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,comp_code VARCHAR(4)  		--//  ENCODE zstd
	,sales_dist VARCHAR(6)  		--//  ENCODE zstd
	,bill_type VARCHAR(4)  		--//  ENCODE zstd
	,bill_date DATE  		--//  ENCODE zstd
	,bill_cat VARCHAR(1)  		--//  ENCODE zstd
	,loc_currcy VARCHAR(8)  		--//  ENCODE zstd
	,cust_group VARCHAR(2)  		--//  ENCODE zstd
	,sold_to VARCHAR(10)  		--//  ENCODE zstd
	,payer VARCHAR(10)  		--//  ENCODE zstd
	,exrate_acc NUMERIC(37,17)  		--//  ENCODE zstd
	,stat_curr VARCHAR(5)  		--//  ENCODE zstd
	,doc_categ VARCHAR(2)  		--//  ENCODE zstd
	,salesorg VARCHAR(4)  		--//  ENCODE zstd
	,distr_chan VARCHAR(2)  		--//  ENCODE zstd
	,doc_currcy VARCHAR(5)  		--//  ENCODE zstd
	,createdon numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,co_area VARCHAR(4)  		--//  ENCODE zstd
	,costcenter VARCHAR(10)  		--//  ENCODE zstd
	,trans_date numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,exchg_rate NUMERIC(37,6)  		--//  ENCODE zstd
	,cust_grp1 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp2 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp3 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp4 VARCHAR(3)  		--//  ENCODE zstd
	,cust_grp5 VARCHAR(3)  		--//  ENCODE zstd
	,matl_group VARCHAR(9)  		--//  ENCODE zstd
	,material VARCHAR(18)  		--//  ENCODE zstd
	,mat_entrd VARCHAR(18)  		--//  ENCODE zstd
	,matl_grp_1 VARCHAR(3)  		--//  ENCODE bytedict
	,matl_grp_2 VARCHAR(3)  		--//  ENCODE bytedict
	,matl_grp_3 VARCHAR(3)  		--//  ENCODE bytedict
	,matl_grp_4 VARCHAR(3)  		--//  ENCODE zstd
	,matl_grp_5 VARCHAR(3)  		--//  ENCODE zstd
	,billtoprty VARCHAR(10)  		--//  ENCODE zstd
	,ship_to VARCHAR(10)  		--//  ENCODE zstd
	,itm_type VARCHAR(1)  		--//  ENCODE zstd
	,prod_hier VARCHAR(18)  		--//  ENCODE bytedict
	,prov_group VARCHAR(2)  		--//  ENCODE zstd
	,price_date numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,item_categ VARCHAR(4)  		--//  ENCODE zstd
	,div_head VARCHAR(2)  		--//  ENCODE zstd
	,division VARCHAR(2)  		--//  ENCODE zstd
	,stat_date numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,refer_doc VARCHAR(10)  		--//  ENCODE zstd
	,refer_itm NUMERIC(37,17)  		--//  ENCODE bytedict
	,sales_off VARCHAR(4)  		--//  ENCODE zstd
	,sales_grp VARCHAR(3)  		--//  ENCODE zstd
	,wbs_elemt VARCHAR(24)  		--//  ENCODE zstd
	,calday numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,calmonth numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,calweek numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,fiscper numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,fiscvarnt VARCHAR(2)  		--//  ENCODE zstd
	,knclass VARCHAR(4)  		--//  ENCODE zstd
	,knorigin VARCHAR(4)  		--//  ENCODE zstd
	,kntyp VARCHAR(4)  		--//  ENCODE zstd
	,knval NUMERIC(37,17)  		--//  ENCODE zstd
	,kprice NUMERIC(37,17)  		--//  ENCODE zstd
	,kinak VARCHAR(1)  		--//  ENCODE zstd
	,kstat VARCHAR(1)  		--//  ENCODE zstd
	,storno VARCHAR(1)  		--//  ENCODE zstd
	,rt_promo VARCHAR(10)  		--//  ENCODE zstd
	,rebate_grp VARCHAR(2)  		--//  ENCODE zstd
	,bwapplnm VARCHAR(30)  		--//  ENCODE zstd
	,processkey numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,eanupc VARCHAR(18)  		--//  ENCODE zstd
	,createdby VARCHAR(12)  		--//  ENCODE zstd
	,serv_date numeric(18,0)		--//  ENCODE bytedict // INTEGER  
	,inv_qty VARCHAR(50)  		--//  ENCODE zstd
	,forwagent VARCHAR(10)  		--//  ENCODE zstd
	,salesemply numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,sales_unit VARCHAR(3)  		--//  ENCODE zstd
	,kappl VARCHAR(2)  		--//  ENCODE zstd
	,acrn_id VARCHAR(2)  		--//  ENCODE zstd
	,recordmode VARCHAR(1)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
cluster by (sold_to)
		--// SORTKEY ( 
		--// 	bill_date
		--// 	)
;		--// ;
--DROP TABLE itg_billing_conditions_history_data;
CREATE TABLE IF NOT EXISTS ITG_BILLING_CONDITIONS_HISTORY_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_billing_conditions_history_data
(
	bill_num VARCHAR(10)  		--//  ENCODE lzo
	,bill_item numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,bic_zstepnum numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,kncounter numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,doc_number VARCHAR(10)  		--//  ENCODE lzo
	,s_ord_item numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,knart VARCHAR(4)  		--//  ENCODE lzo
	,ch_on numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,comp_code VARCHAR(4)  		--//  ENCODE lzo
	,sales_dist VARCHAR(6)  		--//  ENCODE lzo
	,bill_type VARCHAR(4)  		--//  ENCODE lzo
	,bill_date DATE  		--//  ENCODE lzo
	,bill_cat VARCHAR(1)  		--//  ENCODE lzo
	,loc_currcy VARCHAR(8)  		--//  ENCODE lzo
	,cust_group VARCHAR(2)  		--//  ENCODE lzo
	,sold_to VARCHAR(10)  		--//  ENCODE lzo
	,payer VARCHAR(10)  		--//  ENCODE lzo
	,exrate_acc NUMERIC(37,17)  		--//  ENCODE lzo
	,stat_curr VARCHAR(5)  		--//  ENCODE lzo
	,doc_categ VARCHAR(2)  		--//  ENCODE lzo
	,salesorg VARCHAR(4)  		--//  ENCODE lzo
	,distr_chan VARCHAR(2)  		--//  ENCODE lzo
	,doc_currcy VARCHAR(5)  		--//  ENCODE lzo
	,createdon numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,co_area VARCHAR(4)  		--//  ENCODE lzo
	,costcenter VARCHAR(10)  		--//  ENCODE lzo
	,trans_date numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,exchg_rate NUMERIC(37,6)  		--//  ENCODE lzo
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
	,price_date numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,item_categ VARCHAR(4)  		--//  ENCODE lzo
	,div_head VARCHAR(2)  		--//  ENCODE lzo
	,division VARCHAR(2)  		--//  ENCODE lzo
	,stat_date numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,refer_doc VARCHAR(10)  		--//  ENCODE lzo
	,refer_itm NUMERIC(37,17)  		--//  ENCODE lzo
	,sales_off VARCHAR(4)  		--//  ENCODE lzo
	,sales_grp VARCHAR(3)  		--//  ENCODE lzo
	,wbs_elemt VARCHAR(24)  		--//  ENCODE lzo
	,calday numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,calmonth numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,calweek numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fiscper numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fiscvarnt VARCHAR(2)  		--//  ENCODE lzo
	,knclass VARCHAR(4)  		--//  ENCODE lzo
	,knorigin VARCHAR(4)  		--//  ENCODE lzo
	,kntyp VARCHAR(4)  		--//  ENCODE lzo
	,knval NUMERIC(37,17)  		--//  ENCODE lzo
	,kprice NUMERIC(37,17)  		--//  ENCODE lzo
	,kinak VARCHAR(1)  		--//  ENCODE lzo
	,kstat VARCHAR(1)  		--//  ENCODE lzo
	,storno VARCHAR(1)  		--//  ENCODE lzo
	,rt_promo VARCHAR(10)  		--//  ENCODE lzo
	,rebate_grp VARCHAR(2)  		--//  ENCODE lzo
	,bwapplnm VARCHAR(30)  		--//  ENCODE lzo
	,processkey numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,eanupc VARCHAR(18)  		--//  ENCODE lzo
	,createdby VARCHAR(12)  		--//  ENCODE lzo
	,serv_date numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,inv_qty VARCHAR(50)  		--//  ENCODE lzo
	,forwagent VARCHAR(10)  		--//  ENCODE lzo
	,salesemply numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,sales_unit VARCHAR(3)  		--//  ENCODE lzo
	,kappl VARCHAR(2)  		--//  ENCODE lzo
	,acrn_id VARCHAR(2)  		--//  ENCODE lzo
	,recordmode VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_brand_focus_target;
CREATE TABLE IF NOT EXISTS ITG_BRAND_FOCUS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_brand_focus_target
(
	region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,target_value NUMERIC(38,2)  		--//  ENCODE az64
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,variant VARCHAR(50)  		--//  ENCODE zstd
	,measure_type VARCHAR(50)  		--//  ENCODE zstd
	,channel VARCHAR(50)  		--//  ENCODE zstd
	,year_month DATE  		--//  ENCODE az64
)

;
--DROP TABLE itg_business_plan_target;
CREATE TABLE IF NOT EXISTS ITG_BUSINESS_PLAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_business_plan_target
(
	region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,territory VARCHAR(50)  		--//  ENCODE zstd
	,yearmonth DATE  		--//  ENCODE delta
	,value NUMERIC(38,2)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_businesscalender;
CREATE TABLE IF NOT EXISTS ITG_BUSINESSCALENDER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_businesscalender
(
	salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,month VARCHAR(30)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,week VARCHAR(50)  		--//  ENCODE lzo
	,monthkey numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_classmaster;
CREATE TABLE IF NOT EXISTS ITG_CLASSMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_classmaster
(
	tableid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,pkey numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,classid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,classcode VARCHAR(10)  		--//  ENCODE lzo
	,classdesc VARCHAR(100)  		--//  ENCODE lzo
	,turnover NUMERIC(18,4)  		--//  ENCODE lzo
	,availabilty numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,createduserid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,disthierarchyid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_csl_retailerhierarchy;
CREATE TABLE IF NOT EXISTS ITG_CSL_RETAILERHIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_csl_retailerhierarchy
(
	cmpcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrhierdfn_code VARCHAR(25)  		--//  ENCODE zstd
	,rtrhierdfn_name VARCHAR(50)  		--//  ENCODE zstd
	,retlrgroupcode VARCHAR(50)  		--//  ENCODE zstd
	,retlrgroupname VARCHAR(50)  		--//  ENCODE zstd
	,classcode VARCHAR(10)  		--//  ENCODE zstd
	,classdesc VARCHAR(100)  		--//  ENCODE zstd
	,turnover NUMERIC(18,4)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE az64 //  ENCODE az64 // character varying
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_customer;
CREATE TABLE IF NOT EXISTS ITG_CUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_customer
(
	customercode NUMERIC(18,0)  		--//  ENCODE lzo
	,customername VARCHAR(50)  		--//  ENCODE lzo
	,customeraddress1 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress2 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress3 VARCHAR(250)  		--//  ENCODE lzo
	,customerid VARCHAR(20)  		--//  ENCODE lzo
	,sapid VARCHAR(20)
	,regioncode NUMERIC(18,0)  		--//  ENCODE lzo
	,zonecode NUMERIC(18,0)  		--//  ENCODE lzo
	,territorycode NUMERIC(18,0)  		--//  ENCODE lzo
	,statecode NUMERIC(18,0)  		--//  ENCODE lzo
	,towncode NUMERIC(18,0)  		--//  ENCODE lzo
	,typecode NUMERIC(18,0)  		--//  ENCODE lzo
	,psnonps CHAR(1)  		--//  ENCODE lzo
	,emailid VARCHAR(50)  		--//  ENCODE lzo
	,mobilell VARCHAR(50)  		--//  ENCODE lzo
	,suppliedby NUMERIC(18,0)  		--//  ENCODE lzo
	,druglicense CHAR(1)  		--//  ENCODE lzo
	,currentytd NUMERIC(18,0)  		--//  ENCODE lzo
	,dsbychannelcode NUMERIC(18,0)  		--//  ENCODE lzo
	,dsbystrategiccode NUMERIC(18,0)  		--//  ENCODE lzo
	,classificationcode NUMERIC(18,0)  		--//  ENCODE lzo
	,isactive CHAR(1)  		--//  ENCODE lzo
	,deactivedt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,activedt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,annualincentiveplan CHAR(1)  		--//  ENCODE lzo
	,istarangumang CHAR(1)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(50)  		--//  ENCODE lzo
	,isime CHAR(1)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE lzo
	,nkacstores CHAR(1)  		--//  ENCODE lzo
	,parentcustomercode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,isdirectacct VARCHAR(1)  		--//  ENCODE lzo
	,abicode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,distributorcode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,distributorsapid VARCHAR(50)  		--//  ENCODE lzo
	,sapzonecode NUMERIC(18,0)  		--//  ENCODE lzo
	,sapterritorycode NUMERIC(18,0)  		--//  ENCODE lzo
	,isconfirm VARCHAR(1)  		--//  ENCODE lzo
	,nkacregion NUMERIC(18,0)  		--//  ENCODE lzo
	,nkaczone NUMERIC(18,0)  		--//  ENCODE lzo
	,nkacterritory NUMERIC(18,0)  		--//  ENCODE lzo
	,channel VARCHAR(1)  		--//  ENCODE lzo
	,gln VARCHAR(50)  		--//  ENCODE lzo
	,transitdays NUMERIC(9,0)  		--//  ENCODE lzo
	,transithours NUMERIC(9,0)  		--//  ENCODE lzo
	,vmrapply VARCHAR(1)  		--//  ENCODE lzo
	,isdisteligibleforclaims CHAR(1)  		--//  ENCODE lzo
	,custcreatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,custlastupdatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,wavenonwave CHAR(1)  		--//  ENCODE lzo
	,custlastupdatedby NUMERIC(18,0)  		--//  ENCODE lzo
	,sapregioncode NUMERIC(18,0)  		--//  ENCODE lzo
	,consoleregioncode NUMERIC(18,0)  		--//  ENCODE lzo
	,consolezonecode NUMERIC(18,0)  		--//  ENCODE lzo
	,consoleterritorycode NUMERIC(18,0)  		--//  ENCODE lzo
	,consolestatecode NUMERIC(18,0)  		--//  ENCODE lzo
	,consoletowncode NUMERIC(18,0)  		--//  ENCODE lzo
	,channelcode NUMERIC(18,0)  		--//  ENCODE lzo
	,superstockist VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
		--// SORTKEY ( 
		--// 	sapid
		--// 	)
;		--// ;
--DROP TABLE itg_customer_retailer;
CREATE TABLE IF NOT EXISTS ITG_CUSTOMER_RETAILER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_customer_retailer
(
	customercode VARCHAR(50)  		--//  ENCODE lzo
	,customername VARCHAR(50)  		--//  ENCODE lzo
	,customeraddress1 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress2 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress3 VARCHAR(250)  		--//  ENCODE lzo
	,customerid VARCHAR(20)  		--//  ENCODE lzo
	,sapid VARCHAR(20)  		--//  ENCODE lzo
	,regioncode VARCHAR(50)  		--//  ENCODE lzo
	,zonecode VARCHAR(50)  		--//  ENCODE lzo
	,territorycode VARCHAR(50)  		--//  ENCODE lzo
	,statecode VARCHAR(50)  		--//  ENCODE lzo
	,towncode VARCHAR(50)  		--//  ENCODE lzo
	,typecode VARCHAR(50)  		--//  ENCODE lzo
	,psnonps VARCHAR(10)  		--//  ENCODE lzo
	,emailid VARCHAR(50)  		--//  ENCODE lzo
	,mobilell VARCHAR(50)  		--//  ENCODE lzo
	,suppliedby NUMERIC(18,0)  		--//  ENCODE az64
	,druglicense VARCHAR(10)  		--//  ENCODE lzo
	,currentytd NUMERIC(18,0)  		--//  ENCODE az64
	,dsbychannelcode NUMERIC(18,0)  		--//  ENCODE az64
	,dsbystrategiccode NUMERIC(18,0)  		--//  ENCODE az64
	,classificationcode NUMERIC(18,0)  		--//  ENCODE az64
	,isactive VARCHAR(10)  		--//  ENCODE lzo
	,deactivedt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,activedt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,annualincentiveplan VARCHAR(10)  		--//  ENCODE lzo
	,istarangumang VARCHAR(10)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(50)  		--//  ENCODE lzo
	,isime VARCHAR(10)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE az64
	,nkacstores VARCHAR(10)  		--//  ENCODE lzo
	,parentcustomercode VARCHAR(50)  		--//  ENCODE lzo
	,isdirectacct VARCHAR(10)  		--//  ENCODE lzo
	,abicode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distributorsapid VARCHAR(50)  		--//  ENCODE lzo
	,sapzonecode NUMERIC(18,0)  		--//  ENCODE az64
	,sapterritorycode NUMERIC(18,0)  		--//  ENCODE az64
	,isconfirm VARCHAR(10)  		--//  ENCODE lzo
	,nkacregion NUMERIC(18,0)  		--//  ENCODE az64
	,nkaczone NUMERIC(18,0)  		--//  ENCODE az64
	,nkacterritory NUMERIC(18,0)  		--//  ENCODE az64
	,channel VARCHAR(10)  		--//  ENCODE lzo
	,gln VARCHAR(50)  		--//  ENCODE lzo
	,transitdays NUMERIC(9,0)  		--//  ENCODE az64
	,transithours NUMERIC(9,0)  		--//  ENCODE az64
	,vmrapply VARCHAR(10)  		--//  ENCODE lzo
	,custcreatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,custlastupdatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,custlastupdatedby NUMERIC(18,0)  		--//  ENCODE az64
	,isdisteligibleforclaims VARCHAR(10)  		--//  ENCODE lzo
	,isparent VARCHAR(10)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_dailysales;
CREATE TABLE IF NOT EXISTS ITG_DAILYSALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_dailysales
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,salinvdate TIMESTAMP WITHOUT TIME ZONE
	,saldlvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,salinvmode VARCHAR(100)  		--//  ENCODE zstd
	,salinvtype VARCHAR(100)  		--//  ENCODE zstd
	,salgrossamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salspldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salschdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salcashdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saldbdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saltaxamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saldbadjamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salcradjamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salonaccountamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salmktretamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salreplaceamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salotherchargesamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salinvleveldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saltotdedn NUMERIC(38,6)  		--//  ENCODE zstd
	,saltotaddn NUMERIC(38,6)  		--//  ENCODE zstd
	,salroundoffamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salnetamt NUMERIC(38,6)  		--//  ENCODE zstd
	,lcnid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,lcncode VARCHAR(100)  		--//  ENCODE zstd
	,salesmancode VARCHAR(100)  		--//  ENCODE zstd
	,salesmanname VARCHAR(200)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(100)  		--//  ENCODE zstd
	,salesroutename VARCHAR(200)  		--//  ENCODE zstd
	,rtrid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,vechname VARCHAR(100)  		--//  ENCODE zstd
	,dlvboyname VARCHAR(100)  		--//  ENCODE zstd
	,deliveryroutecode VARCHAR(100)  		--//  ENCODE zstd
	,deliveryroutename VARCHAR(200)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdselratebeforetax NUMERIC(38,6)  		--//  ENCODE zstd
	,prdselrateaftertax NUMERIC(38,6)  		--//  ENCODE zstd
	,prdfreeqty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdspldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdcashdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdnetamt NUMERIC(38,6)  		--//  ENCODE zstd
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,migrationflag VARCHAR(1)  		--//  ENCODE zstd
	,salinvlinecount numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,mrp NUMERIC(18,6)  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE zstd
	,creditnoteamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salfreeqtyvalue NUMERIC(38,6)  		--//  ENCODE zstd
	,nrvalue NUMERIC(18,6)  		--//  ENCODE zstd
	,vcpschemeamount NUMERIC(18,6)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	salinvdate
		--// 	)
;		--// ;
--DROP TABLE itg_dailysales_dbrestore;
CREATE TABLE IF NOT EXISTS ITG_DAILYSALES_DBRESTORE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_dailysales_dbrestore
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,saldlvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,salspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,salschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,salcashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,saldbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,saltaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE az64
	,saldbadjamt NUMERIC(38,6)  		--//  ENCODE az64
	,salcradjamt NUMERIC(38,6)  		--//  ENCODE az64
	,salonaccountamt NUMERIC(38,6)  		--//  ENCODE az64
	,salmktretamt NUMERIC(38,6)  		--//  ENCODE az64
	,salreplaceamt NUMERIC(38,6)  		--//  ENCODE az64
	,salotherchargesamt NUMERIC(38,6)  		--//  ENCODE az64
	,salinvleveldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,saltotdedn NUMERIC(38,6)  		--//  ENCODE az64
	,saltotaddn NUMERIC(38,6)  		--//  ENCODE az64
	,salroundoffamt NUMERIC(38,6)  		--//  ENCODE az64
	,salnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,lcncode VARCHAR(50)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,salesmanname VARCHAR(400)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(50)  		--//  ENCODE zstd
	,salesroutename VARCHAR(400)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrname VARCHAR(200)  		--//  ENCODE zstd
	,deliveryroutecode VARCHAR(50)  		--//  ENCODE zstd
	,deliveryroutename VARCHAR(400)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0) NOT NULL 		--//  ENCODE az64 // INTEGER 
	,prdselratebeforetax NUMERIC(38,6)  		--//  ENCODE az64
	,prdselrateaftertax NUMERIC(38,6)  		--//  ENCODE az64
	,prdfreeqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdcashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mrp NUMERIC(18,6)  		--//  ENCODE az64
	,salfreeqtyvalue NUMERIC(38,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(18,6)  		--//  ENCODE az64
	,vcpschemeamount NUMERIC(18,6)  		--//  ENCODE az64
	,rtrurccode VARCHAR(200)  		--//  ENCODE zstd
	,syncid NUMERIC(38,6)  		--//  ENCODE az64
	,creditnoteamt NUMERIC(38,6)  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salinvmode VARCHAR(30)  		--//  ENCODE zstd
	,salinvtype VARCHAR(30)  		--//  ENCODE zstd
	,vechname VARCHAR(100)  		--//  ENCODE zstd
	,dlvboyname VARCHAR(100)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salinvlinecount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mrpcs NUMERIC(18,6)  		--//  ENCODE az64
	,lpvalue NUMERIC(18,6)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE NOT NULL 		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE az64 //  ENCODE az64 // character varying
	,file_name VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_dailysales_undelivered;
CREATE TABLE IF NOT EXISTS ITG_DAILYSALES_UNDELIVERED		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_dailysales_undelivered
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,salinvdate TIMESTAMP WITHOUT TIME ZONE
	,saldlvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,salinvmode VARCHAR(100)  		--//  ENCODE zstd
	,salinvtype VARCHAR(100)  		--//  ENCODE zstd
	,salgrossamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salspldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salschdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salcashdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saldbdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saltaxamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saldbadjamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salcradjamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salonaccountamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salmktretamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salreplaceamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salotherchargesamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salinvleveldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saltotdedn NUMERIC(38,6)  		--//  ENCODE zstd
	,saltotaddn NUMERIC(38,6)  		--//  ENCODE zstd
	,salroundoffamt NUMERIC(38,6)  		--//  ENCODE bytedict
	,salnetamt NUMERIC(38,6)  		--//  ENCODE zstd
	,lcnid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,lcncode VARCHAR(100)  		--//  ENCODE zstd
	,salesmancode VARCHAR(100)  		--//  ENCODE zstd
	,salesmanname VARCHAR(200)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(100)  		--//  ENCODE zstd
	,salesroutename VARCHAR(200)  		--//  ENCODE zstd
	,rtrid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,vechname VARCHAR(100)  		--//  ENCODE zstd
	,dlvboyname VARCHAR(100)  		--//  ENCODE zstd
	,deliveryroutecode VARCHAR(100)  		--//  ENCODE zstd
	,deliveryroutename VARCHAR(200)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdselratebeforetax NUMERIC(38,6)  		--//  ENCODE zstd
	,prdselrateaftertax NUMERIC(38,6)  		--//  ENCODE zstd
	,prdfreeqty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdspldiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdcashdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdnetamt NUMERIC(38,6)  		--//  ENCODE zstd
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,salinvlinecount numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,salinvlvldiscper NUMERIC(18,2)  		--//  ENCODE zstd
	,billstatus SMALLINT  		--//  ENCODE lzo
	,uploadeddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,mrp NUMERIC(18,6)  		--//  ENCODE bytedict
	,nrvalue NUMERIC(18,6)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,del_ind CHAR(1)  DEFAULT 'N'::char		--//  ENCODE lzo // bpchar
)

		--// SORTKEY ( 
		--// 	salinvdate
		--// 	)
;		--// ;
--DROP TABLE itg_day_cls_stock_fact;
CREATE TABLE IF NOT EXISTS ITG_DAY_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_day_cls_stock_fact
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,transdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lcnid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,lcncode VARCHAR(100)  		--//  ENCODE lzo
	,prdid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,prdcode VARCHAR(100)  		--//  ENCODE lzo
	,salopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,offeropenstock NUMERIC(18,0)  		--//  ENCODE delta
	,salpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,salpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salsales NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsales NUMERIC(18,0)  		--//  ENCODE delta
	,offersales NUMERIC(18,0)  		--//  ENCODE delta
	,salstockin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockin NUMERIC(18,0)  		--//  ENCODE delta
	,salstockout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockout NUMERIC(18,0)  		--//  ENCODE delta
	,damagein NUMERIC(18,0)  		--//  ENCODE delta
	,damageout NUMERIC(18,0)  		--//  ENCODE delta
	,salsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offersalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,salreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,offerreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,salclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,offerclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,uploaddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,uploadflag VARCHAR(10)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,syncid NUMERIC(38,0)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,nr DOUBLE PRECISION
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_distributoractivation;
CREATE TABLE IF NOT EXISTS ITG_DISTRIBUTORACTIVATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_distributoractivation
(
	distcode VARCHAR(400)  		--//  ENCODE lzo
	,activefromdate TIMESTAMP WITHOUT TIME ZONE
	,activatedby numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,activatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,inactivefromdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,inactivatedby numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,inactivatedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,activestatus numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
cluster by (distcode)
		--// SORTKEY ( 
		--// 	activefromdate
		--// 	)
;		--// ;
--DROP TABLE itg_fin_sim_miscdata;
CREATE TABLE IF NOT EXISTS ITG_FIN_SIM_MISCDATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_fin_sim_miscdata
(
	matl_num VARCHAR(250)  		--//  ENCODE zstd
	,sku_desc VARCHAR(250)  		--//  ENCODE zstd
	,brand_combi VARCHAR(250)  		--//  ENCODE zstd
	,fisc_yr VARCHAR(250)  		--//  ENCODE zstd
	,month VARCHAR(250)  		--//  ENCODE zstd
	,chnl_desc2 VARCHAR(250)  		--//  ENCODE zstd
	,nature VARCHAR(250)  		--//  ENCODE zstd
	,amt_obj_crncy VARCHAR(250)  		--//  ENCODE zstd
	,qty VARCHAR(250)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(250)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_fin_sim_plandata;
CREATE TABLE IF NOT EXISTS ITG_FIN_SIM_PLANDATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_fin_sim_plandata
(
	matl_num VARCHAR(100)  		--//  ENCODE lzo
	,fisc_yr VARCHAR(10)  		--//  ENCODE lzo
	,month VARCHAR(10)  		--//  ENCODE lzo
	,chnl_desc2 VARCHAR(20)  		--//  ENCODE lzo
	,nature VARCHAR(50)  		--//  ENCODE lzo
	,amt_obj_crncy NUMERIC(38,5)  		--//  ENCODE az64
	,qty NUMERIC(38,5)  		--//  ENCODE az64
	,plan VARCHAR(10)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(150)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_mds_channel_mapping;
CREATE TABLE IF NOT EXISTS ITG_IN_MDS_CHANNEL_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_mds_channel_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel_name VARCHAR(200)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(200)  		--//  ENCODE zstd
	,retailer_class VARCHAR(200)  		--//  ENCODE zstd
	,territory_classification VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_1 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_2 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE zstd
	,report_channel VARCHAR(200)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_change_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_mds_product_category_mapping;
CREATE TABLE IF NOT EXISTS ITG_IN_MDS_PRODUCT_CATEGORY_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_mds_product_category_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_name VARCHAR(200)  		--//  ENCODE zstd
	,brand_name VARCHAR(200)  		--//  ENCODE zstd
	,product_category VARCHAR(200)  		--//  ENCODE zstd
	,variant_name VARCHAR(200)  		--//  ENCODE zstd
	,product_category1 VARCHAR(200)  		--//  ENCODE zstd
	,product_category2 VARCHAR(200)  		--//  ENCODE zstd
	,product_category3 VARCHAR(200)  		--//  ENCODE zstd
	,product_category4 VARCHAR(200)  		--//  ENCODE zstd
	,crt_dtm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_change_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_perfectstore_msl;
CREATE TABLE IF NOT EXISTS ITG_IN_PERFECTSTORE_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_perfectstore_msl
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(10,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_code VARCHAR(255)  		--//  ENCODE lzo
	,product_name VARCHAR(255)  		--//  ENCODE lzo
	,msl VARCHAR(255)  		--//  ENCODE lzo
	,cost_inr NUMERIC(12,4)  		--//  ENCODE az64
	,quantity NUMERIC(10,0)  		--//  ENCODE az64
	,amount_inr NUMERIC(12,4)  		--//  ENCODE az64
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_perfectstore_paid_display;
CREATE TABLE IF NOT EXISTS ITG_IN_PERFECTSTORE_PAID_DISPLAY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_perfectstore_paid_display
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(14,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,asset VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,posm_brand VARCHAR(255)  		--//  ENCODE lzo
	,start_date VARCHAR(255)  		--//  ENCODE lzo
	,end_date VARCHAR(255)  		--//  ENCODE lzo
	,audit_status VARCHAR(255)  		--//  ENCODE lzo
	,is_available VARCHAR(255)  		--//  ENCODE lzo
	,availability_points VARCHAR(255)  		--//  ENCODE lzo
	,visibility_type VARCHAR(255)  		--//  ENCODE lzo
	,visibility_condition VARCHAR(255)  		--//  ENCODE lzo
	,is_planogram_availbale VARCHAR(255)  		--//  ENCODE lzo
	,select_brand VARCHAR(255)  		--//  ENCODE lzo
	,is_correct_brand_displayed VARCHAR(255)  		--//  ENCODE lzo
	,brandavailability_points VARCHAR(255)  		--//  ENCODE lzo
	,stock_status VARCHAR(255)  		--//  ENCODE lzo
	,stock_points VARCHAR(255)  		--//  ENCODE lzo
	,is_near_category VARCHAR(255)  		--//  ENCODE lzo
	,nearcategory_points VARCHAR(255)  		--//  ENCODE lzo
	,audit_score VARCHAR(255)  		--//  ENCODE lzo
	,paid_visibility_score VARCHAR(255)  		--//  ENCODE lzo
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_perfectstore_promo;
CREATE TABLE IF NOT EXISTS ITG_IN_PERFECTSTORE_PROMO		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_perfectstore_promo
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(10,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_name VARCHAR(255)  		--//  ENCODE lzo
	,ispromotionavailable VARCHAR(255)  		--//  ENCODE lzo
	,photopath VARCHAR(500)  		--//  ENCODE lzo
	,countoffacing NUMERIC(10,0)  		--//  ENCODE az64
	,promotionoffertype VARCHAR(255)  		--//  ENCODE lzo
	,notavailablereason VARCHAR(255)  		--//  ENCODE lzo
	,price_off VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_perfectstore_sos;
CREATE TABLE IF NOT EXISTS ITG_IN_PERFECTSTORE_SOS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_perfectstore_sos
(
	visit_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjisp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year NUMERIC(14,0)  		--//  ENCODE az64
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,prod_facings NUMERIC(14,0)  		--//  ENCODE az64
	,total_facings NUMERIC(14,0)  		--//  ENCODE az64
	,facing_contribution VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,yearmo VARCHAR(255)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_in_rcustomer;
CREATE TABLE IF NOT EXISTS ITG_IN_RCUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rcustomer
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrtype VARCHAR(10)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(50)  		--//  ENCODE zstd
	,channelcode VARCHAR(30)  		--//  ENCODE zstd
	,retlrgroupcode VARCHAR(30)  		--//  ENCODE zstd
	,classcode VARCHAR(20)  		--//  ENCODE zstd
	,rtrphoneno VARCHAR(20)  		--//  ENCODE zstd
	,rtrcontactperson VARCHAR(50)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,regdate DATE  		--//  ENCODE az64
	,rtrlicno VARCHAR(100)  		--//  ENCODE zstd
	,rtrlicexpirydate DATE  		--//  ENCODE az64
	,druglno VARCHAR(100)  		--//  ENCODE zstd
	,rtrdrugexpirydate DATE  		--//  ENCODE az64
	,rtrcrbills numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcrdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcrlimit NUMERIC(38,6)  		--//  ENCODE az64
	,relationstatus VARCHAR(10)  		--//  ENCODE zstd
	,parentcode VARCHAR(50)  		--//  ENCODE zstd
	,status VARCHAR(10)  		--//  ENCODE zstd
	,rtrlatitude VARCHAR(20)  		--//  ENCODE zstd
	,rtrlongitude VARCHAR(20)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,keyaccount VARCHAR(10)  		--//  ENCODE zstd
	,rtrfoodlicno VARCHAR(200)  		--//  ENCODE zstd
	,pannumber VARCHAR(15)  		--//  ENCODE zstd
	,retailertype VARCHAR(10)  		--//  ENCODE zstd
	,composite VARCHAR(10)  		--//  ENCODE zstd
	,relatedparty VARCHAR(10)  		--//  ENCODE zstd
	,statename VARCHAR(50)  		--//  ENCODE zstd
	,lastmoddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rcustomerroute;
CREATE TABLE IF NOT EXISTS ITG_IN_RCUSTOMERROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rcustomerroute
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rmcode VARCHAR(100)  		--//  ENCODE zstd
	,routetype VARCHAR(10)  		--//  ENCODE zstd
	,coveragesequence numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rretailergeoextension;
CREATE TABLE IF NOT EXISTS ITG_IN_RRETAILERGEOEXTENSION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rretailergeoextension
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,customercode VARCHAR(50)  		--//  ENCODE zstd
	,cmpcutomercode VARCHAR(50)  		--//  ENCODE zstd
	,distributorcustomercode VARCHAR(50)  		--//  ENCODE zstd
	,latitude VARCHAR(20)  		--//  ENCODE zstd
	,longitude VARCHAR(20)  		--//  ENCODE zstd
	,townname VARCHAR(100)  		--//  ENCODE zstd
	,statename VARCHAR(100)  		--//  ENCODE zstd
	,districtname VARCHAR(100)  		--//  ENCODE zstd
	,subdistrictname VARCHAR(100)  		--//  ENCODE zstd
	,type VARCHAR(10)  		--//  ENCODE zstd
	,villagename VARCHAR(100)  		--//  ENCODE zstd
	,pincode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,uacheck VARCHAR(100)  		--//  ENCODE zstd
	,uaname VARCHAR(100)  		--//  ENCODE zstd
	,population numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,popstrata VARCHAR(100)  		--//  ENCODE zstd
	,finalpopulationwithua numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,modifydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isdeleted CHAR(1)  		--//  ENCODE zstd
	,extrafield1 VARCHAR(100)  		--//  ENCODE zstd
	,extrafield2 VARCHAR(100)  		--//  ENCODE zstd
	,extrafield3 VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rroute;
CREATE TABLE IF NOT EXISTS ITG_IN_RROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rroute
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(50)  		--//  ENCODE zstd
	,rmcode VARCHAR(100)  		--//  ENCODE zstd
	,routetype VARCHAR(10)  		--//  ENCODE zstd
	,rmname VARCHAR(50)  		--//  ENCODE zstd
	,distance numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vanroute VARCHAR(10)  		--//  ENCODE zstd
	,status VARCHAR(10)  		--//  ENCODE zstd
	,rmpopulation numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,localupcountry VARCHAR(10)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rrsrdistributor;
CREATE TABLE IF NOT EXISTS ITG_IN_RRSRDISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rrsrdistributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,rsrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rrsrheader;
CREATE TABLE IF NOT EXISTS ITG_IN_RRSRHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rrsrheader
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,rsrcode VARCHAR(50)  		--//  ENCODE zstd
	,rsrname VARCHAR(100)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,phoneno VARCHAR(15)  		--//  ENCODE zstd
	,dateofbirth TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,approvalstatus VARCHAR(10)  		--//  ENCODE zstd
	,dailyallowance NUMERIC(22,6)  		--//  ENCODE az64
	,monthlysalary NUMERIC(22,6)  		--//  ENCODE az64
	,aadharno VARCHAR(15)  		--//  ENCODE zstd
	,imagepath VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rsalesman;
CREATE TABLE IF NOT EXISTS ITG_IN_RSALESMAN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rsalesman
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,smcode VARCHAR(50)  		--//  ENCODE zstd
	,smname VARCHAR(50)  		--//  ENCODE zstd
	,smphoneno VARCHAR(50)  		--//  ENCODE zstd
	,smemail VARCHAR(50)  		--//  ENCODE zstd
	,rdssmtype VARCHAR(10)  		--//  ENCODE zstd
	,smdailyallowance NUMERIC(38,6)  		--//  ENCODE az64
	,smmonthlysalary NUMERIC(38,6)  		--//  ENCODE az64
	,smmktcredit NUMERIC(38,6)  		--//  ENCODE az64
	,smcreditdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status VARCHAR(10)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,aadhaarno VARCHAR(50)  		--//  ENCODE zstd
	,uniquesalescode VARCHAR(50)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rsalesmanroute;
CREATE TABLE IF NOT EXISTS ITG_IN_RSALESMANROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rsalesmanroute
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,routecode VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rtldistributor;
CREATE TABLE IF NOT EXISTS ITG_IN_RTLDISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rtldistributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rtlheader;
CREATE TABLE IF NOT EXISTS ITG_IN_RTLHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rtlheader
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,tlname VARCHAR(100)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,phoneno VARCHAR(15)  		--//  ENCODE zstd
	,dateofbirth TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,approvalstatus VARCHAR(10)  		--//  ENCODE zstd
	,dailyallowance NUMERIC(22,6)  		--//  ENCODE az64
	,monthlysalary NUMERIC(22,6)  		--//  ENCODE az64
	,aadharno VARCHAR(15)  		--//  ENCODE zstd
	,imagepath VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_in_rtlsalesman;
CREATE TABLE IF NOT EXISTS ITG_IN_RTLSALESMAN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_in_rtlsalesman
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_ittarget;
CREATE TABLE IF NOT EXISTS ITG_ITTARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_ittarget
(
	lakshyat_territory_name VARCHAR(50)  		--//  ENCODE lzo
	,target_variant VARCHAR(50)  		--//  ENCODE lzo
	,janamount NUMERIC(37,5)  		--//  ENCODE lzo
	,febamount NUMERIC(37,5)  		--//  ENCODE lzo
	,maramount NUMERIC(37,5)  		--//  ENCODE lzo
	,apramount NUMERIC(37,5)  		--//  ENCODE lzo
	,mayamount NUMERIC(37,5)  		--//  ENCODE lzo
	,junamount NUMERIC(37,5)  		--//  ENCODE lzo
	,julyamount NUMERIC(37,5)  		--//  ENCODE lzo
	,augamount NUMERIC(37,5)  		--//  ENCODE lzo
	,sepamount NUMERIC(37,5)  		--//  ENCODE lzo
	,octamount NUMERIC(37,5)  		--//  ENCODE lzo
	,novamount NUMERIC(37,5)  		--//  ENCODE lzo
	,decamount NUMERIC(37,5)  		--//  ENCODE lzo
	,ytdamount NUMERIC(37,5)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_jnjreport_muser;
CREATE TABLE IF NOT EXISTS ITG_JNJREPORT_MUSER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_jnjreport_muser
(
	musercode NUMERIC(38,0)  		--//  ENCODE az64
	,musername VARCHAR(50)  		--//  ENCODE lzo
	,muserpassword VARCHAR(50)  		--//  ENCODE lzo
	,mfirstname VARCHAR(50)  		--//  ENCODE lzo
	,mlastname VARCHAR(50)  		--//  ENCODE lzo
	,memailid VARCHAR(200)  		--//  ENCODE lzo
	,userdateofcreation TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,userdateoflastpasschange TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,userpassfailcount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,userdeleted CHAR(1)  		--//  ENCODE lzo
	,encryptedusername VARCHAR(50)  		--//  ENCODE lzo
	,logintime VARCHAR(50)  		--//  ENCODE lzo
	,middlename VARCHAR(50)  		--//  ENCODE lzo
	,personaladdress VARCHAR(2000)  		--//  ENCODE lzo
	,phonenumber VARCHAR(20)  		--//  ENCODE lzo
	,mobilenumber VARCHAR(20)  		--//  ENCODE lzo
	,emergencynumber VARCHAR(20)  		--//  ENCODE lzo
	,isactive CHAR(1)  		--//  ENCODE lzo
	,zonecode VARCHAR(50)  		--//  ENCODE lzo
	,regioncode VARCHAR(50)  		--//  ENCODE lzo
	,wwsapid VARCHAR(15)  		--//  ENCODE lzo
	,usertype CHAR(1)  		--//  ENCODE lzo
	,groupcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,designationcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dateofbirth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,monthofbirth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ipppolicy VARCHAR(1)  		--//  ENCODE lzo
	,dateoflastlogin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,territorycode VARCHAR(50)  		--//  ENCODE lzo
	,distcode VARCHAR(100)  		--//  ENCODE lzo
	,plantcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastloginfailedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,issecretquestionset CHAR(1)  		--//  ENCODE lzo
	,secretqfailcount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwcregion numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwczone numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwcterritory numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_keyaccountsales;
CREATE TABLE IF NOT EXISTS ITG_KEYACCOUNTSALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_keyaccountsales
(
	keyaccountid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distributorcode VARCHAR(10)  		--//  ENCODE lzo
	,salinvno VARCHAR(100)  		--//  ENCODE lzo
	,salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dlvsts numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcode VARCHAR(50)  		--//  ENCODE lzo
	,rtrnm VARCHAR(200)  		--//  ENCODE lzo
	,productid VARCHAR(15)  		--//  ENCODE lzo
	,prdccode VARCHAR(50)  		--//  ENCODE lzo
	,productname VARCHAR(200)  		--//  ENCODE lzo
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdsalerate NUMERIC(38,6)  		--//  ENCODE az64
	,motherskuid VARCHAR(15)  		--//  ENCODE lzo
	,motherskuname VARCHAR(50)  		--//  ENCODE lzo
	,ctgtypid VARCHAR(15)  		--//  ENCODE lzo
	,ctgtypdsc VARCHAR(50)  		--//  ENCODE lzo
	,dlvsts2 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,mnfid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE az64
	,discount NUMERIC(38,6)  		--//  ENCODE az64
	,schid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fromdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,todate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,netrate NUMERIC(38,6)  		--//  ENCODE az64
	,saleflag VARCHAR(3)  		--//  ENCODE lzo
	,weekno numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,confirmsales VARCHAR(1)  		--//  ENCODE lzo
	,subtotal4 NUMERIC(21,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_keyaccountsales_wrk;
CREATE TABLE IF NOT EXISTS ITG_KEYACCOUNTSALES_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_keyaccountsales_wrk
(
	keyaccountid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distributorcode VARCHAR(10)  		--//  ENCODE lzo
	,salinvno VARCHAR(100)  		--//  ENCODE lzo
	,salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dlvsts numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcode VARCHAR(50)  		--//  ENCODE lzo
	,rtrnm VARCHAR(200)  		--//  ENCODE lzo
	,productid VARCHAR(15)  		--//  ENCODE lzo
	,prdccode VARCHAR(50)  		--//  ENCODE lzo
	,productname VARCHAR(200)  		--//  ENCODE lzo
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdsalerate NUMERIC(38,6)  		--//  ENCODE az64
	,motherskuid VARCHAR(15)  		--//  ENCODE lzo
	,motherskuname VARCHAR(50)  		--//  ENCODE lzo
	,ctgtypid VARCHAR(15)  		--//  ENCODE lzo
	,ctgtypdsc VARCHAR(50)  		--//  ENCODE lzo
	,dlvsts2 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,mnfid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE az64
	,discount NUMERIC(38,6)  		--//  ENCODE az64
	,schid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fromdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,todate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,netrate NUMERIC(38,6)  		--//  ENCODE az64
	,saleflag VARCHAR(3)  		--//  ENCODE lzo
	,weekno numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,confirmsales VARCHAR(1)  		--//  ENCODE lzo
	,subtotal4 NUMERIC(21,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_mds_in_businessplan_brand;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_BRAND		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_brand
(
	brand_dsr_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_dsr_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_dsr_name VARCHAR(500)  		--//  ENCODE lzo
	,business_plan NUMERIC(31,5)  		--//  ENCODE az64
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_dsr_code VARCHAR(500)  		--//  ENCODE lzo
	,franchise_dsr_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_dsr_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,variant_name_code VARCHAR(500)  		--//  ENCODE lzo
	,variant_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,variant_name_name VARCHAR(500)  		--//  ENCODE lzo
	,variant_tableau_code VARCHAR(500)  		--//  ENCODE lzo
	,variant_tableau_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,variant_tableau_name VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_name VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_businessplan_brand_final;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_BRAND_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_brand_final
(
	year VARCHAR(500)  		--//  ENCODE zstd
	,month VARCHAR(500)  		--//  ENCODE zstd
	,gsm VARCHAR(500)  		--//  ENCODE zstd
	,region VARCHAR(500)  		--//  ENCODE zstd
	,variant_tableau VARCHAR(500)  		--//  ENCODE zstd
	,variant_name VARCHAR(500)  		--//  ENCODE zstd
	,franchise_dsr VARCHAR(500)  		--//  ENCODE zstd
	,brand_dsr VARCHAR(500)  		--//  ENCODE zstd
	,business_plan NUMERIC(31,5)  		--//  ENCODE az64
	,date VARCHAR(500)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_mds_in_businessplan_brand_raw;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_BRAND_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_brand_raw
(
	brand_dsr_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_dsr_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_dsr_name VARCHAR(500)  		--//  ENCODE lzo
	,business_plan NUMERIC(31,5)  		--//  ENCODE az64
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_dsr_code VARCHAR(500)  		--//  ENCODE lzo
	,franchise_dsr_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_dsr_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,variant_name_code VARCHAR(500)  		--//  ENCODE lzo
	,variant_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,variant_name_name VARCHAR(500)  		--//  ENCODE lzo
	,variant_tableau_code VARCHAR(500)  		--//  ENCODE lzo
	,variant_tableau_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,variant_tableau_name VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_businessplan_channel;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_channel
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel_name_code VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_name VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,plan NUMERIC(31,0)  		--//  ENCODE az64
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_businessplan_channel_final;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_CHANNEL_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_channel_final
(
	region VARCHAR(500)  		--//  ENCODE zstd
	,gsm VARCHAR(500)  		--//  ENCODE zstd
	,channel_name VARCHAR(500)  		--//  ENCODE zstd
	,plan NUMERIC(31,5)  		--//  ENCODE az64
	,year VARCHAR(500)  		--//  ENCODE zstd
	,month VARCHAR(500)  		--//  ENCODE zstd
	,month1 VARCHAR(500)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_mds_in_businessplan_channel_raw;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_CHANNEL_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_channel_raw
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel_name_code VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_name VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,plan NUMERIC(31,0)  		--//  ENCODE az64
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_businessplan_zonewise;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_ZONEWISE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_zonewise
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,zone_name_code VARCHAR(500)  		--//  ENCODE lzo
	,zone_name_name VARCHAR(500)  		--//  ENCODE lzo
	,zone_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name_code VARCHAR(500)  		--//  ENCODE lzo
	,region_name_name VARCHAR(500)  		--//  ENCODE lzo
	,region_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,inv_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,ret_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,plan_name_code VARCHAR(500)  		--//  ENCODE lzo
	,plan_name_name VARCHAR(500)  		--//  ENCODE lzo
	,plan_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_businessplan_zonewise_final;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_ZONEWISE_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_zonewise_final
(
	zone_name VARCHAR(500)  		--//  ENCODE lzo
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm VARCHAR(500)  		--//  ENCODE lzo
	,inv_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,ret_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,plan_name VARCHAR(500)  		--//  ENCODE lzo
	,year VARCHAR(500)  		--//  ENCODE lzo
	,month VARCHAR(500)  		--//  ENCODE lzo
	,month1 VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_businessplan_zonewise_raw;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_BUSINESSPLAN_ZONEWISE_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_businessplan_zonewise_raw
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,zone_name_code VARCHAR(500)  		--//  ENCODE lzo
	,zone_name_name VARCHAR(500)  		--//  ENCODE lzo
	,zone_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name_code VARCHAR(500)  		--//  ENCODE lzo
	,region_name_name VARCHAR(500)  		--//  ENCODE lzo
	,region_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gsm_code VARCHAR(500)  		--//  ENCODE lzo
	,gsm_name VARCHAR(500)  		--//  ENCODE lzo
	,gsm_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cluster_code VARCHAR(500)  		--//  ENCODE lzo
	,cluster_name VARCHAR(500)  		--//  ENCODE lzo
	,cluster_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,inv_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,ret_business_plan NUMERIC(31,7)  		--//  ENCODE az64
	,plan_name_code VARCHAR(500)  		--//  ENCODE lzo
	,plan_name_name VARCHAR(500)  		--//  ENCODE lzo
	,plan_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_ecom_nts_adjustment;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_ECOM_NTS_ADJUSTMENT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_ecom_nts_adjustment
(
	valid_from VARCHAR(20)  		--//  ENCODE zstd
	,valid_to VARCHAR(20)  		--//  ENCODE zstd
	,value NUMERIC(31,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE zstd //  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE zstd //  ENCODE zstd
)

;
--DROP TABLE itg_mds_in_geo_tracker_coordinates;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_GEO_TRACKER_COORDINATES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_geo_tracker_coordinates
(
	zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,lat_zone VARCHAR(200)  		--//  ENCODE lzo
	,long_zone VARCHAR(200)  		--//  ENCODE lzo
	,lat_territory VARCHAR(200)  		--//  ENCODE lzo
	,long_territory VARCHAR(200)  		--//  ENCODE lzo
	,lat_customer VARCHAR(200)  		--//  ENCODE lzo
	,long_customer VARCHAR(200)  		--//  ENCODE lzo
	,updt_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_gl_account_master;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_GL_ACCOUNT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_gl_account_master
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,bw_gl VARCHAR(200)  		--//  ENCODE lzo
	,nature_code VARCHAR(500)  		--//  ENCODE lzo
	,nature_name VARCHAR(500)  		--//  ENCODE lzo
	,nature_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,bravo_mapping_code VARCHAR(500)  		--//  ENCODE lzo
	,bravo_mapping_name VARCHAR(500)  		--//  ENCODE lzo
	,bravo_mapping_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,updt_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_gtm_rds_transitiondate;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_GTM_RDS_TRANSITIONDATE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_gtm_rds_transitiondate
(
	changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distributor_code VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month NUMERIC(31,0)  		--//  ENCODE az64
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,distributor_name VARCHAR(500)  		--//  ENCODE lzo
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year NUMERIC(31,0)  		--//  ENCODE az64
	,load_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_international_customer_details;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_INTERNATIONAL_CUSTOMER_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_international_customer_details
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,code VARCHAR(500)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name_code VARCHAR(500)  		--//  ENCODE zstd
	,region_name_name VARCHAR(500)  		--//  ENCODE zstd
	,region_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,zone_name_code VARCHAR(500)  		--//  ENCODE zstd
	,zone_name_name VARCHAR(500)  		--//  ENCODE zstd
	,zone_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,territory_name_code VARCHAR(500)  		--//  ENCODE zstd
	,territory_name_name VARCHAR(500)  		--//  ENCODE zstd
	,territory_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distributor_type_code VARCHAR(500)  		--//  ENCODE zstd
	,distributor_type_name VARCHAR(500)  		--//  ENCODE zstd
	,distributor_type_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE itg_mds_in_key_accounts_mapping;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_KEY_ACCOUNTS_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_key_accounts_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel_name_code VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_name VARCHAR(500)  		--//  ENCODE lzo
	,channel_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_name_code VARCHAR(500)  		--//  ENCODE lzo
	,account_name_name VARCHAR(500)  		--//  ENCODE lzo
	,account_name_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_orsl_products_target;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_ORSL_PRODUCTS_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_orsl_products_target
(
	changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,code VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hq_code VARCHAR(500)  		--//  ENCODE lzo
	,hq_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hq_name VARCHAR(500)  		--//  ENCODE lzo
	,id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_code VARCHAR(500)  		--//  ENCODE lzo
	,month_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_name VARCHAR(500)  		--//  ENCODE lzo
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,product_category_code VARCHAR(500)  		--//  ENCODE lzo
	,product_category_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_category_name VARCHAR(500)  		--//  ENCODE lzo
	,product_code NUMERIC(31,0)  		--//  ENCODE az64
	,product_name VARCHAR(200)  		--//  ENCODE lzo
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,target NUMERIC(31,0)  		--//  ENCODE az64
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,zone_code VARCHAR(500)  		--//  ENCODE lzo
	,zone_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,zone_name VARCHAR(500)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_mds_in_product_hierarchy;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_PRODUCT_HIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_product_hierarchy
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_combi_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_combi_name VARCHAR(500)  		--//  ENCODE lzo
	,brand_combi_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_combi_var_code VARCHAR(200)  		--//  ENCODE lzo
	,franchise_code VARCHAR(500)  		--//  ENCODE lzo
	,franchise_name VARCHAR(500)  		--//  ENCODE lzo
	,franchise_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_code VARCHAR(500)  		--//  ENCODE lzo
	,group_name VARCHAR(500)  		--//  ENCODE lzo
	,group_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_group_1_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_1_name VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_1_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,brand_group_2_code VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_2_name VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_2_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,updt_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_ps_targets;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_ps_targets
(
	kpi VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(200)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(50)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,target NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_ps_weights;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_ps_weights
(
	kpi VARCHAR(100)  		--//  ENCODE lzo
	,channel VARCHAR(100)  		--//  ENCODE lzo
	,retail_env VARCHAR(100)  		--//  ENCODE lzo
	,weight NUMERIC(20,4)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_sap_distribution_channel;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SAP_DISTRIBUTION_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sap_distribution_channel
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,chnl_desc2_code VARCHAR(500)  		--//  ENCODE lzo
	,chnl_desc2_name VARCHAR(500)  		--//  ENCODE lzo
	,chnl_desc2_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,updt_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_sss_ps_msl;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SSS_PS_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sss_ps_msl
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,re VARCHAR(200)  		--//  ENCODE lzo
	,sku_code VARCHAR(200)  		--//  ENCODE lzo
	,sku_name VARCHAR(510)  		--//  ENCODE lzo
	,valid_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,valid_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_sss_ps_promoter_store_map;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SSS_PS_PROMOTER_STORE_MAP		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sss_ps_promoter_store_map
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(200)  		--//  ENCODE lzo
	,store_name VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_sss_score;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SSS_SCORE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sss_score
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_class VARCHAR(200)  		--//  ENCODE lzo
	,re VARCHAR(200)  		--//  ENCODE lzo
	,brand VARCHAR(200)  		--//  ENCODE lzo
	,kpi VARCHAR(200)  		--//  ENCODE lzo
	,min_value VARCHAR(200)  		--//  ENCODE lzo
	,max_value VARCHAR(200)  		--//  ENCODE lzo
	,value NUMERIC(31,3)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_sss_weights;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SSS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sss_weights
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,re VARCHAR(200)  		--//  ENCODE lzo
	,kpi VARCHAR(200)  		--//  ENCODE lzo
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,store_class VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_mds_in_sss_zonal_rtr_program_mapping;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SSS_ZONAL_RTR_PROGRAM_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sss_zonal_rtr_program_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtruniquecode NUMERIC(31,0)  		--//  ENCODE az64
	,retailer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_code NUMERIC(31,0)  		--//  ENCODE az64
	,program_mapping_code VARCHAR(500)  		--//  ENCODE lzo
	,program_mapping_name VARCHAR(500)  		--//  ENCODE lzo
	,program_mapping_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarter_code VARCHAR(500)  		--//  ENCODE lzo
	,quarter_name VARCHAR(500)  		--//  ENCODE lzo
	,quarter_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year_code VARCHAR(500)  		--//  ENCODE lzo
	,year_name VARCHAR(500)  		--//  ENCODE lzo
	,year_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,load_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_subdtown_district_master;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SUBDTOWN_DISTRICT_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_subdtown_district_master
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE zstd
	,versionname VARCHAR(100)  		--//  ENCODE zstd
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE zstd
	,name VARCHAR(500)  		--//  ENCODE zstd
	,code VARCHAR(500)  		--//  ENCODE zstd
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cust_code VARCHAR(200)  		--//  ENCODE zstd
	,retailer_code VARCHAR(200)  		--//  ENCODE zstd
	,town_name VARCHAR(200)  		--//  ENCODE zstd
	,district_name VARCHAR(200)  		--//  ENCODE zstd
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE zstd
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE zstd
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_mds_in_sv_winculum_master;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SV_WINCULUM_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sv_winculum_master
(
	customercode VARCHAR(200)  		--//  ENCODE lzo
	,customername VARCHAR(200)  		--//  ENCODE lzo
	,regioncode VARCHAR(200)  		--//  ENCODE lzo
	,zonecode VARCHAR(200)  		--//  ENCODE lzo
	,territorycode VARCHAR(200)  		--//  ENCODE lzo
	,statecode VARCHAR(200)  		--//  ENCODE lzo
	,towncode VARCHAR(200)  		--//  ENCODE lzo
	,psnonps VARCHAR(200)  		--//  ENCODE lzo
	,isactive VARCHAR(200)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(200)  		--//  ENCODE lzo
	,parentcustomercode VARCHAR(200)  		--//  ENCODE lzo
	,isdirectacct VARCHAR(200)  		--//  ENCODE lzo
	,abicode VARCHAR(200)  		--//  ENCODE lzo
	,distributorsapid VARCHAR(200)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(18,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_sv_winculum_master_wrk;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_SV_WINCULUM_MASTER_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_sv_winculum_master_wrk
(
	customercode VARCHAR(200)  		--//  ENCODE lzo
	,customername VARCHAR(200)  		--//  ENCODE lzo
	,regioncode VARCHAR(200)  		--//  ENCODE lzo
	,zonecode VARCHAR(200)  		--//  ENCODE lzo
	,territorycode VARCHAR(200)  		--//  ENCODE lzo
	,statecode VARCHAR(200)  		--//  ENCODE lzo
	,towncode VARCHAR(200)  		--//  ENCODE lzo
	,psnonps VARCHAR(200)  		--//  ENCODE lzo
	,isactive VARCHAR(200)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(200)  		--//  ENCODE lzo
	,parentcustomercode VARCHAR(200)  		--//  ENCODE lzo
	,isdirectacct VARCHAR(200)  		--//  ENCODE lzo
	,abicode VARCHAR(200)  		--//  ENCODE lzo
	,distributorsapid VARCHAR(200)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,effective_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,effective_to TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,active VARCHAR(2)  		--//  ENCODE lzo
	,run_id NUMERIC(18,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_in_vent_prod_msku_mapping;
CREATE TABLE IF NOT EXISTS ITG_MDS_IN_VENT_PROD_MSKU_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_in_vent_prod_msku_mapping
(
	id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,muid VARCHAR(36)  		--//  ENCODE lzo
	,versionname VARCHAR(100)  		--//  ENCODE lzo
	,versionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,version_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,versionflag VARCHAR(100)  		--//  ENCODE lzo
	,name VARCHAR(500)  		--//  ENCODE lzo
	,code VARCHAR(500)  		--//  ENCODE lzo
	,changetrackingmask numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mother_sku_cd VARCHAR(200)  		--//  ENCODE lzo
	,product_vent_code VARCHAR(500)  		--//  ENCODE lzo
	,product_vent_name VARCHAR(500)  		--//  ENCODE lzo
	,product_vent_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_log;
CREATE TABLE IF NOT EXISTS ITG_MDS_LOG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_log
(
	cntry_cd VARCHAR(10)  		--//  ENCODE lzo
	,table_name VARCHAR(255)  		--//  ENCODE lzo
	,result VARCHAR(1000)  		--//  ENCODE lzo
	,status VARCHAR(50)  		--//  ENCODE lzo
	,rec_count numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crtd_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_mds_month_end_dates;
CREATE TABLE IF NOT EXISTS ITG_MDS_MONTH_END_DATES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mds_month_end_dates
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,key_account_month_end TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,pathfinder_month_end TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mds_lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updtddatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE itg_mth_cls_stock_fact;
CREATE TABLE IF NOT EXISTS ITG_MTH_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_mth_cls_stock_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,stock_month VARCHAR(10)  		--//  ENCODE lzo
	,stock_year VARCHAR(10)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,calsalclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calunsalclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calofferclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calsalclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,calunsalclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,calofferclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,salstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,salstockadjval NUMERIC(32,7)  		--//  ENCODE delta
	,unsalstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,unsalstockadjval NUMERIC(32,7)  		--//  ENCODE delta
	,offerstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,offerstockadjval NUMERIC(32,7)  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_orderbooking;
CREATE TABLE IF NOT EXISTS ITG_ORDERBOOKING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_orderbooking
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,orderno VARCHAR(50)  		--//  ENCODE zstd
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,orddlvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,allowbackorder VARCHAR(50)  		--//  ENCODE zstd
	,ordtype VARCHAR(50)  		--//  ENCODE zstd
	,ordpriority VARCHAR(50)  		--//  ENCODE zstd
	,orddocref VARCHAR(100)  		--//  ENCODE zstd
	,remarks VARCHAR(500)  		--//  ENCODE zstd
	,roundoffamt NUMERIC(38,6)  		--//  ENCODE az64
	,ordtotalamt NUMERIC(38,6)  		--//  ENCODE az64
	,salesmancode VARCHAR(100)  		--//  ENCODE zstd
	,salesmanname VARCHAR(200)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(100)  		--//  ENCODE zstd
	,salesroutename VARCHAR(200)  		--//  ENCODE zstd
	,rtrid VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdbilledqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdselrate NUMERIC(38,6)  		--//  ENCODE az64
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,recorddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,recommendedsku VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_pf_clstk_mth_ds;
CREATE TABLE IF NOT EXISTS ITG_PF_CLSTK_MTH_DS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pf_clstk_mth_ds
(
	month numeric(18,0)		--// INTEGER
	,year numeric(18,0)		--// INTEGER
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,product_code VARCHAR(50)
	,cl_stck_qty NUMERIC(12,2)  		--//  ENCODE zstd
	,cl_stck_value NUMERIC(18,2)  		--//  ENCODE zstd
	,cl_stck_nr NUMERIC(18,3)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	year
		--// 	, month
		--// 	, product_code
		--// 	)
;		--// ;
--DROP TABLE itg_pilot_nup_target;
CREATE TABLE IF NOT EXISTS ITG_PILOT_NUP_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pilot_nup_target
(
	rtruniquecode VARCHAR(50)  		--//  ENCODE lzo
	,nup_target numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE itg_plant;
CREATE TABLE IF NOT EXISTS ITG_PLANT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_plant
(
	plantcode NUMERIC(38,0)  		--//  ENCODE lzo
	,plantid VARCHAR(50)  		--//  ENCODE lzo
	,plantname VARCHAR(50)  		--//  ENCODE lzo
	,shortname VARCHAR(50)  		--//  ENCODE lzo
	,name2 VARCHAR(200)  		--//  ENCODE lzo
	,statecode NUMERIC(18,0)  		--//  ENCODE lzo
	,active CHAR(1)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE lzo
	,suppliercode VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_pos_category_mapping;
CREATE TABLE IF NOT EXISTS ITG_POS_CATEGORY_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pos_category_mapping
(
	account_name VARCHAR(255)  		--//  ENCODE zstd
	,article_cd VARCHAR(20)  		--//  ENCODE zstd
	,article_desc VARCHAR(255)  		--//  ENCODE zstd
	,ean VARCHAR(20)  		--//  ENCODE zstd
	,sap_cd VARCHAR(20)  		--//  ENCODE zstd
	,mother_sku_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,product_category_name VARCHAR(255)  		--//  ENCODE zstd
	,variant_name VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,internal_category VARCHAR(255)  		--//  ENCODE zstd
	,internal_sub_category VARCHAR(255)  		--//  ENCODE zstd
	,external_category VARCHAR(255)  		--//  ENCODE zstd
	,external_sub_category VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_pos_historical_btl;
CREATE TABLE IF NOT EXISTS ITG_POS_HISTORICAL_BTL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pos_historical_btl
(
	mother_sku_name VARCHAR(255)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,re VARCHAR(255)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,promos NUMERIC(38,6)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_pos_offtake_fact;
CREATE TABLE IF NOT EXISTS ITG_POS_OFFTAKE_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pos_offtake_fact
(
	key_account_name VARCHAR(255)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE zstd
	,store_cd VARCHAR(20)  		--//  ENCODE zstd
	,subcategory VARCHAR(255)  		--//  ENCODE zstd
	,level VARCHAR(10)  		--//  ENCODE zstd
	,article_cd VARCHAR(20)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE zstd
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,source_file_name VARCHAR(255)  		--//  ENCODE zstd
	,file_upload_date DATE  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_pos_re_mapping;
CREATE TABLE IF NOT EXISTS ITG_POS_RE_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pos_re_mapping
(
	store_cd VARCHAR(20)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,zone VARCHAR(255)  		--//  ENCODE zstd
	,re VARCHAR(255)  		--//  ENCODE zstd
	,promotor VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_pricelist;
CREATE TABLE IF NOT EXISTS ITG_PRICELIST		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_pricelist
(
	productcode VARCHAR(50) NOT NULL 		--//  ENCODE lzo
	,productname VARCHAR(100)  		--//  ENCODE lzo
	,pack VARCHAR(100)  		--//  ENCODE lzo
	,mrpperpack DOUBLE PRECISION
	,billingunit DOUBLE PRECISION
	,mrpperunit DOUBLE PRECISION
	,zdst DOUBLE PRECISION
	,retailermarginpercent DOUBLE PRECISION
	,retailermarginamt DOUBLE PRECISION
	,retailerprice DOUBLE PRECISION
	,td DOUBLE PRECISION
	,excisedutyper DOUBLE PRECISION
	,vatrds2ret DOUBLE PRECISION
	,vatrds2retamt DOUBLE PRECISION
	,baseprice DOUBLE PRECISION
	,tdamt DOUBLE PRECISION
	,vatjnj2rds DOUBLE PRECISION
	,vatjnj2rdsamt DOUBLE PRECISION
	,listprice DOUBLE PRECISION
	,cd DOUBLE PRECISION
	,excisedutyamt DOUBLE PRECISION
	,excisecessamt DOUBLE PRECISION
	,excisedutytot DOUBLE PRECISION
	,nr DOUBLE PRECISION
	,state VARCHAR(100)  		--//  ENCODE lzo
	,startdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,enddate VARCHAR(100)  		--//  ENCODE lzo
	,insertedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,statecode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,userid VARCHAR(200)  		--//  ENCODE lzo
	,cststate CHAR(10)  		--//  ENCODE lzo
	,calculateflag CHAR(10)  		--//  ENCODE lzo
	,filecode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_product_uom_master;
CREATE TABLE IF NOT EXISTS ITG_PRODUCT_UOM_MASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_product_uom_master
(
	material VARCHAR(18)
	,mat_unit VARCHAR(3)  		--//  ENCODE lzo
	,objvers VARCHAR(1)  		--//  ENCODE lzo
	,changed VARCHAR(1)  		--//  ENCODE lzo
	,denomintr NUMERIC(17,3)  		--//  ENCODE lzo
	,eanupc VARCHAR(18)  		--//  ENCODE lzo
	,ean_numtyp VARCHAR(2)  		--//  ENCODE lzo
	,gross_wt NUMERIC(17,3)  		--//  ENCODE lzo
	,height NUMERIC(17,3)  		--//  ENCODE lzo
	,len NUMERIC(17,3)  		--//  ENCODE lzo
	,numerator NUMERIC(17,3)  		--//  ENCODE lzo
	,unit VARCHAR(3)  		--//  ENCODE lzo
	,unit_dim VARCHAR(3)  		--//  ENCODE lzo
	,unit_of_wt VARCHAR(3)  		--//  ENCODE lzo
	,volume NUMERIC(17,3)  		--//  ENCODE lzo
	,volumeunit VARCHAR(3)  		--//  ENCODE lzo
	,width NUMERIC(17,3)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	material
		--// 	)
;		--// ;
--DROP TABLE itg_program_store_target;
CREATE TABLE IF NOT EXISTS ITG_PROGRAM_STORE_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_program_store_target
(
	program_name VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,territory VARCHAR(50)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_code VARCHAR(50)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,year_month DATE  		--//  ENCODE delta
	,value NUMERIC(38,2)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_query_parameters;
CREATE TABLE IF NOT EXISTS ITG_QUERY_PARAMETERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_query_parameters
(
	country_code VARCHAR(10)  		--//  ENCODE zstd
	,parameter_name VARCHAR(300)  		--//  ENCODE zstd
	,parameter_value VARCHAR(300)  		--//  ENCODE zstd
	,parameter_type VARCHAR(300)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_rdssmweeklytarget_output;
CREATE TABLE IF NOT EXISTS ITG_RDSSMWEEKLYTARGET_OUTPUT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rdssmweeklytarget_output
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,targetrefno VARCHAR(100)  		--//  ENCODE lzo
	,targetdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,smcode VARCHAR(50)  		--//  ENCODE lzo
	,smname VARCHAR(100)  		--//  ENCODE lzo
	,rmcode VARCHAR(50)  		--//  ENCODE lzo
	,rmname VARCHAR(100)  		--//  ENCODE lzo
	,targetyear numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,targetmonth VARCHAR(30)  		--//  ENCODE lzo
	,targetvalue NUMERIC(36,2)  		--//  ENCODE lzo
	,targetname VARCHAR(50)  		--//  ENCODE lzo
	,week1 NUMERIC(36,2)  		--//  ENCODE lzo
	,week2 NUMERIC(36,2)  		--//  ENCODE lzo
	,week3 NUMERIC(36,2)  		--//  ENCODE lzo
	,week4 NUMERIC(36,2)  		--//  ENCODE lzo
	,week5 NUMERIC(36,2)  		--//  ENCODE lzo
	,targetstatus VARCHAR(10)  		--//  ENCODE lzo
	,targettype VARCHAR(50)  		--//  ENCODE lzo
	,downloadstatus VARCHAR(10)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_region;
CREATE TABLE IF NOT EXISTS ITG_REGION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_region
(
	regioncode NUMERIC(18,0)  		--//  ENCODE lzo
	,regionname VARCHAR(50)  		--//  ENCODE lzo
	,sapid VARCHAR(50)  		--//  ENCODE lzo
	,gtpa NUMERIC(18,0)  		--//  ENCODE lzo
	,gtifjnj NUMERIC(18,0)  		--//  ENCODE lzo
	,gtifrds NUMERIC(18,0)  		--//  ENCODE lzo
	,rdspa NUMERIC(18,0)  		--//  ENCODE lzo
	,rdssmpa NUMERIC(18,0)  		--//  ENCODE lzo
	,ifjnj NUMERIC(18,0)  		--//  ENCODE lzo
	,ifrds NUMERIC(18,0)  		--//  ENCODE lzo
	,bpa NUMERIC(18,0)  		--//  ENCODE lzo
	,bifjnj NUMERIC(18,0)  		--//  ENCODE lzo
	,bifrds NUMERIC(18,0)  		--//  ENCODE lzo
	,ppa NUMERIC(18,0)  		--//  ENCODE lzo
	,pifjnj NUMERIC(18,0)  		--//  ENCODE lzo
	,pifrds NUMERIC(18,0)  		--//  ENCODE lzo
	,isvisible CHAR(1)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE lzo
	,deletionmark VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,uppt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_retailercategory;
CREATE TABLE IF NOT EXISTS ITG_RETAILERCATEGORY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_retailercategory
(
	retailercategorycode VARCHAR(10)  		--//  ENCODE lzo
	,retailercategoryname VARCHAR(50)  		--//  ENCODE lzo
	,ctgcode VARCHAR(40)  		--//  ENCODE lzo
	,ctglinkid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ctglevelid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isbrandshow numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isactive BOOLEAN
	,rowid VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_retailermaster;
CREATE TABLE IF NOT EXISTS ITG_RETAILERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_retailermaster
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrcatlevelid VARCHAR(30)  		--//  ENCODE zstd
	,rtrcategorycode VARCHAR(50)  		--//  ENCODE zstd
	,classcode VARCHAR(50)  		--//  ENCODE zstd
	,keyaccount VARCHAR(50)  		--//  ENCODE zstd
	,regdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,relationstatus VARCHAR(50)  		--//  ENCODE zstd
	,parentcode VARCHAR(50)  		--//  ENCODE zstd
	,geolevel VARCHAR(50)  		--//  ENCODE zstd
	,geolevelvalue VARCHAR(100)  		--//  ENCODE zstd
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createdid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rtraddress1 VARCHAR(100)  		--//  ENCODE zstd
	,rtraddress2 VARCHAR(100)  		--//  ENCODE zstd
	,rtraddress3 VARCHAR(100)  		--//  ENCODE zstd
	,rtrpincode VARCHAR(20)  		--//  ENCODE zstd
	,villageid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,villagecode VARCHAR(100)  		--//  ENCODE zstd
	,villagename VARCHAR(100)  		--//  ENCODE zstd
	,mode VARCHAR(100)  		--//  ENCODE zstd
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,approvalremarks VARCHAR(400)  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,druglno VARCHAR(100)  		--//  ENCODE zstd
	,rtrcrbills numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcrlimit NUMERIC(38,6)  		--//  ENCODE az64
	,rtrcrdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrdayoff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrtinno VARCHAR(100)  		--//  ENCODE zstd
	,rtrcstno VARCHAR(100)  		--//  ENCODE zstd
	,rtrlicno VARCHAR(100)  		--//  ENCODE zstd
	,rtrlicexpirydate VARCHAR(100)  		--//  ENCODE zstd
	,rtrdrugexpirydate VARCHAR(100)  		--//  ENCODE zstd
	,rtrpestlicno VARCHAR(100)  		--//  ENCODE zstd
	,rtrpestexpirydate VARCHAR(100)  		--//  ENCODE zstd
	,approved numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrphoneno VARCHAR(100)  		--//  ENCODE zstd
	,rtrcontactperson VARCHAR(100)  		--//  ENCODE zstd
	,rtrtaxgroup VARCHAR(100)  		--//  ENCODE zstd
	,rtrtype VARCHAR(50)  		--//  ENCODE zstd
	,rtrtaxable VARCHAR(1)  		--//  ENCODE zstd
	,rtrshippadd1 VARCHAR(200)  		--//  ENCODE zstd
	,rtrshippadd2 VARCHAR(200)  		--//  ENCODE zstd
	,rtrshippadd3 VARCHAR(200)  		--//  ENCODE zstd
	,rtrfoodlicno VARCHAR(200)  		--//  ENCODE zstd
	,rtrfoodexpirydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rtrfoodgracedays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrdruggracedays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcosmeticlicno VARCHAR(200)  		--//  ENCODE zstd
	,rtrcosmeticexpirydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rtrcosmeticgracedays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,rtrlatitude VARCHAR(40)  		--//  ENCODE zstd
	,rtrlongitude VARCHAR(40)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,file_rec_dt DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	actv_flg
		--// 	)
;		--// ;
--DROP TABLE itg_retailervalueclass;
CREATE TABLE IF NOT EXISTS ITG_RETAILERVALUECLASS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_retailervalueclass
(
	classid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,valueclasscode VARCHAR(40)  		--//  ENCODE lzo
	,valueclassname VARCHAR(100)  		--//  ENCODE lzo
	,ctgmainid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isactive BOOLEAN
	,distcode VARCHAR(100)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rkeyacccustomer;
CREATE TABLE IF NOT EXISTS ITG_RKEYACCCUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rkeyacccustomer
(
	customercode VARCHAR(50)  		--//  ENCODE lzo
	,customername VARCHAR(50)  		--//  ENCODE lzo
	,customeraddress1 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress2 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress3 VARCHAR(250)  		--//  ENCODE lzo
	,sapid VARCHAR(50)  		--//  ENCODE lzo
	,regioncode VARCHAR(50)  		--//  ENCODE lzo
	,zonecode VARCHAR(50)  		--//  ENCODE lzo
	,territorycode VARCHAR(50)  		--//  ENCODE lzo
	,statecode VARCHAR(50)  		--//  ENCODE lzo
	,towncode VARCHAR(50)  		--//  ENCODE lzo
	,emailid VARCHAR(50)  		--//  ENCODE lzo
	,mobilell VARCHAR(50)  		--//  ENCODE lzo
	,isactive CHAR(1)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(50)  		--//  ENCODE lzo
	,nkacstores CHAR(1)  		--//  ENCODE lzo
	,parentcustomercode VARCHAR(50)  		--//  ENCODE lzo
	,isdirectacct CHAR(1)  		--//  ENCODE lzo
	,isparent CHAR(1)  		--//  ENCODE lzo
	,abicode VARCHAR(50)  		--//  ENCODE lzo
	,distributorsapid VARCHAR(50)  		--//  ENCODE lzo
	,isconfirm VARCHAR(1)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdusercode VARCHAR(50)  		--//  ENCODE lzo
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modusercode VARCHAR(50)  		--//  ENCODE lzo
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rmrpstockprocess_clstk;
CREATE TABLE IF NOT EXISTS ITG_RMRPSTOCKPROCESS_CLSTK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rmrpstockprocess_clstk
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_rmrpstockprocess_clstk_hist;
CREATE TABLE IF NOT EXISTS ITG_RMRPSTOCKPROCESS_CLSTK_HIST		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rmrpstockprocess_clstk_hist
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	distcode
		--// 	)
;		--// ;
--DROP TABLE itg_rmrpstockprocess_opstk;
CREATE TABLE IF NOT EXISTS ITG_RMRPSTOCKPROCESS_OPSTK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rmrpstockprocess_opstk
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock NUMERIC(22,6)  		--//  ENCODE az64
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_rmrpstockprocess_opstk_hist;
CREATE TABLE IF NOT EXISTS ITG_RMRPSTOCKPROCESS_OPSTK_HIST		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rmrpstockprocess_opstk_hist
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock NUMERIC(22,6)  		--//  ENCODE az64
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_routemaster;
CREATE TABLE IF NOT EXISTS ITG_ROUTEMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_routemaster
(
	routecode VARCHAR(50)  		--//  ENCODE lzo
	,routeename VARCHAR(100)  		--//  ENCODE lzo
	,flag CHAR(2)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,distributorcode VARCHAR(28)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rpurchasedetail;
CREATE TABLE IF NOT EXISTS ITG_RPURCHASEDETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rpurchasedetail
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,compinvno VARCHAR(25)  		--//  ENCODE zstd
	,compinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salesorderno VARCHAR(50)  		--//  ENCODE zstd
	,netvalue NUMERIC(38,6)  		--//  ENCODE az64
	,totaltax NUMERIC(38,6)  		--//  ENCODE az64
	,totaldiscount NUMERIC(38,6)  		--//  ENCODE az64
	,totalschemeamount NUMERIC(38,6)  		--//  ENCODE az64
	,totaloctroi NUMERIC(38,6)  		--//  ENCODE az64
	,suppliercode VARCHAR(50)  		--//  ENCODE zstd
	,cfacode VARCHAR(30)  		--//  ENCODE zstd
	,companyname VARCHAR(50)  		--//  ENCODE zstd
	,transportercode VARCHAR(20)  		--//  ENCODE zstd
	,lorryrecno VARCHAR(25)  		--//  ENCODE zstd
	,lorryrecdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,posgrnno VARCHAR(50)  		--//  ENCODE zstd
	,posgrndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,indentqtycs NUMERIC(38,6)  		--//  ENCODE az64
	,indentqtypcs NUMERIC(38,6)  		--//  ENCODE az64
	,uomcode VARCHAR(20)  		--//  ENCODE zstd
	,cashdiscrs NUMERIC(38,6)  		--//  ENCODE az64
	,cashdiscper NUMERIC(38,6)  		--//  ENCODE az64
	,octroi NUMERIC(38,6)  		--//  ENCODE az64
	,linelevelamt NUMERIC(38,6)  		--//  ENCODE az64
	,batchno VARCHAR(50)  		--//  ENCODE zstd
	,mnfgdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,expdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mrp NUMERIC(38,6)  		--//  ENCODE az64
	,lsp NUMERIC(38,6)  		--//  ENCODE az64
	,purtax NUMERIC(38,6)  		--//  ENCODE az64
	,purdisc NUMERIC(38,6)  		--//  ENCODE az64
	,purrate NUMERIC(38,6)  		--//  ENCODE az64
	,sellrate NUMERIC(38,6)  		--//  ENCODE az64
	,sellrateat NUMERIC(38,6)  		--//  ENCODE az64
	,sellrateavat NUMERIC(38,6)  		--//  ENCODE az64
	,vattaxvalue NUMERIC(38,6)  		--//  ENCODE az64
	,status VARCHAR(50)  		--//  ENCODE zstd
	,freescheme numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schemerefrno VARCHAR(100)  		--//  ENCODE zstd
	,waybillno VARCHAR(50)  		--//  ENCODE zstd
	,designcode VARCHAR(50)  		--//  ENCODE zstd
	,bundledeal VARCHAR(50)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,migrationflag VARCHAR(100)  		--//  ENCODE zstd
	,mproductcode VARCHAR(50)  		--//  ENCODE zstd
	,taxablevalue NUMERIC(18,6)  		--//  ENCODE az64
	,cgstper NUMERIC(18,6)  		--//  ENCODE az64
	,sgstper NUMERIC(18,6)  		--//  ENCODE az64
	,utgstper NUMERIC(18,6)  		--//  ENCODE az64
	,igstper NUMERIC(18,6)  		--//  ENCODE az64
	,cessper NUMERIC(18,6)  		--//  ENCODE az64
	,othertax1per NUMERIC(18,6)  		--//  ENCODE az64
	,othertax2per NUMERIC(18,6)  		--//  ENCODE az64
	,othertax3per NUMERIC(18,6)  		--//  ENCODE az64
	,cgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,sgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,utgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,igstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,cessvalue NUMERIC(18,6)  		--//  ENCODE az64
	,othertax1value NUMERIC(18,6)  		--//  ENCODE az64
	,othertax2value NUMERIC(18,6)  		--//  ENCODE az64
	,othertax3value NUMERIC(18,6)  		--//  ENCODE az64
	,reversecharges NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother1 NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother2 NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother3 NUMERIC(18,6)  		--//  ENCODE az64
	,taxtype VARCHAR(10)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,downloadflag VARCHAR(3)  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_rrl_retailer_geocoordinates;
CREATE TABLE IF NOT EXISTS ITG_RRL_RETAILER_GEOCOORDINATES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rrl_retailer_geocoordinates
(
	rgc_id numeric(18,0)		--// INTEGER
	,rgc_usercode VARCHAR(50)  		--//  ENCODE lzo
	,rgc_distcode VARCHAR(50)  		--//  ENCODE lzo
	,rgc_code VARCHAR(50)  		--//  ENCODE lzo
	,rgc_latitude VARCHAR(20)  		--//  ENCODE lzo
	,rgc_longtitude VARCHAR(20)  		--//  ENCODE lzo
	,rgc_geouniqueid VARCHAR(100)  		--//  ENCODE lzo
	,rgc_createdby VARCHAR(20)  		--//  ENCODE lzo
	,rgc_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rgc_modifiedby VARCHAR(20)  		--//  ENCODE lzo
	,rgc_modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rgc_flag VARCHAR(1)  		--//  ENCODE lzo
	,rgc_status_flag VARCHAR(1)  		--//  ENCODE lzo
	,rgc_flex VARCHAR(200)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

cluster by (rgc_id)
		--// SORTKEY ( 
		--// 	rgc_id
		--// 	)
;		--// ;
--DROP TABLE itg_rrl_retailermaster;
CREATE TABLE IF NOT EXISTS ITG_RRL_RETAILERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rrl_retailermaster
(
	retailercode VARCHAR(50)  		--//  ENCODE lzo
	,retailername VARCHAR(100)  		--//  ENCODE lzo
	,routecode VARCHAR(25)  		--//  ENCODE lzo
	,retailerclasscode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,foodlicenseno VARCHAR(50)  		--//  ENCODE lzo
	,druglicenseno VARCHAR(50)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,phone VARCHAR(15)  		--//  ENCODE lzo
	,mobile VARCHAR(15)  		--//  ENCODE lzo
	,prcontact VARCHAR(50)  		--//  ENCODE lzo
	,seccontact VARCHAR(50)  		--//  ENCODE lzo
	,creditlimit NUMERIC(18,0)  		--//  ENCODE lzo
	,creditperiod numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,invoicelimit VARCHAR(30)  		--//  ENCODE lzo
	,isapproved CHAR(1)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,rsrcode VARCHAR(100)  		--//  ENCODE lzo
	,drugvaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,fssaivaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,displaystatus VARCHAR(20)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,ownername VARCHAR(100)  		--//  ENCODE lzo
	,druglicenseno2 VARCHAR(50)  		--//  ENCODE lzo
	,r_statecode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,r_districtcode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,r_tahsilcode numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,address1 VARCHAR(100)  		--//  ENCODE lzo
	,address2 VARCHAR(100)  		--//  ENCODE lzo
	,retailerchannelcode VARCHAR(40)  		--//  ENCODE lzo
	,retailerclassid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,actv_flg CHAR(1)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_rrl_udcmaster;
CREATE TABLE IF NOT EXISTS ITG_RRL_UDCMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rrl_udcmaster
(
	udccode VARCHAR(50)
	,udcname VARCHAR(200)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,rowid VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE KEY
cluster by (udccode)
		--// SORTKEY ( 
		--// 	udccode
		--// 	)
;		--// ;
--DROP TABLE itg_rrl_usermaster;
CREATE TABLE IF NOT EXISTS ITG_RRL_USERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rrl_usermaster
(
	userid numeric(38,0)		--// BIGINT
	,usercode VARCHAR(50)  		--//  ENCODE lzo
	,login VARCHAR(200)  		--//  ENCODE lzo
	,password VARCHAR(100)  		--//  ENCODE lzo
	,eusername VARCHAR(100)  		--//  ENCODE lzo
	,userlevel numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,parentid VARCHAR(100)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,teritoryid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,abnumber VARCHAR(100)  		--//  ENCODE lzo
	,forumcode VARCHAR(100)  		--//  ENCODE lzo
	,regionid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,emailid VARCHAR(200)  		--//  ENCODE lzo
	,currentversion VARCHAR(20)  		--//  ENCODE lzo
	,updateversion VARCHAR(20)  		--//  ENCODE lzo
	,imei VARCHAR(100)  		--//  ENCODE lzo
	,mobileno VARCHAR(50)  		--//  ENCODE lzo
	,locationid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ishht CHAR(1)  		--//  ENCODE lzo
	,user_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,distuserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,freezeday numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

cluster by (userid)
		--// SORTKEY ( 
		--// 	userid
		--// 	)
;		--// ;
--DROP TABLE itg_rsdmaster;
CREATE TABLE IF NOT EXISTS ITG_RSDMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rsdmaster
(
	rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,rsdname VARCHAR(50)  		--//  ENCODE lzo
	,rsdfirm VARCHAR(50)  		--//  ENCODE lzo
	,rsrcode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,montlyincome NUMERIC(18,2)  		--//  ENCODE az64
	,manpower numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,godownspace VARCHAR(50)  		--//  ENCODE lzo
	,address VARCHAR(200)  		--//  ENCODE lzo
	,contactno VARCHAR(50)  		--//  ENCODE lzo
	,druglicense VARCHAR(50)  		--//  ENCODE lzo
	,foodlicense VARCHAR(50)  		--//  ENCODE lzo
	,isownhouse CHAR(1)  		--//  ENCODE lzo
	,isnative CHAR(1)  		--//  ENCODE lzo
	,drugvaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fssaivaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isapproved CHAR(1)  		--//  ENCODE lzo
	,flag CHAR(1)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,createdby VARCHAR(50)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifiedby VARCHAR(50)  		--//  ENCODE lzo
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,deleteddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,channelname VARCHAR(100)  		--//  ENCODE lzo
	,subchannelid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,subchannelname VARCHAR(100)  		--//  ENCODE lzo
	,categoryid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,categoryname VARCHAR(100)  		--//  ENCODE lzo
	,outlettype VARCHAR(30)  		--//  ENCODE lzo
	,modaloutlet VARCHAR(30)  		--//  ENCODE lzo
	,synctimestamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,contactperson VARCHAR(100)  		--//  ENCODE lzo
	,rsdemailid VARCHAR(50)  		--//  ENCODE lzo
	,druglicenseno2 VARCHAR(50)  		--//  ENCODE lzo
	,rsdemailid1 VARCHAR(50)  		--//  ENCODE lzo
	,rsdemailid2 VARCHAR(50)  		--//  ENCODE lzo
	,salesrepemailid VARCHAR(50)  		--//  ENCODE lzo
	,routecode VARCHAR(100)  		--//  ENCODE lzo
	,rtrclassid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rstockdiscrepancy_withproduct;
CREATE TABLE IF NOT EXISTS ITG_RSTOCKDISCREPANCY_WITHPRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rstockdiscrepancy_withproduct
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,openingdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,closingdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salsales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalsales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offersales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,damagein numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,damageout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salsalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalsalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offersalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sallcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsallcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerlcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sallcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsallcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerlcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salreplacement numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerreplacement numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calsalclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calunsalclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calofferclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,lp NUMERIC(18,6)  		--//  ENCODE az64
	,ptr NUMERIC(18,6)  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updtdttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_rtbl_idtmanagementupload;
CREATE TABLE IF NOT EXISTS ITG_RTBL_IDTMANAGEMENTUPLOAD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rtbl_idtmanagementupload
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,idtmngrefno VARCHAR(50)  		--//  ENCODE zstd
	,idtmngdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,idtdistcode VARCHAR(50)  		--//  ENCODE zstd
	,idtdistname VARCHAR(100)  		--//  ENCODE zstd
	,lcncode VARCHAR(50)  		--//  ENCODE zstd
	,lcnname VARCHAR(200)  		--//  ENCODE zstd
	,stkmgmttype VARCHAR(30)  		--//  ENCODE zstd
	,docrefno VARCHAR(100)  		--//  ENCODE zstd
	,lrnumber VARCHAR(100)  		--//  ENCODE zstd
	,lrdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,remarks VARCHAR(200)  		--//  ENCODE zstd
	,status VARCHAR(100)  		--//  ENCODE zstd
	,idtgrossamt NUMERIC(38,2)  		--//  ENCODE az64
	,idttaxamt NUMERIC(38,2)  		--//  ENCODE az64
	,idtnetamt NUMERIC(38,2)  		--//  ENCODE az64
	,idtpaidamt NUMERIC(38,2)  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,batchcode VARCHAR(50)  		--//  ENCODE zstd
	,systemstocktype numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stocktype VARCHAR(50)  		--//  ENCODE zstd
	,baseqty NUMERIC(18,0)  		--//  ENCODE az64
	,mrp NUMERIC(18,2)  		--//  ENCODE az64
	,listprice NUMERIC(18,2)  		--//  ENCODE az64
	,productgrossamt NUMERIC(38,2)  		--//  ENCODE az64
	,producttaxamt NUMERIC(38,2)  		--//  ENCODE az64
	,productnetamt NUMERIC(38,2)  		--//  ENCODE az64
	,downloadstatus VARCHAR(4)  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,serverdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mnfdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,expdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_ruralstoreorderdetail;
CREATE TABLE IF NOT EXISTS ITG_RURALSTOREORDERDETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_ruralstoreorderdetail
(
	orderid VARCHAR(100)  		--//  ENCODE lzo
	,productid VARCHAR(100)  		--//  ENCODE lzo
	,uomid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,qty numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,price NUMERIC(18,2)  		--//  ENCODE lzo
	,netprice NUMERIC(18,2)  		--//  ENCODE lzo
	,discountvalue numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,foc numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,tax NUMERIC(4,2)  		--//  ENCODE lzo
	,status CHAR(4)  		--//  ENCODE lzo
	,flag CHAR(1)  		--//  ENCODE lzo
	,usercode VARCHAR(50)  		--//  ENCODE lzo
	,ordd_distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,uom VARCHAR(50)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_ruralstoreorderheader;
CREATE TABLE IF NOT EXISTS ITG_RURALSTOREORDERHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_ruralstoreorderheader
(
	orderid VARCHAR(50)  		--//  ENCODE lzo
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,deliverydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,ovid numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,usercode VARCHAR(100)  		--//  ENCODE lzo
	,ordervalue NUMERIC(18,2)  		--//  ENCODE lzo
	,linespercall numeric(18,0)		--//  ENCODE delta // INTEGER  
	,feedback VARCHAR(500)  		--//  ENCODE lzo
	,orderstarttime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,orderendtime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,islocked BOOLEAN
	,flag CHAR(2)  		--//  ENCODE lzo
	,saletype CHAR(2)  		--//  ENCODE lzo
	,retailerid VARCHAR(100)  		--//  ENCODE lzo
	,invoicestatus CHAR(2)  		--//  ENCODE lzo
	,billdiscount NUMERIC(18,2)  		--//  ENCODE lzo
	,tax NUMERIC(18,2)  		--//  ENCODE lzo
	,isjointcall BOOLEAN
	,ord_distributorcode VARCHAR(100)  		--//  ENCODE lzo
	,weekid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,rsd_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE KEY
cluster by (orderid)
;
--DROP TABLE itg_rx_cx_pre_target_data;
CREATE TABLE IF NOT EXISTS ITG_RX_CX_PRE_TARGET_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rx_cx_pre_target_data
(
	urc NUMERIC(30,0)  		--//  ENCODE az64
	,rx_product VARCHAR(200)  		--//  ENCODE lzo
	,rx_units NUMERIC(38,2)  		--//  ENCODE az64
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,presc_mnth_cnt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sales_actv_mnth_cnt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,urc_name VARCHAR(150)  		--//  ENCODE lzo
	,region_sales VARCHAR(50)  		--//  ENCODE lzo
	,territory_sales VARCHAR(50)  		--//  ENCODE lzo
	,zone_sales VARCHAR(50)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rx_cx_prod_reg_std_cutoffs;
CREATE TABLE IF NOT EXISTS ITG_RX_CX_PROD_REG_STD_CUTOFFS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rx_cx_prod_reg_std_cutoffs
(
	rx_product VARCHAR(200)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_cohort VARCHAR(20)  		--//  ENCODE lzo
	,lower_cut NUMERIC(38,2)  		--//  ENCODE az64
	,upper_cut NUMERIC(38,2)  		--//  ENCODE az64
	,sales_percentile_25 NUMERIC(38,2)  		--//  ENCODE az64
	,outlet_count numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,created_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE itg_rx_cx_target_data;
CREATE TABLE IF NOT EXISTS ITG_RX_CX_TARGET_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_rx_cx_target_data
(
	urc VARCHAR(50)  		--//  ENCODE lzo
	,product_vent VARCHAR(25)  		--//  ENCODE lzo
	,year NUMERIC(18,0)  		--//  ENCODE az64
	,quarter VARCHAR(2)  		--//  ENCODE lzo
	,lysq_ach_nr NUMERIC(10,2)  		--//  ENCODE az64
	,lysq_qty NUMERIC(18,0)  		--//  ENCODE az64
	,lysq_presc NUMERIC(10,2)  		--//  ENCODE az64
	,target_qty NUMERIC(18,0)  		--//  ENCODE az64
	,target_presc NUMERIC(10,2)  		--//  ENCODE az64
	,case NUMERIC(18,0)  		--//  ENCODE az64
	,prescription_action VARCHAR(100)  		--//  ENCODE lzo
	,sales_action VARCHAR(100)  		--//  ENCODE lzo
	,hcp VARCHAR(2000)  		--//  ENCODE lzo
	,prescriptions_needed NUMERIC(10,2)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_salesinvoiceorders;
CREATE TABLE IF NOT EXISTS ITG_SALESINVOICEORDERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesinvoiceorders
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,orderno VARCHAR(50)
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	orderno
		--// 	)
;		--// ;
--DROP TABLE itg_salesman_target;
CREATE TABLE IF NOT EXISTS ITG_SALESMAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesman_target
(
	ctry_cd VARCHAR(2)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(3)  		--//  ENCODE zstd
	,fisc_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,fisc_mnth VARCHAR(2)  		--//  ENCODE zstd
	,month_nm VARCHAR(20)  		--//  ENCODE zstd
	,dist_code VARCHAR(50)  		--//  ENCODE zstd
	,sm_code VARCHAR(50)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,brand_focus VARCHAR(50)  		--//  ENCODE zstd
	,measure_type VARCHAR(50)  		--//  ENCODE zstd
	,sm_tgt_amt NUMERIC(38,6)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_salesmanmaster;
CREATE TABLE IF NOT EXISTS ITG_SALESMANMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesmanmaster
(
	distcode VARCHAR(100)  		--//  ENCODE lzo
	,smid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,smcode VARCHAR(100)  		--//  ENCODE lzo
	,smname VARCHAR(100)  		--//  ENCODE lzo
	,smphoneno VARCHAR(100)  		--//  ENCODE lzo
	,smemail VARCHAR(100)  		--//  ENCODE lzo
	,smotherdetails VARCHAR(500)  		--//  ENCODE lzo
	,smdailyallowance NUMERIC(38,6)  		--//  ENCODE az64
	,smmonthlysalary NUMERIC(38,6)  		--//  ENCODE az64
	,smmktcredit NUMERIC(38,6)  		--//  ENCODE az64
	,smcreditdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status VARCHAR(20)  		--//  ENCODE lzo
	,rmid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rmcode VARCHAR(100)  		--//  ENCODE lzo
	,rmname VARCHAR(100)  		--//  ENCODE lzo
	,uploadflag VARCHAR(1)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,rdssmtype VARCHAR(100)  		--//  ENCODE lzo
	,uniquesalescode VARCHAR(15)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_rec_dt DATE  		--//  ENCODE az64
)

;
--DROP TABLE itg_salesperson_mothersku_tmp;
CREATE TABLE IF NOT EXISTS ITG_SALESPERSON_MOTHERSKU_TMP		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesperson_mothersku_tmp
(
	cmpcode VARCHAR(50)  		--//  ENCODE lzo
	,distrcode VARCHAR(50)  		--//  ENCODE lzo
	,distrbrcode VARCHAR(50)  		--//  ENCODE lzo
	,salesmancode VARCHAR(250)  		--//  ENCODE lzo
	,skuline VARCHAR(250)  		--//  ENCODE lzo
	,skuhierlevelcode VARCHAR(250)  		--//  ENCODE lzo
	,skuhiervaluecode VARCHAR(250)  		--//  ENCODE lzo
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,motherskucd VARCHAR(250)  		--//  ENCODE lzo
)

;
--DROP TABLE itg_salesreturn;
CREATE TABLE IF NOT EXISTS ITG_SALESRETURN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesreturn
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,srnrefno VARCHAR(50)  		--//  ENCODE zstd
	,srnreftype VARCHAR(100)  		--//  ENCODE zstd
	,srndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,srnmode VARCHAR(100)  		--//  ENCODE zstd
	,srntype VARCHAR(100)  		--//  ENCODE zstd
	,srngrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srncashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srndbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srntaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnroundoffamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,salesmanname VARCHAR(100)  		--//  ENCODE zstd
	,salesroutename VARCHAR(100)  		--//  ENCODE zstd
	,rtrid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,prdsalinvno VARCHAR(50)  		--//  ENCODE zstd
	,prdlcnid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdlcncode VARCHAR(100)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdsalqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdunsalqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdofferqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdselrate NUMERIC(38,6)  		--//  ENCODE az64
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdcashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,uploadflag VARCHAR(10)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,migrationflag VARCHAR(1)  		--//  ENCODE zstd
	,mrp NUMERIC(18,6)  		--//  ENCODE az64
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,rtnfreeqtyvalue NUMERIC(38,6)  		--//  ENCODE az64
	,rtnlinecount NUMERIC(18,0)  		--//  ENCODE az64
	,referencetype VARCHAR(100)  		--//  ENCODE zstd
	,salesmancode VARCHAR(200)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(200)  		--//  ENCODE zstd
	,nrvalue NUMERIC(18,6)  		--//  ENCODE az64
	,prdselrateaftertax NUMERIC(18,6)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	modifieddate
		--// 	)
;		--// ;
--DROP TABLE itg_salesreturn_dbrestore;
CREATE TABLE IF NOT EXISTS ITG_SALESRETURN_DBRESTORE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_salesreturn_dbrestore
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,srnrefno VARCHAR(50)  		--//  ENCODE zstd
	,srnreftype VARCHAR(200)  		--//  ENCODE zstd
	,srndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,srnmode VARCHAR(50)  		--//  ENCODE zstd
	,srntype VARCHAR(50)  		--//  ENCODE zstd
	,srngrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srncashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srndbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,srntaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnroundoffamt NUMERIC(38,6)  		--//  ENCODE az64
	,srnnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,salesmanname VARCHAR(100)  		--//  ENCODE zstd
	,salesroutename VARCHAR(100)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,prdsalinvno VARCHAR(50)  		--//  ENCODE zstd
	,prdlcncode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdsalqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdunsalqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdofferqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdselrate NUMERIC(38,6)  		--//  ENCODE az64
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdspldiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdcashdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdnetamt NUMERIC(38,6)  		--//  ENCODE az64
	,mrp NUMERIC(18,6)  		--//  ENCODE az64
	,rtnfreeqtyvalue NUMERIC(38,6)  		--//  ENCODE az64
	,referencetype VARCHAR(100)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(50)  		--//  ENCODE zstd
	,nrvalue NUMERIC(18,6)  		--//  ENCODE az64
	,prdselrateaftertax NUMERIC(18,6)  		--//  ENCODE az64
	,mrpcs NUMERIC(18,6)  		--//  ENCODE az64
	,lpvalue NUMERIC(18,6)  		--//  ENCODE az64
	,rtnwindowdisplayamt NUMERIC(38,6)  		--//  ENCODE az64
	,cradjamt NUMERIC(38,6)  		--//  ENCODE az64
	,rtrurccode VARCHAR(50)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,6)  		--//  ENCODE az64
	,rtnlinecount NUMERIC(18,6)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE az64 //  ENCODE az64 // character varying
	,file_name VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_scheme_header;
CREATE TABLE IF NOT EXISTS ITG_SCHEME_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_scheme_header
(
	schid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schcode VARCHAR(20)  		--//  ENCODE zstd
	,schdsc VARCHAR(100)  		--//  ENCODE zstd
	,claimable SMALLINT  		--//  ENCODE az64
	,clmamton SMALLINT  		--//  ENCODE az64
	,cmpschcode VARCHAR(20)  		--//  ENCODE zstd
	,schlevel_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schtype VARCHAR(10)  		--//  ENCODE zstd
	,flexisch SMALLINT  		--//  ENCODE az64
	,flexischtype SMALLINT  		--//  ENCODE az64
	,combisch SMALLINT  		--//  ENCODE az64
	,range SMALLINT  		--//  ENCODE az64
	,prorata SMALLINT  		--//  ENCODE az64
	,qps VARCHAR(10)  		--//  ENCODE zstd
	,qpsreset SMALLINT  		--//  ENCODE az64
	,schvalidfrom TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,schvalidtill TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,schstatus SMALLINT  		--//  ENCODE az64
	,budget NUMERIC(18,2)  		--//  ENCODE az64
	,adjwindisponlyonce SMALLINT  		--//  ENCODE az64
	,purofevery SMALLINT  		--//  ENCODE az64
	,apyqpssch SMALLINT  		--//  ENCODE az64
	,setwindowdisp SMALLINT  		--//  ENCODE az64
	,editscheme SMALLINT  		--//  ENCODE az64
	,schlvlmode SMALLINT  		--//  ENCODE az64
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifiedby VARCHAR(40)  		--//  ENCODE zstd
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,versionno numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,serialno VARCHAR(100)  		--//  ENCODE zstd
	,claimgrpcode VARCHAR(40)  		--//  ENCODE zstd
	,fbm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,combitype numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allowuncheck numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,settlementtype numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,consumerpromo numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,wdsbillscount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,wdscapamount NUMERIC(38,6)  		--//  ENCODE az64
	,wdsminpurqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_schemeutilization;
CREATE TABLE IF NOT EXISTS ITG_SCHEMEUTILIZATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_schemeutilization
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,schemecode VARCHAR(50)  		--//  ENCODE zstd
	,schemedescription VARCHAR(200)  		--//  ENCODE zstd
	,invoiceno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,company VARCHAR(100)  		--//  ENCODE zstd
	,schdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,schemetype VARCHAR(50)  		--//  ENCODE zstd
	,schemeutilizedamt NUMERIC(18,6)  		--//  ENCODE az64
	,schemefreeproduct VARCHAR(50)  		--//  ENCODE zstd
	,schemeutilizedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,companyschemecode VARCHAR(50)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,migrationflag VARCHAR(1)  		--//  ENCODE zstd
	,schememode VARCHAR(50)  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,schlinecount NUMERIC(18,0)  		--//  ENCODE az64
	,schvaluetype VARCHAR(100)  		--//  ENCODE zstd
	,slabid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,billedprdccode VARCHAR(100)  		--//  ENCODE zstd
	,billedprdbatcode VARCHAR(100)  		--//  ENCODE zstd
	,billedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schdiscperc NUMERIC(38,6)  		--//  ENCODE az64
	,freeprdbatcode VARCHAR(100)  		--//  ENCODE zstd
	,billedrate NUMERIC(38,6)  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,servicecrnrefno VARCHAR(100)  		--//  ENCODE zstd
	,rtrurccode VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_sku_recom_flag;
CREATE TABLE IF NOT EXISTS ITG_SKU_RECOM_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_sku_recom_flag
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,dist_outlet_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag VARCHAR(50)  		--//  ENCODE lzo
	,cs_flag VARCHAR(50)  		--//  ENCODE lzo
	,soq VARCHAR(50)  		--//  ENCODE lzo
	,unique_ret_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE itg_sku_recom_flag_msl_spike;
CREATE TABLE IF NOT EXISTS ITG_SKU_RECOM_FLAG_MSL_SPIKE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_sku_recom_flag_msl_spike
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,dist_outlet_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag VARCHAR(50)  		--//  ENCODE lzo
	,cs_flag VARCHAR(50)  		--//  ENCODE lzo
	,soq VARCHAR(50)  		--//  ENCODE lzo
	,unique_ret_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE itg_sku_recom_flag_msl_spike_mi;
CREATE TABLE IF NOT EXISTS ITG_SKU_RECOM_FLAG_MSL_SPIKE_MI		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_sku_recom_flag_msl_spike_mi
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,dist_outlet_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag VARCHAR(50)  		--//  ENCODE lzo
	,cs_flag VARCHAR(50)  		--//  ENCODE lzo
	,soq VARCHAR(50)  		--//  ENCODE lzo
	,unique_ret_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE itg_sku_recom_orangestoretarget;
CREATE TABLE IF NOT EXISTS ITG_SKU_RECOM_ORANGESTORETARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_sku_recom_orangestoretarget
(
	fromdate numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,todate numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rsmcode VARCHAR(50)  		--//  ENCODE lzo
	,rsmname VARCHAR(50)  		--//  ENCODE lzo
	,asmcode VARCHAR(50)  		--//  ENCODE lzo
	,asmname VARCHAR(50)  		--//  ENCODE lzo
	,abicode VARCHAR(50)  		--//  ENCODE lzo
	,abiname VARCHAR(50)  		--//  ENCODE lzo
	,channelcode VARCHAR(50)  		--//  ENCODE lzo
	,channelname VARCHAR(50)  		--//  ENCODE lzo
	,subchannelcode VARCHAR(50)  		--//  ENCODE lzo
	,subchannelname VARCHAR(50)  		--//  ENCODE lzo
	,class VARCHAR(50)  		--//  ENCODE lzo
	,percentage NUMERIC(38,2)  		--//  ENCODE az64
	,dateofsharing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64 //  ENCODE az64 // character varying
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE itg_sss_scorecard_data;
CREATE TABLE IF NOT EXISTS ITG_SSS_SCORECARD_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_sss_scorecard_data
(
	program_type VARCHAR(50)  		--//  ENCODE zstd
	,jnj_id VARCHAR(50)  		--//  ENCODE zstd
	,rs_id VARCHAR(50)  		--//  ENCODE zstd
	,outlet_name VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,territory VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,kpi VARCHAR(50)  		--//  ENCODE zstd
	,quarter VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,target VARCHAR(50)  		--//  ENCODE zstd
	,actual VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE itg_tbl_schemewise_apno;
CREATE TABLE IF NOT EXISTS ITG_TBL_SCHEMEWISE_APNO		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tbl_schemewise_apno
(
	schid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,apno VARCHAR(200)  		--//  ENCODE lzo
	,createduserid numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,schcategorytype1code VARCHAR(50)  		--//  ENCODE lzo
	,schcategorytype2code VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_clstkm;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_CLSTKM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_clstkm
(
	serno numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mon numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,salopenstock NUMERIC(18,3)  		--//  ENCODE az64
	,unsalopenstock NUMERIC(18,3)  		--//  ENCODE az64
	,offeropenstock NUMERIC(18,3)  		--//  ENCODE az64
	,unsalclsstock NUMERIC(18,3)  		--//  ENCODE az64
	,offerclsstock NUMERIC(18,3)  		--//  ENCODE az64
	,clstckqty NUMERIC(18,3)  		--//  ENCODE az64
	,lp NUMERIC(18,3)  		--//  ENCODE az64
	,ptr NUMERIC(18,3)  		--//  ENCODE az64
	,nr NUMERIC(18,3)  		--//  ENCODE az64
	,src VARCHAR(10)  		--//  ENCODE zstd
	,value NUMERIC(18,3)  		--//  ENCODE az64
	,lpvalue NUMERIC(18,3)  		--//  ENCODE az64
	,ptrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,iscubeprocess CHAR(2)  		--//  ENCODE zstd
	,salclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,unsalclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,offerclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	distcode
		--// 	)
;		--// ;
--DROP TABLE itg_tblpf_clstkm_wrk;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_CLSTKM_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_clstkm_wrk
(
	serno numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mon numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,salopenstock NUMERIC(18,3)  		--//  ENCODE az64
	,unsalopenstock NUMERIC(18,3)  		--//  ENCODE az64
	,offeropenstock NUMERIC(18,3)  		--//  ENCODE az64
	,unsalclsstock NUMERIC(18,3)  		--//  ENCODE az64
	,offerclsstock NUMERIC(18,3)  		--//  ENCODE az64
	,clstckqty NUMERIC(18,3)  		--//  ENCODE az64
	,lp NUMERIC(18,3)  		--//  ENCODE az64
	,ptr NUMERIC(18,3)  		--//  ENCODE az64
	,nr NUMERIC(18,3)  		--//  ENCODE az64
	,src VARCHAR(10)  		--//  ENCODE zstd
	,value NUMERIC(18,3)  		--//  ENCODE az64
	,lpvalue NUMERIC(18,3)  		--//  ENCODE az64
	,ptrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,iscubeprocess CHAR(2)  		--//  ENCODE zstd
	,salclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,unsalclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,offerclsnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_idt;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_IDT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_idt
(
	serno VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,idtmngdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,stkmgmttype VARCHAR(30)  		--//  ENCODE zstd
	,status VARCHAR(100)  		--//  ENCODE zstd
	,stocktype VARCHAR(50)  		--//  ENCODE zstd
	,baseqty NUMERIC(18,0)  		--//  ENCODE az64
	,mrp NUMERIC(18,2)  		--//  ENCODE az64
	,listprice NUMERIC(18,2)  		--//  ENCODE az64
	,nr NUMERIC(18,2)  		--//  ENCODE az64
	,ptr NUMERIC(18,2)  		--//  ENCODE az64
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,src VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_idt_wrk;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_IDT_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_idt_wrk
(
	serno VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,idtmngdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,stkmgmttype VARCHAR(30)  		--//  ENCODE zstd
	,status VARCHAR(100)  		--//  ENCODE zstd
	,stocktype VARCHAR(50)  		--//  ENCODE zstd
	,baseqty NUMERIC(18,0)  		--//  ENCODE az64
	,mrp NUMERIC(18,2)  		--//  ENCODE az64
	,listprice NUMERIC(18,2)  		--//  ENCODE az64
	,nr NUMERIC(18,2)  		--//  ENCODE az64
	,ptr NUMERIC(18,2)  		--//  ENCODE az64
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,src VARCHAR(10)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_secsalesm_wrk;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_SECSALESM_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_secsalesm_wrk
(
	serno numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mon numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,nr NUMERIC(12,4)  		--//  ENCODE az64
	,prdqty NUMERIC(16,4)  		--//  ENCODE az64
	,prdgrossamt NUMERIC(16,4)  		--//  ENCODE az64
	,ptrvalue NUMERIC(16,4)  		--//  ENCODE az64
	,src VARCHAR(5)  		--//  ENCODE zstd
	,prdnrvalue NUMERIC(18,3)  		--//  ENCODE az64
	,lpvalue NUMERIC(18,3)  		--//  ENCODE az64
	,trinqty NUMERIC(16,3)  		--//  ENCODE az64
	,troutqty NUMERIC(16,3)  		--//  ENCODE az64
	,trinval NUMERIC(16,3)  		--//  ENCODE az64
	,troutval NUMERIC(16,3)  		--//  ENCODE az64
	,nonwaveopenqty NUMERIC(16,3)  		--//  ENCODE az64
	,runmm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,runyr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,iscubeprocess CHAR(1)  		--//  ENCODE zstd
	,pricelistlp NUMERIC(16,3)  		--//  ENCODE az64
	,pricelistptr NUMERIC(16,3)  		--//  ENCODE az64
	,prdqty_new NUMERIC(16,4)  		--//  ENCODE az64
	,prdnrvalue_new NUMERIC(16,4)  		--//  ENCODE az64
	,ptrvalue_new NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_nr_prev NUMERIC(12,4)  		--//  ENCODE az64
	,dbrestore_prdqty_prev NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_prdgrossamt_prev NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_prdnrvalue_prev NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_ptrvalue_prev NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_nr_current NUMERIC(12,4)  		--//  ENCODE az64
	,dbrestore_prdqty_current NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_prdgrossamt_current NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_prdnrvalue_current NUMERIC(16,4)  		--//  ENCODE az64
	,dbrestore_ptrvalue_current NUMERIC(16,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_sit;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_SIT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_sit
(
	serno VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,subdcode VARCHAR(50)  		--//  ENCODE zstd
	,sitqty NUMERIC(20,4)  		--//  ENCODE az64
	,sitvalue NUMERIC(20,4)  		--//  ENCODE az64
	,src VARCHAR(10)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_tblpf_sit_wrk;
CREATE TABLE IF NOT EXISTS ITG_TBLPF_SIT_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_tblpf_sit_wrk
(
	serno VARCHAR(50)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,subdcode VARCHAR(50)  		--//  ENCODE zstd
	,sitqty NUMERIC(20,4)  		--//  ENCODE az64
	,sitvalue NUMERIC(20,4)  		--//  ENCODE az64
	,src VARCHAR(10)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_territory;
CREATE TABLE IF NOT EXISTS ITG_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_territory
(
	territorycode NUMERIC(18,0)  		--//  ENCODE lzo
	,territoryname VARCHAR(50)  		--//  ENCODE lzo
	,zonecode NUMERIC(18,0)  		--//  ENCODE lzo
	,sapid VARCHAR(50)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE lzo
	,isdeleted CHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,uppt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE itg_townmaster;
CREATE TABLE IF NOT EXISTS ITG_TOWNMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_townmaster
(
	routecode VARCHAR(50)
	,villagecode VARCHAR(50)
	,villagename VARCHAR(200)  		--//  ENCODE lzo
	,population numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rsrcode VARCHAR(50)
	,rsdcode VARCHAR(50)
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,sarpanchname VARCHAR(50)  		--//  ENCODE lzo
	,sarpanchno VARCHAR(50)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdby VARCHAR(50)  		--//  ENCODE lzo
	,updateddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updatedby VARCHAR(50)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE KEY
cluster by (villagecode)
		--// SORTKEY ( 
		--// 	villagecode
		--// 	, routecode
		--// 	, rsdcode
		--// 	, rsrcode
		--// 	)
;		--// ;
--DROP TABLE itg_udcdetails;
CREATE TABLE IF NOT EXISTS ITG_UDCDETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_udcdetails
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,masterid numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,mastername VARCHAR(200)  		--//  ENCODE zstd
	,mastervaluecode VARCHAR(200)
	,mastervaluename VARCHAR(200)  		--//  ENCODE zstd
	,columnname VARCHAR(100)  		--//  ENCODE zstd
	,columnvalue VARCHAR(100)  		--//  ENCODE zstd
	,uploadflag VARCHAR(1)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE delta32k
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	mastervaluecode
		--// 	)
;		--// ;
--DROP TABLE itg_udcmapping;
CREATE TABLE IF NOT EXISTS ITG_UDCMAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_udcmapping
(
	distcode VARCHAR(100)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,outletcode VARCHAR(50)  		--//  ENCODE lzo
	,usercode VARCHAR(100)  		--//  ENCODE lzo
	,udccode VARCHAR(50)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive BOOLEAN
	,isdelflag CHAR(1)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_udcmaster;
CREATE TABLE IF NOT EXISTS ITG_UDCMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_udcmaster
(
	udcmasterid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,masterid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mastername VARCHAR(100)  		--//  ENCODE zstd
	,columnname VARCHAR(100)  		--//  ENCODE zstd
	,columndatatype VARCHAR(50)  		--//  ENCODE zstd
	,columnsize numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,columnprecision numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,editable numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,columnmandatory numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pickfromdefault numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,downloadstatus VARCHAR(5)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieduserid VARCHAR(50)  		--//  ENCODE zstd
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,udcstatus numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE itg_ventasys_jnj_prod_mapping;
CREATE TABLE IF NOT EXISTS ITG_VENTASYS_JNJ_PROD_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_ventasys_jnj_prod_mapping
(
	prod_name VARCHAR(200)  		--//  ENCODE lzo
	,prod_vent VARCHAR(200)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,upd_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE itg_winculum_dailysales;
CREATE TABLE IF NOT EXISTS ITG_WINCULUM_DAILYSALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_winculum_dailysales
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,total_price NUMERIC(18,6)  		--//  ENCODE az64
	,tax NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_winculum_salesreturn;
CREATE TABLE IF NOT EXISTS ITG_WINCULUM_SALESRETURN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_winculum_salesreturn
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,srndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,srnrefno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,total_price NUMERIC(18,6)  		--//  ENCODE az64
	,tax NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE itg_xdm_batchmaster;
CREATE TABLE IF NOT EXISTS ITG_XDM_BATCHMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_batchmaster
(
	statecode VARCHAR(10)  		--//  ENCODE zstd
	,prodcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(18,6)  		--//  ENCODE az64
	,lsp NUMERIC(18,6)  		--//  ENCODE az64
	,sellrate NUMERIC(18,6)  		--//  ENCODE az64
	,purchrate NUMERIC(18,6)  		--//  ENCODE az64
	,claimrate NUMERIC(18,6)  		--//  ENCODE az64
	,netrate NUMERIC(18,6)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_businesscalendar;
CREATE TABLE IF NOT EXISTS ITG_XDM_BUSINESSCALENDAR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_businesscalendar
(
	salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week VARCHAR(50)  		--//  ENCODE zstd
	,monthkey numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_channelmaster;
CREATE TABLE IF NOT EXISTS ITG_XDM_CHANNELMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_channelmaster
(
	cmpcode VARCHAR(50)  		--//  ENCODE zstd
	,channelcode VARCHAR(100)  		--//  ENCODE zstd
	,channelname VARCHAR(100)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,clicktype VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_distributor;
CREATE TABLE IF NOT EXISTS ITG_XDM_DISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_distributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,usercode VARCHAR(50)  		--//  ENCODE zstd
	,distrname VARCHAR(150)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,distrbrtype VARCHAR(2)  		--//  ENCODE zstd
	,distrbrname VARCHAR(100)  		--//  ENCODE zstd
	,distrbraddr1 VARCHAR(200)  		--//  ENCODE zstd
	,distrbraddr2 VARCHAR(200)  		--//  ENCODE zstd
	,distrbraddr3 VARCHAR(200)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,distrbrstate VARCHAR(50)  		--//  ENCODE zstd
	,postalcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country VARCHAR(50)  		--//  ENCODE zstd
	,contactperson VARCHAR(100)  		--//  ENCODE zstd
	,phone VARCHAR(15)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,isactive VARCHAR(1)  		--//  ENCODE zstd
	,gstinnumber VARCHAR(15)  		--//  ENCODE zstd
	,gstdistrtype VARCHAR(1)  		--//  ENCODE zstd
	,gststatecode VARCHAR(3)  		--//  ENCODE zstd
	,others1 VARCHAR(15)  		--//  ENCODE zstd
	,isdirectacct VARCHAR(1)  		--//  ENCODE zstd
	,typecode VARCHAR(50)  		--//  ENCODE zstd
	,psnonps VARCHAR(1)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_distributor_supplier;
CREATE TABLE IF NOT EXISTS ITG_XDM_DISTRIBUTOR_SUPPLIER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_distributor_supplier
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,supcode VARCHAR(30)  		--//  ENCODE zstd
	,isdefault CHAR(1)  DEFAULT 'Y'::char		--//  ENCODE zstd // bpchar
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_geohierarchy;
CREATE TABLE IF NOT EXISTS ITG_XDM_GEOHIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_geohierarchy
(
	statecode VARCHAR(50)  		--//  ENCODE zstd
	,statename VARCHAR(100)  		--//  ENCODE zstd
	,districtcode VARCHAR(50)  		--//  ENCODE zstd
	,districtname VARCHAR(100)  		--//  ENCODE zstd
	,thesilcode VARCHAR(50)  		--//  ENCODE zstd
	,thesilname VARCHAR(100)  		--//  ENCODE zstd
	,citycode VARCHAR(50)  		--//  ENCODE zstd
	,cityname VARCHAR(100)  		--//  ENCODE zstd
	,distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorname VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_product;
CREATE TABLE IF NOT EXISTS ITG_XDM_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_product
(
	prodcompany VARCHAR(10)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,productuom VARCHAR(5)  		--//  ENCODE zstd
	,uomconvfactor numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prodhierarchylvl VARCHAR(50)  		--//  ENCODE zstd
	,prodhierarchyval VARCHAR(50)  		--//  ENCODE zstd
	,productname VARCHAR(400)  		--//  ENCODE zstd
	,prodshortname VARCHAR(400)  		--//  ENCODE zstd
	,productcmpcode VARCHAR(50)  		--//  ENCODE zstd
	,stockcovdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,productweight NUMERIC(9,3)  		--//  ENCODE az64
	,productunit VARCHAR(5)  		--//  ENCODE zstd
	,productstatus VARCHAR(1)  		--//  ENCODE zstd
	,productdrugtype VARCHAR(1)  		--//  ENCODE zstd
	,serialno VARCHAR(1)  		--//  ENCODE zstd
	,shelflife numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchisecode VARCHAR(50)  		--//  ENCODE zstd
	,franchisename VARCHAR(100)  		--//  ENCODE zstd
	,brandcode VARCHAR(50)  		--//  ENCODE zstd
	,brandname VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,variantcode VARCHAR(50)  		--//  ENCODE zstd
	,variantname VARCHAR(100)  		--//  ENCODE zstd
	,motherskucode VARCHAR(50)  		--//  ENCODE zstd
	,motherskuname VARCHAR(100)  		--//  ENCODE zstd
	,eanno VARCHAR(50)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_productuom;
CREATE TABLE IF NOT EXISTS ITG_XDM_PRODUCTUOM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_productuom
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,prodcode VARCHAR(50)  		--//  ENCODE zstd
	,uomcode VARCHAR(5)  		--//  ENCODE zstd
	,uomconvfactor numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_salesheirarchy;
CREATE TABLE IF NOT EXISTS ITG_XDM_SALESHEIRARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_salesheirarchy
(
	rsmcode VARCHAR(50)  		--//  ENCODE zstd
	,rsmname VARCHAR(100)  		--//  ENCODE zstd
	,rsm_flmasmcode VARCHAR(50)  		--//  ENCODE zstd
	,flmasmcode VARCHAR(50)  		--//  ENCODE zstd
	,flmasmname VARCHAR(100)  		--//  ENCODE zstd
	,flmasm_abicode VARCHAR(50)  		--//  ENCODE zstd
	,abicode VARCHAR(50)  		--//  ENCODE zstd
	,abiname VARCHAR(100)  		--//  ENCODE zstd
	,abi_distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorname VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_salesmanskulinemapping;
CREATE TABLE IF NOT EXISTS ITG_XDM_SALESMANSKULINEMAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_salesmanskulinemapping
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(50)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  DEFAULT ''::varchar		--//  ENCODE zstd // character varying
	,skuline VARCHAR(50)  		--//  ENCODE zstd
	,skuhierlevelcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,skuhiervaluecode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,motherskucode VARCHAR(50)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_xdm_supplier;
CREATE TABLE IF NOT EXISTS ITG_XDM_SUPPLIER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_xdm_supplier
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,supcode VARCHAR(30)  		--//  ENCODE zstd
	,suptype VARCHAR(1)  		--//  ENCODE zstd
	,supname VARCHAR(100)  		--//  ENCODE zstd
	,supaddr1 VARCHAR(50)  		--//  ENCODE zstd
	,supaddr2 VARCHAR(50)  		--//  ENCODE zstd
	,supaddr3 VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,state VARCHAR(50)  		--//  ENCODE zstd
	,country VARCHAR(50)  		--//  ENCODE zstd
	,gststatecode VARCHAR(30)  		--//  ENCODE zstd
	,suppliergstin VARCHAR(15)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)

;
--DROP TABLE itg_zone;
CREATE TABLE IF NOT EXISTS ITG_ZONE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS itg_zone
(
	zonecode NUMERIC(18,0)  		--//  ENCODE lzo
	,zonename VARCHAR(50)  		--//  ENCODE lzo
	,regioncode NUMERIC(18,0)  		--//  ENCODE lzo
	,sapid VARCHAR(50)  		--//  ENCODE lzo
	,isvisible CHAR(1)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE lzo
	,deletionmark VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,uppt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
;
--DROP TABLE tblpf_prisalesm;
CREATE TABLE IF NOT EXISTS TBLPF_PRISALESM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS tblpf_prisalesm
(
	serno numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mon numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,billtype VARCHAR(50)  		--//  ENCODE zstd
	,qty NUMERIC(16,4)  		--//  ENCODE az64
	,value NUMERIC(16,4)  		--//  ENCODE az64
	,nr NUMERIC(12,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE tblpf_prisalesm_wrk;
CREATE TABLE IF NOT EXISTS TBLPF_PRISALESM_WRK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS tblpf_prisalesm_wrk
(
	serno numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,mon numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,billtype VARCHAR(50)  		--//  ENCODE zstd
	,qty NUMERIC(16,4)  		--//  ENCODE az64
	,value NUMERIC(16,4)  		--//  ENCODE az64
	,nr NUMERIC(12,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;



USE SCHEMA INDWKS_INTEGRATION;

--DROP TABLE day_cls_stock_fact;
CREATE TABLE IF NOT EXISTS DAY_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS day_cls_stock_fact
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,transdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lcnid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,lcncode VARCHAR(100)  		--//  ENCODE lzo
	,prdid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,prdcode VARCHAR(100)  		--//  ENCODE lzo
	,salopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,offeropenstock NUMERIC(18,0)  		--//  ENCODE delta
	,salpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,salpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salsales NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsales NUMERIC(18,0)  		--//  ENCODE delta
	,offersales NUMERIC(18,0)  		--//  ENCODE delta
	,salstockin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockin NUMERIC(18,0)  		--//  ENCODE delta
	,salstockout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockout NUMERIC(18,0)  		--//  ENCODE delta
	,damagein NUMERIC(18,0)  		--//  ENCODE delta
	,damageout NUMERIC(18,0)  		--//  ENCODE delta
	,salsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offersalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,salreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,offerreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,salclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,offerclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,uploaddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,uploadflag VARCHAR(10)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,syncid NUMERIC(38,0)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,nr DOUBLE PRECISION
)
		--// DISTSTYLE EVEN
;
--DROP TABLE gtm_churn_packs_tbl;
CREATE TABLE IF NOT EXISTS GTM_CHURN_PACKS_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_churn_packs_tbl
(
	fisc_per NUMERIC(18,0)  		--//  ENCODE az64
	,fisc_year NUMERIC(20,0)  		--//  ENCODE az64
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,pack_status VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE gtm_dataset2_base_tbl;
CREATE TABLE IF NOT EXISTS GTM_DATASET2_BASE_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_dataset2_base_tbl
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_year VARCHAR(11)  		--//  ENCODE lzo
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
)

;
--DROP TABLE gtm_dataset2_ch_packs_tbl;
CREATE TABLE IF NOT EXISTS GTM_DATASET2_CH_PACKS_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_dataset2_ch_packs_tbl
(
	fisc_per NUMERIC(18,0)  		--//  ENCODE az64
	,fisc_year NUMERIC(20,0)  		--//  ENCODE az64
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,pack_status VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE gtm_dataset2_club_tbl;
CREATE TABLE IF NOT EXISTS GTM_DATASET2_CLUB_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_dataset2_club_tbl
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_year VARCHAR(11)  		--//  ENCODE lzo
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE gtm_dataset2_ex_new_packs_tbl;
CREATE TABLE IF NOT EXISTS GTM_DATASET2_EX_NEW_PACKS_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_dataset2_ex_new_packs_tbl
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_year VARCHAR(11)  		--//  ENCODE lzo
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,pack_status VARCHAR(13)  		--//  ENCODE lzo
)

;
--DROP TABLE gtm_existing_packs_tbl;
CREATE TABLE IF NOT EXISTS GTM_EXISTING_PACKS_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_existing_packs_tbl
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_year VARCHAR(11)  		--//  ENCODE lzo
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,pack_status VARCHAR(13)  		--//  ENCODE lzo
)

;
--DROP TABLE gtm_new_packs_tbl;
CREATE TABLE IF NOT EXISTS GTM_NEW_PACKS_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_new_packs_tbl
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_year VARCHAR(11)  		--//  ENCODE lzo
	,fisc_month VARCHAR(11)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,pack_status VARCHAR(8)  		--//  ENCODE lzo
)

;
--DROP TABLE in_gtm_dist_sales;
CREATE TABLE IF NOT EXISTS IN_GTM_DIST_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS in_gtm_dist_sales
(
	fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE lzo
	,num_buying_retailers VARCHAR(100)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(15)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,num_bills VARCHAR(151)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,gtm_year NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_month NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_date DATE  		--//  ENCODE az64
	,ret_date DATE  		--//  ENCODE az64
	,customer_gtm_flag VARCHAR(3)  		--//  ENCODE lzo
)

;
--DROP TABLE in_gtm_pre_rpt_tbl;
CREATE TABLE IF NOT EXISTS IN_GTM_PRE_RPT_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS in_gtm_pre_rpt_tbl
(
	fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE lzo
	,num_buying_retailers VARCHAR(100)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(15)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,num_bills VARCHAR(151)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,gtm_year NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_month NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_date DATE  		--//  ENCODE az64
	,ret_date DATE  		--//  ENCODE az64
	,customer_gtm_flag VARCHAR(3)  		--//  ENCODE lzo
	,pre_post_flag VARCHAR(8)  		--//  ENCODE lzo
	,retailer_gtm_flag VARCHAR(150)  		--//  ENCODE lzo
	,retailer_tag VARCHAR(9)  		--//  ENCODE lzo
)

;
--DROP TABLE in_gtm_ret_gtm_flag;
CREATE TABLE IF NOT EXISTS IN_GTM_RET_GTM_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS in_gtm_ret_gtm_flag
(
	fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,retailer_class VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE lzo
	,num_buying_retailers VARCHAR(100)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(15)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,num_bills VARCHAR(151)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,gtm_year NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_month NUMERIC(31,0)  		--//  ENCODE az64
	,gtm_date DATE  		--//  ENCODE az64
	,ret_date DATE  		--//  ENCODE az64
	,customer_gtm_flag VARCHAR(3)  		--//  ENCODE lzo
	,pre_post_flag VARCHAR(8)  		--//  ENCODE lzo
	,retailer_gtm_flag VARCHAR(150)  		--//  ENCODE lzo
)

;
--DROP TABLE in_gtm_ret_sales_agg;
CREATE TABLE IF NOT EXISTS IN_GTM_RET_SALES_AGG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS in_gtm_ret_sales_agg
(
	fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE in_gtm_ret_tag;
CREATE TABLE IF NOT EXISTS IN_GTM_RET_TAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS in_gtm_ret_tag
(
	fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,cumulative_nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,total_ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,perc_of_total NUMERIC(38,4)  		--//  ENCODE az64
	,retailer_tag VARCHAR(9)  		--//  ENCODE lzo
)

;
--DROP TABLE msl_extract_combine_program_type;
CREATE TABLE IF NOT EXISTS MSL_EXTRACT_COMBINE_PROGRAM_TYPE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS msl_extract_combine_program_type
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(251)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(12)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(150)  		--//  ENCODE lzo
	,target VARCHAR(1)  		--//  ENCODE lzo
	,actual VARCHAR(1)  		--//  ENCODE lzo
	,program_type VARCHAR(50)  		--//  ENCODE lzo
	,latest_outlet_name VARCHAR(150)  		--//  ENCODE lzo
)

;
--DROP TABLE msl_franchise_achievement_score_calc;
CREATE TABLE IF NOT EXISTS MSL_FRANCHISE_ACHIEVEMENT_SCORE_CALC		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS msl_franchise_achievement_score_calc
(
	country VARCHAR(7)  		--//  ENCODE lzo
	,region_name VARCHAR(75)  		--//  ENCODE lzo
	,zone_name VARCHAR(75)  		--//  ENCODE lzo
	,territory_name VARCHAR(75)  		--//  ENCODE lzo
	,channel_name VARCHAR(225)  		--//  ENCODE lzo
	,retail_environment VARCHAR(75)  		--//  ENCODE lzo
	,salesman_code VARCHAR(150)  		--//  ENCODE lzo
	,salesman_name VARCHAR(300)  		--//  ENCODE lzo
	,distributor_code VARCHAR(75)  		--//  ENCODE lzo
	,distributor_name VARCHAR(225)  		--//  ENCODE lzo
	,qtr VARCHAR(16)  		--//  ENCODE lzo
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(150)  		--//  ENCODE lzo
	,store_name VARCHAR(376)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(18)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(75)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(225)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(225)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(225)  		--//  ENCODE lzo
	,target VARCHAR(1)  		--//  ENCODE lzo
	,actual VARCHAR(1)  		--//  ENCODE lzo
	,program_type VARCHAR(75)  		--//  ENCODE lzo
	,latest_outlet_name VARCHAR(225)  		--//  ENCODE lzo
	,actual_l3 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,target_l3 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,franchise_msl_achievement NUMERIC(31,16)  		--//  ENCODE az64
	,franchise_score NUMERIC(31,3)  		--//  ENCODE az64
)

;
--DROP TABLE pf_weekly_os_perc_actual_logic_sm;
CREATE TABLE IF NOT EXISTS PF_WEEKLY_OS_PERC_ACTUAL_LOGIC_SM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS pf_weekly_os_perc_actual_logic_sm
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,os_week0 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_week1 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_week2 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_week3 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_week4 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_week5 numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,total_stores numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,os_perc_week0 NUMERIC(38,22)  		--//  ENCODE az64
	,os_perc_week1 NUMERIC(38,22)  		--//  ENCODE az64
	,os_perc_week2 NUMERIC(38,22)  		--//  ENCODE az64
	,os_perc_week3 NUMERIC(38,22)  		--//  ENCODE az64
	,os_perc_week4 NUMERIC(38,22)  		--//  ENCODE az64
	,os_perc_week5 NUMERIC(38,22)  		--//  ENCODE az64
)

;
--DROP TABLE sales_actual_achnr_qty;
CREATE TABLE IF NOT EXISTS SALES_ACTUAL_ACHNR_QTY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sales_actual_achnr_qty
(
	cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prod_vent VARCHAR(200)  		--//  ENCODE lzo
	,urc VARCHAR(100)  		--//  ENCODE lzo
	,actual_ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,actual_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE sss_scorecard_kpi_compliance_calc;
CREATE TABLE IF NOT EXISTS SSS_SCORECARD_KPI_COMPLIANCE_CALC		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_scorecard_kpi_compliance_calc
(
	program_type VARCHAR(50)  		--//  ENCODE zstd
	,jnj_id VARCHAR(50)  		--//  ENCODE zstd
	,outlet_name VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,territory VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,kpi VARCHAR(50)  		--//  ENCODE zstd
	,quarter VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,source_target VARCHAR(50)  		--//  ENCODE zstd
	,source_actual VARCHAR(50)  		--//  ENCODE zstd
	,target VARCHAR(50)  		--//  ENCODE zstd
	,actual VARCHAR(50)  		--//  ENCODE zstd
	,compliance NUMERIC(30,24)  		--//  ENCODE zstd
)

;
--DROP TABLE sss_scorecard_kpi_endcap_addn_visblty_score_calc;
CREATE TABLE IF NOT EXISTS SSS_SCORECARD_KPI_ENDCAP_ADDN_VISBLTY_SCORE_CALC		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_scorecard_kpi_endcap_addn_visblty_score_calc
(
	program_type VARCHAR(50)  		--//  ENCODE lzo
	,jnj_id VARCHAR(50)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,region VARCHAR(50)  		--//  ENCODE lzo
	,zone VARCHAR(50)  		--//  ENCODE lzo
	,territory VARCHAR(50)  		--//  ENCODE lzo
	,city VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,target VARCHAR(50)  		--//  ENCODE lzo
	,actual VARCHAR(50)  		--//  ENCODE lzo
	,score_value NUMERIC(38,3)  		--//  ENCODE az64
)

;
--DROP TABLE sss_scorecard_kpi_plano_score_calc;
CREATE TABLE IF NOT EXISTS SSS_SCORECARD_KPI_PLANO_SCORE_CALC		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_scorecard_kpi_plano_score_calc
(
	program_type VARCHAR(50)  		--//  ENCODE lzo
	,jnj_id VARCHAR(50)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,region VARCHAR(50)  		--//  ENCODE lzo
	,zone VARCHAR(50)  		--//  ENCODE lzo
	,territory VARCHAR(50)  		--//  ENCODE lzo
	,city VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,target VARCHAR(50)  		--//  ENCODE lzo
	,actual VARCHAR(50)  		--//  ENCODE lzo
	,score_value NUMERIC(38,3)  		--//  ENCODE az64
)

;
--DROP TABLE sss_scorecard_kpi_promo_pos;
CREATE TABLE IF NOT EXISTS SSS_SCORECARD_KPI_PROMO_POS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_scorecard_kpi_promo_pos
(
	program_type VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,jnj_id VARCHAR(50)  		--//  ENCODE lzo
	,outlet_name VARCHAR(100)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,neu numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,deno numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE sss_scorecard_msl_extract;
CREATE TABLE IF NOT EXISTS SSS_SCORECARD_MSL_EXTRACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_scorecard_msl_extract
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(251)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(12)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(150)  		--//  ENCODE lzo
	,msl_flag VARCHAR(1)  		--//  ENCODE lzo
	,msl_hit VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE sss_zonal_base_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_BASE_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_base_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
)

;
--DROP TABLE sss_zonal_final_tmp_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_FINAL_TMP_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_final_tmp_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,prev_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,nocb_growth_percent NUMERIC(34,22)  		--//  ENCODE az64
	,cur_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,achievement_nr_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,tot_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,signature_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,premium_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,nonprog_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_nocb_achnr_growth_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_NOCB_ACHNR_GROWTH_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_nocb_achnr_growth_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,prev_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,nocb_growth_percent NUMERIC(34,22)  		--//  ENCODE az64
	,cur_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,achievement_nr_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_nocb_achnr_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_NOCB_ACHNR_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_nocb_achnr_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_nonprog_growth_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_NONPROG_GROWTH_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_nonprog_growth_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,nonprog_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_nonprog_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_NONPROG_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_nonprog_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_prem_growth_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_PREM_GROWTH_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_prem_growth_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,premium_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_prem_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_PREM_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_prem_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_sign_growth_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_SIGN_GROWTH_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_sign_growth_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,signature_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_sign_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_SIGN_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_sign_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_tot_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_TOT_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_tot_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE sss_zonal_totgrowth_tbl;
CREATE TABLE IF NOT EXISTS SSS_ZONAL_TOTGROWTH_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sss_zonal_totgrowth_tbl
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,tot_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE ventasys_proddim_mapping;
CREATE TABLE IF NOT EXISTS VENTASYS_PRODDIM_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ventasys_proddim_mapping
(
	prod_name VARCHAR(200)  		--//  ENCODE zstd
	,prod_vent VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE wks_cockpit_billing_stock_values;
CREATE TABLE IF NOT EXISTS WKS_COCKPIT_BILLING_STOCK_VALUES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_cockpit_billing_stock_values
(
	dataset VARCHAR(7)  		--//  ENCODE lzo
	,mnth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week VARCHAR(11)  		--//  ENCODE lzo
	,region_name VARCHAR(75)  		--//  ENCODE lzo
	,zone_name VARCHAR(75)  		--//  ENCODE lzo
	,territory_name VARCHAR(75)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(1)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_type VARCHAR(50)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_name VARCHAR(1)  		--//  ENCODE lzo
	,channel_name VARCHAR(1)  		--//  ENCODE lzo
	,cl_stck_value NUMERIC(38,2)  		--//  ENCODE az64
	,billing_value NUMERIC(38,8)  		--//  ENCODE az64
)

;
--DROP TABLE wks_cockpit_targets;
CREATE TABLE IF NOT EXISTS WKS_COCKPIT_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_cockpit_targets
(
	dataset VARCHAR(18)  		--//  ENCODE lzo
	,year_month DATE  		--//  ENCODE az64
	,region VARCHAR(75)  		--//  ENCODE lzo
	,zone VARCHAR(75)  		--//  ENCODE lzo
	,territory VARCHAR(75)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,variant VARCHAR(50)  		--//  ENCODE lzo
	,measure_type VARCHAR(50)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,brand_focus_target NUMERIC(38,2)  		--//  ENCODE az64
	,business_plan_target NUMERIC(38,2)  		--//  ENCODE az64
)

;
--DROP TABLE wks_csl_classmaster;
CREATE TABLE IF NOT EXISTS WKS_CSL_CLASSMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_csl_classmaster
(
	tableid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pkey numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,classid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,classcode VARCHAR(10)  		--//  ENCODE lzo
	,classdesc VARCHAR(100)  		--//  ENCODE lzo
	,turnover NUMERIC(18,4)  		--//  ENCODE az64
	,availabilty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,disthierarchyid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_csl_orderbooking_raw;
CREATE TABLE IF NOT EXISTS WKS_CSL_ORDERBOOKING_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_csl_orderbooking_raw
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,orderno VARCHAR(50)  		--//  ENCODE zstd
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,orddlvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,allowbackorder VARCHAR(50)  		--//  ENCODE zstd
	,ordtype VARCHAR(50)  		--//  ENCODE zstd
	,ordpriority VARCHAR(50)  		--//  ENCODE zstd
	,orddocref VARCHAR(200)  		--//  ENCODE zstd
	,remarks VARCHAR(1000)  		--//  ENCODE zstd
	,roundoffamt NUMERIC(38,6)  		--//  ENCODE az64
	,ordtotalamt NUMERIC(38,6)  		--//  ENCODE az64
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,salesmanname VARCHAR(400)  		--//  ENCODE zstd
	,salesroutecode VARCHAR(50)  		--//  ENCODE zstd
	,salesroutename VARCHAR(400)  		--//  ENCODE zstd
	,urccode VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrname VARCHAR(200)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,prdbatcde VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdbilledqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prdselrate NUMERIC(38,6)  		--//  ENCODE az64
	,prdgrossamt NUMERIC(38,6)  		--//  ENCODE az64
	,recorddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,6)  		--//  ENCODE az64
	,recommendedsku VARCHAR(10)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_csl_salesinvoiceorders_raw;
CREATE TABLE IF NOT EXISTS WKS_CSL_SALESINVOICEORDERS_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_csl_salesinvoiceorders_raw
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,orderno VARCHAR(50)  		--//  ENCODE zstd
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_csl_scheme_utilization_raw;
CREATE TABLE IF NOT EXISTS WKS_CSL_SCHEME_UTILIZATION_RAW		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_csl_scheme_utilization_raw
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,schemecode VARCHAR(50)  		--//  ENCODE zstd
	,schemedescription VARCHAR(200)  		--//  ENCODE zstd
	,invoiceno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,schdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,schemetype VARCHAR(50)  		--//  ENCODE zstd
	,schemeutilizedamt NUMERIC(18,6)  		--//  ENCODE az64
	,schemefreeproduct VARCHAR(50)  		--//  ENCODE zstd
	,schemeutilizedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,companyschemecode VARCHAR(50)  		--//  ENCODE zstd
	,schlinecount NUMERIC(18,6)  		--//  ENCODE az64
	,schvaluetype VARCHAR(200)  		--//  ENCODE zstd
	,slabid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,billedprdccode VARCHAR(50)  		--//  ENCODE zstd
	,billedprdbatcode VARCHAR(50)  		--//  ENCODE zstd
	,billedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schdiscperc NUMERIC(38,6)  		--//  ENCODE az64
	,freeprdbatcode VARCHAR(50)  		--//  ENCODE zstd
	,billedrate NUMERIC(38,6)  		--//  ENCODE az64
	,servicecrnrefno VARCHAR(100)  		--//  ENCODE zstd
	,rtrurccode VARCHAR(200)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,syncid NUMERIC(38,6)  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_customer_dim;
CREATE TABLE IF NOT EXISTS WKS_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_customer_dim
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,region_code VARCHAR(50)  		--//  ENCODE lzo
	,region_name VARCHAR(100)  		--//  ENCODE lzo
	,zone_code VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(100)  		--//  ENCODE lzo
	,zone_classification VARCHAR(1)  		--//  ENCODE lzo
	,territory_code VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(100)  		--//  ENCODE lzo
	,territory_classification VARCHAR(1)  		--//  ENCODE lzo
	,state_code VARCHAR(50)  		--//  ENCODE lzo
	,state_name VARCHAR(100)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(100)  		--//  ENCODE lzo
	,town_classification VARCHAR(1)  		--//  ENCODE lzo
	,city VARCHAR(100)  		--//  ENCODE lzo
	,type_code VARCHAR(50)  		--//  ENCODE lzo
	,type_name VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,customer_address1 VARCHAR(200)  		--//  ENCODE lzo
	,customer_address2 VARCHAR(200)  		--//  ENCODE lzo
	,customer_address3 VARCHAR(200)  		--//  ENCODE lzo
	,active_flag VARCHAR(1)  		--//  ENCODE lzo
	,active_start_date TIMESTAMP WITH TIME ZONE  		--//  ENCODE az64
	,wholesalercode VARCHAR(1)  		--//  ENCODE lzo
	,super_stockiest VARCHAR(1)  		--//  ENCODE lzo
	,direct_account_flag VARCHAR(7)  		--//  ENCODE lzo
	,abi_code numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,abi_name VARCHAR(100)  		--//  ENCODE lzo
	,rds_size VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
	,psnonps VARCHAR(1)  		--//  ENCODE lzo
	,suppliedby VARCHAR(50)  		--//  ENCODE lzo
	,cfa VARCHAR(16)  		--//  ENCODE lzo
	,cfa_name VARCHAR(100)  		--//  ENCODE lzo
)

CLUSTER BY (customer_code)
;
--DROP TABLE wks_day_cls_stock_fact;
CREATE TABLE IF NOT EXISTS WKS_DAY_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_day_cls_stock_fact
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,transdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lcnid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,lcncode VARCHAR(100)  		--//  ENCODE lzo
	,prdid numeric(18,0)		--//  ENCODE delta // INTEGER  
	,prdcode VARCHAR(100)  		--//  ENCODE lzo
	,salopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalopenstock NUMERIC(18,0)  		--//  ENCODE delta
	,offeropenstock NUMERIC(18,0)  		--//  ENCODE delta
	,salpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurchase NUMERIC(18,0)  		--//  ENCODE delta
	,salpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offerpurreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salsales NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsales NUMERIC(18,0)  		--//  ENCODE delta
	,offersales NUMERIC(18,0)  		--//  ENCODE delta
	,salstockin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockin NUMERIC(18,0)  		--//  ENCODE delta
	,salstockout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockout NUMERIC(18,0)  		--//  ENCODE delta
	,damagein NUMERIC(18,0)  		--//  ENCODE delta
	,damageout NUMERIC(18,0)  		--//  ENCODE delta
	,salsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,unsalsalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,offersalesreturn NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurin NUMERIC(18,0)  		--//  ENCODE delta
	,salstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstkjurout NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrin NUMERIC(18,0)  		--//  ENCODE delta
	,salbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerbattfrout NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrin NUMERIC(18,0)  		--//  ENCODE delta
	,sallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,unsallcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,offerlcntfrout NUMERIC(18,0)  		--//  ENCODE delta
	,salreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,offerreplacement NUMERIC(18,0)  		--//  ENCODE delta
	,salclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,unsalclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,offerclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,uploaddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,uploadflag VARCHAR(10)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,syncid NUMERIC(38,0)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_fin_sim_base;
CREATE TABLE IF NOT EXISTS WKS_FIN_SIM_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_fin_sim_base
(
	matl_num VARCHAR(18)  		--//  ENCODE lzo
	,chrt_acct VARCHAR(4)  		--//  ENCODE lzo
	,acct_num VARCHAR(10)  		--//  ENCODE lzo
	,dstr_chnl VARCHAR(2)  		--//  ENCODE lzo
	,ctry_key VARCHAR(3)  		--//  ENCODE lzo
	,caln_yr_mo numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amt_obj_crncy NUMERIC(38,5)  		--//  ENCODE az64
	,qty NUMERIC(38,5)  		--//  ENCODE az64
	,acct_hier_desc VARCHAR(100)  		--//  ENCODE lzo
	,acct_hier_shrt_desc VARCHAR(100)  		--//  ENCODE lzo
	,chnl_desc1 VARCHAR(500)  		--//  ENCODE lzo
	,chnl_desc2 VARCHAR(500)  		--//  ENCODE lzo
	,bw_gl VARCHAR(200)  		--//  ENCODE lzo
	,nature VARCHAR(500)  		--//  ENCODE lzo
	,sap_gl VARCHAR(500)  		--//  ENCODE lzo
	,descp VARCHAR(500)  		--//  ENCODE lzo
	,bravo_mapping VARCHAR(500)  		--//  ENCODE lzo
	,sku_desc VARCHAR(500)  		--//  ENCODE lzo
	,brand_combi VARCHAR(500)  		--//  ENCODE lzo
	,franchise VARCHAR(500)  		--//  ENCODE lzo
	,"group" VARCHAR(500)  		--//  ENCODE lzo
	,mrp NUMERIC(38,2)  		--//  ENCODE az64
	,cogs_per_unit NUMERIC(38,2)  		--//  ENCODE az64
	,plan VARCHAR(10)  		--//  ENCODE lzo
	,brand_group_1 VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_2 VARCHAR(500)  		--//  ENCODE lzo
	,co_cd VARCHAR(4)  		--//  ENCODE lzo
	,brand_combi_var VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_fin_sim_copa_trans_fact;
CREATE TABLE IF NOT EXISTS WKS_FIN_SIM_COPA_TRANS_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_fin_sim_copa_trans_fact
(
	matl_num VARCHAR(18)  		--//  ENCODE lzo
	,chrt_acct VARCHAR(4)  		--//  ENCODE lzo
	,acct_num VARCHAR(10)  		--//  ENCODE lzo
	,dstr_chnl VARCHAR(2)  		--//  ENCODE lzo
	,ctry_key VARCHAR(3)  		--//  ENCODE lzo
	,caln_yr_mo numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,acct_hier_desc VARCHAR(100)  		--//  ENCODE lzo
	,acct_hier_shrt_desc VARCHAR(100)  		--//  ENCODE lzo
	,co_cd VARCHAR(4)  		--//  ENCODE lzo
	,amt_obj_crncy NUMERIC(38,5)  		--//  ENCODE az64
	,qty NUMERIC(38,5)  		--//  ENCODE az64
)

;
--DROP TABLE wks_fin_sim_miscdata;
CREATE TABLE IF NOT EXISTS WKS_FIN_SIM_MISCDATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_fin_sim_miscdata
(
	matl_num VARCHAR(250)  		--//  ENCODE zstd
	,sku_desc VARCHAR(250)  		--//  ENCODE zstd
	,brand_combi VARCHAR(250)  		--//  ENCODE zstd
	,fisc_yr VARCHAR(250)  		--//  ENCODE zstd
	,month VARCHAR(250)  		--//  ENCODE zstd
	,chnl_desc2 VARCHAR(250)  		--//  ENCODE zstd
	,nature VARCHAR(250)  		--//  ENCODE zstd
	,amt_obj_crncy VARCHAR(250)  		--//  ENCODE zstd
	,qty VARCHAR(250)  		--//  ENCODE zstd
)

;
--DROP TABLE wks_fin_sim_plandata;
CREATE TABLE IF NOT EXISTS WKS_FIN_SIM_PLANDATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_fin_sim_plandata
(
	matl_num VARCHAR(100)  		--//  ENCODE lzo
	,fisc_yr VARCHAR(10)  		--//  ENCODE lzo
	,month VARCHAR(10)  		--//  ENCODE lzo
	,chnl_desc2 VARCHAR(20)  		--//  ENCODE lzo
	,nature VARCHAR(50)  		--//  ENCODE lzo
	,amt_obj_crncy NUMERIC(38,5)  		--//  ENCODE az64
	,qty NUMERIC(38,5)  		--//  ENCODE az64
	,plan VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_in_invoice_fact;
CREATE TABLE IF NOT EXISTS WKS_IN_INVOICE_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_invoice_fact
(
	customer_code VARCHAR(10)  		--//  ENCODE lzo
	,product_code VARCHAR(18)  		--//  ENCODE lzo
	,invoice_no VARCHAR(10)  		--//  ENCODE lzo
	,invoice_date DATE  		--//  ENCODE az64
	,invoice_val NUMERIC(38,17)  		--//  ENCODE az64
	,invoice_qty DOUBLE PRECISION
	,wt_invoice_qty DOUBLE PRECISION
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_perfectstore_msl;
CREATE TABLE IF NOT EXISTS WKS_IN_PERFECTSTORE_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_perfectstore_msl
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year VARCHAR(255)  		--//  ENCODE lzo
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_code VARCHAR(255)  		--//  ENCODE lzo
	,product_name VARCHAR(255)  		--//  ENCODE lzo
	,msl VARCHAR(255)  		--//  ENCODE lzo
	,cost_inr VARCHAR(255)  		--//  ENCODE lzo
	,quantity VARCHAR(255)  		--//  ENCODE lzo
	,amount_inr VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_in_perfectstore_paid_display;
CREATE TABLE IF NOT EXISTS WKS_IN_PERFECTSTORE_PAID_DISPLAY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_perfectstore_paid_display
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year VARCHAR(255)  		--//  ENCODE lzo
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,asset VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,posm_brand VARCHAR(255)  		--//  ENCODE lzo
	,start_date VARCHAR(255)  		--//  ENCODE lzo
	,end_date VARCHAR(255)  		--//  ENCODE lzo
	,audit_status VARCHAR(255)  		--//  ENCODE lzo
	,is_available VARCHAR(255)  		--//  ENCODE lzo
	,availability_points VARCHAR(255)  		--//  ENCODE lzo
	,visibility_type VARCHAR(255)  		--//  ENCODE lzo
	,visibility_condition VARCHAR(255)  		--//  ENCODE lzo
	,is_planogram_availbale VARCHAR(255)  		--//  ENCODE lzo
	,select_brand VARCHAR(255)  		--//  ENCODE lzo
	,is_correct_brand_displayed VARCHAR(255)  		--//  ENCODE lzo
	,brandavailability_points VARCHAR(255)  		--//  ENCODE lzo
	,stock_status VARCHAR(255)  		--//  ENCODE lzo
	,stock_points VARCHAR(255)  		--//  ENCODE lzo
	,is_near_category VARCHAR(255)  		--//  ENCODE lzo
	,nearcategory_points VARCHAR(255)  		--//  ENCODE lzo
	,audit_score VARCHAR(255)  		--//  ENCODE lzo
	,paid_visibility_score VARCHAR(255)  		--//  ENCODE lzo
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_in_perfectstore_promo;
CREATE TABLE IF NOT EXISTS WKS_IN_PERFECTSTORE_PROMO		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_perfectstore_promo
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_datetime VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year VARCHAR(255)  		--//  ENCODE lzo
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,product_category VARCHAR(255)  		--//  ENCODE lzo
	,product_brand VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_product_name VARCHAR(255)  		--//  ENCODE lzo
	,ispromotionavailable VARCHAR(255)  		--//  ENCODE lzo
	,photopath VARCHAR(500)  		--//  ENCODE lzo
	,countoffacing VARCHAR(255)  		--//  ENCODE lzo
	,promotionoffertype VARCHAR(255)  		--//  ENCODE lzo
	,notavailablereason VARCHAR(255)  		--//  ENCODE lzo
	,price_off VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_in_perfectstore_sos;
CREATE TABLE IF NOT EXISTS WKS_IN_PERFECTSTORE_SOS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_perfectstore_sos
(
	visit_datetime VARCHAR(255)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,jnjrkam VARCHAR(255)  		--//  ENCODE lzo
	,jnjzm_code VARCHAR(255)  		--//  ENCODE lzo
	,jnj_abi_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjsupervisor_code VARCHAR(255)  		--//  ENCODE lzo
	,isp_code VARCHAR(255)  		--//  ENCODE lzo
	,jnjisp_name VARCHAR(255)  		--//  ENCODE lzo
	,month VARCHAR(255)  		--//  ENCODE lzo
	,year VARCHAR(255)  		--//  ENCODE lzo
	,format VARCHAR(255)  		--//  ENCODE lzo
	,chain_code VARCHAR(255)  		--//  ENCODE lzo
	,chain VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(255)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,prod_facings VARCHAR(255)  		--//  ENCODE lzo
	,total_facings VARCHAR(255)  		--//  ENCODE lzo
	,facing_contribution VARCHAR(255)  		--//  ENCODE lzo
	,priority_store VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_in_retailer;
CREATE TABLE IF NOT EXISTS WKS_IN_RETAILER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_retailer
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rtrtype VARCHAR(10)  		--//  ENCODE zstd
	,rtrname VARCHAR(100)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(50)  		--//  ENCODE zstd
	,channelcode VARCHAR(30)  		--//  ENCODE zstd
	,retlrgroupcode VARCHAR(30)  		--//  ENCODE zstd
	,classcode VARCHAR(20)  		--//  ENCODE zstd
	,rtrphoneno VARCHAR(20)  		--//  ENCODE zstd
	,rtrcontactperson VARCHAR(50)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,regdate DATE  		--//  ENCODE az64
	,rtrlicno VARCHAR(100)  		--//  ENCODE zstd
	,rtrlicexpirydate DATE  		--//  ENCODE az64
	,druglno VARCHAR(100)  		--//  ENCODE zstd
	,rtrdrugexpirydate DATE  		--//  ENCODE az64
	,rtrcrbills numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcrdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rtrcrlimit NUMERIC(38,6)  		--//  ENCODE az64
	,relationstatus VARCHAR(10)  		--//  ENCODE zstd
	,parentcode VARCHAR(50)  		--//  ENCODE zstd
	,status VARCHAR(10)  		--//  ENCODE zstd
	,rtrlatitude VARCHAR(20)  		--//  ENCODE zstd
	,rtrlongitude VARCHAR(20)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,keyaccount VARCHAR(10)  		--//  ENCODE zstd
	,rtrfoodlicno VARCHAR(200)  		--//  ENCODE zstd
	,pannumber VARCHAR(15)  		--//  ENCODE zstd
	,retailertype VARCHAR(10)  		--//  ENCODE zstd
	,composite VARCHAR(10)  		--//  ENCODE zstd
	,relatedparty VARCHAR(10)  		--//  ENCODE zstd
	,statename VARCHAR(50)  		--//  ENCODE zstd
	,lastmoddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_retailer_route;
CREATE TABLE IF NOT EXISTS WKS_IN_RETAILER_ROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_retailer_route
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,rtrcode VARCHAR(50)  		--//  ENCODE zstd
	,rmcode VARCHAR(100)  		--//  ENCODE zstd
	,routetype VARCHAR(10)  		--//  ENCODE zstd
	,coveragesequence numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_route;
CREATE TABLE IF NOT EXISTS WKS_IN_ROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_route
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(50)  		--//  ENCODE zstd
	,rmcode VARCHAR(100)  		--//  ENCODE zstd
	,routetype VARCHAR(10)  		--//  ENCODE zstd
	,rmname VARCHAR(50)  		--//  ENCODE zstd
	,distance numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,vanroute VARCHAR(10)  		--//  ENCODE zstd
	,status VARCHAR(10)  		--//  ENCODE zstd
	,rmpopulation numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,localupcountry VARCHAR(10)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rretailergeoextension;
CREATE TABLE IF NOT EXISTS WKS_IN_RRETAILERGEOEXTENSION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rretailergeoextension
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,customercode VARCHAR(50)  		--//  ENCODE zstd
	,cmpcutomercode VARCHAR(50)  		--//  ENCODE zstd
	,distributorcustomercode VARCHAR(50)  		--//  ENCODE zstd
	,latitude VARCHAR(20)  		--//  ENCODE zstd
	,longitude VARCHAR(20)  		--//  ENCODE zstd
	,townname VARCHAR(100)  		--//  ENCODE zstd
	,statename VARCHAR(100)  		--//  ENCODE zstd
	,districtname VARCHAR(100)  		--//  ENCODE zstd
	,subdistrictname VARCHAR(100)  		--//  ENCODE zstd
	,type VARCHAR(10)  		--//  ENCODE zstd
	,villagename VARCHAR(100)  		--//  ENCODE zstd
	,pincode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,uacheck VARCHAR(100)  		--//  ENCODE zstd
	,uaname VARCHAR(100)  		--//  ENCODE zstd
	,population numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,popstrata VARCHAR(100)  		--//  ENCODE zstd
	,finalpopulationwithua numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,modifydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isdeleted CHAR(1)  		--//  ENCODE zstd
	,extrafield1 VARCHAR(100)  		--//  ENCODE zstd
	,extrafield2 VARCHAR(100)  		--//  ENCODE zstd
	,extrafield3 VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rrsrdistributor;
CREATE TABLE IF NOT EXISTS WKS_IN_RRSRDISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rrsrdistributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,rsrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rrsrheader;
CREATE TABLE IF NOT EXISTS WKS_IN_RRSRHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rrsrheader
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,rsrcode VARCHAR(50)  		--//  ENCODE zstd
	,rsrname VARCHAR(100)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,phoneno VARCHAR(15)  		--//  ENCODE zstd
	,dateofbirth TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,approvalstatus VARCHAR(10)  		--//  ENCODE zstd
	,dailyallowance NUMERIC(22,6)  		--//  ENCODE az64
	,monthlysalary NUMERIC(22,6)  		--//  ENCODE az64
	,aadharno VARCHAR(15)  		--//  ENCODE zstd
	,imagepath VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rtldistributor;
CREATE TABLE IF NOT EXISTS WKS_IN_RTLDISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rtldistributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rtlheader;
CREATE TABLE IF NOT EXISTS WKS_IN_RTLHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rtlheader
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,tlname VARCHAR(100)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,phoneno VARCHAR(15)  		--//  ENCODE zstd
	,dateofbirth TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,approvalstatus VARCHAR(10)  		--//  ENCODE zstd
	,dailyallowance NUMERIC(22,6)  		--//  ENCODE az64
	,monthlysalary NUMERIC(22,6)  		--//  ENCODE az64
	,aadharno VARCHAR(15)  		--//  ENCODE zstd
	,imagepath VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_rtlsalesman;
CREATE TABLE IF NOT EXISTS WKS_IN_RTLSALESMAN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_rtlsalesman
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,tlcode VARCHAR(50)  		--//  ENCODE zstd
	,distrcode VARCHAR(30)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,dateofjoin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive CHAR(1)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_salesman;
CREATE TABLE IF NOT EXISTS WKS_IN_SALESMAN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_salesman
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,smcode VARCHAR(50)  		--//  ENCODE zstd
	,smname VARCHAR(50)  		--//  ENCODE zstd
	,smphoneno VARCHAR(50)  		--//  ENCODE zstd
	,smemail VARCHAR(50)  		--//  ENCODE zstd
	,rdssmtype VARCHAR(10)  		--//  ENCODE zstd
	,smdailyallowance NUMERIC(38,6)  		--//  ENCODE az64
	,smmonthlysalary NUMERIC(38,6)  		--//  ENCODE az64
	,smmktcredit NUMERIC(38,6)  		--//  ENCODE az64
	,smcreditdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status VARCHAR(10)  		--//  ENCODE zstd
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,aadhaarno VARCHAR(50)  		--//  ENCODE zstd
	,uniquesalescode VARCHAR(50)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_in_salesman_route;
CREATE TABLE IF NOT EXISTS WKS_IN_SALESMAN_ROUTE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_in_salesman_route
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  		--//  ENCODE zstd
	,routecode VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_india_regional_sellout;
CREATE TABLE IF NOT EXISTS WKS_INDIA_REGIONAL_SELLOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_india_regional_sellout
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,mnth_id VARCHAR(11)  		--//  ENCODE lzo
	,mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(65535)  		--//  ENCODE lzo
	,soldto_code VARCHAR(500)  		--//  ENCODE lzo
	,distributor_code VARCHAR(255)  		--//  ENCODE lzo
	,distributor_name VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(100)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(40)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(40)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(255)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(50)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(5)  		--//  ENCODE lzo
	,to_currency VARCHAR(5)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity DOUBLE PRECISION
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,msl_product_code VARCHAR(50)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(150)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_india_regional_sellout_base;
CREATE TABLE IF NOT EXISTS WKS_INDIA_REGIONAL_SELLOUT_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_india_regional_sellout_base
(
	cntry_cd VARCHAR(2)  		--//  ENCODE lzo
	,cntry_nm VARCHAR(5)  		--//  ENCODE lzo
	,data_src VARCHAR(8)  		--//  ENCODE lzo
	,distributor_code VARCHAR(255)  		--//  ENCODE lzo
	,distributor_name VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type_code VARCHAR(255)  		--//  ENCODE lzo
	,sold_to_code VARCHAR(500)  		--//  ENCODE lzo
	,sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(65535)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(255)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mnth_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,so_sls_qty DOUBLE PRECISION
	,so_sls_value DOUBLE PRECISION
	,msl_product_code VARCHAR(50)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(150)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(100)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(40)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(40)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_india_regional_sellout_npd;
CREATE TABLE IF NOT EXISTS WKS_INDIA_REGIONAL_SELLOUT_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_india_regional_sellout_npd
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,mnth_id VARCHAR(11)  		--//  ENCODE lzo
	,mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(65535)  		--//  ENCODE lzo
	,soldto_code VARCHAR(500)  		--//  ENCODE lzo
	,distributor_code VARCHAR(255)  		--//  ENCODE lzo
	,distributor_name VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(100)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(40)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(40)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(255)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(50)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(5)  		--//  ENCODE lzo
	,to_currency VARCHAR(5)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity DOUBLE PRECISION
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,msl_product_code VARCHAR(50)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(150)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,customer_min_date DATE  		--//  ENCODE az64
	,market_min_date DATE  		--//  ENCODE az64
	,rn_cus numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rn_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,customer_product_min_date DATE  		--//  ENCODE az64
	,market_product_min_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE wks_issue_pf_cuml_sales;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_PF_CUML_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_pf_cuml_sales
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,week0_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week1_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week2_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week3_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week4_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week5_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,orange_percentage NUMERIC(4,2)  		--//  ENCODE az64
	,week0_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week1_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week2_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week3_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week4_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week5_sales_cuml numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_issue_pf_orng_perc;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_PF_ORNG_PERC		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_pf_orng_perc
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,week0_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week1_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week2_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week3_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week4_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week5_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,orange_percentage NUMERIC(4,2)  		--//  ENCODE az64
)

;
--DROP TABLE wks_issue_pf_os_flag;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_PF_OS_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_pf_os_flag
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,compl_week0 NUMERIC(38,22)  		--//  ENCODE az64
	,compl_week1 NUMERIC(38,22)  		--//  ENCODE az64
	,compl_week2 NUMERIC(38,22)  		--//  ENCODE az64
	,compl_week3 NUMERIC(38,22)  		--//  ENCODE az64
	,compl_week4 NUMERIC(38,22)  		--//  ENCODE az64
	,compl_week5 NUMERIC(38,22)  		--//  ENCODE az64
	,os_flag_week0 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,os_flag_week1 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,os_flag_week2 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,os_flag_week3 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,os_flag_week4 numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,os_flag_week5 numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_issue_pf_ret_dim;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_PF_RET_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_pf_ret_dim
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,week0_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week1_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week2_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week3_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week4_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week5_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_pf_weekly_sales_flag_pivot;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_PF_WEEKLY_SALES_FLAG_PIVOT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_pf_weekly_sales_flag_pivot
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,week0_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week1_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week2_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week3_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week4_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,week5_sales_flag numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_issue_rev_edw_dailysales_fact;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_EDW_DAILYSALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_edw_dailysales_fact
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr_val NUMERIC(38,6)  		--//  ENCODE az64
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,num_packs numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sales_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE wks_issue_rev_edw_retailer_dim;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_EDW_RETAILER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_edw_retailer_dim
(
	retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_address1 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address2 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address3 VARCHAR(250)  		--//  ENCODE lzo
	,region_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_classification VARCHAR(50)  		--//  ENCODE lzo
	,territory_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_classification VARCHAR(50)  		--//  ENCODE lzo
	,state_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,state_name VARCHAR(50)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,town_classification VARCHAR(50)  		--//  ENCODE lzo
	,class_code VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,outlet_type VARCHAR(50)  		--//  ENCODE lzo
	,channel_code VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,loyalty_desc VARCHAR(50)  		--//  ENCODE lzo
	,registration_date numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status_cd VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,actv_flg CHAR(1)  		--//  ENCODE lzo
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_rec_dt DATE  		--//  ENCODE az64
	,rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_issue_rev_itg_in_rsalesman;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_ITG_IN_RSALESMAN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_itg_in_rsalesman
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,rtrcode VARCHAR(50)  		--//  ENCODE lzo
	,rmcode VARCHAR(100)  		--//  ENCODE lzo
	,rmname VARCHAR(50)  		--//  ENCODE lzo
	,smcode VARCHAR(50)  		--//  ENCODE lzo
	,smname VARCHAR(50)  		--//  ENCODE lzo
	,status VARCHAR(10)  		--//  ENCODE lzo
	,uniquesalescode VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_rev_itg_sku_recom_flag;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_ITG_SKU_RECOM_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_itg_sku_recom_flag
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
)

;
--DROP TABLE wks_issue_rev_pf_weekly_sales_flag;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_PF_WEEKLY_SALES_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_pf_weekly_sales_flag
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,sales_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_issue_rev_sku_recom_sm_gtm;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_SKU_RECOM_SM_GTM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_sku_recom_sm_gtm
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(50)  		--//  ENCODE lzo
)

CLUSTER BY (retailer_cd)
;
--DROP TABLE wks_issue_rev_sku_recom_sm_nongtm;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_SKU_RECOM_SM_NONGTM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_sku_recom_sm_nongtm
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_rev_sku_recom_tmp1;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_SKU_RECOM_TMP1		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_sku_recom_tmp1
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_rev_sku_recom_tmp2;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_SKU_RECOM_TMP2		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_sku_recom_tmp2
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_rev_sku_recom_tmp3;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_SKU_RECOM_TMP3		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_sku_recom_tmp3
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,reco_date DATE  		--//  ENCODE az64
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(50)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr_val NUMERIC(38,6)  		--//  ENCODE az64
	,hit_ms_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_issue_rev_udc_details;
CREATE TABLE IF NOT EXISTS WKS_ISSUE_REV_UDC_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_issue_rev_udc_details
(
	distcode VARCHAR(50)  		--//  ENCODE lzo
	,mastervaluecode VARCHAR(200)  		--//  ENCODE lzo
	,mastervaluename VARCHAR(300)  		--//  ENCODE lzo
	,columnname VARCHAR(100)  		--//  ENCODE lzo
	,columnvalue VARCHAR(150)  		--//  ENCODE lzo
	,rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_jnj_calendar;
CREATE TABLE IF NOT EXISTS WKS_JNJ_CALENDAR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_jnj_calendar
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_ka_sales_fact;
CREATE TABLE IF NOT EXISTS WKS_KA_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_ka_sales_fact
(
	customer_code VARCHAR(10)  		--//  ENCODE lzo
	,invoice_date numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_name VARCHAR(200)  		--//  ENCODE lzo
	,product_code VARCHAR(50)  		--//  ENCODE lzo
	,invoice_no VARCHAR(100)  		--//  ENCODE lzo
	,prdqty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE az64
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE az64
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE az64
	,saleflag VARCHAR(3)  		--//  ENCODE lzo
	,confirmsales VARCHAR(1)  		--//  ENCODE lzo
	,subtotal4 NUMERIC(38,3)  		--//  ENCODE az64
	,totalgrosssalesincltax NUMERIC(38,6)  		--//  ENCODE az64
	,totalsalesnr NUMERIC(38,6)  		--//  ENCODE az64
	,totalsalesconfirmed NUMERIC(38,6)  		--//  ENCODE az64
	,totalsalesnrconfirmed NUMERIC(38,6)  		--//  ENCODE az64
	,totalsalesunconfirmed NUMERIC(38,6)  		--//  ENCODE az64
	,totalsalesnrunconfirmed NUMERIC(38,6)  		--//  ENCODE az64
	,totalqtyconfirmed numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,totalqtyunconfirmed numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,buyingoutlets VARCHAR(61)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY (buyingoutlets)
;
--DROP TABLE wks_lks_plant;
CREATE TABLE IF NOT EXISTS WKS_LKS_PLANT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_lks_plant
(
	plantcode NUMERIC(38,0)  		--//  ENCODE az64
	,plantid VARCHAR(50)  		--//  ENCODE lzo
	,plantname VARCHAR(50)  		--//  ENCODE lzo
	,shortname VARCHAR(50)  		--//  ENCODE lzo
	,name2 VARCHAR(200)  		--//  ENCODE lzo
	,statecode NUMERIC(18,0)  		--//  ENCODE az64
	,active CHAR(1)  		--//  ENCODE lzo
	,createdby NUMERIC(18,0)  		--//  ENCODE az64
	,suppliercode VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_mgmnt_orange_stores;
CREATE TABLE IF NOT EXISTS WKS_MGMNT_ORANGE_STORES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mgmnt_orange_stores
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,"os_y/n_flag" VARCHAR(3)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_mngmnt_hit_achnr;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_HIT_ACHNR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_hit_achnr
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(200)  		--//  ENCODE lzo
	,zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,channel_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,achievement_nr_msl_msku NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE wks_mngmnt_rpt_nup;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_RPT_NUP		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_rpt_nup
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,retailer_name VARCHAR(100)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,ret_msku VARCHAR(251)  		--//  ENCODE lzo
	,sales_date DATE  		--//  ENCODE az64
	,ms_flag VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_mngmnt_rpt_nup_l3m;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_RPT_NUP_L3M		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_rpt_nup_l3m
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,ret_msku VARCHAR(251)  		--//  ENCODE lzo
	,ms_flag VARCHAR(1)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE wks_mngmnt_rpt_nup_l3m_with_os;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_RPT_NUP_L3M_WITH_OS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_rpt_nup_l3m_with_os
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,ret_msku VARCHAR(251)  		--//  ENCODE lzo
	,ms_flag VARCHAR(1)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,os_flag VARCHAR(3)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_mngmnt_rpt_tdp;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_RPT_TDP		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_rpt_tdp
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(200)  		--//  ENCODE lzo
	,zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,channel_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,total_stores numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,total_recos NUMERIC(38,0)  		--//  ENCODE az64
	,total_hits NUMERIC(38,0)  		--//  ENCODE az64
)

;
--DROP TABLE wks_mngmnt_sales_achnr;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_SALES_ACHNR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_sales_achnr
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,achievement_nr_all_msku NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE wks_mngmnt_sales_hit_nr;
CREATE TABLE IF NOT EXISTS WKS_MNGMNT_SALES_HIT_NR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mngmnt_sales_hit_nr
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,achievement_nr_all_msku NUMERIC(38,6)  		--//  ENCODE az64
	,achievement_nr_msl_msku NUMERIC(38,4)  		--//  ENCODE az64
)

;
--DROP TABLE wks_mt_sellin_vs_sellout_fctr_tbl;
CREATE TABLE IF NOT EXISTS WKS_MT_SELLIN_VS_SELLOUT_FCTR_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mt_sellin_vs_sellout_fctr_tbl
(
	mth_mm_cal numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_name VARCHAR(300)  		--//  ENCODE lzo
	,mth_mm_fi VARCHAR(300)  		--//  ENCODE lzo
	,factor VARCHAR(300)  		--//  ENCODE lzo
	,rnk numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_mt_sellin_vs_sellout_fctr_tbl_fnl;
CREATE TABLE IF NOT EXISTS WKS_MT_SELLIN_VS_SELLOUT_FCTR_TBL_FNL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mt_sellin_vs_sellout_fctr_tbl_fnl
(
	mth_mm_cal numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_name VARCHAR(300)  		--//  ENCODE lzo
	,mth_mm_fi VARCHAR(300)  		--//  ENCODE lzo
	,factor VARCHAR(300)  		--//  ENCODE lzo
	,rnk numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_mt_sellin_vs_sellout_pre_rpt_tbl;
CREATE TABLE IF NOT EXISTS WKS_MT_SELLIN_VS_SELLOUT_PRE_RPT_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mt_sellin_vs_sellout_pre_rpt_tbl
(
	fisc_yr VARCHAR(12)  		--//  ENCODE lzo
	,mth_mm VARCHAR(14)  		--//  ENCODE lzo
	,common_account_name VARCHAR(500)  		--//  ENCODE lzo
	,common_channel_name VARCHAR(11)  		--//  ENCODE lzo
	,channel_name_sellin VARCHAR(750)  		--//  ENCODE lzo
	,franchise_name_sellin VARCHAR(75)  		--//  ENCODE lzo
	,brand_name_sellin VARCHAR(75)  		--//  ENCODE lzo
	,variant_name_sellin VARCHAR(225)  		--//  ENCODE lzo
	,product_category_name_sellin VARCHAR(225)  		--//  ENCODE lzo
	,mothersku_name_sellin VARCHAR(225)  		--//  ENCODE lzo
	,invoice_quantity_sellin NUMERIC(38,4)  		--//  ENCODE az64
	,invoice_value_sellin NUMERIC(38,8)  		--//  ENCODE az64
	,data_source_sellout VARCHAR(12)  		--//  ENCODE lzo
	,pos_offtake_level_sellout VARCHAR(10)  		--//  ENCODE lzo
	,account_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,mother_sku_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,brand_name_sellout VARCHAR(2000)  		--//  ENCODE lzo
	,franchise_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,internal_category_sellout VARCHAR(255)  		--//  ENCODE lzo
	,internal_subcategory_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_category_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_subcategory_sellout VARCHAR(255)  		--//  ENCODE lzo
	,product_category_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,variant_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_mothersku_code_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_mothersku_name_sellout VARCHAR(65535)  		--//  ENCODE lzo
	,sls_qty_sellout DOUBLE PRECISION
	,sls_val_lcy_sellout DOUBLE PRECISION
	,factorized_sls_val_lcy_sellout DOUBLE PRECISION
	,sales_factor_sellout NUMERIC(10,2)  		--//  ENCODE az64
	,sales_factor_ref_month_sellout VARCHAR(300)  		--//  ENCODE lzo
	,internal_mothersku_name VARCHAR(382)  		--//  ENCODE lzo
	,internal_brand_name VARCHAR(382)  		--//  ENCODE lzo
	,internal_franchise_name VARCHAR(382)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_mth_cls_stock_fact_i;
CREATE TABLE IF NOT EXISTS WKS_MTH_CLS_STOCK_FACT_I		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mth_cls_stock_fact_i
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,stock_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stock_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,calsalclosing NUMERIC(18,0)  		--//  ENCODE az64
	,calunsalclosing NUMERIC(18,0)  		--//  ENCODE az64
	,calofferclosing NUMERIC(18,0)  		--//  ENCODE az64
	,calsalclosingval DOUBLE PRECISION
	,calunsalclosingval DOUBLE PRECISION
	,calofferclosingval DOUBLE PRECISION
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_mth_cls_stock_fact_ii;
CREATE TABLE IF NOT EXISTS WKS_MTH_CLS_STOCK_FACT_II		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_mth_cls_stock_fact_ii
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,stock_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stock_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,salstockadjqty NUMERIC(38,0)  		--//  ENCODE az64
	,salstockadjval DOUBLE PRECISION
	,unsalstockadjqty NUMERIC(38,0)  		--//  ENCODE az64
	,unsalstockadjval DOUBLE PRECISION
	,offerstockadjqty NUMERIC(38,0)  		--//  ENCODE az64
	,offerstockadjval DOUBLE PRECISION
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_muser;
CREATE TABLE IF NOT EXISTS WKS_MUSER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_muser
(
	musercode VARCHAR(1)  		--//  ENCODE lzo
	,musername VARCHAR(50)  		--//  ENCODE lzo
	,muserpassword VARCHAR(1)  		--//  ENCODE lzo
	,mfirstname VARCHAR(50)  		--//  ENCODE lzo
	,mlastname VARCHAR(50)  		--//  ENCODE lzo
	,memailid VARCHAR(200)  		--//  ENCODE lzo
	,userdateofcreation TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,userdateoflastpasschange TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,userpassfailcount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,userdeleted CHAR(1)  		--//  ENCODE lzo
	,encryptedusername VARCHAR(1)  		--//  ENCODE lzo
	,logintime VARCHAR(1)  		--//  ENCODE lzo
	,middlename VARCHAR(1)  		--//  ENCODE lzo
	,personaladdress VARCHAR(1)  		--//  ENCODE lzo
	,phonenumber VARCHAR(1)  		--//  ENCODE lzo
	,mobilenumber VARCHAR(1)  		--//  ENCODE lzo
	,emergencynumber VARCHAR(1)  		--//  ENCODE lzo
	,isactive CHAR(1)  		--//  ENCODE lzo
	,zonecode VARCHAR(50)  		--//  ENCODE lzo
	,regioncode VARCHAR(50)  		--//  ENCODE lzo
	,wwsapid VARCHAR(15)  		--//  ENCODE lzo
	,usertype CHAR(1)  		--//  ENCODE lzo
	,groupcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,designationcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,dateofbirth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,monthofbirth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ipppolicy VARCHAR(1)  		--//  ENCODE lzo
	,dateoflastlogin TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,territorycode VARCHAR(50)  		--//  ENCODE lzo
	,distcode VARCHAR(1)  		--//  ENCODE lzo
	,plantcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastloginfailedon TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,issecretquestionset CHAR(1)  		--//  ENCODE lzo
	,secretqfailcount numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwcregion numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwczone numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nwcterritory numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_pos_abrl;
CREATE TABLE IF NOT EXISTS WKS_POS_ABRL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_abrl
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_apollo;
CREATE TABLE IF NOT EXISTS WKS_POS_APOLLO		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_apollo
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_category_mapping;
CREATE TABLE IF NOT EXISTS WKS_POS_CATEGORY_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_category_mapping
(
	account_name VARCHAR(255)  		--//  ENCODE zstd
	,article_cd VARCHAR(20)  		--//  ENCODE zstd
	,article_desc VARCHAR(255)  		--//  ENCODE zstd
	,ean VARCHAR(20)  		--//  ENCODE zstd
	,sap_cd VARCHAR(20)  		--//  ENCODE zstd
	,mother_sku_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,product_category_name VARCHAR(255)  		--//  ENCODE zstd
	,variant_name VARCHAR(255)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,internal_category VARCHAR(255)  		--//  ENCODE zstd
	,internal_sub_category VARCHAR(255)  		--//  ENCODE zstd
	,external_category VARCHAR(255)  		--//  ENCODE zstd
	,external_sub_category VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_dabur;
CREATE TABLE IF NOT EXISTS WKS_POS_DABUR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_dabur
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_dmart;
CREATE TABLE IF NOT EXISTS WKS_POS_DMART		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_dmart
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_frl;
CREATE TABLE IF NOT EXISTS WKS_POS_FRL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_frl
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_hg;
CREATE TABLE IF NOT EXISTS WKS_POS_HG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_hg
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_historical_btl;
CREATE TABLE IF NOT EXISTS WKS_POS_HISTORICAL_BTL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_historical_btl
(
	mother_sku_name VARCHAR(255)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,re VARCHAR(255)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,promos NUMERIC(38,6)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_max;
CREATE TABLE IF NOT EXISTS WKS_POS_MAX		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_max
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_re_mapping;
CREATE TABLE IF NOT EXISTS WKS_POS_RE_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_re_mapping
(
	store_cd VARCHAR(20)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,zone VARCHAR(255)  		--//  ENCODE zstd
	,re VARCHAR(255)  		--//  ENCODE zstd
	,promotor VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_ril;
CREATE TABLE IF NOT EXISTS WKS_POS_RIL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_ril
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_spencer;
CREATE TABLE IF NOT EXISTS WKS_POS_SPENCER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_spencer
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_tesco;
CREATE TABLE IF NOT EXISTS WKS_POS_TESCO		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_tesco
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_pos_vmm;
CREATE TABLE IF NOT EXISTS WKS_POS_VMM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_pos_vmm
(
	key_account_name VARCHAR(200)  		--//  ENCODE zstd
	,pos_dt DATE  		--//  ENCODE az64
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,article_code VARCHAR(50)  		--//  ENCODE zstd
	,subcategory VARCHAR(50)  		--//  ENCODE zstd
	,level VARCHAR(50)  		--//  ENCODE zstd
	,sls_qty NUMERIC(38,6)  		--//  ENCODE az64
	,sls_val_lcy NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_upload_date DATE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_product_dim;
CREATE TABLE IF NOT EXISTS WKS_PRODUCT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_product_dim
(
	product_code VARCHAR(50)  		--//  ENCODE lzo
	,product_name VARCHAR(400)  		--//  ENCODE lzo
	,product_desc VARCHAR(400)  		--//  ENCODE lzo
	,franchise_code VARCHAR(50)  		--//  ENCODE lzo
	,franchise_name VARCHAR(100)  		--//  ENCODE lzo
	,brand_code VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(100)  		--//  ENCODE lzo
	,product_category_code VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(100)  		--//  ENCODE lzo
	,variant_code VARCHAR(50)  		--//  ENCODE lzo
	,variant_name VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(100)  		--//  ENCODE lzo
	,uom VARCHAR(1)  		--//  ENCODE lzo
	,std_nr VARCHAR(1)  		--//  ENCODE lzo
	,case_lot VARCHAR(1)  		--//  ENCODE lzo
	,sale_uom VARCHAR(1)  		--//  ENCODE lzo
	,sale_conversion_factor numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,base_uom VARCHAR(1)  		--//  ENCODE lzo
	,int_uom VARCHAR(1)  		--//  ENCODE lzo
	,gross_wt VARCHAR(1)  		--//  ENCODE lzo
	,net_wt VARCHAR(1)  		--//  ENCODE lzo
	,active_flag VARCHAR(7)  		--//  ENCODE lzo
	,delete_flag VARCHAR(1)  		--//  ENCODE lzo
	,shelf_life numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_retailermaster_upd;
CREATE TABLE IF NOT EXISTS WKS_RETAILERMASTER_UPD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_retailermaster_upd
(
	retailer_code VARCHAR(50)  		--//  ENCODE zstd
	,start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_address1 VARCHAR(250)  		--//  ENCODE zstd
	,retailer_address2 VARCHAR(250)  		--//  ENCODE zstd
	,retailer_address3 VARCHAR(250)  		--//  ENCODE zstd
	,region_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_classification VARCHAR(50)  		--//  ENCODE zstd
	,territory_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_classification VARCHAR(50)  		--//  ENCODE zstd
	,state_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,state_name VARCHAR(50)  		--//  ENCODE zstd
	,town_name VARCHAR(50)  		--//  ENCODE zstd
	,town_classification VARCHAR(50)  		--//  ENCODE zstd
	,rtrcatlevelid VARCHAR(30)  		--//  ENCODE zstd
	,rtrcategorycode VARCHAR(50)  		--//  ENCODE zstd
	,class_code VARCHAR(50)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,outlet_type VARCHAR(50)  		--//  ENCODE zstd
	,channel_code VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,loyalty_desc VARCHAR(50)  		--//  ENCODE zstd
	,registration_date numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status_cd VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(10)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(50)  		--//  ENCODE zstd
	,rtrlatitude VARCHAR(40)  		--//  ENCODE zstd
	,rtrlongitude VARCHAR(40)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_rec_dt DATE  		--//  ENCODE az64
	,type_name VARCHAR(50)  		--//  ENCODE zstd
	,town_code VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	region_code
		--// 	, zone_code
		--// 	, territory_code
		--// 	)
;		--// ;
--DROP TABLE wks_rkeyacccustomer;
CREATE TABLE IF NOT EXISTS WKS_RKEYACCCUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rkeyacccustomer
(
	customercode VARCHAR(50)  		--//  ENCODE lzo
	,customername VARCHAR(50)  		--//  ENCODE lzo
	,customeraddress1 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress2 VARCHAR(250)  		--//  ENCODE lzo
	,customeraddress3 VARCHAR(250)  		--//  ENCODE lzo
	,sapid VARCHAR(50)  		--//  ENCODE lzo
	,regioncode VARCHAR(50)  		--//  ENCODE lzo
	,zonecode VARCHAR(50)  		--//  ENCODE lzo
	,territorycode VARCHAR(50)  		--//  ENCODE lzo
	,statecode VARCHAR(50)  		--//  ENCODE lzo
	,towncode VARCHAR(50)  		--//  ENCODE lzo
	,emailid VARCHAR(50)  		--//  ENCODE lzo
	,mobilell VARCHAR(50)  		--//  ENCODE lzo
	,isactive CHAR(1)  		--//  ENCODE lzo
	,wholesalercode VARCHAR(50)  		--//  ENCODE lzo
	,urc VARCHAR(50)  		--//  ENCODE lzo
	,nkacstores CHAR(1)  		--//  ENCODE lzo
	,parentcustomercode VARCHAR(50)  		--//  ENCODE lzo
	,isdirectacct CHAR(1)  		--//  ENCODE lzo
	,isparent CHAR(1)  		--//  ENCODE lzo
	,abicode VARCHAR(50)  		--//  ENCODE lzo
	,distributorsapid VARCHAR(50)  		--//  ENCODE lzo
	,isconfirm VARCHAR(1)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdusercode VARCHAR(50)  		--//  ENCODE lzo
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modusercode VARCHAR(50)  		--//  ENCODE lzo
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(1)  		--//  ENCODE lzo
)

CLUSTER BY (customercode)
;
--DROP TABLE wks_rmrpstockprocess_clstk;
CREATE TABLE IF NOT EXISTS WKS_RMRPSTOCKPROCESS_CLSTK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rmrpstockprocess_clstk
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_rmrpstockprocess_opstk;
CREATE TABLE IF NOT EXISTS WKS_RMRPSTOCKPROCESS_OPSTK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rmrpstockprocess_opstk
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,transdate DATE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,mrp NUMERIC(22,6)  		--//  ENCODE az64
	,lsp NUMERIC(22,6)  		--//  ENCODE az64
	,selrate NUMERIC(22,6)  		--//  ENCODE az64
	,nrvalue NUMERIC(22,6)  		--//  ENCODE az64
	,salopenstock NUMERIC(22,6)  		--//  ENCODE az64
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,syncid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_rpt_regional_sellout_offtake_npd;
CREATE TABLE IF NOT EXISTS WKS_RPT_REGIONAL_SELLOUT_OFFTAKE_NPD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rpt_regional_sellout_offtake_npd
(
	year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qrtr_no VARCHAR(11)  		--//  ENCODE lzo
	,mnth_id VARCHAR(11)  		--//  ENCODE lzo
	,mnth_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_date DATE  		--//  ENCODE az64
	,univ_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,univ_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,country_name VARCHAR(5)  		--//  ENCODE lzo
	,data_source VARCHAR(8)  		--//  ENCODE lzo
	,customer_product_desc VARCHAR(65535)  		--//  ENCODE lzo
	,soldto_code VARCHAR(500)  		--//  ENCODE lzo
	,distributor_code VARCHAR(255)  		--//  ENCODE lzo
	,distributor_name VARCHAR(255)  		--//  ENCODE lzo
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,distributor_additional_attribute1 VARCHAR(100)  		--//  ENCODE lzo
	,distributor_additional_attribute2 VARCHAR(40)  		--//  ENCODE lzo
	,distributor_additional_attribute3 VARCHAR(40)  		--//  ENCODE lzo
	,sap_parent_customer_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_parent_customer_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_customer_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_customer_sub_channel_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_sub_channel_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_go_to_mdl_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_go_to_mdl_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_description VARCHAR(75)  		--//  ENCODE lzo
	,sap_banner_format_key VARCHAR(12)  		--//  ENCODE lzo
	,sap_banner_format_description VARCHAR(75)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(255)  		--//  ENCODE lzo
	,zone_or_area VARCHAR(255)  		--//  ENCODE lzo
	,customer_segment_key VARCHAR(12)  		--//  ENCODE lzo
	,customer_segment_description VARCHAR(50)  		--//  ENCODE lzo
	,global_product_franchise VARCHAR(30)  		--//  ENCODE lzo
	,global_product_brand VARCHAR(30)  		--//  ENCODE lzo
	,global_product_sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,global_product_variant VARCHAR(100)  		--//  ENCODE lzo
	,global_product_segment VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subsegment VARCHAR(100)  		--//  ENCODE lzo
	,global_product_category VARCHAR(50)  		--//  ENCODE lzo
	,global_product_subcategory VARCHAR(50)  		--//  ENCODE lzo
	,global_put_up_description VARCHAR(100)  		--//  ENCODE lzo
	,ean VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(50)  		--//  ENCODE lzo
	,sku_description VARCHAR(150)  		--//  ENCODE lzo
	,pka_product_key VARCHAR(68)  		--//  ENCODE lzo
	,pka_product_key_description VARCHAR(255)  		--//  ENCODE lzo
	,from_currency VARCHAR(5)  		--//  ENCODE lzo
	,to_currency VARCHAR(5)  		--//  ENCODE lzo
	,exchange_rate NUMERIC(15,5)  		--//  ENCODE az64
	,sellout_sales_quantity DOUBLE PRECISION
	,sellout_sales_value DOUBLE PRECISION
	,sellout_sales_value_usd NUMERIC(38,11)  		--//  ENCODE az64
	,sellout_value_list_price numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellout_value_list_price_usd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_min_date DATE  		--//  ENCODE az64
	,customer_product_min_date DATE  		--//  ENCODE az64
	,market_min_date DATE  		--//  ENCODE az64
	,market_product_min_date DATE  		--//  ENCODE az64
	,rn_cus numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rn_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,msl_product_code VARCHAR(50)  		--//  ENCODE lzo
	,msl_product_desc VARCHAR(150)  		--//  ENCODE lzo
	,store_grade VARCHAR(50)  		--//  ENCODE lzo
	,retail_env VARCHAR(50)  		--//  ENCODE lzo
	,channel VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,first_scan_flag_parent_customer_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level_initial VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_parent_customer_level VARCHAR(1)  		--//  ENCODE lzo
	,first_scan_flag_market_level VARCHAR(1)  		--//  ENCODE lzo
	,selling_price DOUBLE PRECISION
	,cnt_mkt numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_rpt_sss_scorecard;
CREATE TABLE IF NOT EXISTS WKS_RPT_SSS_SCORECARD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rpt_sss_scorecard
(
	country VARCHAR(5)  		--//  ENCODE zstd
	,region VARCHAR(75)  		--//  ENCODE zstd
	,zone VARCHAR(75)  		--//  ENCODE zstd
	,territory VARCHAR(75)  		--//  ENCODE zstd
	,channel VARCHAR(225)  		--//  ENCODE zstd
	,retail_environment VARCHAR(75)  		--//  ENCODE zstd
	,salesman_name VARCHAR(200)  		--//  ENCODE zstd
	,salesman_code VARCHAR(100)  		--//  ENCODE zstd
	,distributor_code VARCHAR(50)  		--//  ENCODE zstd
	,distributor_name VARCHAR(150)  		--//  ENCODE zstd
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,store_name VARCHAR(1000)  		--//  ENCODE zstd
	,program_type VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,kpi VARCHAR(75)  		--//  ENCODE zstd
	,quarter VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,source_actual_value VARCHAR(50)  		--//  ENCODE zstd
	,source_target_value VARCHAR(50)  		--//  ENCODE zstd
	,actual_value NUMERIC(30,24)  		--//  ENCODE az64
	,target_value NUMERIC(30,24)  		--//  ENCODE az64
	,compliance NUMERIC(30,24)  		--//  ENCODE az64
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,kpi_score NUMERIC(30,24)  		--//  ENCODE az64
	,kpi_achievement NUMERIC(30,24)  		--//  ENCODE az64
	,max_potential_brand_score NUMERIC(38,26)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE zstd
	,prod_hier_l1 VARCHAR(5)  		--//  ENCODE zstd
	,prod_hier_l2 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l3 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l5 VARCHAR(50)  		--//  ENCODE zstd
	,prod_hier_l6 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l7 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l8 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l9 VARCHAR(100)  		--//  ENCODE zstd
	,actual_value_msl_l9 VARCHAR(5)  		--//  ENCODE zstd
	,target_value_msl_l9 VARCHAR(5)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,crt_dtt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_rpt_sss_scorecard_base;
CREATE TABLE IF NOT EXISTS WKS_RPT_SSS_SCORECARD_BASE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rpt_sss_scorecard_base
(
	table_rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,country VARCHAR(5)  		--//  ENCODE lzo
	,region VARCHAR(75)  		--//  ENCODE lzo
	,zone VARCHAR(75)  		--//  ENCODE lzo
	,territory VARCHAR(75)  		--//  ENCODE lzo
	,channel VARCHAR(225)  		--//  ENCODE lzo
	,retail_environment VARCHAR(75)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(1000)  		--//  ENCODE lzo
	,program_type VARCHAR(50)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(75)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,actual_value NUMERIC(30,24)  		--//  ENCODE az64
	,target_value NUMERIC(30,24)  		--//  ENCODE az64
	,compliance NUMERIC(30,24)  		--//  ENCODE az64
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,kpi_score NUMERIC(30,24)  		--//  ENCODE az64
	,kpi_achievement NUMERIC(30,24)  		--//  ENCODE az64
	,max_potential_brand_score NUMERIC(38,26)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,prod_hier_l1 VARCHAR(26)  		--//  ENCODE lzo
	,prod_hier_l2 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(100)  		--//  ENCODE lzo
	,actual_value_msl_l9 VARCHAR(5)  		--//  ENCODE lzo
	,target_value_msl_l9 VARCHAR(5)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,promotor_store VARCHAR(1)  		--//  ENCODE lzo
	,crt_dtt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_rpurchasedetail;
CREATE TABLE IF NOT EXISTS WKS_RPURCHASEDETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rpurchasedetail
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,compinvno VARCHAR(25)  		--//  ENCODE zstd
	,compinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salesorderno VARCHAR(50)  		--//  ENCODE zstd
	,netvalue NUMERIC(38,6)  		--//  ENCODE az64
	,totaltax NUMERIC(38,6)  		--//  ENCODE az64
	,totaldiscount NUMERIC(38,6)  		--//  ENCODE az64
	,totalschemeamount NUMERIC(38,6)  		--//  ENCODE az64
	,totaloctroi NUMERIC(38,6)  		--//  ENCODE az64
	,suppliercode VARCHAR(50)  		--//  ENCODE zstd
	,cfacode VARCHAR(30)  		--//  ENCODE zstd
	,companyname VARCHAR(50)  		--//  ENCODE zstd
	,transportercode VARCHAR(20)  		--//  ENCODE zstd
	,lorryrecno VARCHAR(25)  		--//  ENCODE zstd
	,lorryrecdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,posgrnno VARCHAR(50)  		--//  ENCODE zstd
	,posgrndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,indentqtycs NUMERIC(38,6)  		--//  ENCODE az64
	,indentqtypcs NUMERIC(38,6)  		--//  ENCODE az64
	,uomcode VARCHAR(20)  		--//  ENCODE zstd
	,cashdiscrs NUMERIC(38,6)  		--//  ENCODE az64
	,cashdiscper NUMERIC(38,6)  		--//  ENCODE az64
	,octroi NUMERIC(38,6)  		--//  ENCODE az64
	,linelevelamt NUMERIC(38,6)  		--//  ENCODE az64
	,batchno VARCHAR(50)  		--//  ENCODE zstd
	,mnfgdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,expdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mrp NUMERIC(38,6)  		--//  ENCODE az64
	,lsp NUMERIC(38,6)  		--//  ENCODE az64
	,purtax NUMERIC(38,6)  		--//  ENCODE az64
	,purdisc NUMERIC(38,6)  		--//  ENCODE az64
	,purrate NUMERIC(38,6)  		--//  ENCODE az64
	,sellrate NUMERIC(38,6)  		--//  ENCODE az64
	,sellrateat NUMERIC(38,6)  		--//  ENCODE az64
	,sellrateavat NUMERIC(38,6)  		--//  ENCODE az64
	,vattaxvalue NUMERIC(38,6)  		--//  ENCODE az64
	,status VARCHAR(50)  		--//  ENCODE zstd
	,freescheme numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schemerefrno VARCHAR(100)  		--//  ENCODE zstd
	,waybillno VARCHAR(50)  		--//  ENCODE zstd
	,designcode VARCHAR(50)  		--//  ENCODE zstd
	,bundledeal VARCHAR(50)  		--//  ENCODE zstd
	,createduserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,migrationflag VARCHAR(100)  		--//  ENCODE zstd
	,mproductcode VARCHAR(50)  		--//  ENCODE zstd
	,taxablevalue NUMERIC(18,6)  		--//  ENCODE az64
	,cgstper NUMERIC(18,6)  		--//  ENCODE az64
	,sgstper NUMERIC(18,6)  		--//  ENCODE az64
	,utgstper NUMERIC(18,6)  		--//  ENCODE az64
	,igstper NUMERIC(18,6)  		--//  ENCODE az64
	,cessper NUMERIC(18,6)  		--//  ENCODE az64
	,othertax1per NUMERIC(18,6)  		--//  ENCODE az64
	,othertax2per NUMERIC(18,6)  		--//  ENCODE az64
	,othertax3per NUMERIC(18,6)  		--//  ENCODE az64
	,cgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,sgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,utgstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,igstvalue NUMERIC(18,6)  		--//  ENCODE az64
	,cessvalue NUMERIC(18,6)  		--//  ENCODE az64
	,othertax1value NUMERIC(18,6)  		--//  ENCODE az64
	,othertax2value NUMERIC(18,6)  		--//  ENCODE az64
	,othertax3value NUMERIC(18,6)  		--//  ENCODE az64
	,reversecharges NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother1 NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother2 NUMERIC(18,6)  		--//  ENCODE az64
	,taxvalueother3 NUMERIC(18,6)  		--//  ENCODE az64
	,taxtype VARCHAR(10)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,downloadflag VARCHAR(3)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_rrl_retailermaster;
CREATE TABLE IF NOT EXISTS WKS_RRL_RETAILERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rrl_retailermaster
(
	retailercode VARCHAR(50)  		--//  ENCODE lzo
	,src_retailername VARCHAR(100)  		--//  ENCODE lzo
	,tgt_retailername VARCHAR(100)  		--//  ENCODE lzo
	,routecode VARCHAR(25)  		--//  ENCODE lzo
	,retailerclasscode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,foodlicenseno VARCHAR(50)  		--//  ENCODE lzo
	,druglicenseno VARCHAR(50)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,phone VARCHAR(15)  		--//  ENCODE lzo
	,mobile VARCHAR(15)  		--//  ENCODE lzo
	,prcontact VARCHAR(50)  		--//  ENCODE lzo
	,seccontact VARCHAR(50)  		--//  ENCODE lzo
	,creditlimit NUMERIC(18,0)  		--//  ENCODE az64
	,creditperiod numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invoicelimit VARCHAR(30)  		--//  ENCODE lzo
	,isapproved CHAR(1)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,rsrcode VARCHAR(100)  		--//  ENCODE lzo
	,drugvaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fssaivaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,src_displaystatus VARCHAR(20)  		--//  ENCODE lzo
	,tgt_displaystatus VARCHAR(20)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,ownername VARCHAR(100)  		--//  ENCODE lzo
	,druglicenseno2 VARCHAR(50)  		--//  ENCODE lzo
	,r_statecode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,r_districtcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,r_tahsilcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,address1 VARCHAR(100)  		--//  ENCODE lzo
	,address2 VARCHAR(100)  		--//  ENCODE lzo
	,retailerchannelcode VARCHAR(40)  		--//  ENCODE lzo
	,retailerclassid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,chng_flg VARCHAR(2)  		--//  ENCODE lzo
	,actv_flg VARCHAR(1)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rstockdiscrepancy_withproduct;
CREATE TABLE IF NOT EXISTS WKS_RSTOCKDISCREPANCY_WITHPRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rstockdiscrepancy_withproduct
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,prdcode VARCHAR(50)  		--//  ENCODE zstd
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,openingdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,closingdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalopenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offeropenstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerpurchase numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerpurreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salsales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalsales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offersales numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstockin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstockout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,damagein numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,damageout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salsalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalsalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offersalesreturn numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstkjurin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerstkjurout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerbattfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerbattfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sallcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsallcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerlcntfrin numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sallcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsallcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerlcntfrout numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salreplacement numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerreplacement numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclsstock numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calsalclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calunsalclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,unsalclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,calofferclosing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,offerclosingdiff numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,lp NUMERIC(18,6)  		--//  ENCODE az64
	,ptr NUMERIC(18,6)  		--//  ENCODE az64
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_rt_geocoordinates;
CREATE TABLE IF NOT EXISTS WKS_RT_GEOCOORDINATES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_geocoordinates
(
	rgc_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rgc_usercode VARCHAR(50)  		--//  ENCODE lzo
	,rgc_distcode VARCHAR(50)  		--//  ENCODE lzo
	,rgc_code VARCHAR(50)  		--//  ENCODE lzo
	,rgc_latitude VARCHAR(20)  		--//  ENCODE lzo
	,rgc_longtitude VARCHAR(20)  		--//  ENCODE lzo
	,rgc_geouniqueid VARCHAR(100)  		--//  ENCODE lzo
	,rgc_createdby VARCHAR(20)  		--//  ENCODE lzo
	,rgc_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rgc_modifiedby VARCHAR(20)  		--//  ENCODE lzo
	,rgc_modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rgc_flag VARCHAR(1)  		--//  ENCODE lzo
	,rgc_status_flag VARCHAR(1)  		--//  ENCODE lzo
	,rgc_flex VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_retailercategory;
CREATE TABLE IF NOT EXISTS WKS_RT_RETAILERCATEGORY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_retailercategory
(
	retailercategorycode VARCHAR(10)  		--//  ENCODE lzo
	,retailercategoryname VARCHAR(50)  		--//  ENCODE lzo
	,ctgcode VARCHAR(40)  		--//  ENCODE lzo
	,ctglinkid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ctglevelid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isbrandshow numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isactive BOOLEAN
	,rowid VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_retailermaster;
CREATE TABLE IF NOT EXISTS WKS_RT_RETAILERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_retailermaster
(
	retailercode VARCHAR(50)  		--//  ENCODE lzo
	,retailername VARCHAR(100)  		--//  ENCODE lzo
	,routecode VARCHAR(25)  		--//  ENCODE lzo
	,retailerclasscode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,foodlicenseno VARCHAR(50)  		--//  ENCODE lzo
	,druglicenseno VARCHAR(50)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,phone VARCHAR(15)  		--//  ENCODE lzo
	,mobile VARCHAR(15)  		--//  ENCODE lzo
	,prcontact VARCHAR(50)  		--//  ENCODE lzo
	,seccontact VARCHAR(50)  		--//  ENCODE lzo
	,creditlimit NUMERIC(18,0)  		--//  ENCODE az64
	,creditperiod numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,invoicelimit VARCHAR(30)  		--//  ENCODE lzo
	,isapproved CHAR(1)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,rsrcode VARCHAR(100)  		--//  ENCODE lzo
	,drugvaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fssaivaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,displaystatus VARCHAR(20)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,ownername VARCHAR(100)  		--//  ENCODE lzo
	,druglicenseno2 VARCHAR(50)  		--//  ENCODE lzo
	,r_statecode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,r_districtcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,r_tahsilcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,address1 VARCHAR(100)  		--//  ENCODE lzo
	,address2 VARCHAR(100)  		--//  ENCODE lzo
	,retailerchannelcode VARCHAR(40)  		--//  ENCODE lzo
	,retailerclassid numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_rt_retailervalueclass;
CREATE TABLE IF NOT EXISTS WKS_RT_RETAILERVALUECLASS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_retailervalueclass
(
	classid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,valueclasscode VARCHAR(40)  		--//  ENCODE lzo
	,valueclassname VARCHAR(100)  		--//  ENCODE lzo
	,ctgmainid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isactive BOOLEAN
	,distcode VARCHAR(100)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_routemaster;
CREATE TABLE IF NOT EXISTS WKS_RT_ROUTEMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_routemaster
(
	routecode VARCHAR(50)  		--//  ENCODE lzo
	,routeename VARCHAR(100)  		--//  ENCODE lzo
	,flag CHAR(2)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,distributorcode VARCHAR(28)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_rsdmaster;
CREATE TABLE IF NOT EXISTS WKS_RT_RSDMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_rsdmaster
(
	rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,rsdname VARCHAR(50)  		--//  ENCODE lzo
	,rsdfirm VARCHAR(50)  		--//  ENCODE lzo
	,rsrcode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,montlyincome NUMERIC(18,2)  		--//  ENCODE az64
	,manpower numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,godownspace VARCHAR(50)  		--//  ENCODE lzo
	,address VARCHAR(200)  		--//  ENCODE lzo
	,contactno VARCHAR(50)  		--//  ENCODE lzo
	,druglicense VARCHAR(50)  		--//  ENCODE lzo
	,foodlicense VARCHAR(50)  		--//  ENCODE lzo
	,isownhouse CHAR(1)  		--//  ENCODE lzo
	,isnative CHAR(1)  		--//  ENCODE lzo
	,drugvaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,fssaivaliditydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isapproved CHAR(1)  		--//  ENCODE lzo
	,flag CHAR(1)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,createdby VARCHAR(50)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modifiedby VARCHAR(50)  		--//  ENCODE lzo
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,deleteddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,channelname VARCHAR(100)  		--//  ENCODE lzo
	,subchannelid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,subchannelname VARCHAR(100)  		--//  ENCODE lzo
	,categoryid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,categoryname VARCHAR(100)  		--//  ENCODE lzo
	,outlettype VARCHAR(30)  		--//  ENCODE lzo
	,modaloutlet VARCHAR(30)  		--//  ENCODE lzo
	,synctimestamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,contactperson VARCHAR(100)  		--//  ENCODE lzo
	,rsdemailid VARCHAR(50)  		--//  ENCODE lzo
	,druglicenseno2 VARCHAR(50)  		--//  ENCODE lzo
	,rsdemailid1 VARCHAR(50)  		--//  ENCODE lzo
	,rsdemailid2 VARCHAR(50)  		--//  ENCODE lzo
	,salesrepemailid VARCHAR(50)  		--//  ENCODE lzo
	,routecode VARCHAR(100)  		--//  ENCODE lzo
	,rtrclassid numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_rt_ruralstoreorderdetail;
CREATE TABLE IF NOT EXISTS WKS_RT_RURALSTOREORDERDETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_ruralstoreorderdetail
(
	orderid VARCHAR(100)  		--//  ENCODE lzo
	,productid VARCHAR(100)  		--//  ENCODE lzo
	,uomid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,price NUMERIC(18,2)  		--//  ENCODE az64
	,netprice NUMERIC(18,2)  		--//  ENCODE az64
	,discountvalue numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,foc numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,tax NUMERIC(4,2)  		--//  ENCODE az64
	,status CHAR(4)  		--//  ENCODE lzo
	,flag CHAR(1)  		--//  ENCODE lzo
	,usercode VARCHAR(50)  		--//  ENCODE lzo
	,ordd_distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,uom VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_ruralstoreorderheader;
CREATE TABLE IF NOT EXISTS WKS_RT_RURALSTOREORDERHEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_ruralstoreorderheader
(
	orderid VARCHAR(50)  		--//  ENCODE lzo
	,orderdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,deliverydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,ovid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,usercode VARCHAR(100)  		--//  ENCODE lzo
	,ordervalue NUMERIC(18,2)  		--//  ENCODE az64
	,linespercall numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,feedback VARCHAR(500)  		--//  ENCODE lzo
	,orderstarttime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,orderendtime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,outletsignature VARCHAR(100)  		--//  ENCODE lzo
	,islocked BOOLEAN
	,flag CHAR(2)  		--//  ENCODE lzo
	,saletype CHAR(2)  		--//  ENCODE lzo
	,retailerid VARCHAR(100)  		--//  ENCODE lzo
	,invoicestatus CHAR(2)  		--//  ENCODE lzo
	,billdiscount NUMERIC(18,2)  		--//  ENCODE az64
	,tax NUMERIC(18,2)  		--//  ENCODE az64
	,isjointcall BOOLEAN
	,ord_distributorcode VARCHAR(100)  		--//  ENCODE lzo
	,weekid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rsd_code VARCHAR(50)  		--//  ENCODE lzo
	,row_id VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_townmaster;
CREATE TABLE IF NOT EXISTS WKS_RT_TOWNMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_townmaster
(
	routecode VARCHAR(50)  		--//  ENCODE lzo
	,villagecode VARCHAR(50)  		--//  ENCODE lzo
	,villagename VARCHAR(200)  		--//  ENCODE lzo
	,population numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,rsrcode VARCHAR(50)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,distributorcode VARCHAR(50)  		--//  ENCODE lzo
	,sarpanchname VARCHAR(50)  		--//  ENCODE lzo
	,sarpanchno VARCHAR(50)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdby VARCHAR(50)  		--//  ENCODE lzo
	,updateddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updatedby VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_udcmapping;
CREATE TABLE IF NOT EXISTS WKS_RT_UDCMAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_udcmapping
(
	distcode VARCHAR(100)  		--//  ENCODE lzo
	,rsdcode VARCHAR(50)  		--//  ENCODE lzo
	,outletcode VARCHAR(50)  		--//  ENCODE lzo
	,usercode VARCHAR(100)  		--//  ENCODE lzo
	,udccode VARCHAR(50)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,isactive BOOLEAN
	,isdelflag CHAR(1)  		--//  ENCODE lzo
	,rowid VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_udcmaster;
CREATE TABLE IF NOT EXISTS WKS_RT_UDCMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_udcmaster
(
	udccode VARCHAR(50)  		--//  ENCODE lzo
	,udcname VARCHAR(200)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,rowid VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_rt_usermaster;
CREATE TABLE IF NOT EXISTS WKS_RT_USERMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rt_usermaster
(
	userid numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,usercode VARCHAR(50)  		--//  ENCODE lzo
	,login VARCHAR(200)  		--//  ENCODE lzo
	,password VARCHAR(100)  		--//  ENCODE lzo
	,eusername VARCHAR(100)  		--//  ENCODE lzo
	,userlevel numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,parentid VARCHAR(100)  		--//  ENCODE lzo
	,isactive BOOLEAN
	,teritoryid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,abnumber VARCHAR(100)  		--//  ENCODE lzo
	,forumcode VARCHAR(100)  		--//  ENCODE lzo
	,regionid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,emailid VARCHAR(200)  		--//  ENCODE lzo
	,currentversion VARCHAR(20)  		--//  ENCODE lzo
	,updateversion VARCHAR(20)  		--//  ENCODE lzo
	,imei VARCHAR(100)  		--//  ENCODE lzo
	,mobileno VARCHAR(50)  		--//  ENCODE lzo
	,locationid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ishht CHAR(1)  		--//  ENCODE lzo
	,user_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,distuserid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,freezeday numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_rtbl_idtmanagementupload;
CREATE TABLE IF NOT EXISTS WKS_RTBL_IDTMANAGEMENTUPLOAD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_rtbl_idtmanagementupload
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,idtmngrefno VARCHAR(50)  		--//  ENCODE zstd
	,idtmngdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,idtdistcode VARCHAR(50)  		--//  ENCODE zstd
	,idtdistname VARCHAR(100)  		--//  ENCODE zstd
	,lcncode VARCHAR(50)  		--//  ENCODE zstd
	,lcnname VARCHAR(200)  		--//  ENCODE zstd
	,stkmgmttype VARCHAR(30)  		--//  ENCODE zstd
	,docrefno VARCHAR(100)  		--//  ENCODE zstd
	,lrnumber VARCHAR(100)  		--//  ENCODE zstd
	,lrdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,remarks VARCHAR(200)  		--//  ENCODE zstd
	,status VARCHAR(100)  		--//  ENCODE zstd
	,idtgrossamt NUMERIC(38,2)  		--//  ENCODE az64
	,idttaxamt NUMERIC(38,2)  		--//  ENCODE az64
	,idtnetamt NUMERIC(38,2)  		--//  ENCODE az64
	,idtpaidamt NUMERIC(38,2)  		--//  ENCODE az64
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,batchcode VARCHAR(50)  		--//  ENCODE zstd
	,systemstocktype numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,stocktype VARCHAR(50)  		--//  ENCODE zstd
	,baseqty NUMERIC(18,0)  		--//  ENCODE az64
	,mrp NUMERIC(18,2)  		--//  ENCODE az64
	,listprice NUMERIC(18,2)  		--//  ENCODE az64
	,productgrossamt NUMERIC(38,2)  		--//  ENCODE az64
	,producttaxamt NUMERIC(38,2)  		--//  ENCODE az64
	,productnetamt NUMERIC(38,2)  		--//  ENCODE az64
	,downloadstatus VARCHAR(4)  		--//  ENCODE zstd
	,syncid NUMERIC(38,0)  		--//  ENCODE az64
	,serverdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mnfdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,expdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_sku_recom_flag;
CREATE TABLE IF NOT EXISTS WKS_SKU_RECOM_FLAG		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sku_recom_flag
(
	mnth_id VARCHAR(50)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,dist_outlet_cd VARCHAR(50)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,oos_flag VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag VARCHAR(50)  		--//  ENCODE lzo
	,cs_flag VARCHAR(50)  		--//  ENCODE lzo
	,soq VARCHAR(50)  		--//  ENCODE lzo
	,unique_ret_cd VARCHAR(50)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_sku_recom_orangestoretarget;
CREATE TABLE IF NOT EXISTS WKS_SKU_RECOM_ORANGESTORETARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sku_recom_orangestoretarget
(
	fromdate numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,todate numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rsmcode VARCHAR(50)  		--//  ENCODE lzo
	,rsmname VARCHAR(50)  		--//  ENCODE lzo
	,asmcode VARCHAR(50)  		--//  ENCODE lzo
	,asmname VARCHAR(50)  		--//  ENCODE lzo
	,abicode VARCHAR(50)  		--//  ENCODE lzo
	,abiname VARCHAR(50)  		--//  ENCODE lzo
	,channelcode VARCHAR(50)  		--//  ENCODE lzo
	,channelname VARCHAR(50)  		--//  ENCODE lzo
	,subchannelcode VARCHAR(50)  		--//  ENCODE lzo
	,subchannelname VARCHAR(50)  		--//  ENCODE lzo
	,class VARCHAR(50)  		--//  ENCODE lzo
	,percentage NUMERIC(38,2)  		--//  ENCODE az64
	,dateofsharing numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_sku_recom_spike_msl_mthly_analytics;
CREATE TABLE IF NOT EXISTS WKS_SKU_RECOM_SPIKE_MSL_MTHLY_ANALYTICS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sku_recom_spike_msl_mthly_analytics
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,count_of_retailers numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,recos NUMERIC(38,0)  		--//  ENCODE az64
	,hits NUMERIC(38,0)  		--//  ENCODE az64
	,no_of_orange_stores numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,orange_store_perc NUMERIC(38,17)  		--//  ENCODE az64
	,store_tag VARCHAR(28)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_skurecom_mi_target_retdim;
CREATE TABLE IF NOT EXISTS WKS_SKURECOM_MI_TARGET_RETDIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_skurecom_mi_target_retdim
(
	retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,customer_code VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_address1 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address2 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address3 VARCHAR(250)  		--//  ENCODE lzo
	,region_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_classification VARCHAR(50)  		--//  ENCODE lzo
	,territory_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_classification VARCHAR(50)  		--//  ENCODE lzo
	,state_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,state_name VARCHAR(50)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,town_classification VARCHAR(50)  		--//  ENCODE lzo
	,class_code VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,outlet_type VARCHAR(50)  		--//  ENCODE lzo
	,channel_code VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,loyalty_desc VARCHAR(50)  		--//  ENCODE lzo
	,registration_date numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,status_cd VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,actv_flg CHAR(1)  		--//  ENCODE lzo
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_rec_dt DATE  		--//  ENCODE az64
	,rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_skurecom_mi_target_tmp1;
CREATE TABLE IF NOT EXISTS WKS_SKURECOM_MI_TARGET_TMP1		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_skurecom_mi_target_tmp1
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
)

;
--DROP TABLE wks_skurecom_mi_target_tmp2;
CREATE TABLE IF NOT EXISTS WKS_SKURECOM_MI_TARGET_TMP2		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_skurecom_mi_target_tmp2
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mothersku_name VARCHAR(100)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_skurecom_mi_target_tmp3;
CREATE TABLE IF NOT EXISTS WKS_SKURECOM_MI_TARGET_TMP3		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_skurecom_mi_target_tmp3
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mothersku_name VARCHAR(100)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_sss_msl_sales_combined_master_data;
CREATE TABLE IF NOT EXISTS WKS_SSS_MSL_SALES_COMBINED_MASTER_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sss_msl_sales_combined_master_data
(
	region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(251)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(12)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(150)  		--//  ENCODE lzo
)

;
--DROP TABLE wks_sss_rebase_calculation;
CREATE TABLE IF NOT EXISTS WKS_SSS_REBASE_CALCULATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sss_rebase_calculation
(
	table_rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(1000)  		--//  ENCODE lzo
	,program_type VARCHAR(50)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(75)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,kpi_score NUMERIC(30,24)  		--//  ENCODE az64
	,sum_kpi_score NUMERIC(38,24)  		--//  ENCODE az64
	,calculated_kpi_score NUMERIC(38,13)  		--//  ENCODE az64
	,rebase_score NUMERIC(38,28)  		--//  ENCODE az64
)

;
--DROP TABLE wks_sss_sales_base_data_for_msl;
CREATE TABLE IF NOT EXISTS WKS_SSS_SALES_BASE_DATA_FOR_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sss_sales_base_data_for_msl
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,retail_environment VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(100)  		--//  ENCODE lzo
	,store_name VARCHAR(251)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,prod_hier_l3 VARCHAR(12)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l5 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l6 VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l7 VARCHAR(1)  		--//  ENCODE lzo
	,prod_hier_l8 VARCHAR(1)  		--//  ENCODE lzo
	,product_code VARCHAR(150)  		--//  ENCODE lzo
	,prod_hier_l9 VARCHAR(150)  		--//  ENCODE lzo
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
)

;
--DROP TABLE wks_sss_score_det_for_rebase;
CREATE TABLE IF NOT EXISTS WKS_SSS_SCORE_DET_FOR_REBASE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sss_score_det_for_rebase
(
	table_rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,store_code VARCHAR(50)  		--//  ENCODE lzo
	,store_name VARCHAR(1000)  		--//  ENCODE lzo
	,program_type VARCHAR(50)  		--//  ENCODE lzo
	,franchise VARCHAR(50)  		--//  ENCODE lzo
	,prod_hier_l4 VARCHAR(100)  		--//  ENCODE lzo
	,kpi VARCHAR(75)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,kpi_score NUMERIC(30,24)  		--//  ENCODE az64
)

;
--DROP TABLE wks_sss_scorecard_data;
CREATE TABLE IF NOT EXISTS WKS_SSS_SCORECARD_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_sss_scorecard_data
(
	program_type VARCHAR(50)  		--//  ENCODE zstd
	,jnj_id VARCHAR(50)  		--//  ENCODE zstd
	,rs_id VARCHAR(50)  		--//  ENCODE zstd
	,outlet_name VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(50)  		--//  ENCODE zstd
	,zone VARCHAR(50)  		--//  ENCODE zstd
	,territory VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(50)  		--//  ENCODE zstd
	,kpi VARCHAR(50)  		--//  ENCODE zstd
	,quarter VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,target VARCHAR(50)  		--//  ENCODE zstd
	,actual VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE wks_stores_with_msl_in_year_qtr;
CREATE TABLE IF NOT EXISTS WKS_STORES_WITH_MSL_IN_YEAR_QTR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_stores_with_msl_in_year_qtr
(
	store_code VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,no_of_distinct_kpi numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

;
--DROP TABLE wks_tmp_date;
CREATE TABLE IF NOT EXISTS WKS_TMP_DATE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_tmp_date
(
	currtime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE wks_winculum_dailysales;
CREATE TABLE IF NOT EXISTS WKS_WINCULUM_DAILYSALES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_winculum_dailysales
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,salinvno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,total_price NUMERIC(18,6)  		--//  ENCODE az64
	,tax NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE wks_winculum_salesreturn;
CREATE TABLE IF NOT EXISTS WKS_WINCULUM_SALESRETURN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_winculum_salesreturn
(
	distcode VARCHAR(50)  		--//  ENCODE zstd
	,srndate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,srnrefno VARCHAR(50)  		--//  ENCODE zstd
	,rtrcode VARCHAR(100)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nr NUMERIC(18,6)  		--//  ENCODE az64
	,total_price NUMERIC(18,6)  		--//  ENCODE az64
	,tax NUMERIC(38,6)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE wks_xdm_businesscalendar;
CREATE TABLE IF NOT EXISTS WKS_XDM_BUSINESSCALENDAR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_businesscalendar
(
	salinvdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,month VARCHAR(50)  		--//  ENCODE zstd
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week VARCHAR(50)  		--//  ENCODE zstd
	,monthkey numeric(18,0)		--//  ENCODE az64 // INTEGER  
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_channelmaster;
CREATE TABLE IF NOT EXISTS WKS_XDM_CHANNELMASTER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_channelmaster
(
	cmpcode VARCHAR(50)  		--//  ENCODE zstd
	,channelcode VARCHAR(100)  		--//  ENCODE zstd
	,channelname VARCHAR(100)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,clicktype VARCHAR(10)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_distributor;
CREATE TABLE IF NOT EXISTS WKS_XDM_DISTRIBUTOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_distributor
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,usercode VARCHAR(50)  		--//  ENCODE zstd
	,distrname VARCHAR(150)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(30)  		--//  ENCODE zstd
	,distrbrtype VARCHAR(2)  		--//  ENCODE zstd
	,distrbrname VARCHAR(100)  		--//  ENCODE zstd
	,distrbraddr1 VARCHAR(200)  		--//  ENCODE zstd
	,distrbraddr2 VARCHAR(200)  		--//  ENCODE zstd
	,distrbraddr3 VARCHAR(200)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,distrbrstate VARCHAR(50)  		--//  ENCODE zstd
	,postalcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,country VARCHAR(50)  		--//  ENCODE zstd
	,contactperson VARCHAR(100)  		--//  ENCODE zstd
	,phone VARCHAR(15)  		--//  ENCODE zstd
	,emailid VARCHAR(50)  		--//  ENCODE zstd
	,isactive VARCHAR(1)  		--//  ENCODE zstd
	,gstinnumber VARCHAR(15)  		--//  ENCODE zstd
	,gstdistrtype VARCHAR(1)  		--//  ENCODE zstd
	,gststatecode VARCHAR(3)  		--//  ENCODE zstd
	,others1 VARCHAR(15)  		--//  ENCODE zstd
	,isdirectacct VARCHAR(1)  		--//  ENCODE zstd
	,typecode VARCHAR(50)  		--//  ENCODE zstd
	,psnonps VARCHAR(1)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_geohierarchy;
CREATE TABLE IF NOT EXISTS WKS_XDM_GEOHIERARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_geohierarchy
(
	statecode VARCHAR(50)  		--//  ENCODE zstd
	,statename VARCHAR(100)  		--//  ENCODE zstd
	,districtcode VARCHAR(50)  		--//  ENCODE zstd
	,districtname VARCHAR(100)  		--//  ENCODE zstd
	,thesilcode VARCHAR(50)  		--//  ENCODE zstd
	,thesilname VARCHAR(100)  		--//  ENCODE zstd
	,citycode VARCHAR(50)  		--//  ENCODE zstd
	,cityname VARCHAR(100)  		--//  ENCODE zstd
	,distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorname VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_product;
CREATE TABLE IF NOT EXISTS WKS_XDM_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_product
(
	prodcompany VARCHAR(10)  		--//  ENCODE zstd
	,productcode VARCHAR(50)  		--//  ENCODE zstd
	,productuom VARCHAR(5)  		--//  ENCODE zstd
	,uomconvfactor numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prodhierarchylvl VARCHAR(50)  		--//  ENCODE zstd
	,prodhierarchyval VARCHAR(50)  		--//  ENCODE zstd
	,productname VARCHAR(400)  		--//  ENCODE zstd
	,prodshortname VARCHAR(400)  		--//  ENCODE zstd
	,productcmpcode VARCHAR(50)  		--//  ENCODE zstd
	,stockcovdays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,productweight NUMERIC(9,3)  		--//  ENCODE az64
	,productunit VARCHAR(5)  		--//  ENCODE zstd
	,productstatus VARCHAR(1)  		--//  ENCODE zstd
	,productdrugtype VARCHAR(1)  		--//  ENCODE zstd
	,serialno VARCHAR(1)  		--//  ENCODE zstd
	,shelflife numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchisecode VARCHAR(50)  		--//  ENCODE zstd
	,franchisename VARCHAR(100)  		--//  ENCODE zstd
	,brandcode VARCHAR(50)  		--//  ENCODE zstd
	,brandname VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,variantcode VARCHAR(50)  		--//  ENCODE zstd
	,variantname VARCHAR(100)  		--//  ENCODE zstd
	,motherskucode VARCHAR(50)  		--//  ENCODE zstd
	,motherskuname VARCHAR(100)  		--//  ENCODE zstd
	,eanno VARCHAR(50)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_productuom;
CREATE TABLE IF NOT EXISTS WKS_XDM_PRODUCTUOM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_productuom
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,prodcode VARCHAR(50)  		--//  ENCODE zstd
	,uomcode VARCHAR(5)  		--//  ENCODE zstd
	,uomconvfactor numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,modusercode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_salesheirarchy;
CREATE TABLE IF NOT EXISTS WKS_XDM_SALESHEIRARCHY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_salesheirarchy
(
	rsmcode VARCHAR(50)  		--//  ENCODE zstd
	,rsmname VARCHAR(100)  		--//  ENCODE zstd
	,rsm_flmasmcode VARCHAR(50)  		--//  ENCODE zstd
	,flmasmcode VARCHAR(50)  		--//  ENCODE zstd
	,flmasmname VARCHAR(100)  		--//  ENCODE zstd
	,flmasm_abicode VARCHAR(50)  		--//  ENCODE zstd
	,abicode VARCHAR(50)  		--//  ENCODE zstd
	,abiname VARCHAR(100)  		--//  ENCODE zstd
	,abi_distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorcode VARCHAR(50)  		--//  ENCODE zstd
	,distributorname VARCHAR(100)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_salesmanskulinemapping;
CREATE TABLE IF NOT EXISTS WKS_XDM_SALESMANSKULINEMAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_salesmanskulinemapping
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,distrcode VARCHAR(50)  		--//  ENCODE zstd
	,distrbrcode VARCHAR(50)  		--//  ENCODE zstd
	,salesmancode VARCHAR(50)  DEFAULT ''::varchar		--//  ENCODE zstd // character varying
	,skuline VARCHAR(50)  		--//  ENCODE zstd
	,skuhierlevelcode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,skuhiervaluecode VARCHAR(50)  		--//  ENCODE zstd
	,moddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,motherskucode VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE wks_xdm_supplier;
CREATE TABLE IF NOT EXISTS WKS_XDM_SUPPLIER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS wks_xdm_supplier
(
	cmpcode VARCHAR(10)  		--//  ENCODE zstd
	,supcode VARCHAR(30)  		--//  ENCODE zstd
	,suptype VARCHAR(1)  		--//  ENCODE zstd
	,supname VARCHAR(100)  		--//  ENCODE zstd
	,supaddr1 VARCHAR(50)  		--//  ENCODE zstd
	,supaddr2 VARCHAR(50)  		--//  ENCODE zstd
	,supaddr3 VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,state VARCHAR(50)  		--//  ENCODE zstd
	,country VARCHAR(50)  		--//  ENCODE zstd
	,gststatecode VARCHAR(30)  		--//  ENCODE zstd
	,suppliergstin VARCHAR(15)  		--//  ENCODE zstd
	,createddt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;


use schema INDEDW_INTEGRATION;

--DROP TABLE edw_customer_dim;
CREATE TABLE IF NOT EXISTS EDW_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_customer_dim
(
	customer_code VARCHAR(20)  		--//  ENCODE zstd
	,region_code NUMERIC(18,0)  		--//  ENCODE az64
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_code NUMERIC(18,0)  		--//  ENCODE az64
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_classification VARCHAR(50)  		--//  ENCODE zstd
	,territory_code NUMERIC(18,0)  		--//  ENCODE az64
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_classification VARCHAR(50)  		--//  ENCODE zstd
	,state_code NUMERIC(18,0)  		--//  ENCODE az64
	,state_name VARCHAR(50)  		--//  ENCODE zstd
	,town_name VARCHAR(50)  		--//  ENCODE zstd
	,town_classification VARCHAR(100)  		--//  ENCODE zstd
	,city VARCHAR(150)  		--//  ENCODE zstd
	,type_code NUMERIC(18,0)  		--//  ENCODE az64
	,type_name VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,customer_address1 VARCHAR(150)  		--//  ENCODE zstd
	,customer_address2 VARCHAR(150)  		--//  ENCODE zstd
	,customer_address3 VARCHAR(150)  		--//  ENCODE zstd
	,active_flag VARCHAR(7)  		--//  ENCODE zstd
	,active_start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,wholesalercode VARCHAR(50)  		--//  ENCODE zstd
	,super_stockiest VARCHAR(50)  		--//  ENCODE zstd
	,direct_account_flag VARCHAR(7)  		--//  ENCODE zstd
	,abi_code numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,abi_name VARCHAR(101)  		--//  ENCODE zstd
	,rds_size VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,num_of_retailers VARCHAR(20)  		--//  ENCODE zstd
	,customer_type VARCHAR(255)  		--//  ENCODE zstd
	,psnonps CHAR(1)  		--//  ENCODE zstd
	,suppliedby NUMERIC(18,0)  		--//  ENCODE az64
	,cfa VARCHAR(50)  		--//  ENCODE zstd
	,cfa_name VARCHAR(50)  		--//  ENCODE zstd
	,town_code VARCHAR(50)  		--//  ENCODE zstd
)

;
--DROP TABLE edw_dailysales_fact;
CREATE TABLE IF NOT EXISTS EDW_DAILYSALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_dailysales_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE zstd
	,invoice_no VARCHAR(50)  		--//  ENCODE zstd
	,invoice_date numeric(18,0)		--// INTEGER
	,retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,retailer_name VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(100)  		--//  ENCODE zstd
	,salesman_name VARCHAR(200)  		--//  ENCODE zstd
	,route_code VARCHAR(100)  		--//  ENCODE zstd
	,route_name VARCHAR(200)  		--//  ENCODE zstd
	,quantity numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,gross_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,tax_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,net_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,nr_value NUMERIC(38,6)  		--//  ENCODE zstd
	,achievement_nr_val NUMERIC(38,6)  		--//  ENCODE zstd
	,srnreftype VARCHAR(100)  		--//  ENCODE zstd
	,salinvmode VARCHAR(100)  		--//  ENCODE zstd
	,salinvtype VARCHAR(100)  		--//  ENCODE zstd
	,sku_rec_qty numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,sku_rec_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,qps_qty numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,qps_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,achievement_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,prd_sch_disc_amt NUMERIC(38,6)  		--//  ENCODE zstd
	,no_of_lines numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,no_of_reco_sku_lines numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,saleflag VARCHAR(1)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,status VARCHAR(20)  		--//  ENCODE zstd
	,prdfreeqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	invoice_date
		--// 	)
;		--// ;
--DROP TABLE edw_day_cls_stock_fact;
CREATE TABLE IF NOT EXISTS EDW_DAY_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_day_cls_stock_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,stock_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,salclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,salclsstockval DOUBLE PRECISION
	,unsalclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,offerclsstock NUMERIC(18,0)  		--//  ENCODE delta
	,salstockin NUMERIC(18,0)  		--//  ENCODE delta
	,salstockout NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockin NUMERIC(18,0)  		--//  ENCODE delta
	,unsalstockout NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockin NUMERIC(18,0)  		--//  ENCODE delta
	,offerstockout NUMERIC(18,0)  		--//  ENCODE delta
	,nr DOUBLE PRECISION
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_ecommerce_offtake;
CREATE TABLE IF NOT EXISTS EDW_ECOMMERCE_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_ecommerce_offtake
(
	transaction_date VARCHAR(20) NOT NULL 		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,platform VARCHAR(255)  		--//  ENCODE lzo
	,account_name VARCHAR(255) NOT NULL 		--//  ENCODE lzo
	,account_sku_code VARCHAR(255)  		--//  ENCODE lzo
	,retailer_product_name VARCHAR(65535)  		--//  ENCODE lzo
	,retailer_brand VARCHAR(2000)  		--//  ENCODE lzo
	,ean VARCHAR(255) NOT NULL 		--//  ENCODE lzo
	,sales_qty DOUBLE PRECISION
	,sales_value DOUBLE PRECISION
	,transaction_currency VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,generic_product_code VARCHAR(255)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_gt_target_dim;
CREATE TABLE IF NOT EXISTS EDW_GT_TARGET_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_gt_target_dim
(
	customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)
	,salesman_name VARCHAR(50)  		--//  ENCODE zstd
	,route_code VARCHAR(50)  		--//  ENCODE zstd
	,route_name VARCHAR(50)  		--//  ENCODE zstd
	,year numeric(18,0)		--// INTEGER
	,month numeric(18,0)		--// INTEGER
	,week numeric(18,0)		--// INTEGER
	,target_gt NUMERIC(32,8)  		--//  ENCODE zstd
	,target_bills NUMERIC(32,8)  		--//  ENCODE zstd
	,target_packs NUMERIC(32,8)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	year
		--// 	, month
		--// 	, week
		--// 	, salesman_code
		--// 	)
;		--// ;
--DROP TABLE edw_in_invoice_fact;
CREATE TABLE IF NOT EXISTS EDW_IN_INVOICE_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_in_invoice_fact
(
	customer_code VARCHAR(10)  		--//  ENCODE zstd
	,product_code VARCHAR(18)  		--//  ENCODE zstd
	,invoice_no VARCHAR(10)  		--//  ENCODE zstd
	,invoice_date DATE
	,invoice_val NUMERIC(38,17)  		--//  ENCODE zstd
	,invoice_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,wt_invoice_qty DOUBLE PRECISION  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	invoice_date
		--// 	)
;		--// ;
--DROP TABLE edw_it_target_dim;
CREATE TABLE IF NOT EXISTS EDW_IT_TARGET_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_it_target_dim
(
	territory_code numeric(18,0)		--// INTEGER
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,year numeric(18,0)		--// INTEGER
	,month numeric(18,0)		--// INTEGER
	,target NUMERIC(37,5)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
		--// SORTKEY ( 
		--// 	year
		--// 	, month
		--// 	, territory_code
		--// 	)
;		--// ;
--DROP TABLE edw_ka_sales_fact;
CREATE TABLE IF NOT EXISTS EDW_KA_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_ka_sales_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE zstd
	,invoice_date numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,retailer_code VARCHAR(50)
	,retailer_name VARCHAR(200)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,invoice_no VARCHAR(100)  		--//  ENCODE zstd
	,prdqty numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,prdtaxamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prdschdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,prddbdiscamt NUMERIC(38,6)  		--//  ENCODE zstd
	,salwdsamt NUMERIC(38,6)  		--//  ENCODE zstd
	,saleflag VARCHAR(10)  		--//  ENCODE zstd
	,confirmsales VARCHAR(10)  		--//  ENCODE zstd
	,subtotal4 NUMERIC(21,3)  		--//  ENCODE zstd
	,totalgrosssalesincltax NUMERIC(38,6)  		--//  ENCODE zstd
	,totalsalesnr NUMERIC(38,6)  		--//  ENCODE zstd
	,totalsalesconfirmed NUMERIC(38,6)  		--//  ENCODE zstd
	,totalsalesnrconfirmed NUMERIC(38,6)  		--//  ENCODE zstd
	,totalsalesunconfirmed NUMERIC(38,6)  		--//  ENCODE zstd
	,totalsalesnrunconfirmed NUMERIC(38,6)  		--//  ENCODE zstd
	,totalqtyconfirmed numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,totalqtyunconfirmed numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,buyingoutlets VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	retailer_code
		--// 	)
;		--// ;
--DROP TABLE edw_key_account_dim;
CREATE TABLE IF NOT EXISTS EDW_KEY_ACCOUNT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_key_account_dim
(
	ka_code VARCHAR(50)  		--//  ENCODE lzo
	,ka_name VARCHAR(50)  		--//  ENCODE lzo
	,ka_flag CHAR(1)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(50)  		--//  ENCODE lzo
	,parent_code VARCHAR(50)  		--//  ENCODE lzo
	,parent_name VARCHAR(50)  		--//  ENCODE lzo
	,ka_address1 VARCHAR(250)  		--//  ENCODE lzo
	,ka_address2 VARCHAR(250)  		--//  ENCODE lzo
	,ka_address3 VARCHAR(250)  		--//  ENCODE lzo
	,region_code VARCHAR(50)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_code VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_code VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,state_code VARCHAR(50)  		--//  ENCODE lzo
	,state_name VARCHAR(50)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,active_flag CHAR(1)  		--//  ENCODE lzo
	,abi_code VARCHAR(50)  		--//  ENCODE lzo
	,abi_name VARCHAR(50)  		--//  ENCODE lzo
	,plant VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_mi_sales_details;
CREATE TABLE IF NOT EXISTS EDW_MI_SALES_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_mi_sales_details
(
	week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE zstd
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,retailer_name VARCHAR(100)  		--//  ENCODE zstd
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(100)  		--//  ENCODE zstd
	,salesman_name VARCHAR(200)  		--//  ENCODE zstd
	,salesman_status VARCHAR(20)  		--//  ENCODE zstd
	,status_desc VARCHAR(10)  		--//  ENCODE zstd
	,active_flag VARCHAR(8)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,achievement_amt NUMERIC(38,6)  		--//  ENCODE az64
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,product_category_name VARCHAR(150)  		--//  ENCODE zstd
	,variant_name VARCHAR(50)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE zstd // INTEGER  
)

		--// SORTKEY ( 
		--// 	channel_name
		--// 	)
;		--// ;
--DROP TABLE edw_msl_spike_mi_msku_list;
CREATE TABLE IF NOT EXISTS EDW_MSL_SPIKE_MI_MSKU_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_msl_spike_mi_msku_list
(
	region_name VARCHAR(100)  		--//  ENCODE lzo
	,zone_name VARCHAR(100)  		--//  ENCODE lzo
	,total_subd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mothersku_name VARCHAR(100)  		--//  ENCODE lzo
	,period VARCHAR(6)  		--//  ENCODE zstd
)

;
--DROP TABLE edw_mth_cls_stock_fact;
CREATE TABLE IF NOT EXISTS EDW_MTH_CLS_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_mth_cls_stock_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,stock_month VARCHAR(10)  		--//  ENCODE lzo
	,stock_year VARCHAR(10)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,calsalclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calunsalclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calofferclosing NUMERIC(32,7)  		--//  ENCODE delta
	,calsalclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,calunsalclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,calofferclosingval NUMERIC(32,7)  		--//  ENCODE delta
	,salstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,salstockadjval NUMERIC(32,7)  		--//  ENCODE delta
	,unsalstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,unsalstockadjval NUMERIC(32,7)  		--//  ENCODE delta
	,offerstockadjqty NUMERIC(32,7)  		--//  ENCODE delta
	,offerstockadjval NUMERIC(32,7)  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_order_fact;
CREATE TABLE IF NOT EXISTS EDW_ORDER_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_order_fact
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,retailer_name VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(50)  		--//  ENCODE lzo
	,order_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,order_no VARCHAR(50)  		--//  ENCODE lzo
	,ord_qty numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,ord_amt NUMERIC(38,6)  		--//  ENCODE lzo
	,invoice_no VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_product_dim;
CREATE TABLE IF NOT EXISTS EDW_PRODUCT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_product_dim
(
	product_code VARCHAR(50)
	,product_name VARCHAR(50)  		--//  ENCODE lzo
	,product_desc VARCHAR(100)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,uom NUMERIC(18,0)  		--//  ENCODE lzo
	,std_nr NUMERIC(18,2)  		--//  ENCODE lzo
	,case_lot NUMERIC(18,2)  		--//  ENCODE lzo
	,sale_uom NUMERIC(18,0)  		--//  ENCODE lzo
	,sale_conversion_factor NUMERIC(18,0)  		--//  ENCODE lzo
	,base_uom NUMERIC(18,0)  		--//  ENCODE lzo
	,int_uom NUMERIC(18,0)  		--//  ENCODE lzo
	,gross_wt NUMERIC(13,3)  		--//  ENCODE lzo
	,net_wt NUMERIC(13,3)  		--//  ENCODE lzo
	,active_flag CHAR(1)  		--//  ENCODE lzo
	,delete_flag CHAR(1)  		--//  ENCODE lzo
	,shelf_life NUMERIC(18,0)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,franchise_code VARCHAR(50)  		--//  ENCODE zstd
	,brand_code VARCHAR(50)  		--//  ENCODE zstd
	,product_category_code VARCHAR(50)  		--//  ENCODE zstd
	,variant_code VARCHAR(50)  		--//  ENCODE zstd
	,mothersku_code VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE ALL
		--// SORTKEY ( 
		--// 	product_code
		--// 	)
;		--// ;
--DROP TABLE edw_retailer_calendar_dim;
CREATE TABLE IF NOT EXISTS EDW_RETAILER_CALENDAR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_retailer_calendar_dim
(
	caldate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,day numeric(18,0)		--// INTEGER
	,week numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mth_mm numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,mth_yyyymm numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,yyyyqtr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,month_nm VARCHAR(20)  		--//  ENCODE lzo
	,month_nm_shrt VARCHAR(3)  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
		--// SORTKEY ( 
		--// 	day
		--// 	)
;		--// ;
--DROP TABLE edw_retailer_dim;
CREATE TABLE IF NOT EXISTS EDW_RETAILER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_retailer_dim
(
	retailer_code VARCHAR(50)  		--//  ENCODE zstd
	,start_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_address1 VARCHAR(250)  		--//  ENCODE zstd
	,retailer_address2 VARCHAR(250)  		--//  ENCODE zstd
	,retailer_address3 VARCHAR(250)  		--//  ENCODE zstd
	,region_code numeric(38,0)		--// BIGINT
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_code numeric(38,0)		--// BIGINT
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_classification VARCHAR(50)  		--//  ENCODE zstd
	,territory_code numeric(38,0)		--// BIGINT
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_classification VARCHAR(50)  		--//  ENCODE zstd
	,state_code numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,state_name VARCHAR(50)  		--//  ENCODE zstd
	,town_name VARCHAR(50)  		--//  ENCODE text255
	,town_classification VARCHAR(50)  		--//  ENCODE zstd
	,class_code VARCHAR(50)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,outlet_type VARCHAR(50)  		--//  ENCODE zstd
	,channel_code VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,loyalty_desc VARCHAR(50)  		--//  ENCODE zstd
	,registration_date numeric(18,0)		--//  ENCODE zstd // INTEGER  
	,status_cd VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(10)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(50)  		--//  ENCODE zstd
	,rtrlatitude VARCHAR(40)  		--//  ENCODE zstd
	,rtrlongitude VARCHAR(40)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,file_rec_dt DATE  		--//  ENCODE zstd
	,type_name VARCHAR(50)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	region_code
		--// 	, zone_code
		--// 	, territory_code
		--// 	)
;		--// ;
--DROP TABLE edw_retailer_geocoordinates;
CREATE TABLE IF NOT EXISTS EDW_RETAILER_GEOCOORDINATES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_retailer_geocoordinates
(
	rgc_id numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rgc_usercode VARCHAR(50)  		--//  ENCODE zstd
	,rgc_distcode VARCHAR(50)  		--//  ENCODE zstd
	,rgc_code VARCHAR(50)  		--//  ENCODE zstd
	,rgc_latitude VARCHAR(20)  		--//  ENCODE zstd
	,rgc_longtitude VARCHAR(20)  		--//  ENCODE zstd
	,rgc_geouniqueid VARCHAR(100)  		--//  ENCODE zstd
	,rgc_createdby VARCHAR(20)  		--//  ENCODE zstd
	,rgc_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,rgc_modifiedby VARCHAR(20)  		--//  ENCODE zstd
	,rgc_modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,rgc_flag VARCHAR(1)  		--//  ENCODE zstd
	,rgc_status_flag VARCHAR(1)  		--//  ENCODE zstd
	,rgc_flex VARCHAR(200)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_route_dim;
CREATE TABLE IF NOT EXISTS EDW_ROUTE_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_route_dim
(
	distcode VARCHAR(100)  		--//  ENCODE zstd
	,smcode VARCHAR(100)
	,rmcode VARCHAR(100)
	,smname VARCHAR(100)  		--//  ENCODE zstd
	,rmname VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	smcode
		--// 	, rmcode
		--// 	)
;		--// ;
--DROP TABLE edw_rpt_cockpit_invoice_targets;
CREATE TABLE IF NOT EXISTS EDW_RPT_COCKPIT_INVOICE_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_cockpit_invoice_targets
(
	dataset VARCHAR(50)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(75)  		--//  ENCODE zstd
	,zone_name VARCHAR(75)  		--//  ENCODE zstd
	,territory_name VARCHAR(75)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_type VARCHAR(50)  		--//  ENCODE zstd
	,retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,brand_name VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,closing_stock NUMERIC(38,2)  		--//  ENCODE az64
	,billing_value NUMERIC(38,4)  		--//  ENCODE az64
	,measure_type VARCHAR(50)  		--//  ENCODE zstd
	,brand_focus_target NUMERIC(38,2)  		--//  ENCODE az64
	,business_plan_target NUMERIC(38,2)  		--//  ENCODE az64
	,mothersku_name VARCHAR(150)  		--//  ENCODE zstd
	,variant VARCHAR(150)  		--//  ENCODE zstd
)

;
--DROP TABLE edw_rpt_customer_growth;
CREATE TABLE IF NOT EXISTS EDW_RPT_CUSTOMER_GROWTH		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_customer_growth
(
	retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,town_name VARCHAR(50)  		--//  ENCODE zstd
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,brand_name VARCHAR(50)  		--//  ENCODE zstd
	,variant_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_name VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_1 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_3 VARCHAR(200)  		--//  ENCODE zstd
	,zone_code NUMERIC(18,0)  		--//  ENCODE az64
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_code NUMERIC(18,0)  		--//  ENCODE az64
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,type_name VARCHAR(50)  		--//  ENCODE zstd
	,super_stockiest VARCHAR(50)  		--//  ENCODE zstd
	,state_code NUMERIC(18,0)  		--//  ENCODE az64
	,class_code VARCHAR(50)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(100)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(15)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nocb_count numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,max_ach_nr_sum NUMERIC(38,6)  		--//  ENCODE az64
	,retailer_count numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE edw_rpt_decision_cockpit_details;
CREATE TABLE IF NOT EXISTS EDW_RPT_DECISION_COCKPIT_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_decision_cockpit_details
(
	dataset VARCHAR(30)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,customer_code VARCHAR(50)  		--//  ENCODE zstd
	,retailer_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,brand_name VARCHAR(50)  		--//  ENCODE zstd
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_classification VARCHAR(50)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(50)  		--//  ENCODE zstd
	,salesman_name VARCHAR(100)  		--//  ENCODE zstd
	,salesman_code VARCHAR(200)  		--//  ENCODE zstd
	,uniquesalescode VARCHAR(20)  		--//  ENCODE zstd
	,sales_cube_program_name VARCHAR(50)  		--//  ENCODE zstd
	,target_value NUMERIC(38,4)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,4)  		--//  ENCODE az64
	,num_buying_retailers VARCHAR(200)  		--//  ENCODE zstd
	,num_bills VARCHAR(250)  		--//  ENCODE zstd
	,num_packs VARCHAR(50)  		--//  ENCODE zstd
	,num_lines numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_type VARCHAR(50)  		--//  ENCODE zstd
	,closing_stock NUMERIC(38,4)  		--//  ENCODE az64
	,billing_value NUMERIC(38,4)  		--//  ENCODE az64
	,measure_type VARCHAR(50)  		--//  ENCODE zstd
	,brand_focus_target NUMERIC(38,4)  		--//  ENCODE az64
	,business_plan_target NUMERIC(38,4)  		--//  ENCODE az64
	,schemeamt NUMERIC(38,6)  		--//  ENCODE az64
	,franchise_channel VARCHAR(50)  		--//  ENCODE zstd
	,franchise_datasource VARCHAR(50)  		--//  ENCODE zstd
	,sls_actl_val NUMERIC(38,4)  		--//  ENCODE az64
	,targetbills NUMERIC(38,6)  		--//  ENCODE az64
	,product_category1 VARCHAR(50)  		--//  ENCODE zstd
	,product_category2 VARCHAR(50)  		--//  ENCODE zstd
	,product_category3 VARCHAR(50)  		--//  ENCODE zstd
	,product_category4 VARCHAR(50)  		--//  ENCODE zstd
	,retailer_channel_level1 VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_level2 VARCHAR(100)  		--//  ENCODE zstd
	,fiscyr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fiscmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,day numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,salesman_status VARCHAR(20)  		--//  ENCODE zstd
	,net_amt NUMERIC(38,6)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE zstd //  ENCODE zstd
	,retailer_channel_level3 VARCHAR(100)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,mothersku_code VARCHAR(50)  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	brand_name
		--// 	)
;		--// ;
--DROP TABLE edw_rpt_finance_simulator;
CREATE TABLE IF NOT EXISTS EDW_RPT_FINANCE_SIMULATOR		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_finance_simulator
(
	matl_num VARCHAR(18)  		--//  ENCODE lzo
	,chrt_acct VARCHAR(4)  		--//  ENCODE lzo
	,acct_num VARCHAR(10)  		--//  ENCODE lzo
	,dstr_chnl VARCHAR(2)  		--//  ENCODE lzo
	,ctry_key VARCHAR(3)  		--//  ENCODE lzo
	,caln_yr_mo numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amt_obj_crncy NUMERIC(38,5)  		--//  ENCODE az64
	,qty NUMERIC(38,5)  		--//  ENCODE az64
	,acct_hier_desc VARCHAR(100)  		--//  ENCODE lzo
	,acct_hier_shrt_desc VARCHAR(100)  		--//  ENCODE lzo
	,chnl_desc1 VARCHAR(500)  		--//  ENCODE lzo
	,chnl_desc2 VARCHAR(500)  		--//  ENCODE lzo
	,bw_gl VARCHAR(200)  		--//  ENCODE lzo
	,nature VARCHAR(500)  		--//  ENCODE lzo
	,sap_gl VARCHAR(500)  		--//  ENCODE lzo
	,descp VARCHAR(500)  		--//  ENCODE lzo
	,bravo_mapping VARCHAR(500)  		--//  ENCODE lzo
	,sku_desc VARCHAR(500)  		--//  ENCODE lzo
	,brand_combi VARCHAR(500)  		--//  ENCODE lzo
	,franchise VARCHAR(500)  		--//  ENCODE lzo
	,"group" VARCHAR(500)  		--//  ENCODE lzo
	,mrp NUMERIC(38,2)  		--//  ENCODE az64
	,cogs_per_unit NUMERIC(38,2)  		--//  ENCODE az64
	,plan VARCHAR(10)  		--//  ENCODE lzo
	,brand_group_1 VARCHAR(500)  		--//  ENCODE lzo
	,brand_group_2 VARCHAR(500)  		--//  ENCODE lzo
	,co_cd VARCHAR(4)  		--//  ENCODE lzo
	,brand_combi_var VARCHAR(200)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_geotagg_tracker;
CREATE TABLE IF NOT EXISTS EDW_RPT_GEOTAGG_TRACKER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_geotagg_tracker
(
	fisc_yr VARCHAR(200)  		--//  ENCODE lzo
	,mth_mm VARCHAR(200)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(200)  		--//  ENCODE lzo
	,retailer_name VARCHAR(200)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(200)  		--//  ENCODE lzo
	,channel_name VARCHAR(100)  		--//  ENCODE lzo
	,retailer_channel_3 VARCHAR(100)  		--//  ENCODE lzo
	,loyalty_program_name VARCHAR(200)  		--//  ENCODE lzo
	,qtr_target NUMERIC(18,6)  		--//  ENCODE az64
	,qtr_actuals NUMERIC(18,6)  		--//  ENCODE az64
	,month_target NUMERIC(18,6)  		--//  ENCODE az64
	,month_actuals NUMERIC(18,6)  		--//  ENCODE az64
	,os_flag VARCHAR(200)  		--//  ENCODE lzo
	,msl_recom NUMERIC(18,0)  		--//  ENCODE az64
	,msl_lines_sold_p3m NUMERIC(18,0)  		--//  ENCODE az64
	,region_name VARCHAR(200)  		--//  ENCODE lzo
	,zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(200)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(200)  		--//  ENCODE lzo
	,latest_customer_code VARCHAR(200)  		--//  ENCODE lzo
	,latest_customer_name VARCHAR(200)  		--//  ENCODE lzo
	,latest_salesman_code VARCHAR(200)  		--//  ENCODE lzo
	,latest_salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,latest_uniquesalescode VARCHAR(100)  		--//  ENCODE lzo
	,latest_region_name VARCHAR(100)  		--//  ENCODE lzo
	,latest_zone_name VARCHAR(100)  		--//  ENCODE lzo
	,latest_territory_name VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_popstrata VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_statename VARCHAR(100)  		--//  ENCODE lzo
	,lat_zone VARCHAR(200)  		--//  ENCODE lzo
	,long_zone VARCHAR(200)  		--//  ENCODE lzo
	,lat_territory VARCHAR(200)  		--//  ENCODE lzo
	,long_territory VARCHAR(200)  		--//  ENCODE lzo
	,lat_customer VARCHAR(200)  		--//  ENCODE lzo
	,long_customer VARCHAR(200)  		--//  ENCODE lzo
	,dataset VARCHAR(50)  		--//  ENCODE lzo
)
		--// DISTSTYLE KEY
CLUSTER BY (rtruniquecode)
;
--DROP TABLE edw_rpt_mi_msl_dashboard;
CREATE TABLE IF NOT EXISTS EDW_RPT_MI_MSL_DASHBOARD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_mi_msl_dashboard
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_code VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,retailer_name VARCHAR(200)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,region_name VARCHAR(100)  		--//  ENCODE zstd
	,zone_name VARCHAR(100)  		--//  ENCODE zstd
	,territory_name VARCHAR(100)  		--//  ENCODE zstd
	,channel_name VARCHAR(100)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(100)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)  		--//  ENCODE zstd
	,salesman_name VARCHAR(255)  		--//  ENCODE zstd
	,mothersku_code_recom VARCHAR(50)  		--//  ENCODE zstd
	,mothersku_name_recom VARCHAR(200)  		--//  ENCODE zstd
	,mothersku_code_sold VARCHAR(50)  		--//  ENCODE zstd
	,mothersku_name_sold VARCHAR(200)  		--//  ENCODE zstd
	,quantity NUMERIC(38,2)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,2)  		--//  ENCODE az64
	,total_subd numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,period VARCHAR(6)  		--//  ENCODE zstd
	,msl_count numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,msl_sold_count numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ms_flag VARCHAR(1)  		--//  ENCODE zstd
	,hit_ms_flag VARCHAR(1)  		--//  ENCODE zstd
	,dataset VARCHAR(100)  		--//  ENCODE zstd
	,crtdtm DATE  		--//  ENCODE az64
	,upddtm DATE  		--//  ENCODE az64
	,latest_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,latest_customer_name VARCHAR(150)  		--//  ENCODE lzo
	,latest_territory_code VARCHAR(50)  		--//  ENCODE lzo
	,latest_territory_name VARCHAR(150)  		--//  ENCODE lzo
	,latest_salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,latest_salesman_name VARCHAR(150)  		--//  ENCODE lzo
	,latest_uniquesalescode VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_mi_psr_perf_tracker;
CREATE TABLE IF NOT EXISTS EDW_RPT_MI_PSR_PERF_TRACKER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_mi_psr_perf_tracker
(
	year VARCHAR(200)  		--//  ENCODE lzo
	,month VARCHAR(200)  		--//  ENCODE lzo
	,qtr VARCHAR(200)  		--//  ENCODE lzo
	,rtruniquecode_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,customer_code_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,retailer_code_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,smcode_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,smname_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,uniquesalescode_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,region_name_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,zone_name_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,territory_name_rt_dim VARCHAR(200)  		--//  ENCODE lzo
	,channel_name VARCHAR(200)  		--//  ENCODE lzo
	,achievement_amt_sales_cube VARCHAR(200)  		--//  ENCODE lzo
	,rtruniquecode_sales_cube VARCHAR(200)  		--//  ENCODE lzo
	,retailer_code_sales_cube VARCHAR(200)  		--//  ENCODE lzo
	,latest_customer_code VARCHAR(200)  		--//  ENCODE lzo
	,latest_customer_name VARCHAR(200)  		--//  ENCODE lzo
	,latest_salesman_code VARCHAR(200)  		--//  ENCODE lzo
	,latest_salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,latest_uniquesalescode VARCHAR(200)  		--//  ENCODE lzo
	,rtruniquecode_rt_cube VARCHAR(200)  		--//  ENCODE lzo
	,subd_retailer_code VARCHAR(200)  		--//  ENCODE lzo
	,total_number_of_bills VARCHAR(200)  		--//  ENCODE lzo
	,no_of_packs VARCHAR(200)  		--//  ENCODE lzo
	,achievement_amt_rt_cube VARCHAR(200)  		--//  ENCODE lzo
	,total_outlets_subd_dim VARCHAR(200)  		--//  ENCODE lzo
	,total_outlets_cust_subd_dim VARCHAR(200)  		--//  ENCODE lzo
	,os_flag VARCHAR(200)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_mi_rt_sales_details;
CREATE TABLE IF NOT EXISTS EDW_RPT_MI_RT_SALES_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_mi_rt_sales_details
(
	data_source VARCHAR(10)  		--//  ENCODE lzo
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,superstockiest_code VARCHAR(50)  		--//  ENCODE lzo
	,superstockiest_name VARCHAR(225)  		--//  ENCODE lzo
	,subd_cmp_code VARCHAR(100)  		--//  ENCODE lzo
	,subd_name VARCHAR(100)  		--//  ENCODE lzo
	,retailer_code VARCHAR(50)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(151)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,salesman_status VARCHAR(20)  		--//  ENCODE lzo
	,district_name VARCHAR(200)  		--//  ENCODE lzo
	,town_name VARCHAR(200)  		--//  ENCODE lzo
	,rt_retailer_status VARCHAR(10)  		--//  ENCODE lzo
	,rt_subd_status VARCHAR(10)  		--//  ENCODE lzo
	,sc_status_desc VARCHAR(10)  		--//  ENCODE lzo
	,sc_active_flag VARCHAR(8)  		--//  ENCODE lzo
	,achievement_amt NUMERIC(38,6)  		--//  ENCODE az64
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_mt_sellin_vs_sellout_tbl;
CREATE TABLE IF NOT EXISTS EDW_RPT_MT_SELLIN_VS_SELLOUT_TBL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_mt_sellin_vs_sellout_tbl
(
	fisc_yr VARCHAR(12)  		--//  ENCODE lzo
	,mth_mm VARCHAR(14)  		--//  ENCODE lzo
	,common_account_name VARCHAR(255)  		--//  ENCODE lzo
	,common_channel_name VARCHAR(50)  		--//  ENCODE lzo
	,channel_name_sellin VARCHAR(255)  		--//  ENCODE lzo
	,franchise_name_sellin VARCHAR(255)  		--//  ENCODE lzo
	,brand_name_sellin VARCHAR(255)  		--//  ENCODE lzo
	,variant_name_sellin VARCHAR(255)  		--//  ENCODE lzo
	,product_category_name_sellin VARCHAR(225)  		--//  ENCODE lzo
	,mothersku_name_sellin VARCHAR(225)  		--//  ENCODE lzo
	,invoice_quantity_sellin NUMERIC(38,4)  		--//  ENCODE az64
	,invoice_value_sellin NUMERIC(38,8)  		--//  ENCODE az64
	,data_source_sellout VARCHAR(20)  		--//  ENCODE lzo
	,pos_offtake_level_sellout VARCHAR(20)  		--//  ENCODE lzo
	,account_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,mother_sku_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,brand_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,franchise_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,internal_category_sellout VARCHAR(255)  		--//  ENCODE lzo
	,internal_subcategory_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_category_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_subcategory_sellout VARCHAR(255)  		--//  ENCODE lzo
	,product_category_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,variant_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_mothersku_code_sellout VARCHAR(255)  		--//  ENCODE lzo
	,external_mothersku_name_sellout VARCHAR(255)  		--//  ENCODE lzo
	,sls_qty_sellout NUMERIC(38,4)  		--//  ENCODE az64
	,sls_val_lcy_sellout NUMERIC(38,4)  		--//  ENCODE az64
	,factorized_sls_val_lcy_sellout NUMERIC(38,4)  		--//  ENCODE az64
	,sales_factor_sellout NUMERIC(10,2)  		--//  ENCODE az64
	,sales_factor_ref_month_sellout VARCHAR(14)  		--//  ENCODE lzo
	,internal_mothersku_name VARCHAR(255)  		--//  ENCODE lzo
	,internal_brand_name VARCHAR(255)  		--//  ENCODE lzo
	,internal_franchise_name VARCHAR(255)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_pob_cx_final;
CREATE TABLE IF NOT EXISTS EDW_RPT_POB_CX_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_pob_cx_final
(
	urc VARCHAR(100)  		--//  ENCODE lzo
	,ventasys_product VARCHAR(200)  		--//  ENCODE lzo
	,year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date VARCHAR(16)  		--//  ENCODE lzo
	,pob_units NUMERIC(38,2)  		--//  ENCODE az64
	,sales_ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,sales_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,urc_name VARCHAR(150)  		--//  ENCODE lzo
	,region_sales VARCHAR(50)  		--//  ENCODE lzo
	,territory_sales VARCHAR(50)  		--//  ENCODE lzo
	,zone_sales VARCHAR(50)  		--//  ENCODE lzo
	,distributor_code VARCHAR(50)  		--//  ENCODE lzo
	,distributor_name VARCHAR(150)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_rx_cx_sku_recom_target;
CREATE TABLE IF NOT EXISTS EDW_RPT_RX_CX_SKU_RECOM_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_rx_cx_sku_recom_target
(
	urc VARCHAR(50)  		--//  ENCODE zstd
	,mother_sku_cd VARCHAR(100)  		--//  ENCODE zstd
	,year NUMERIC(18,0)  		--//  ENCODE az64
	,quarter VARCHAR(2)  		--//  ENCODE zstd
	,month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,quarterly_soq NUMERIC(18,2)  		--//  ENCODE az64
	,monthly_soq NUMERIC(18,2)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE edw_rpt_rx_to_cx;
CREATE TABLE IF NOT EXISTS EDW_RPT_RX_TO_CX		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_rx_to_cx
(
	urc VARCHAR(50)  		--//  ENCODE zstd
	,lysq_ach_nr NUMERIC(10,2)  		--//  ENCODE az64
	,lysq_qty NUMERIC(18,0)  		--//  ENCODE az64
	,lysq_presc NUMERIC(10,2)  		--//  ENCODE az64
	,quarter VARCHAR(2)  		--//  ENCODE zstd
	,target_presc NUMERIC(10,2)  		--//  ENCODE az64
	,target_qty NUMERIC(18,0)  		--//  ENCODE az64
	,case NUMERIC(18,0)  		--//  ENCODE az64
	,prescription_action VARCHAR(100)  		--//  ENCODE zstd
	,sales_action VARCHAR(100)  		--//  ENCODE zstd
	,product_vent VARCHAR(25)  		--//  ENCODE zstd
	,year NUMERIC(18,0)  		--//  ENCODE az64
	,hcp VARCHAR(2000)  		--//  ENCODE zstd
	,prescriptions_needed NUMERIC(10,2)  		--//  ENCODE az64
	,hcp_name VARCHAR(500)  		--//  ENCODE zstd
	,emp_name VARCHAR(500)  		--//  ENCODE zstd
	,emp_id VARCHAR(500)  		--//  ENCODE zstd
	,region_vent VARCHAR(500)  		--//  ENCODE zstd
	,territory_vent VARCHAR(500)  		--//  ENCODE zstd
	,zone_vent VARCHAR(500)  		--//  ENCODE zstd
	,actual_ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,actual_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,actual_presc NUMERIC(38,6)  		--//  ENCODE az64
	,urc_name VARCHAR(150)  		--//  ENCODE zstd
	,region_sales VARCHAR(50)  		--//  ENCODE zstd
	,territory_sales VARCHAR(50)  		--//  ENCODE zstd
	,zone_sales VARCHAR(50)  		--//  ENCODE zstd
	,distributor_code VARCHAR(50)  		--//  ENCODE zstd
	,distributor_name VARCHAR(150)  		--//  ENCODE zstd
	,gtm_flag VARCHAR(150)  		--//  ENCODE zstd
	,product_name_sales VARCHAR(65535)  		--//  ENCODE zstd
	,franchise_code VARCHAR(50)  		--//  ENCODE zstd
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code_sales VARCHAR(50)  		--//  ENCODE zstd
	,salesman_name_sales VARCHAR(50)  		--//  ENCODE zstd
	,load_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_rx_to_cx_to_pob;
CREATE TABLE IF NOT EXISTS EDW_RPT_RX_TO_CX_TO_POB		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_rx_to_cx_to_pob
(
	year VARCHAR(10)  		--//  ENCODE zstd
	,month VARCHAR(10)  		--//  ENCODE zstd
	,urc VARCHAR(100)  		--//  ENCODE zstd
	,ventasys_product VARCHAR(200)  		--//  ENCODE zstd
	,franchise_name VARCHAR(100)  		--//  ENCODE zstd
	,brand_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sales_ach_nr NUMERIC(38,6)  		--//  ENCODE az64
	,pob_units NUMERIC(38,2)  		--//  ENCODE az64
	,hcp_id VARCHAR(50)  		--//  ENCODE zstd
	,hcp_name VARCHAR(100)  		--//  ENCODE zstd
	,rx_factorized NUMERIC(38,2)  		--//  ENCODE az64
	,rx_units NUMERIC(38,2)  		--//  ENCODE az64
	,urc_name VARCHAR(100)  		--//  ENCODE zstd
	,distributor_code VARCHAR(50)  		--//  ENCODE zstd
	,distributor_name VARCHAR(100)  		--//  ENCODE zstd
	,channel_name VARCHAR(100)  		--//  ENCODE zstd
	,class_desc VARCHAR(100)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_1 VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_2 VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_3 VARCHAR(100)  		--//  ENCODE zstd
	,region_name VARCHAR(100)  		--//  ENCODE zstd
	,zone_name VARCHAR(100)  		--//  ENCODE zstd
	,territory_name VARCHAR(100)  		--//  ENCODE zstd
	,salesman_code_sales VARCHAR(50)  		--//  ENCODE zstd
	,salesman_name_sales VARCHAR(100)  		--//  ENCODE zstd
	,emp_id VARCHAR(50)  		--//  ENCODE zstd
	,emp_name VARCHAR(100)  		--//  ENCODE zstd
	,region_vent VARCHAR(100)  		--//  ENCODE zstd
	,territory_vent VARCHAR(100)  		--//  ENCODE zstd
	,zone_vent VARCHAR(100)  		--//  ENCODE zstd
	,"urc active flag" VARCHAR(1)  		--//  ENCODE zstd
	,"urc active flag ventasys" VARCHAR(1)  		--//  ENCODE zstd
	,load_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE KEY
CLUSTER BY (urc)
;
--DROP TABLE edw_rpt_sales_details;
CREATE TABLE IF NOT EXISTS EDW_RPT_SALES_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_sales_details
(
	customer_code VARCHAR(50)  		--//  ENCODE lzo
	,invoice_no VARCHAR(50)  		--//  ENCODE lzo
	,invoice_date numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_code VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(50)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,invoice_status VARCHAR(1)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_classification VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,outlet VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,loyalty_desc VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,product_name VARCHAR(50)  		--//  ENCODE lzo
	,product_desc VARCHAR(100)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,day numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_yyyymm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,yyyyqtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cal_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE lzo
	,retailer_name VARCHAR(100)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,salesman_code VARCHAR(100)  		--//  ENCODE lzo
	,route_name VARCHAR(200)  		--//  ENCODE lzo
	,route_code VARCHAR(100)  		--//  ENCODE lzo
	,active_flag VARCHAR(8)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(15)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,retailer_address1 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address2 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address3 VARCHAR(250)  		--//  ENCODE lzo
	,zone_classification VARCHAR(50)  		--//  ENCODE lzo
	,state_name VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,town_classification VARCHAR(100)  		--//  ENCODE lzo
	,achievement_amt NUMERIC(38,6)  		--//  ENCODE az64
	,gross_amt NUMERIC(38,6)  		--//  ENCODE az64
	,net_amt NUMERIC(38,6)  		--//  ENCODE az64
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,prd_disc_sch_amt NUMERIC(38,6)  		--//  ENCODE az64
	,tax_amt NUMERIC(38,6)  		--//  ENCODE az64
	,qps_amt NUMERIC(38,6)  		--//  ENCODE az64
	,qps_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,sku_rec_amt NUMERIC(38,6)  		--//  ENCODE az64
	,sku_rec_qty numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,num_recsku_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,num_buying_retailers VARCHAR(100)  		--//  ENCODE lzo
	,num_bills VARCHAR(151)  		--//  ENCODE lzo
	,num_packs VARCHAR(50)  		--//  ENCODE lzo
	,udc_hsacounter VARCHAR(150)  		--//  ENCODE lzo
	,udc_keyaccountname VARCHAR(150)  		--//  ENCODE lzo
	,udc_pharmacychain VARCHAR(150)  		--//  ENCODE lzo
	,udc_onedetailingbaby VARCHAR(150)  		--//  ENCODE lzo
	,udc_sanprovisibilitysss VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssconsoffer VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumclub2018 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssendcaps VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumclub2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_signature2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_premium2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_gstn VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22019new VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogram2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssscheme2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssspromoter2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_rtrtypeattr VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32019new VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bbastore VARCHAR(150)  		--//  ENCODE lzo
	,udc_avbabybodydocq42019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_orslcac2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_babyprofesionalcac2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq42019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_schemesss2020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_daudq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_daudq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq42020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_samriddhi VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_orslcac2021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_daudq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq12021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_daudq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq32021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_newgtm VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq12022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssspharmacystore VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssstotstores VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq12022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bssaveenoudc2022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq22022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq22022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ecommerce VARCHAR(150)  		--//  ENCODE lzo
	,udc_babytopstoreactivation VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq32022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq42022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq42022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_aarogyam VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq22023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssshyperstores2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_winbirth2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_winclinic2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq22023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq32023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_aveenosssstores VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq42023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq12024 VARCHAR(150)  		--//  ENCODE lzo
	,udc_q124bssprogram VARCHAR(150)  		--//  ENCODE lzo
	,udc_specialtyprofessional2024 VARCHAR(150)  		--//  ENCODE lzo
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,abi_ntid VARCHAR(1)  		--//  ENCODE lzo
	,flm_ntid VARCHAR(1)  		--//  ENCODE lzo
	,bdm_ntid VARCHAR(1)  		--//  ENCODE lzo
	,rsm_ntid VARCHAR(1)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,type_name VARCHAR(50)  		--//  ENCODE lzo
	,customer_type VARCHAR(255)  		--//  ENCODE lzo
	,mothersku_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_status VARCHAR(20)  		--//  ENCODE lzo
	,total_retailers numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prd_free_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_channel_1 VARCHAR(200)  		--//  ENCODE lzo
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE lzo
	,retailer_channel_3 VARCHAR(200)  		--//  ENCODE lzo
	,report_channel VARCHAR(200)  		--//  ENCODE lzo
	,latest_customer_code VARCHAR(50)  		--//  ENCODE lzo
	,latest_customer_name VARCHAR(150)  		--//  ENCODE lzo
	,latest_territory_code NUMERIC(18,0)  		--//  ENCODE az64
	,latest_territory_name VARCHAR(50)  		--//  ENCODE lzo
	,tlcode VARCHAR(50)  		--//  ENCODE lzo
	,tlname VARCHAR(100)  		--//  ENCODE lzo
	,rsrcode VARCHAR(50)  		--//  ENCODE lzo
	,rsrname VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_townname VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_statename VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_districtname VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_subdistrictname VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_type VARCHAR(10)  		--//  ENCODE lzo
	,nielsen_villagename VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_pincode numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nielsen_uacheck VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_uaname VARCHAR(100)  		--//  ENCODE lzo
	,nielsen_popstrata VARCHAR(100)  		--//  ENCODE lzo
	,latest_salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,latest_salesman_name VARCHAR(50)  		--//  ENCODE lzo
	,latest_uniquesalescode VARCHAR(50)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_schemeutilize_cube;
CREATE TABLE IF NOT EXISTS EDW_RPT_SCHEMEUTILIZE_CUBE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_schemeutilize_cube
(
	schdate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,day numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,mth_yyyymm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month_nm VARCHAR(20)  		--//  ENCODE lzo
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,billedprdccode VARCHAR(100)  		--//  ENCODE lzo
	,billedprdbatcode VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(50)  		--//  ENCODE lzo
	,franchise_name VARCHAR(50)  		--//  ENCODE lzo
	,brand_name VARCHAR(50)  		--//  ENCODE lzo
	,product_category_name VARCHAR(150)  		--//  ENCODE lzo
	,variant_name VARCHAR(150)
	,mothersku_name VARCHAR(150)  		--//  ENCODE lzo
	,invoiceno VARCHAR(50)  		--//  ENCODE lzo
	,distcode VARCHAR(50)  		--//  ENCODE lzo
	,customer_name VARCHAR(150)  		--//  ENCODE lzo
	,schemecode VARCHAR(50)  		--//  ENCODE lzo
	,schemedescription VARCHAR(200)  		--//  ENCODE lzo
	,schtype VARCHAR(10)  		--//  ENCODE lzo
	,schemetype VARCHAR(50)  		--//  ENCODE lzo
	,schvalidfrom TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,schvalidtill TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,claimable VARCHAR(7)  		--//  ENCODE lzo
	,rtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_name VARCHAR(150)  		--//  ENCODE lzo
	,retailer_address1 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address2 VARCHAR(250)  		--//  ENCODE lzo
	,retailer_address3 VARCHAR(250)  		--//  ENCODE lzo
	,region_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,region_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,zone_name VARCHAR(50)  		--//  ENCODE lzo
	,zone_classification VARCHAR(50)  		--//  ENCODE lzo
	,territory_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,territory_name VARCHAR(50)  		--//  ENCODE lzo
	,territory_classification VARCHAR(50)  		--//  ENCODE lzo
	,state_code numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,state_name VARCHAR(50)  		--//  ENCODE lzo
	,town_code VARCHAR(50)  		--//  ENCODE lzo
	,town_name VARCHAR(50)  		--//  ENCODE lzo
	,town_classification VARCHAR(50)  		--//  ENCODE lzo
	,class_code VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,outlet_type VARCHAR(50)  		--//  ENCODE lzo
	,channel_code VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(150)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,loyalty_desc VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(10)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(50)  		--//  ENCODE lzo
	,retailer_category_cd VARCHAR(25)  		--//  ENCODE lzo
	,retailer_category_name VARCHAR(50)  		--//  ENCODE lzo
	,rtrlatitude VARCHAR(40)  		--//  ENCODE lzo
	,rtrlongitude VARCHAR(40)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,schemeutilizedamt NUMERIC(18,6)  		--//  ENCODE az64
	,schemefreeproduct VARCHAR(50)  		--//  ENCODE lzo
	,schemeutilizedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,companyschemecode VARCHAR(50)  		--//  ENCODE lzo
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,migrationflag VARCHAR(1)  		--//  ENCODE lzo
	,schememode VARCHAR(50)  		--//  ENCODE lzo
	,schlinecount NUMERIC(18,0)  		--//  ENCODE az64
	,schvaluetype VARCHAR(100)  		--//  ENCODE lzo
	,slabid numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,billedqty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,schdiscperc NUMERIC(38,6)  		--//  ENCODE az64
	,freeprdbatcode VARCHAR(100)  		--//  ENCODE lzo
	,billedrate NUMERIC(38,6)  		--//  ENCODE az64
	,modifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,servicecrnrefno VARCHAR(100)  		--//  ENCODE lzo
	,rtrurccode VARCHAR(100)  		--//  ENCODE lzo
	,schemetype1 VARCHAR(50)  		--//  ENCODE lzo
	,schemetype2 VARCHAR(50)  		--//  ENCODE lzo
	,type_name VARCHAR(50)  		--//  ENCODE lzo
	,udc_hsacounter VARCHAR(150)  		--//  ENCODE lzo
	,udc_keyaccountname VARCHAR(150)  		--//  ENCODE lzo
	,udc_pharmacychain VARCHAR(150)  		--//  ENCODE lzo
	,udc_onedetailingbaby VARCHAR(150)  		--//  ENCODE lzo
	,udc_sanprovisibilitysss VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssconsoffer VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumclub2018 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssendcaps VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumclub2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_signature2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_premium2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_gstn VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22019new VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogram2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssscheme2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssspromoter2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_rtrtypeattr VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32019new VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq22019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bbastore VARCHAR(150)  		--//  ENCODE lzo
	,udc_avbabybodydocq42019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_orslcac2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_babyprofesionalcac2019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq42019 VARCHAR(150)  		--//  ENCODE lzo
	,udc_schemesss2020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_bhagidariq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_directorclubq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_umangq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_daudq32020 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq22021 VARCHAR(150)  		--//  ENCODE lzo
	,udc_newgtm VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq12022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssspharmacystore VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssstotstores VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ecommerce VARCHAR(150)  		--//  ENCODE lzo
	,udc_babytopstoreactivation VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq32022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq32022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq42022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq42022 VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_platinumq12023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_aarogyam VARCHAR(150)  		--//  ENCODE lzo
	,udc_sssprogramq22023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_ssshyperstores2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_winbirth2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_winclinic2023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq22023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq32023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_aveenosssstores VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq42023 VARCHAR(150)  		--//  ENCODE lzo
	,udc_hsacounterq12024 VARCHAR(150)  		--//  ENCODE lzo
	,udc_q124bssprogram VARCHAR(150)  		--//  ENCODE lzo
	,udc_specialtyprofessional2024 VARCHAR(150)  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	variant_name
		--// 	)
;		--// ;
--DROP TABLE edw_rpt_sku_recom_management;
CREATE TABLE IF NOT EXISTS EDW_RPT_SKU_RECOM_MANAGEMENT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_sku_recom_management
(
	dataset VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,region_name VARCHAR(200)  		--//  ENCODE lzo
	,zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,cust_cd VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,salesman_name VARCHAR(200)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(50)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_name VARCHAR(255)  		--//  ENCODE lzo
	,brand_name VARCHAR(255)  		--//  ENCODE lzo
	,product_category_name VARCHAR(255)  		--//  ENCODE lzo
	,variant_name VARCHAR(255)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(255)  		--//  ENCODE lzo
	,total_stores NUMERIC(18,0)  		--//  ENCODE az64
	,total_recos NUMERIC(18,0)  		--//  ENCODE az64
	,total_hits NUMERIC(18,0)  		--//  ENCODE az64
	,total_os NUMERIC(18,0)  		--//  ENCODE az64
	,total_os_perc NUMERIC(6,2)  		--//  ENCODE az64
	,store_tag VARCHAR(200)  		--//  ENCODE lzo
	,os_week0 NUMERIC(18,0)  		--//  ENCODE az64
	,os_week1 NUMERIC(18,0)  		--//  ENCODE az64
	,os_week2 NUMERIC(18,0)  		--//  ENCODE az64
	,os_week3 NUMERIC(18,0)  		--//  ENCODE az64
	,os_week4 NUMERIC(18,0)  		--//  ENCODE az64
	,os_week5 NUMERIC(18,0)  		--//  ENCODE az64
	,os_perc_week0 NUMERIC(6,2)  		--//  ENCODE az64
	,os_perc_week1 NUMERIC(6,2)  		--//  ENCODE az64
	,os_perc_week2 NUMERIC(6,2)  		--//  ENCODE az64
	,os_perc_week3 NUMERIC(6,2)  		--//  ENCODE az64
	,os_perc_week4 NUMERIC(6,2)  		--//  ENCODE az64
	,os_perc_week5 NUMERIC(6,2)  		--//  ENCODE az64
	,achievement_nr_all_msku NUMERIC(38,4)  		--//  ENCODE az64
	,achievement_nr_msl_msku NUMERIC(38,4)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,quantity numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,ret_msku VARCHAR(255)  		--//  ENCODE lzo
	,ms_flag VARCHAR(1)  		--//  ENCODE lzo
	,os_flag VARCHAR(10)  		--//  ENCODE lzo
)

;
--DROP TABLE edw_rpt_skurecom_gt_sss;
CREATE TABLE IF NOT EXISTS EDW_RPT_SKURECOM_GT_SSS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_skurecom_gt_sss
(
	mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,month VARCHAR(3)  		--//  ENCODE zstd
	,cust_cd VARCHAR(50)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,retailer_cd VARCHAR(100)  		--//  ENCODE zstd
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE zstd
	,oos_flag NUMERIC(38,0)  		--//  ENCODE az64
	,ms_flag NUMERIC(38,0)  		--//  ENCODE az64
	,cs_flag NUMERIC(38,0)  		--//  ENCODE az64
	,soq NUMERIC(38,0)  		--//  ENCODE az64
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,class_desc_a_sa_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(10)  		--//  ENCODE zstd
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,retailer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(50)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(50)  		--//  ENCODE zstd
	,nup_target numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,nup_actual_ly numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,nup_actual_cy numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,hit_ms_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hit_cs_flag numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,brand_name VARCHAR(50)  		--//  ENCODE zstd
	,product_category_name VARCHAR(150)  		--//  ENCODE zstd
	,variant_name VARCHAR(150)  		--//  ENCODE zstd
	,mothersku_name VARCHAR(150)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)  		--//  ENCODE zstd
	,salesman_name VARCHAR(50)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(50)  		--//  ENCODE zstd
	,route_code VARCHAR(100)  		--//  ENCODE zstd
	,route_name VARCHAR(50)  		--//  ENCODE zstd
	,reco_flag VARCHAR(50)  		--//  ENCODE zstd
	,sales_flag VARCHAR(50)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_sss_scorecard;
CREATE TABLE IF NOT EXISTS EDW_RPT_SSS_SCORECARD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_sss_scorecard
(
	table_rn numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,country VARCHAR(5)  		--//  ENCODE zstd
	,region VARCHAR(75)  		--//  ENCODE zstd
	,zone VARCHAR(75)  		--//  ENCODE zstd
	,territory VARCHAR(75)  		--//  ENCODE zstd
	,channel VARCHAR(225)  		--//  ENCODE zstd
	,retail_environment VARCHAR(75)  		--//  ENCODE zstd
	,salesman_name VARCHAR(200)  		--//  ENCODE zstd
	,salesman_code VARCHAR(100)  		--//  ENCODE zstd
	,distributor_code VARCHAR(50)  		--//  ENCODE zstd
	,distributor_name VARCHAR(150)  		--//  ENCODE zstd
	,store_code VARCHAR(50)  		--//  ENCODE zstd
	,store_name VARCHAR(1000)  		--//  ENCODE zstd
	,program_type VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,kpi VARCHAR(75)  		--//  ENCODE zstd
	,quarter VARCHAR(50)  		--//  ENCODE zstd
	,year VARCHAR(50)  		--//  ENCODE zstd
	,source_actual_value VARCHAR(50)  		--//  ENCODE zstd
	,source_target_value VARCHAR(50)  		--//  ENCODE zstd
	,actual_value NUMERIC(30,24)  		--//  ENCODE az64
	,target_value NUMERIC(30,24)  		--//  ENCODE az64
	,compliance NUMERIC(30,24)  		--//  ENCODE az64
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,kpi_score NUMERIC(30,24)  		--//  ENCODE az64
	,kpi_achievement NUMERIC(30,24)  		--//  ENCODE az64
	,max_potential_brand_score NUMERIC(38,26)  		--//  ENCODE az64
	,achievement_nr NUMERIC(38,6)  		--//  ENCODE az64
	,prod_hier_l1 VARCHAR(50)  		--//  ENCODE zstd
	,prod_hier_l2 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l3 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l4 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l5 VARCHAR(50)  		--//  ENCODE zstd
	,prod_hier_l6 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l7 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l8 VARCHAR(100)  		--//  ENCODE zstd
	,prod_hier_l9 VARCHAR(100)  		--//  ENCODE zstd
	,actual_value_msl_l9 VARCHAR(5)  		--//  ENCODE zstd
	,target_value_msl_l9 VARCHAR(5)  		--//  ENCODE zstd
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,promotor_store VARCHAR(1)  		--//  ENCODE zstd
	,crt_dtt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rebase_score NUMERIC(38,28)  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_sss_zonal;
CREATE TABLE IF NOT EXISTS EDW_RPT_SSS_ZONAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_sss_zonal
(
	zone_name VARCHAR(50)  		--//  ENCODE lzo
	,mth_mm numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,prev_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cur_mnth VARCHAR(3)  		--//  ENCODE lzo
	,prev_mnth VARCHAR(3)  		--//  ENCODE lzo
	,cur_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,prev_nocb numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,nocb_growth_percent NUMERIC(34,22)  		--//  ENCODE az64
	,cur_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr NUMERIC(38,6)  		--//  ENCODE az64
	,achievement_nr_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_tot NUMERIC(38,6)  		--//  ENCODE az64
	,tot_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_signature NUMERIC(38,6)  		--//  ENCODE az64
	,signature_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_premium NUMERIC(38,6)  		--//  ENCODE az64
	,premium_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,cur_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,prev_achvmnt_nr_nonprog NUMERIC(38,6)  		--//  ENCODE az64
	,nonprog_growth_percent NUMERIC(38,4)  		--//  ENCODE az64
	,load_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_rpt_urc_vent_ret_mapping;
CREATE TABLE IF NOT EXISTS EDW_RPT_URC_VENT_RET_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rpt_urc_vent_ret_mapping
(
	urc VARCHAR(50)  		--//  ENCODE zstd
	,v_custid_rtl VARCHAR(50)  		--//  ENCODE zstd
	,cust_name VARCHAR(500)  		--//  ENCODE zstd
	,cust_endtered_date DATE  		--//  ENCODE az64
	,period VARCHAR(10)  		--//  ENCODE zstd
	,flex1 VARCHAR(200)  		--//  ENCODE zstd
	,flex2 VARCHAR(200)  		--//  ENCODE zstd
	,udc_orslcac2021_flag VARCHAR(200)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--// 	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)

;
--DROP TABLE edw_rt_sales_fact;
CREATE TABLE IF NOT EXISTS EDW_RT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_rt_sales_fact
(
	subd_ret_code VARCHAR(100)  		--//  ENCODE zstd
	,subd_code VARCHAR(50)  		--//  ENCODE zstd
	,customer_code VARCHAR(100)  		--//  ENCODE zstd
	,user_code VARCHAR(100)  		--//  ENCODE zstd
	,product_code VARCHAR(100)
	,order_date TIMESTAMP WITHOUT TIME ZONE
	,order_id VARCHAR(100)  		--//  ENCODE zstd
	,qty numeric(38,0)		--//  ENCODE zstd // BIGINT  
	,price NUMERIC(18,2)  		--//  ENCODE zstd
	,achievement_amt NUMERIC(18,2)  		--//  ENCODE zstd
	,lines VARCHAR(1)  		--//  ENCODE zstd
	,buying_retailers VARCHAR(151)  		--//  ENCODE zstd
	,bills VARCHAR(151)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	order_date
		--// 	, product_code
		--// 	)
;		--// ;
--DROP TABLE edw_salesman_dim;
CREATE TABLE IF NOT EXISTS EDW_SALESMAN_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_salesman_dim
(
	distcode VARCHAR(100)  		--//  ENCODE zstd
	,smcode VARCHAR(100)
	,smname VARCHAR(100)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
		--// SORTKEY ( 
		--// 	smcode
		--// 	)
;		--// ;
--DROP TABLE edw_salesman_target;
CREATE TABLE IF NOT EXISTS EDW_SALESMAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_salesman_target
(
	ctry_cd VARCHAR(2)  		--//  ENCODE zstd
	,crncy_cd VARCHAR(3)  		--//  ENCODE zstd
	,fisc_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,fisc_mnth VARCHAR(2)  		--//  ENCODE zstd
	,month_nm VARCHAR(20)  		--//  ENCODE zstd
	,dist_code VARCHAR(50)  		--//  ENCODE zstd
	,sm_code VARCHAR(50)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,brand_focus VARCHAR(50)  		--//  ENCODE zstd
	,measure_type VARCHAR(50)  		--//  ENCODE zstd
	,sm_tgt_amt NUMERIC(38,6)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_scheme_dim;
CREATE TABLE IF NOT EXISTS EDW_SCHEME_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_scheme_dim
(
	scheme_id numeric(18,0)		--//  ENCODE lzo // INTEGER  
	,scheme_code VARCHAR(20)  		--//  ENCODE lzo
	,claimable SMALLINT  		--//  ENCODE lzo
	,company_scheme_code VARCHAR(20)
	,valid_from TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,valid_till TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,scheme_status SMALLINT  		--//  ENCODE lzo
	,scheme_desc VARCHAR(100)  		--//  ENCODE lzo
	,sch_cat_type1 VARCHAR(50)  		--//  ENCODE lzo
	,sch_cat_type2 VARCHAR(50)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE ALL
		--// SORTKEY ( 
		--// 	company_scheme_code
		--// 	)
;		--// ;
--DROP TABLE edw_sku_recom;
CREATE TABLE IF NOT EXISTS EDW_SKU_RECOM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_sku_recom
(
	cust_cd VARCHAR(100)
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE zstd
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)
	,route_code VARCHAR(50)
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,region_name VARCHAR(200)  		--//  ENCODE zstd
	,zone_name VARCHAR(200)  		--//  ENCODE zstd
	,territory_name VARCHAR(200)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(50)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,product_category_name VARCHAR(255)  		--//  ENCODE zstd
	,variant_name VARCHAR(255)  		--//  ENCODE zstd
	,mothersku_name VARCHAR(255)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE delta // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE delta // INTEGER  
	,fisc_yr numeric(18,0)		--// INTEGER
	,month VARCHAR(50)
	,retailer_name VARCHAR(255)  		--//  ENCODE zstd
	,salesman_name VARCHAR(255)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_nr_val NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_amt NUMERIC(38,4)  		--//  ENCODE delta
	,num_packs VARCHAR(100)  		--//  ENCODE zstd
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,retailer_category_name VARCHAR(255)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(100)  		--//  ENCODE zstd
	,oos_flag VARCHAR(50)  		--//  ENCODE zstd
	,ms_flag VARCHAR(50)  		--//  ENCODE zstd
	,cs_flag VARCHAR(50)  		--//  ENCODE zstd
	,soq VARCHAR(50)  		--//  ENCODE zstd
	,hit_oos_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_ms_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_cs_flag VARCHAR(10)  		--//  ENCODE zstd
	,target_ms_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,target_cs_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,orange_percentage NUMERIC(6,2)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_sku_recom_spike_msl;
CREATE TABLE IF NOT EXISTS EDW_SKU_RECOM_SPIKE_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_sku_recom_spike_msl
(
	cust_cd VARCHAR(100)
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE zstd
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE zstd
	,salesman_code VARCHAR(50)
	,route_code VARCHAR(50)
	,rtruniquecode VARCHAR(100)  		--//  ENCODE zstd
	,customer_name VARCHAR(200)  		--//  ENCODE zstd
	,region_name VARCHAR(200)  		--//  ENCODE zstd
	,zone_name VARCHAR(200)  		--//  ENCODE zstd
	,territory_name VARCHAR(200)  		--//  ENCODE zstd
	,class_desc VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(50)  		--//  ENCODE zstd
	,business_channel VARCHAR(50)  		--//  ENCODE zstd
	,status_desc VARCHAR(100)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,product_category_name VARCHAR(255)  		--//  ENCODE zstd
	,variant_name VARCHAR(255)  		--//  ENCODE zstd
	,mothersku_name VARCHAR(255)  		--//  ENCODE zstd
	,mth_mm numeric(18,0)		--//  ENCODE delta // INTEGER  
	,qtr numeric(18,0)		--//  ENCODE delta // INTEGER  
	,fisc_yr numeric(18,0)		--// INTEGER
	,month VARCHAR(50)
	,retailer_name VARCHAR(255)  		--//  ENCODE zstd
	,salesman_name VARCHAR(255)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_nr_val NUMERIC(38,4)  		--//  ENCODE delta
	,achievement_amt NUMERIC(38,4)  		--//  ENCODE delta
	,num_packs VARCHAR(100)  		--//  ENCODE zstd
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,retailer_category_name VARCHAR(255)  		--//  ENCODE zstd
	,csrtrcode VARCHAR(100)  		--//  ENCODE zstd
	,oos_flag VARCHAR(50)  		--//  ENCODE zstd
	,ms_flag VARCHAR(50)  		--//  ENCODE zstd
	,cs_flag VARCHAR(50)  		--//  ENCODE zstd
	,soq VARCHAR(50)  		--//  ENCODE zstd
	,hit_oos_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_ms_flag VARCHAR(10)  		--//  ENCODE zstd
	,hit_cs_flag VARCHAR(10)  		--//  ENCODE zstd
	,br_flag VARCHAR(10)  		--//  ENCODE zstd
	,target_ms_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,target_cs_mothersku_name VARCHAR(65535)  		--//  ENCODE zstd
	,orange_percentage NUMERIC(6,2)  		--//  ENCODE az64
	,retailer_channel_level_1 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_2 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE zstd
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE edw_sku_recom_spike_msl_mi;
CREATE TABLE IF NOT EXISTS EDW_SKU_RECOM_SPIKE_MSL_MI		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_sku_recom_spike_msl_mi
(
	cust_cd VARCHAR(100)  		--//  ENCODE lzo
	,retailer_cd VARCHAR(100)  		--//  ENCODE lzo
	,product_code VARCHAR(100)  		--//  ENCODE lzo
	,mother_sku_cd VARCHAR(50)  		--//  ENCODE lzo
	,salesman_code VARCHAR(50)  		--//  ENCODE lzo
	,route_code VARCHAR(50)  		--//  ENCODE lzo
	,rtruniquecode VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,region_name VARCHAR(200)  		--//  ENCODE lzo
	,zone_name VARCHAR(200)  		--//  ENCODE lzo
	,territory_name VARCHAR(200)  		--//  ENCODE lzo
	,class_desc VARCHAR(50)  		--//  ENCODE lzo
	,channel_name VARCHAR(50)  		--//  ENCODE lzo
	,business_channel VARCHAR(50)  		--//  ENCODE lzo
	,status_desc VARCHAR(100)  		--//  ENCODE lzo
	,product_name VARCHAR(255)  		--//  ENCODE lzo
	,franchise_name VARCHAR(255)  		--//  ENCODE lzo
	,brand_name VARCHAR(255)  		--//  ENCODE lzo
	,product_category_name VARCHAR(255)  		--//  ENCODE lzo
	,variant_name VARCHAR(255)  		--//  ENCODE lzo
	,mothersku_name VARCHAR(255)  		--//  ENCODE lzo
	,qtr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,fisc_yr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_name VARCHAR(255)  		--//  ENCODE lzo
	,salesman_name VARCHAR(255)  		--//  ENCODE lzo
	,unique_sales_code VARCHAR(50)  		--//  ENCODE lzo
	,route_name VARCHAR(255)  		--//  ENCODE lzo
	,quantity NUMERIC(38,4)  		--//  ENCODE az64
	,achievement_nr_val NUMERIC(38,4)  		--//  ENCODE az64
	,achievement_amt NUMERIC(38,4)  		--//  ENCODE az64
	,num_packs VARCHAR(100)  		--//  ENCODE lzo
	,num_lines numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,retailer_category_name VARCHAR(255)  		--//  ENCODE lzo
	,csrtrcode VARCHAR(100)  		--//  ENCODE lzo
	,oos_flag VARCHAR(50)  		--//  ENCODE lzo
	,ms_flag VARCHAR(50)  		--//  ENCODE lzo
	,cs_flag VARCHAR(50)  		--//  ENCODE lzo
	,soq VARCHAR(50)  		--//  ENCODE lzo
	,hit_oos_flag VARCHAR(10)  		--//  ENCODE lzo
	,hit_ms_flag VARCHAR(10)  		--//  ENCODE lzo
	,hit_cs_flag VARCHAR(10)  		--//  ENCODE lzo
	,br_flag VARCHAR(10)  		--//  ENCODE lzo
	,target_ms_mothersku_name VARCHAR(65535)  		--//  ENCODE lzo
	,target_cs_mothersku_name VARCHAR(65535)  		--//  ENCODE lzo
	,orange_percentage NUMERIC(6,2)  		--//  ENCODE az64
	,retailer_channel_level_1 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_2 VARCHAR(200)  		--//  ENCODE zstd
	,retailer_channel_level_3 VARCHAR(200)  		--//  ENCODE zstd
	,actv_flg CHAR(1)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_sss_kpi_data;
CREATE TABLE IF NOT EXISTS EDW_SSS_KPI_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_sss_kpi_data
(
	program_type VARCHAR(50)  		--//  ENCODE lzo
	,jnj_id VARCHAR(50)  		--//  ENCODE lzo
	,outlet_name VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(50)  		--//  ENCODE lzo
	,zone VARCHAR(50)  		--//  ENCODE lzo
	,territory VARCHAR(50)  		--//  ENCODE lzo
	,city VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,source_target VARCHAR(50)  		--//  ENCODE lzo
	,source_actual VARCHAR(50)  		--//  ENCODE lzo
	,target VARCHAR(50)  		--//  ENCODE lzo
	,actual VARCHAR(50)  		--//  ENCODE lzo
	,compliance NUMERIC(30,24)  		--//  ENCODE az64
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,score_value NUMERIC(31,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE edw_sss_kpi_data_final;
CREATE TABLE IF NOT EXISTS EDW_SSS_KPI_DATA_FINAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS edw_sss_kpi_data_final
(
	program_type VARCHAR(50)  		--//  ENCODE lzo
	,jnj_id VARCHAR(50)  		--//  ENCODE lzo
	,outlet_name VARCHAR(50)  		--//  ENCODE lzo
	,region VARCHAR(50)  		--//  ENCODE lzo
	,zone VARCHAR(50)  		--//  ENCODE lzo
	,territory VARCHAR(50)  		--//  ENCODE lzo
	,city VARCHAR(50)  		--//  ENCODE lzo
	,brand VARCHAR(50)  		--//  ENCODE lzo
	,kpi VARCHAR(50)  		--//  ENCODE lzo
	,quarter VARCHAR(50)  		--//  ENCODE lzo
	,year VARCHAR(50)  		--//  ENCODE lzo
	,source_target VARCHAR(50)  		--//  ENCODE lzo
	,source_actual VARCHAR(50)  		--//  ENCODE lzo
	,target VARCHAR(50)  		--//  ENCODE lzo
	,actual VARCHAR(50)  		--//  ENCODE lzo
	,compliance NUMERIC(30,24)  		--//  ENCODE az64
	,weight NUMERIC(31,2)  		--//  ENCODE az64
	,score_value NUMERIC(31,3)  		--//  ENCODE az64
	,max_potential_brand_score NUMERIC(31,3)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;

--DROP TABLE gtm_kpi_dasboard;
CREATE TABLE IF NOT EXISTS GTM_KPI_DASBOARD		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS gtm_kpi_dasboard
(
	fisc_per numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,week numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,data_type VARCHAR(20)  		--//  ENCODE zstd
	,region_name VARCHAR(50)  		--//  ENCODE zstd
	,zone_name VARCHAR(50)  		--//  ENCODE zstd
	,territory_name VARCHAR(50)  		--//  ENCODE zstd
	,channel_name VARCHAR(150)  		--//  ENCODE zstd
	,customer_code VARCHAR(200)  		--//  ENCODE zstd
	,customer_name VARCHAR(150)  		--//  ENCODE zstd
	,retailer_category_name VARCHAR(50)  		--//  ENCODE zstd
	,retailer_class VARCHAR(50)  		--//  ENCODE zstd
	,urc VARCHAR(100)  		--//  ENCODE zstd
	,retailer_channel_2 VARCHAR(200)  		--//  ENCODE zstd
	,num_buying_retailers VARCHAR(151)  		--//  ENCODE zstd
	,unique_sales_code VARCHAR(15)  		--//  ENCODE zstd
	,salesman_name VARCHAR(200)  		--//  ENCODE zstd
	,franchise_name VARCHAR(50)  		--//  ENCODE zstd
	,brand_name VARCHAR(50)  		--//  ENCODE zstd
	,variant_name VARCHAR(150)  		--//  ENCODE zstd
	,mothersku_name VARCHAR(150)  		--//  ENCODE zstd
	,gtm_flag VARCHAR(200)  		--//  ENCODE zstd
	,nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,cust_control_nr_value NUMERIC(38,6)  		--//  ENCODE az64
	,num_bills VARCHAR(202)  		--//  ENCODE zstd
	,retailer_code VARCHAR(100)  		--//  ENCODE zstd
	,gtm_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,gtm_month numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pre_post_flag VARCHAR(25)  		--//  ENCODE lzo
	,customer_gtm_flag VARCHAR(200)  		--//  ENCODE lzo
	,retailer_gtm_flag VARCHAR(200)  		--//  ENCODE lzo
	,retailer_tag VARCHAR(200)  		--//  ENCODE lzo
	,pack_status VARCHAR(50)  		--//  ENCODE lzo
)

		--// SORTKEY ( 
		--// 	fisc_per
		--// 	, customer_code
		--// 	)
;		--// ;
--DROP TABLE rpt_orsl_products_target_vs_actual;
CREATE TABLE IF NOT EXISTS RPT_ORSL_PRODUCTS_TARGET_VS_ACTUAL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS rpt_orsl_products_target_vs_actual
(
	region VARCHAR(500)  		--//  ENCODE zstd
	,zone VARCHAR(500)  		--//  ENCODE zstd
	,hq VARCHAR(500)  		--//  ENCODE zstd
	,source VARCHAR(500)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,product_name VARCHAR(50)  		--//  ENCODE zstd
	,product_category VARCHAR(200)  		--//  ENCODE zstd
	,year numeric(18,0) NOT NULL 		--//  ENCODE az64 // INTEGER 
	,month VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,sales_value NUMERIC(38,6)  		--//  ENCODE az64
	,count_msr numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retailer_channel_1 VARCHAR(200)  		--//  ENCODE zstd
)

		--// SORTKEY ( 
		--// 	year
		--// 	, month
		--// 	)
;		--// ;


CREATE TABLE IF NOT EXISTS SDL_CSL_DAILYSALES_RAW (
	DISTCODE VARCHAR(50),
	SALINVNO VARCHAR(50),
	SALINVDATE TIMESTAMP_NTZ(9),
	SALDLVDATE TIMESTAMP_NTZ(9),
	SALINVMODE VARCHAR(100),
	SALINVTYPE VARCHAR(100),
	SALGROSSAMT NUMBER(38,6),
	SALSPLDISCAMT NUMBER(38,6),
	SALSCHDISCAMT NUMBER(38,6),
	SALCASHDISCAMT NUMBER(38,6),
	SALDBDISCAMT NUMBER(38,6),
	SALTAXAMT NUMBER(38,6),
	SALWDSAMT NUMBER(38,6),
	SALDBADJAMT NUMBER(38,6),
	SALCRADJAMT NUMBER(38,6),
	SALONACCOUNTAMT NUMBER(38,6),
	SALMKTRETAMT NUMBER(38,6),
	SALREPLACEAMT NUMBER(38,6),
	SALOTHERCHARGESAMT NUMBER(38,6),
	SALINVLEVELDISCAMT NUMBER(38,6),
	SALTOTDEDN NUMBER(38,6),
	SALTOTADDN NUMBER(38,6),
	SALROUNDOFFAMT NUMBER(38,6),
	SALNETAMT NUMBER(38,6),
	LCNID NUMBER(18,0),
	LCNCODE VARCHAR(100),
	SALESMANCODE VARCHAR(100),
	SALESMANNAME VARCHAR(200),
	SALESROUTECODE VARCHAR(100),
	SALESROUTENAME VARCHAR(200),
	RTRID NUMBER(18,0),
	RTRCODE VARCHAR(100),
	RTRNAME VARCHAR(100),
	VECHNAME VARCHAR(100),
	DLVBOYNAME VARCHAR(100),
	DELIVERYROUTECODE VARCHAR(100),
	DELIVERYROUTENAME VARCHAR(200),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDQTY NUMBER(18,0),
	PRDSELRATEBEFORETAX NUMBER(38,6),
	PRDSELRATEAFTERTAX NUMBER(38,6),
	PRDFREEQTY NUMBER(18,0),
	PRDGROSSAMT NUMBER(38,6),
	PRDSPLDISCAMT NUMBER(38,6),
	PRDSCHDISCAMT NUMBER(38,6),
	PRDCASHDISCAMT NUMBER(38,6),
	PRDDBDISCAMT NUMBER(38,6),
	PRDTAXAMT NUMBER(38,6),
	PRDNETAMT NUMBER(38,6),
	UPLOADFLAG VARCHAR(10),
	CREATEDUSERID NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MIGRATIONFLAG VARCHAR(1),
	SALINVLINECOUNT NUMBER(18,0),
	MRP NUMBER(18,6),
	SYNCID NUMBER(38,0),
	CREDITNOTEAMT NUMBER(38,6),
	SALFREEQTYVALUE NUMBER(38,6),
	NRVALUE NUMBER(18,6),
	VCPSCHEMEAMOUNT NUMBER(18,6),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_DAILYSALES_UNDELIVERED_RAW (
	DISTCODE VARCHAR(50),
	SALINVNO VARCHAR(50),
	SALINVDATE TIMESTAMP_NTZ(9),
	SALDLVDATE TIMESTAMP_NTZ(9),
	SALINVMODE VARCHAR(100),
	SALINVTYPE VARCHAR(100),
	SALGROSSAMT NUMBER(38,6),
	SALSPLDISCAMT NUMBER(38,6),
	SALSCHDISCAMT NUMBER(38,6),
	SALCASHDISCAMT NUMBER(38,6),
	SALDBDISCAMT NUMBER(38,6),
	SALTAXAMT NUMBER(38,6),
	SALWDSAMT NUMBER(38,6),
	SALDBADJAMT NUMBER(38,6),
	SALCRADJAMT NUMBER(38,6),
	SALONACCOUNTAMT NUMBER(38,6),
	SALMKTRETAMT NUMBER(38,6),
	SALREPLACEAMT NUMBER(38,6),
	SALOTHERCHARGESAMT NUMBER(38,6),
	SALINVLEVELDISCAMT NUMBER(38,6),
	SALTOTDEDN NUMBER(38,6),
	SALTOTADDN NUMBER(38,6),
	SALROUNDOFFAMT NUMBER(38,6),
	SALNETAMT NUMBER(38,6),
	LCNID NUMBER(18,0),
	LCNCODE VARCHAR(100),
	SALESMANCODE VARCHAR(100),
	SALESMANNAME VARCHAR(200),
	SALESROUTECODE VARCHAR(100),
	SALESROUTENAME VARCHAR(200),
	RTRID NUMBER(18,0),
	RTRCODE VARCHAR(100),
	RTRNAME VARCHAR(100),
	VECHNAME VARCHAR(100),
	DLVBOYNAME VARCHAR(100),
	DELIVERYROUTECODE VARCHAR(100),
	DELIVERYROUTENAME VARCHAR(200),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDQTY NUMBER(18,0),
	PRDSELRATEBEFORETAX NUMBER(38,6),
	PRDSELRATEAFTERTAX NUMBER(38,6),
	PRDFREEQTY NUMBER(18,0),
	PRDGROSSAMT NUMBER(38,6),
	PRDSPLDISCAMT NUMBER(38,6),
	PRDSCHDISCAMT NUMBER(38,6),
	PRDCASHDISCAMT NUMBER(38,6),
	PRDDBDISCAMT NUMBER(38,6),
	PRDTAXAMT NUMBER(38,6),
	PRDNETAMT NUMBER(38,6),
	UPLOADFLAG VARCHAR(10),
	SALINVLINECOUNT NUMBER(18,0),
	SALINVLVLDISCPER NUMBER(18,2),
	BILLSTATUS NUMBER(38,0),
	UPLOADEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MRP NUMBER(18,6),
	NRVALUE NUMBER(18,6),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_DISTRIBUTORACTIVATION_RAW (
	DISTCODE VARCHAR(400),
	ACTIVEFROMDATE TIMESTAMP_NTZ(9),
	ACTIVATEDBY NUMBER(18,0),
	ACTIVATEDON TIMESTAMP_NTZ(9),
	INACTIVEFROMDATE TIMESTAMP_NTZ(9),
	INACTIVATEDBY NUMBER(18,0),
	INACTIVATEDON TIMESTAMP_NTZ(9),
	ACTIVESTATUS NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS SDL_CSL_ORDERBOOKING_RAW (
	DISTCODE VARCHAR(50),
	ORDERNO VARCHAR(50),
	ORDERDATE TIMESTAMP_NTZ(9),
	ORDDLVDATE TIMESTAMP_NTZ(9),
	ALLOWBACKORDER VARCHAR(50),
	ORDTYPE VARCHAR(50),
	ORDPRIORITY VARCHAR(50),
	ORDDOCREF VARCHAR(100),
	REMARKS VARCHAR(500),
	ROUNDOFFAMT NUMBER(38,6),
	ORDTOTALAMT NUMBER(38,6),
	SALESMANCODE VARCHAR(100),
	SALESMANNAME VARCHAR(200),
	SALESROUTECODE VARCHAR(100),
	SALESROUTENAME VARCHAR(200),
	RTRID VARCHAR(100),
	RTRCODE VARCHAR(100),
	RTRNAME VARCHAR(100),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDQTY NUMBER(18,0),
	PRDBILLEDQTY NUMBER(18,0),
	PRDSELRATE NUMBER(38,6),
	PRDGROSSAMT NUMBER(38,6),
	UPLOADFLAG VARCHAR(10),
	RECORDDATE TIMESTAMP_NTZ(9),
	CREATEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	RECOMMENDEDSKU VARCHAR(10),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_RDSSMWEEKLYTARGET_OUTPUT_RAW (
	DISTCODE VARCHAR(50),
	TARGETREFNO VARCHAR(100),
	TARGETDATE TIMESTAMP_NTZ(9),
	SMCODE VARCHAR(50),
	SMNAME VARCHAR(100),
	RMCODE VARCHAR(50),
	RMNAME VARCHAR(100),
	TARGETYEAR NUMBER(18,0),
	TARGETMONTH VARCHAR(30),
	TARGETVALUE NUMBER(36,2),
	TARGETNAME VARCHAR(50),
	WEEK1 NUMBER(36,2),
	WEEK2 NUMBER(36,2),
	WEEK3 NUMBER(36,2),
	WEEK4 NUMBER(36,2),
	WEEK5 NUMBER(36,2),
	TARGETSTATUS VARCHAR(10),
	TARGETTYPE VARCHAR(50),
	DOWNLOADSTATUS VARCHAR(10),
	CREATEDDATE TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_RETAILERHIERARCHY_RAW (
	CMPCODE VARCHAR(50),
	RTRHIERDFN_CODE VARCHAR(25),
	RTRHIERDFN_NAME VARCHAR(50),
	RETLRGROUPCODE VARCHAR(50),
	RETLRGROUPNAME VARCHAR(50),
	CLASSCODE VARCHAR(10),
	CLASSDESC VARCHAR(100),
	TURNOVER NUMBER(18,4),
	CREATEDDT TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS SDL_CSL_RETAILERMASTER_RAW (
	DISTCODE VARCHAR(50),
	RTRID NUMBER(18,0),
	RTRCODE VARCHAR(50),
	RTRNAME VARCHAR(100),
	CSRTRCODE VARCHAR(50),
	RTRCATLEVELID VARCHAR(30),
	RTRCATEGORYCODE VARCHAR(50),
	CLASSCODE VARCHAR(50),
	KEYACCOUNT VARCHAR(50),
	REGDATE TIMESTAMP_NTZ(9),
	RELATIONSTATUS VARCHAR(50),
	PARENTCODE VARCHAR(50),
	GEOLEVEL VARCHAR(50),
	GEOLEVELVALUE VARCHAR(100),
	STATUS NUMBER(18,0),
	CREATEDID NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	RTRADDRESS1 VARCHAR(100),
	RTRADDRESS2 VARCHAR(100),
	RTRADDRESS3 VARCHAR(100),
	RTRPINCODE VARCHAR(20),
	VILLAGEID NUMBER(18,0),
	VILLAGECODE VARCHAR(100),
	VILLAGENAME VARCHAR(100),
	MODE VARCHAR(100),
	UPLOADFLAG VARCHAR(10),
	APPROVALREMARKS VARCHAR(400),
	SYNCID NUMBER(38,0),
	DRUGLNO VARCHAR(100),
	RTRCRBILLS NUMBER(18,0),
	RTRCRLIMIT NUMBER(38,6),
	RTRCRDAYS NUMBER(18,0),
	RTRDAYOFF NUMBER(18,0),
	RTRTINNO VARCHAR(100),
	RTRCSTNO VARCHAR(100),
	RTRLICNO VARCHAR(100),
	RTRLICEXPIRYDATE VARCHAR(100),
	RTRDRUGEXPIRYDATE VARCHAR(100),
	RTRPESTLICNO VARCHAR(100),
	RTRPESTEXPIRYDATE VARCHAR(100),
	APPROVED NUMBER(18,0),
	RTRPHONENO VARCHAR(100),
	RTRCONTACTPERSON VARCHAR(100),
	RTRTAXGROUP VARCHAR(100),
	RTRTYPE VARCHAR(20),
	RTRTAXABLE VARCHAR(1),
	RTRSHIPPADD1 VARCHAR(200),
	RTRSHIPPADD2 VARCHAR(200),
	RTRSHIPPADD3 VARCHAR(200),
	RTRFOODLICNO VARCHAR(200),
	RTRFOODEXPIRYDATE TIMESTAMP_NTZ(9),
	RTRFOODGRACEDAYS NUMBER(18,0),
	RTRDRUGGRACEDAYS NUMBER(18,0),
	RTRCOSMETICLICNO VARCHAR(200),
	RTRCOSMETICEXPIRYDATE TIMESTAMP_NTZ(9),
	RTRCOSMETICGRACEDAYS NUMBER(18,0),
	RTRLATITUDE VARCHAR(40),
	RTRLONGITUDE VARCHAR(40),
	RTRUNIQUECODE VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_RETAILERROUTE_RAW (
	DISTCODE VARCHAR(100),
	RTRID NUMBER(18,0),
	RTRCODE VARCHAR(100),
	RTRNAME VARCHAR(100),
	RMID NUMBER(18,0),
	RMCODE VARCHAR(100),
	RMNAME VARCHAR(100),
	ROUTETYPE VARCHAR(100),
	UPLOADFLAG VARCHAR(10),
	CREATEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_SALESINVOICEORDERS_RAW (
	DISTCODE VARCHAR(50),
	SALINVNO VARCHAR(50),
	ORDERNO VARCHAR(50),
	ORDERDATE TIMESTAMP_NTZ(9),
	UPLOADFLAG VARCHAR(10),
	CREATEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_SALESMANMASTER_RAW (
	DISTCODE VARCHAR(100),
	SMID NUMBER(18,0),
	SMCODE VARCHAR(100),
	SMNAME VARCHAR(100),
	SMPHONENO VARCHAR(100),
	SMEMAIL VARCHAR(100),
	SMOTHERDETAILS VARCHAR(500),
	SMDAILYALLOWANCE NUMBER(38,6),
	SMMONTHLYSALARY NUMBER(38,6),
	SMMKTCREDIT NUMBER(38,6),
	SMCREDITDAYS NUMBER(18,0),
	STATUS VARCHAR(20),
	RMID NUMBER(18,0),
	RMCODE VARCHAR(100),
	RMNAME VARCHAR(100),
	UPLOADFLAG VARCHAR(1),
	CREATEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	RDSSMTYPE VARCHAR(100),
	UNIQUESALESCODE VARCHAR(15),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_SALESRETURN_RAW (
	DISTCODE VARCHAR(50),
	SRNREFNO VARCHAR(50),
	SRNREFTYPE VARCHAR(100),
	SRNDATE TIMESTAMP_NTZ(9),
	SRNMODE VARCHAR(100),
	SRNTYPE VARCHAR(100),
	SRNGROSSAMT NUMBER(38,6),
	SRNSPLDISCAMT NUMBER(38,6),
	SRNSCHDISCAMT NUMBER(38,6),
	SRNCASHDISCAMT NUMBER(38,6),
	SRNDBDISCAMT NUMBER(38,6),
	SRNTAXAMT NUMBER(38,6),
	SRNROUNDOFFAMT NUMBER(38,6),
	SRNNETAMT NUMBER(38,6),
	SALESMANNAME VARCHAR(100),
	SALESROUTENAME VARCHAR(100),
	RTRID NUMBER(18,0),
	RTRCODE VARCHAR(100),
	RTRNAME VARCHAR(100),
	PRDSALINVNO VARCHAR(50),
	PRDLCNID NUMBER(18,0),
	PRDLCNCODE VARCHAR(100),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDSALQTY NUMBER(18,0),
	PRDUNSALQTY NUMBER(18,0),
	PRDOFFERQTY NUMBER(18,0),
	PRDSELRATE NUMBER(38,6),
	PRDGROSSAMT NUMBER(38,6),
	PRDSPLDISCAMT NUMBER(38,6),
	PRDSCHDISCAMT NUMBER(38,6),
	PRDCASHDISCAMT NUMBER(38,6),
	PRDDBDISCAMT NUMBER(38,6),
	PRDTAXAMT NUMBER(38,6),
	PRDNETAMT NUMBER(38,6),
	UPLOADFLAG VARCHAR(10),
	CREATEDUSERID NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MIGRATIONFLAG VARCHAR(1),
	MRP NUMBER(18,6),
	SYNCID NUMBER(38,0),
	RTNFREEQTYVALUE NUMBER(38,6),
	RTNLINECOUNT NUMBER(18,0),
	REFERENCETYPE VARCHAR(100),
	SALESMANCODE VARCHAR(200),
	SALESROUTECODE VARCHAR(200),
	NRVALUE NUMBER(18,6),
	PRDSELRATEAFTERTAX NUMBER(18,6),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_SCHEMEUTILIZATION_RAW (
	DISTCODE VARCHAR(50),
	SCHEMECODE VARCHAR(50),
	SCHEMEDESCRIPTION VARCHAR(200),
	INVOICENO VARCHAR(50),
	RTRCODE VARCHAR(50),
	COMPANY VARCHAR(100),
	SCHDATE TIMESTAMP_NTZ(9),
	SCHEMETYPE VARCHAR(50),
	SCHEMEUTILIZEDAMT NUMBER(18,6),
	SCHEMEFREEPRODUCT VARCHAR(50),
	SCHEMEUTILIZEDQTY NUMBER(18,0),
	COMPANYSCHEMECODE VARCHAR(50),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MIGRATIONFLAG VARCHAR(1),
	SCHEMEMODE VARCHAR(50),
	SYNCID NUMBER(38,0),
	SCHLINECOUNT NUMBER(18,0),
	SCHVALUETYPE VARCHAR(100),
	SLABID NUMBER(18,0),
	BILLEDPRDCCODE VARCHAR(100),
	BILLEDPRDBATCODE VARCHAR(100),
	BILLEDQTY NUMBER(18,0),
	SCHDISCPERC NUMBER(38,6),
	FREEPRDBATCODE VARCHAR(100),
	BILLEDRATE NUMBER(38,6),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	SERVICECRNREFNO VARCHAR(100),
	RTRURCCODE VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_SCHEME_HEADER_RAW (
	SCHID NUMBER(18,0),
	SCHCODE VARCHAR(20),
	SCHDSC VARCHAR(100),
	CLAIMABLE NUMBER(38,0),
	CLMAMTON NUMBER(38,0),
	CMPSCHCODE VARCHAR(20),
	SCHLEVEL_ID NUMBER(18,0),
	SCHTYPE VARCHAR(10),
	FLEXISCH NUMBER(38,0),
	FLEXISCHTYPE NUMBER(38,0),
	COMBISCH NUMBER(38,0),
	RANGE NUMBER(38,0),
	PRORATA NUMBER(38,0),
	QPS VARCHAR(10),
	QPSRESET NUMBER(38,0),
	SCHVALIDFROM TIMESTAMP_NTZ(9),
	SCHVALIDTILL TIMESTAMP_NTZ(9),
	SCHSTATUS NUMBER(38,0),
	BUDGET NUMBER(18,2),
	ADJWINDISPONLYONCE NUMBER(38,0),
	PUROFEVERY NUMBER(38,0),
	APYQPSSCH NUMBER(38,0),
	SETWINDOWDISP NUMBER(38,0),
	EDITSCHEME NUMBER(38,0),
	SCHLVLMODE NUMBER(38,0),
	CREATEDUSERID NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MODIFIEDBY VARCHAR(40),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	VERSIONNO NUMBER(18,0),
	SERIALNO VARCHAR(100),
	CLAIMGRPCODE VARCHAR(40),
	FBM NUMBER(18,0),
	COMBITYPE NUMBER(18,0),
	ALLOWUNCHECK NUMBER(18,0),
	SETTLEMENTTYPE NUMBER(18,0),
	CONSUMERPROMO NUMBER(18,0),
	WDSBILLSCOUNT NUMBER(18,0),
	WDSCAPAMOUNT NUMBER(38,6),
	WDSMINPURQTY NUMBER(18,0),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_UDCDETAILS_RAW (
	DISTCODE VARCHAR(50),
	MASTERID NUMBER(18,0),
	MASTERNAME VARCHAR(200),
	MASTERVALUECODE VARCHAR(200),
	MASTERVALUENAME VARCHAR(200),
	COLUMNNAME VARCHAR(100),
	COLUMNVALUE VARCHAR(100),
	UPLOADFLAG VARCHAR(1),
	CREATEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,0),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_CSL_UDCMASTER_RAW (
	UDCMASTERID NUMBER(18,0),
	MASTERID NUMBER(18,0),
	MASTERNAME VARCHAR(100),
	COLUMNNAME VARCHAR(100),
	COLUMNDATATYPE VARCHAR(50),
	COLUMNSIZE NUMBER(18,0),
	COLUMNPRECISION NUMBER(18,0),
	EDITABLE NUMBER(18,0),
	COLUMNMANDATORY NUMBER(18,0),
	PICKFROMDEFAULT NUMBER(18,0),
	DOWNLOADSTATUS VARCHAR(5),
	CREATEDUSERID NUMBER(18,0),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MODIFIEDUSERID VARCHAR(50),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	UDCSTATUS NUMBER(18,0),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_DAILYSALES_DEL_RAW (
	DISTCODE VARCHAR(50),
	SALINVNO VARCHAR(50),
	SALINVDATE TIMESTAMP_NTZ(9),
	SALDLVDATE TIMESTAMP_NTZ(9),
	SALGROSSAMT NUMBER(38,6),
	SALSPLDISCAMT NUMBER(38,6),
	SALSCHDISCAMT NUMBER(38,6),
	SALCASHDISCAMT NUMBER(38,6),
	SALDBDISCAMT NUMBER(38,6),
	SALTAXAMT NUMBER(38,6),
	SALWDSAMT NUMBER(38,6),
	SALDBADJAMT NUMBER(38,6),
	SALCRADJAMT NUMBER(38,6),
	SALONACCOUNTAMT NUMBER(38,6),
	SALMKTRETAMT NUMBER(38,6),
	SALREPLACEAMT NUMBER(38,6),
	SALOTHERCHARGESAMT NUMBER(38,6),
	SALINVLEVELDISCAMT NUMBER(38,6),
	SALTOTDEDN NUMBER(38,6),
	SALTOTADDN NUMBER(38,6),
	SALROUNDOFFAMT NUMBER(38,6),
	SALNETAMT NUMBER(38,6),
	LCNCODE VARCHAR(50),
	SALESMANCODE VARCHAR(50),
	SALESMANNAME VARCHAR(400),
	SALESROUTECODE VARCHAR(50),
	SALESROUTENAME VARCHAR(400),
	RTRCODE VARCHAR(50),
	RTRNAME VARCHAR(200),
	DELIVERYROUTECODE VARCHAR(50),
	DELIVERYROUTENAME VARCHAR(400),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDQTY NUMBER(18,0) NOT NULL,
	PRDSELRATEBEFORETAX NUMBER(38,6),
	PRDSELRATEAFTERTAX NUMBER(38,6),
	PRDFREEQTY NUMBER(18,0),
	PRDGROSSAMT NUMBER(38,6),
	PRDSPLDISCAMT NUMBER(38,6),
	PRDSCHDISCAMT NUMBER(38,6),
	PRDCASHDISCAMT NUMBER(38,6),
	PRDDBDISCAMT NUMBER(38,6),
	PRDTAXAMT NUMBER(38,6),
	PRDNETAMT NUMBER(38,6),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MRP NUMBER(18,6),
	SALFREEQTYVALUE NUMBER(38,6),
	NRVALUE NUMBER(18,6),
	VCPSCHEMEAMOUNT NUMBER(18,6),
	RTRURCCODE VARCHAR(200),
	SYNCID NUMBER(38,6),
	CREDITNOTEAMT NUMBER(38,6),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	SALINVMODE VARCHAR(30),
	SALINVTYPE VARCHAR(30),
	VECHNAME VARCHAR(100),
	DLVBOYNAME VARCHAR(100),
	CREATEDUSERID NUMBER(18,0),
	SALINVLINECOUNT NUMBER(18,0),
	MRPCS NUMBER(18,6),
	LPVALUE NUMBER(18,6),
	CREATEDDT TIMESTAMP_NTZ(9) NOT NULL,
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_FIN_SIM_MISCDATA_RAW (
	MATL_NUM VARCHAR(250),
	SKU_DESC VARCHAR(250),
	BRAND_COMBI VARCHAR(250),
	FISC_YR VARCHAR(250),
	MONTH VARCHAR(250),
	CHNL_DESC2 VARCHAR(250),
	NATURE VARCHAR(250),
	AMT_OBJ_CRNCY VARCHAR(250),
	QTY VARCHAR(250),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(250),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS SDL_FIN_SIM_PLANDATA_RAW (
	MATL_NUM VARCHAR(100),
	FISC_YR VARCHAR(10),
	MONTH VARCHAR(10),
	CHNL_DESC2 VARCHAR(20),
	NATURE VARCHAR(50),
	AMT_OBJ_CRNCY NUMBER(38,5),
	QTY NUMBER(38,5),
	PLAN VARCHAR(10),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(150),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS SDL_RAW_IN_PERFECTSTORE_MSL (
	VISIT_ID VARCHAR(255),
	VISIT_DATETIME VARCHAR(255),
	REGION VARCHAR(255),
	JNJRKAM VARCHAR(255),
	JNJZM_CODE VARCHAR(255),
	JNJ_ABI_CODE VARCHAR(255),
	JNJSUPERVISOR_CODE VARCHAR(255),
	ISP_CODE VARCHAR(255),
	ISP_NAME VARCHAR(255),
	MONTH VARCHAR(255),
	YEAR NUMBER(10,0),
	FORMAT VARCHAR(255),
	CHAIN_CODE VARCHAR(255),
	CHAIN VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_NAME VARCHAR(255),
	MSL VARCHAR(255),
	COST_INR NUMBER(12,4),
	QUANTITY NUMBER(10,0),
	AMOUNT_INR NUMBER(12,4),
	PRIORITY_STORE VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_PERFECTSTORE_PAID_DISPLAY (
	VISIT_ID VARCHAR(255),
	VISIT_DATETIME VARCHAR(255),
	REGION VARCHAR(255),
	JNJRKAM VARCHAR(255),
	JNJZM_CODE VARCHAR(255),
	JNJ_ABI_CODE VARCHAR(255),
	JNJSUPERVISOR_CODE VARCHAR(255),
	ISP_CODE VARCHAR(255),
	ISP_NAME VARCHAR(255),
	MONTH VARCHAR(255),
	YEAR NUMBER(14,0),
	FORMAT VARCHAR(255),
	CHAIN_CODE VARCHAR(255),
	CHAIN VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	ASSET VARCHAR(255),
	PRODUCT_CATEGORY VARCHAR(255),
	PRODUCT_BRAND VARCHAR(255),
	POSM_BRAND VARCHAR(255),
	START_DATE VARCHAR(255),
	END_DATE VARCHAR(255),
	AUDIT_STATUS VARCHAR(255),
	IS_AVAILABLE VARCHAR(255),
	AVAILABILITY_POINTS VARCHAR(255),
	VISIBILITY_TYPE VARCHAR(255),
	VISIBILITY_CONDITION VARCHAR(255),
	IS_PLANOGRAM_AVAILBALE VARCHAR(255),
	SELECT_BRAND VARCHAR(255),
	IS_CORRECT_BRAND_DISPLAYED VARCHAR(255),
	BRANDAVAILABILITY_POINTS VARCHAR(255),
	STOCK_STATUS VARCHAR(255),
	STOCK_POINTS VARCHAR(255),
	IS_NEAR_CATEGORY VARCHAR(255),
	NEARCATEGORY_POINTS VARCHAR(255),
	AUDIT_SCORE VARCHAR(255),
	PAID_VISIBILITY_SCORE VARCHAR(255),
	REASON VARCHAR(255),
	PRIORITY_STORE VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_PERFECTSTORE_PROMO (
	VISIT_ID VARCHAR(255),
	VISIT_DATETIME VARCHAR(255),
	REGION VARCHAR(255),
	JNJRKAM VARCHAR(255),
	JNJZM_CODE VARCHAR(255),
	JNJ_ABI_CODE VARCHAR(255),
	JNJSUPERVISOR_CODE VARCHAR(255),
	ISP_CODE VARCHAR(255),
	ISP_NAME VARCHAR(255),
	MONTH VARCHAR(255),
	YEAR NUMBER(10,0),
	FORMAT VARCHAR(255),
	CHAIN_CODE VARCHAR(255),
	CHAIN VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	PRODUCT_CATEGORY VARCHAR(255),
	PRODUCT_BRAND VARCHAR(255),
	PROMOTION_PRODUCT_CODE VARCHAR(255),
	PROMOTION_PRODUCT_NAME VARCHAR(255),
	ISPROMOTIONAVAILABLE VARCHAR(255),
	PHOTOPATH VARCHAR(500),
	COUNTOFFACING NUMBER(10,0),
	PROMOTIONOFFERTYPE VARCHAR(255),
	NOTAVAILABLEREASON VARCHAR(255),
	PRICE_OFF VARCHAR(255),
	PRIORITY_STORE VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_PERFECTSTORE_SOS (
	VISIT_DATETIME VARCHAR(255),
	REGION VARCHAR(255),
	JNJRKAM VARCHAR(255),
	JNJZM_CODE VARCHAR(255),
	JNJ_ABI_CODE VARCHAR(255),
	JNJSUPERVISOR_CODE VARCHAR(255),
	ISP_CODE VARCHAR(255),
	JNJISP_NAME VARCHAR(255),
	MONTH VARCHAR(255),
	YEAR NUMBER(10,0),
	FORMAT VARCHAR(255),
	CHAIN_CODE VARCHAR(255),
	CHAIN VARCHAR(255),
	STORE_CODE VARCHAR(255),
	STORE_NAME VARCHAR(255),
	CATEGORY VARCHAR(255),
	PROD_FACINGS NUMBER(10,0),
	TOTAL_FACINGS NUMBER(10,0),
	FACING_CONTRIBUTION VARCHAR(255),
	PRIORITY_STORE VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RETAILER (
	CMPCODE VARCHAR(10),
	DISTCODE VARCHAR(50),
	DISTRBRCODE VARCHAR(30),
	RTRCODE VARCHAR(50),
	RTRTYPE VARCHAR(10),
	RTRNAME VARCHAR(100),
	RTRUNIQUECODE VARCHAR(50),
	CHANNELCODE VARCHAR(30),
	RETLRGROUPCODE VARCHAR(30),
	CLASSCODE VARCHAR(20),
	RTRPHONENO VARCHAR(20),
	RTRCONTACTPERSON VARCHAR(50),
	EMAILID VARCHAR(50),
	REGDATE DATE,
	RTRLICNO VARCHAR(100),
	RTRLICEXPIRYDATE DATE,
	DRUGLNO VARCHAR(100),
	RTRDRUGEXPIRYDATE DATE,
	RTRCRBILLS NUMBER(18,0),
	RTRCRDAYS NUMBER(18,0),
	RTRCRLIMIT NUMBER(38,6),
	RELATIONSTATUS VARCHAR(10),
	PARENTCODE VARCHAR(50),
	STATUS VARCHAR(10),
	RTRLATITUDE VARCHAR(20),
	RTRLONGITUDE VARCHAR(20),
	CSRTRCODE VARCHAR(50),
	KEYACCOUNT VARCHAR(10),
	RTRFOODLICNO VARCHAR(200),
	PANNUMBER VARCHAR(15),
	RETAILERTYPE VARCHAR(10),
	COMPOSITE VARCHAR(10),
	RELATEDPARTY VARCHAR(10),
	STATENAME VARCHAR(50),
	LASTMODDATE TIMESTAMP_NTZ(9),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RETAILER_ROUTE (
	CMPCODE VARCHAR(10),
	DISTCODE VARCHAR(50),
	DISTRBRCODE VARCHAR(30),
	RTRCODE VARCHAR(50),
	RMCODE VARCHAR(100),
	ROUTETYPE VARCHAR(10),
	COVERAGESEQUENCE NUMBER(18,0),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_ROUTE (
	CMPCODE VARCHAR(10),
	DISTCODE VARCHAR(50),
	DISTRBRCODE VARCHAR(50),
	RMCODE VARCHAR(100),
	ROUTETYPE VARCHAR(10),
	RMNAME VARCHAR(50),
	DISTANCE NUMBER(18,0),
	VANROUTE VARCHAR(10),
	STATUS VARCHAR(10),
	RMPOPULATION NUMBER(18,0),
	LOCALUPCOUNTRY VARCHAR(10),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RRETAILERGEOEXTENSION (
	CMPCODE VARCHAR(10),
	DISTRCODE VARCHAR(50),
	CUSTOMERCODE VARCHAR(50),
	CMPCUTOMERCODE VARCHAR(50),
	DISTRIBUTORCUSTOMERCODE VARCHAR(50),
	LATITUDE VARCHAR(20),
	LONGITUDE VARCHAR(20),
	TOWNNAME VARCHAR(100),
	STATENAME VARCHAR(100),
	DISTRICTNAME VARCHAR(100),
	SUBDISTRICTNAME VARCHAR(100),
	TYPE VARCHAR(10),
	VILLAGENAME VARCHAR(100),
	PINCODE NUMBER(18,0),
	UACHECK VARCHAR(100),
	UANAME VARCHAR(100),
	POPULATION NUMBER(18,0),
	POPSTRATA VARCHAR(100),
	FINALPOPULATIONWITHUA NUMBER(18,0),
	MODIFYDATE TIMESTAMP_NTZ(9),
	CREATEDDATE TIMESTAMP_NTZ(9),
	ISDELETED VARCHAR(1),
	EXTRAFIELD1 VARCHAR(100),
	EXTRAFIELD2 VARCHAR(100),
	EXTRAFIELD3 VARCHAR(100),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RRSRHEADER (
	CMPCODE VARCHAR(10),
	RSRCODE VARCHAR(50),
	RSRNAME VARCHAR(100),
	EMAILID VARCHAR(50),
	PHONENO VARCHAR(15),
	DATEOFBIRTH TIMESTAMP_NTZ(9),
	DATEOFJOIN TIMESTAMP_NTZ(9),
	ISACTIVE VARCHAR(1),
	MODUSERCODE VARCHAR(50),
	MODDT TIMESTAMP_NTZ(9),
	APPROVALSTATUS VARCHAR(10),
	DAILYALLOWANCE NUMBER(22,6),
	MONTHLYSALARY NUMBER(22,6),
	AADHARNO VARCHAR(15),
	IMAGEPATH VARCHAR(100),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RTLDISTRIBUTOR (
	CMPCODE VARCHAR(10),
	TLCODE VARCHAR(50),
	DISTRCODE VARCHAR(30),
	DATEOFJOIN TIMESTAMP_NTZ(9),
	ISACTIVE VARCHAR(1),
	MODUSERCODE VARCHAR(50),
	MODDT TIMESTAMP_NTZ(9),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RTLHEADER (
	CMPCODE VARCHAR(10),
	TLCODE VARCHAR(50),
	TLNAME VARCHAR(100),
	EMAILID VARCHAR(50),
	PHONENO VARCHAR(15),
	DATEOFBIRTH TIMESTAMP_NTZ(9),
	DATEOFJOIN TIMESTAMP_NTZ(9),
	ISACTIVE VARCHAR(1),
	MODUSERCODE VARCHAR(50),
	MODDT TIMESTAMP_NTZ(9),
	APPROVALSTATUS VARCHAR(10),
	DAILYALLOWANCE NUMBER(22,6),
	MONTHLYSALARY NUMBER(22,6),
	AADHARNO VARCHAR(15),
	IMAGEPATH VARCHAR(100),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_RTLSALESMAN (
	CMPCODE VARCHAR(10),
	TLCODE VARCHAR(50),
	DISTRCODE VARCHAR(30),
	DISTRBRCODE VARCHAR(30),
	SALESMANCODE VARCHAR(50),
	DATEOFJOIN TIMESTAMP_NTZ(9),
	ISACTIVE VARCHAR(1),
	MODUSERCODE VARCHAR(50),
	MODDT TIMESTAMP_NTZ(9),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_SALESMAN (
	CMPCODE VARCHAR(10),
	DISTCODE VARCHAR(50),
	DISTRBRCODE VARCHAR(30),
	SMCODE VARCHAR(50),
	SMNAME VARCHAR(50),
	SMPHONENO VARCHAR(50),
	SMEMAIL VARCHAR(50),
	RDSSMTYPE VARCHAR(10),
	SMDAILYALLOWANCE NUMBER(38,6),
	SMMONTHLYSALARY NUMBER(38,6),
	SMMKTCREDIT NUMBER(38,6),
	SMCREDITDAYS NUMBER(18,0),
	STATUS VARCHAR(10),
	MODUSERCODE VARCHAR(50),
	MODDT TIMESTAMP_NTZ(9),
	AADHAARNO VARCHAR(50),
	UNIQUESALESCODE VARCHAR(50),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_IN_SALESMAN_ROUTE (
	CMPCODE VARCHAR(10),
	DISTRCODE VARCHAR(50),
	DISTRBRCODE VARCHAR(30),
	SALESMANCODE VARCHAR(50),
	ROUTECODE VARCHAR(100),
	CREATEDDT TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_APOLLO (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_CATEGORY_MAPPING (
	ACCOUNT_NAME VARCHAR(255),
	ARTICLE_CD VARCHAR(20),
	ARTICLE_DESC VARCHAR(255),
	EAN VARCHAR(20),
	SAP_CD VARCHAR(20),
	MOTHER_SKU_NAME VARCHAR(255),
	BRAND_NAME VARCHAR(255),
	FRANCHISE_NAME VARCHAR(255),
	PRODUCT_CATEGORY_NAME VARCHAR(255),
	VARIANT_NAME VARCHAR(255),
	PRODUCT_NAME VARCHAR(255),
	INTERNAL_CATEGORY VARCHAR(255),
	INTERNAL_SUB_CATEGORY VARCHAR(255),
	EXTERNAL_CATEGORY VARCHAR(255),
	EXTERNAL_SUB_CATEGORY VARCHAR(255),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_DABUR (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_DMART (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_FRL (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_HG (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_HISTORICAL_BTL (
	MOTHER_SKU_NAME VARCHAR(255),
	ACCOUNT_NAME VARCHAR(255),
	RE VARCHAR(255),
	POS_DT DATE,
	PROMOS NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_MAX (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_RE_MAPPING (
	STORE_CD VARCHAR(20),
	ACCOUNT_NAME VARCHAR(255),
	STORE_NAME VARCHAR(255),
	REGION VARCHAR(255),
	ZONE VARCHAR(255),
	RE VARCHAR(255),
	PROMOTOR VARCHAR(10),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_RIL (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_SPENCER (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_TESCO (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_POS_VMM (
	KEY_ACCOUNT_NAME VARCHAR(200),
	POS_DT DATE,
	STORE_CODE VARCHAR(50),
	ARTICLE_CODE VARCHAR(50),
	SUBCATEGORY VARCHAR(50),
	LEVEL VARCHAR(50),
	SLS_QTY NUMBER(38,6),
	SLS_VAL_LCY NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	FILE_UPLOAD_DATE DATE,
	CRT_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_WINCULUM_DAILYSALES (
	DISTCODE VARCHAR(50),
	SALINVDATE TIMESTAMP_NTZ(9),
	SALINVNO VARCHAR(50),
	RTRCODE VARCHAR(100),
	PRODUCTCODE VARCHAR(50),
	PRDQTY NUMBER(18,0),
	NR NUMBER(18,6),
	TOTAL_PRICE NUMBER(18,6),
	TAX NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);
CREATE TABLE IF NOT EXISTS SDL_RAW_WINCULUM_SALESRETURN (
	DISTCODE VARCHAR(50),
	SRNDATE TIMESTAMP_NTZ(9),
	SRNREFNO VARCHAR(50),
	RTRCODE VARCHAR(100),
	PRODUCTCODE VARCHAR(50),
	PRDQTY NUMBER(18,0),
	NR NUMBER(18,6),
	TOTAL_PRICE NUMBER(18,6),
	TAX NUMBER(38,6),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS SDL_SALESRETURN_DEL_RAW (
	DISTCODE VARCHAR(50),
	SRNREFNO VARCHAR(50),
	SRNREFTYPE VARCHAR(200),
	SRNDATE TIMESTAMP_NTZ(9),
	SRNMODE VARCHAR(50),
	SRNTYPE VARCHAR(50),
	SRNGROSSAMT NUMBER(38,6),
	SRNSPLDISCAMT NUMBER(38,6),
	SRNSCHDISCAMT NUMBER(38,6),
	SRNCASHDISCAMT NUMBER(38,6),
	SRNDBDISCAMT NUMBER(38,6),
	SRNTAXAMT NUMBER(38,6),
	SRNROUNDOFFAMT NUMBER(38,6),
	SRNNETAMT NUMBER(38,6),
	SALESMANNAME VARCHAR(100),
	SALESROUTENAME VARCHAR(100),
	RTRCODE VARCHAR(50),
	RTRNAME VARCHAR(100),
	PRDSALINVNO VARCHAR(50),
	PRDLCNCODE VARCHAR(50),
	PRDCODE VARCHAR(50),
	PRDBATCDE VARCHAR(50),
	PRDSALQTY NUMBER(18,0),
	PRDUNSALQTY NUMBER(18,0),
	PRDOFFERQTY NUMBER(18,0),
	PRDSELRATE NUMBER(38,6),
	PRDGROSSAMT NUMBER(38,6),
	PRDSPLDISCAMT NUMBER(38,6),
	PRDSCHDISCAMT NUMBER(38,6),
	PRDCASHDISCAMT NUMBER(38,6),
	PRDDBDISCAMT NUMBER(38,6),
	PRDTAXAMT NUMBER(38,6),
	PRDNETAMT NUMBER(38,6),
	MRP NUMBER(18,6),
	RTNFREEQTYVALUE NUMBER(38,6),
	REFERENCETYPE VARCHAR(100),
	SALESMANCODE VARCHAR(50),
	SALESROUTECODE VARCHAR(50),
	NRVALUE NUMBER(18,6),
	PRDSELRATEAFTERTAX NUMBER(18,6),
	MRPCS NUMBER(18,6),
	LPVALUE NUMBER(18,6),
	RTNWINDOWDISPLAYAMT NUMBER(38,6),
	CRADJAMT NUMBER(38,6),
	RTRURCCODE VARCHAR(50),
	CREATEDDATE TIMESTAMP_NTZ(9),
	MODIFIEDDATE TIMESTAMP_NTZ(9),
	SYNCID NUMBER(38,6),
	RTNLINECOUNT NUMBER(18,6),
	CREATEDDT TIMESTAMP_NTZ(9),
	RUN_ID NUMBER(14,0),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	FILE_NAME VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SDL_SKU_RECOM_ORANGESTORETARGET_RAW (
	FROMDATE NUMBER(18,0),
	TODATE NUMBER(18,0),
	RSMCODE VARCHAR(50),
	RSMNAME VARCHAR(50),
	ASMCODE VARCHAR(50),
	ASMNAME VARCHAR(50),
	ABICODE VARCHAR(50),
	ABINAME VARCHAR(50),
	CHANNELCODE VARCHAR(50),
	CHANNELNAME VARCHAR(50),
	SUBCHANNELCODE VARCHAR(50),
	SUBCHANNELNAME VARCHAR(50),
	CLASS VARCHAR(50),
	PERCENTAGE NUMBER(38,2),
	DATEOFSHARING NUMBER(18,0),
	MTH_MM NUMBER(18,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID NUMBER(38,0)
);
