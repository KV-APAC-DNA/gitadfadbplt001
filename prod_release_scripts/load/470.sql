--DROP TABLE ASPSDL_RAW.cust_customer;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.CUST_CUSTOMER		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.cust_customer
(
	region VARCHAR(32)  		--//  ENCODE lzo
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,remotekey VARCHAR(64)  		--//  ENCODE lzo
	,customername VARCHAR(256)  		--//  ENCODE lzo
	,country VARCHAR(256)  		--//  ENCODE lzo
	,county VARCHAR(256)  		--//  ENCODE lzo
	,district VARCHAR(256)  		--//  ENCODE lzo
	,city VARCHAR(128)  		--//  ENCODE lzo
	,postcode VARCHAR(64)  		--//  ENCODE lzo
	,streetname VARCHAR(512)  		--//  ENCODE lzo
	,streetnumber VARCHAR(64)  		--//  ENCODE lzo
	,storereference VARCHAR(64)  		--//  ENCODE lzo
	,email VARCHAR(128)  		--//  ENCODE lzo
	,phonenumber VARCHAR(32)  		--//  ENCODE lzo
	,storetype VARCHAR(128)  		--//  ENCODE lzo
	,website VARCHAR(128)  		--//  ENCODE lzo
	,ecommerceflag VARCHAR(16)  		--//  ENCODE lzo
	,marketingpermission VARCHAR(16)  		--//  ENCODE lzo
	,channel VARCHAR(128)  		--//  ENCODE lzo
	,salesgroup VARCHAR(128)  		--//  ENCODE lzo
	,secondarytradecode VARCHAR(128)  		--//  ENCODE lzo
	,secondarytradename VARCHAR(256)  		--//  ENCODE lzo
	,soldtoparty VARCHAR(128)  		--//  ENCODE lzo
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(256)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.itg_sfa_pm;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.ITG_SFA_PM		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.itg_sfa_pm
(
	taskid VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(255)  		--//  ENCODE lzo
	,path VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,mrchr_visitdate VARCHAR(32)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,remotekey VARCHAR(64)  		--//  ENCODE lzo
	,customername VARCHAR(256)  		--//  ENCODE lzo
	,salesgroup VARCHAR(128)  		--//  ENCODE lzo
	,storetype VARCHAR(128)  		--//  ENCODE lzo
	,dist_chnl VARCHAR(128)  		--//  ENCODE lzo
	,country VARCHAR(256)  		--//  ENCODE lzo
	,salescyclename VARCHAR(256)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(256)  		--//  ENCODE lzo
	,mastertaskname VARCHAR(256)  		--//  ENCODE lzo
	,salesperson_firstname VARCHAR(128)  		--//  ENCODE lzo
	,salesperson_lastname VARCHAR(128)  		--//  ENCODE lzo
	,secondarytradecode VARCHAR(128)  		--//  ENCODE lzo
	,secondarytradename VARCHAR(256)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPSDL_RAW.kpi2data_mapping;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.KPI2DATA_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.kpi2data_mapping
(
	ctry VARCHAR(20)  		--//  ENCODE lzo
	,data_type VARCHAR(30)  		--//  ENCODE lzo
	,identifier VARCHAR(30)  		--//  ENCODE lzo
	,kpi_name VARCHAR(30)  		--//  ENCODE lzo
	,store_type VARCHAR(50)  		--//  ENCODE lzo
	,category VARCHAR(50)  		--//  ENCODE lzo
	,segment VARCHAR(50)  		--//  ENCODE lzo
	,value VARCHAR(50)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE lzo
	,end_date DATE  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.mrchr_merchandisingresponse;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.MRCHR_MERCHANDISINGRESPONSE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.mrchr_merchandisingresponse
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,merchandisingresponseid VARCHAR(64)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,salespersonid VARCHAR(64)  		--//  ENCODE lzo
	,salescampaignid VARCHAR(64)  		--//  ENCODE lzo
	,visitid VARCHAR(64)  		--//  ENCODE lzo
	,taskid VARCHAR(64)  		--//  ENCODE lzo
	,mastertaskid VARCHAR(64)  		--//  ENCODE lzo
	,merchandisingid VARCHAR(64)  		--//  ENCODE lzo
	,businessunitid VARCHAR(64)  		--//  ENCODE lzo
	,startdate VARCHAR(32)  		--//  ENCODE lzo
	,starttime VARCHAR(32)
	,enddate VARCHAR(32)  		--//  ENCODE lzo
	,endtime VARCHAR(32)
	,status VARCHAR(32)  		--//  ENCODE lzo
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE runlength
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.mrchr_responses;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.MRCHR_RESPONSES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.mrchr_responses
(
	region VARCHAR(32)  		--//  ENCODE lzo
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--//  ENCODE delta32k // BIGINT  
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,merchandisingresponseid VARCHAR(64)  		--//  ENCODE lzo
	,productid VARCHAR(64)  		--//  ENCODE lzo
	,primaryhierarchynodeid VARCHAR(64)  		--//  ENCODE lzo
	,mustcarryitem VARCHAR(16)  		--//  ENCODE lzo
	,presence VARCHAR(16)  		--//  ENCODE lzo
	,pricepresence VARCHAR(16)  		--//  ENCODE lzo
	,pricedetails DOUBLE PRECISION  		--//  ENCODE bytedict
	,promopresence VARCHAR(16)  		--//  ENCODE lzo
	,promopackpresence VARCHAR(16)  		--//  ENCODE lzo
	,facings DOUBLE PRECISION  		--//  ENCODE bytedict
	,stockcount DOUBLE PRECISION
	,outofstock VARCHAR(32)  		--//  ENCODE lzo
	,horizontalposition VARCHAR(16)  		--//  ENCODE lzo
	,verticalposition VARCHAR(16)  		--//  ENCODE bytedict
	,storeposition VARCHAR(16)  		--//  ENCODE lzo
	,promodetails VARCHAR(256)  		--//  ENCODE lzo
	,categorylength DOUBLE PRECISION
	,categoryfacings DOUBLE PRECISION
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE runlength
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.ms_mastersurvey;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.MS_MASTERSURVEY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.ms_mastersurvey
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,mastersurveyid VARCHAR(64)
	,mastersurveyname VARCHAR(256)
	,mastersurveydescription VARCHAR(256)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.prod_product;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.PROD_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.prod_product
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,productid VARCHAR(64)
	,remotekey VARCHAR(64)
	,productname VARCHAR(256)
	,eannumber VARCHAR(64)
	,width DOUBLE PRECISION
	,materialnumber VARCHAR(64)
	,salesunitofmeasure VARCHAR(16)
	,unitofmeasure VARCHAR(16)
	,widthunitofmeasure VARCHAR(16)
	,deliveryunit VARCHAR(16)
	,islisted VARCHAR(16)
	,isorderable VARCHAR(16)
	,isreturnable VARCHAR(16)
	,maximumorderquantity DOUBLE PRECISION
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.prodbu_productbusinessunit;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.PRODBU_PRODUCTBUSINESSUNIT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.prodbu_productbusinessunit
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)
	,productbusinessunitid VARCHAR(64)
	,remotekey VARCHAR(64)
	,productid VARCHAR(64)
	,productremotekey VARCHAR(64)
	,producttext VARCHAR(256)
	,producttype VARCHAR(32)
	,businessunitid VARCHAR(64)
	,businessunitremotekey VARCHAR(64)
	,businessunittext VARCHAR(256)
	,businessunittype VARCHAR(32)
	,hier1id VARCHAR(64)
	,hier1 VARCHAR(256)
	,hier2id VARCHAR(64)
	,hier2 VARCHAR(256)
	,hier3id VARCHAR(64)
	,hier3 VARCHAR(256)
	,hier4id VARCHAR(64)
	,hier4 VARCHAR(256)
	,hier5id VARCHAR(64)
	,hier5 VARCHAR(256)
	,hier6id VARCHAR(64)
	,hier6 VARCHAR(256)
	,hier7id VARCHAR(64)
	,hier7 VARCHAR(256)
	,hier8id VARCHAR(64)
	,hier8 VARCHAR(256)
	,deliveryunit VARCHAR(16)
	,islisted VARCHAR(16)
	,isorderable VARCHAR(16)
	,isreturnable VARCHAR(16)
	,maximumorderquantity DOUBLE PRECISION
	,salesunitofmeasure VARCHAR(16)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.prodtr_producttranslation;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.PRODTR_PRODUCTTRANSLATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.prodtr_producttranslation
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,producttranslationid VARCHAR(64)
	,remotekey VARCHAR(64)
	,producttranslationname VARCHAR(256)
	,productid VARCHAR(64)
	,eannumber VARCHAR(64)
	,language VARCHAR(16)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.slsc_mastertasks;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SLSC_MASTERTASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.slsc_mastertasks
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,salescampaignid VARCHAR(64)
	,mastertaskid VARCHAR(64)
	,remotekey VARCHAR(64)
	,text VARCHAR(256)
	,type VARCHAR(32)
	,validfrom VARCHAR(32)
	,validto VARCHAR(32)
	,startdate VARCHAR(32)
	,enddate VARCHAR(32)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.slsc_salescampaign;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SLSC_SALESCAMPAIGN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.slsc_salescampaign
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,salescampaignid VARCHAR(64)
	,salescampaignname VARCHAR(256)
	,salescampaigndescription VARCHAR(256)
	,startdate VARCHAR(64)
	,enddate VARCHAR(32)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.slsc_targetgroups;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SLSC_TARGETGROUPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.slsc_targetgroups
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,salescampaignid VARCHAR(64)
	,targetgroupid VARCHAR(64)
	,remotekey VARCHAR(64)
	,name VARCHAR(256)
	,type VARCHAR(32)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.slscyc_salescycle;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SLSCYC_SALESCYCLE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.slscyc_salescycle
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,salescycleid VARCHAR(64)
	,salescyclename VARCHAR(256)
	,salescycledescription VARCHAR(256)
	,startdate VARCHAR(64)
	,enddate VARCHAR(32)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.slsp_salesperson;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SLSP_SALESPERSON		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.slsp_salesperson
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,salespersonid VARCHAR(64)
	,remotekey VARCHAR(64)
	,title VARCHAR(64)
	,firstname VARCHAR(128)
	,lastname VARCHAR(128)
	,jobrole VARCHAR(32)
	,remoteenabled VARCHAR(16)
	,language VARCHAR(64)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.sr_response_values;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SR_RESPONSE_VALUES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.sr_response_values
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,surveyresponseid VARCHAR(64)
	,questionkey VARCHAR(64)
	,valuekey VARCHAR(64)
	,value VARCHAR(1500)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.sr_responses;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SR_RESPONSES		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.sr_responses
(
	region VARCHAR(32)  		--//  ENCODE lzo
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,surveyresponseid VARCHAR(64)  		--//  ENCODE lzo
	,questionkey VARCHAR(64)  		--//  ENCODE lzo
	,questiontext VARCHAR(512)  		--//  ENCODE lzo
	,valuekey VARCHAR(256)  		--//  ENCODE lzo
	,value VARCHAR(1500)  		--//  ENCODE lzo
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.sr_surveyresponse;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.SR_SURVEYRESPONSE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.sr_surveyresponse
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)
	,surveyresponseid VARCHAR(64)  		--//  ENCODE lzo
	,businessunitid VARCHAR(64)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,salespersonid VARCHAR(64)  		--//  ENCODE lzo
	,salescampaignid VARCHAR(64)  		--//  ENCODE lzo
	,visitid VARCHAR(64)  		--//  ENCODE lzo
	,taskid VARCHAR(64)  		--//  ENCODE lzo
	,mastertaskid VARCHAR(64)  		--//  ENCODE lzo
	,mastersurveyid VARCHAR(64)  		--//  ENCODE lzo
	,status VARCHAR(64)
	,enddate VARCHAR(32)
	,endtime VARCHAR(32)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.tgtg_items;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.TGTG_ITEMS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.tgtg_items
(
	region VARCHAR(32)  		--//  ENCODE lzo
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,targetgroupid VARCHAR(64)  		--//  ENCODE lzo
	,itemid VARCHAR(64)  		--//  ENCODE lzo
	,itemtext VARCHAR(256)
	,groupid VARCHAR(64)
	,grouptext VARCHAR(256)
	,groupkeyid VARCHAR(64)
	,groupkeytext VARCHAR(256)
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.tgtg_targetgroup;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.TGTG_TARGETGROUP		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.tgtg_targetgroup
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)
	,azuredatetime VARCHAR(32)
	,targetgroupid VARCHAR(64)
	,remotekey VARCHAR(64)
	,targetgroupname VARCHAR(256)
	,targetgroupdescription VARCHAR(256)
	,target VARCHAR(64)
	,groupkeyid VARCHAR(64)
	,groupkeytext VARCHAR(64)
	,groupcode VARCHAR(32)
	,parentgroupid VARCHAR(64)
	,parentgrouptext VARCHAR(64)
	,parentgroupcode VARCHAR(32)
	,count DOUBLE PRECISION
	,created VARCHAR(32)
	,createdby VARCHAR(64)
	,targetgroupformat VARCHAR(64)
	,level DOUBLE PRECISION
	,status VARCHAR(64)
	,topnodeid VARCHAR(64)
	,type VARCHAR(64)
	,cdl_datetime VARCHAR(24)
	,cdl_source_file VARCHAR(255)
	,load_key VARCHAR(256)
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.tsk_task;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.TSK_TASK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.tsk_task
(
	region VARCHAR(32)
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--// BIGINT
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(32)  		--//  ENCODE lzo
	,taskid VARCHAR(64)  		--//  ENCODE lzo
	,businessunitid VARCHAR(64)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,salespersonid VARCHAR(64)  		--//  ENCODE lzo
	,salescampaignid VARCHAR(64)  		--//  ENCODE lzo
	,visitid VARCHAR(64)  		--//  ENCODE lzo
	,mastertaskid VARCHAR(64)  		--//  ENCODE lzo
	,surveyresponseid VARCHAR(64)  		--//  ENCODE lzo
	,merchandisingresponseid VARCHAR(64)  		--//  ENCODE lzo
	,startdate VARCHAR(32)  		--//  ENCODE lzo
	,starttime VARCHAR(32)
	,enddate VARCHAR(32)
	,endtime VARCHAR(32)
	,status VARCHAR(32)
	,ismandatory VARCHAR(16)
	,ispriority VARCHAR(16)
	,isrecurring VARCHAR(16)
	,canaddcontacts VARCHAR(16)
	,canaddimages VARCHAR(16)
	,mustaddcontacts VARCHAR(16)
	,mustaddimages VARCHAR(16)
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE ASPSDL_RAW.vst_visit;
CREATE TABLE IF NOT EXISTS ASPSDL_RAW.VST_VISIT		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPSDL_RAW.vst_visit
(
	region VARCHAR(32)  		--//  ENCODE lzo
	,fetcheddatetime VARCHAR(32)  		--//  ENCODE lzo
	,fetchedsequence numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,azurefile VARCHAR(128)  		--//  ENCODE lzo
	,azuredatetime VARCHAR(128)  		--//  ENCODE lzo
	,visitid VARCHAR(64)  		--//  ENCODE lzo
	,businessunitid VARCHAR(64)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,salespersonid VARCHAR(64)  		--//  ENCODE lzo
	,salescycleid VARCHAR(64)  		--//  ENCODE lzo
	,salesunitid VARCHAR(64)  		--//  ENCODE lzo
	,targetgroupid VARCHAR(64)  		--//  ENCODE lzo
	,scheduleddate VARCHAR(32)  		--//  ENCODE lzo
	,scheduledtime VARCHAR(32)  		--//  ENCODE lzo
	,duration VARCHAR(64)  		--//  ENCODE lzo
	,status VARCHAR(64)  		--//  ENCODE lzo
	,defaultduration VARCHAR(64)  		--//  ENCODE lzo
	,targetvisits DOUBLE PRECISION
	,visittargetid VARCHAR(64)  		--//  ENCODE lzo
	,activeduration VARCHAR(64)  		--//  ENCODE lzo
	,addlocationwarning VARCHAR(64)  		--//  ENCODE lzo
	,pauseduration VARCHAR(64)  		--//  ENCODE lzo
	,endtime VARCHAR(64)  		--//  ENCODE lzo
	,cdl_datetime VARCHAR(24)  		--//  ENCODE lzo
	,cdl_source_file VARCHAR(255)  		--//  ENCODE lzo
	,load_key VARCHAR(256)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;

--DROP TABLE jpnsdl_raw.sdl_pop6_jp_display_plans;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_displays;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,display_type VARCHAR(255)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE zstd
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_exclusion;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_EXCLUSION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_exclusion
(
	exclude_kpi VARCHAR(10)  		--//  ENCODE zstd
	,visit_date DATE  		--//  ENCODE az64
	,pop_code VARCHAR(40)  		--//  ENCODE zstd
	,country VARCHAR(10)  		--//  ENCODE zstd
	,merchandiser_userid VARCHAR(100)  		--//  ENCODE zstd
	,audit_form_name VARCHAR(500)  		--//  ENCODE zstd
	,section_name VARCHAR(500)  		--//  ENCODE zstd
	,operation_type VARCHAR(10)  		--//  ENCODE zstd
	,file_name VARCHAR(40)  		--//  ENCODE zstd
	,run_id VARCHAR(40)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_executed_visits;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE zstd
	,check_out_photo VARCHAR(200)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,superior_username VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE zstd
	,cancellation_note VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_general_audits;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,subsection_id VARCHAR(255)  		--//  ENCODE zstd
	,subsection VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_planned_visits;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_pop_lists;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_pop_lists
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pop_list VARCHAR(25)  		--//  ENCODE zstd
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,pop_list_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_pops;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_pops
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(255)  		--//  ENCODE zstd
	,business_unit_name VARCHAR(255)  		--//  ENCODE zstd
	,country VARCHAR(200)  		--//  ENCODE zstd
	,channel VARCHAR(200)  		--//  ENCODE zstd
	,retail_environment_ps VARCHAR(200)  		--//  ENCODE zstd
	,sales_group_name VARCHAR(200)  		--//  ENCODE zstd
	,customer VARCHAR(200)  		--//  ENCODE zstd
	,sales_group_code VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,territory_or_region VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_product_attribute_audits;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_product_lists_allocation;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_product_lists_allocation
(
	product_group_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_group VARCHAR(25)  		--//  ENCODE zstd
	,product_list_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_list VARCHAR(100)  		--//  ENCODE zstd
	,product_list_code VARCHAR(100)  		--//  ENCODE zstd
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_attribute VARCHAR(200)  		--//  ENCODE zstd
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_product_lists_pops;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_product_lists_pops
(
	product_list VARCHAR(100)  		--//  ENCODE zstd
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_product_lists_products;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_product_lists_products
(
	product_list VARCHAR(100)  		--//  ENCODE zstd
	,product_list_code VARCHAR(100)  		--//  ENCODE zstd
	,productdb_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(150)  		--//  ENCODE zstd
	,msl_ranking VARCHAR(20)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_products;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_products
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,productdb_id VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(150)  		--//  ENCODE zstd
	,sku VARCHAR(150)  		--//  ENCODE zstd
	,unit_price NUMERIC(18,2)  		--//  ENCODE az64
	,display_order numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,launch_date VARCHAR(20)  		--//  ENCODE zstd
	,largest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,middle_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,smallest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,company VARCHAR(200)  		--//  ENCODE zstd
	,sku_english VARCHAR(200)  		--//  ENCODE zstd
	,sku_code VARCHAR(200)  		--//  ENCODE zstd
	,ps_category VARCHAR(200)  		--//  ENCODE zstd
	,ps_segment VARCHAR(200)  		--//  ENCODE zstd
	,ps_category_segment VARCHAR(200)  		--//  ENCODE zstd
	,country_l1 VARCHAR(200)  		--//  ENCODE zstd
	,regional_franchise_l2 VARCHAR(200)  		--//  ENCODE zstd
	,franchise_l3 VARCHAR(200)  		--//  ENCODE zstd
	,brand_l4 VARCHAR(200)  		--//  ENCODE zstd
	,sub_category_l5 VARCHAR(200)  		--//  ENCODE zstd
	,platform_l6 VARCHAR(200)  		--//  ENCODE zstd
	,variance_l7 VARCHAR(200)  		--//  ENCODE zstd
	,pack_size_l8 VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_promotion_plans;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE zstd
	,photo_field VARCHAR(255)  		--//  ENCODE zstd
	,reason_field VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_promotions;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,promotion_type VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE zstd
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE zstd
	,photo VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_service_levels;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,team VARCHAR(200)  		--//  ENCODE zstd
	,visit_frequency VARCHAR(10)  		--//  ENCODE zstd
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE zstd
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_sku_audits;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_tasks;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnsdl_raw.sdl_pop6_jp_users;
CREATE TABLE IF NOT EXISTS JPNSDL_RAW.SDL_POP6_JP_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnsdl_raw.sdl_pop6_jp_users
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,userdb_id VARCHAR(255)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,first_name VARCHAR(50)  		--//  ENCODE zstd
	,last_name VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,authorisation_group VARCHAR(50)  		--//  ENCODE zstd
	,email_address VARCHAR(50)  		--//  ENCODE zstd
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(200)  		--//  ENCODE zstd
	,business_unit_name VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)

;

--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_display_plans;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_displays;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,display_type VARCHAR(255)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE lzo
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_executed_visits;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE lzo
	,check_out_photo VARCHAR(200)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,superior_username VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE lzo
	,cancellation_note VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_general_audits;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,subsection_id VARCHAR(255)  		--//  ENCODE lzo
	,subsection VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_planned_visits;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_pop_lists;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_pop_lists
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pop_list VARCHAR(25)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,pop_list_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_pops;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_pops
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(200)  		--//  ENCODE lzo
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,retail_environment_ps VARCHAR(200)  		--//  ENCODE lzo
	,sales_group_name VARCHAR(200)  		--//  ENCODE lzo
	,customer VARCHAR(200)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,territory_or_region VARCHAR(200)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_product_attribute_audits;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_product_lists_allocation;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_product_lists_allocation
(
	product_group_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_group VARCHAR(25)  		--//  ENCODE lzo
	,product_list_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_list VARCHAR(100)  		--//  ENCODE lzo
	,product_list_code VARCHAR(100)  		--//  ENCODE lzo
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute VARCHAR(200)  		--//  ENCODE lzo
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_product_lists_pops;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_product_lists_pops
(
	product_list VARCHAR(100)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_product_lists_products;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_product_lists_products
(
	product_list VARCHAR(100)  		--//  ENCODE lzo
	,product_list_code VARCHAR(100)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(150)  		--//  ENCODE lzo
	,msl_ranking VARCHAR(20)  		--//  ENCODE lzo
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_products;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_products
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(150)  		--//  ENCODE lzo
	,sku VARCHAR(150)  		--//  ENCODE lzo
	,unit_price NUMERIC(18,2)  		--//  ENCODE az64
	,display_order numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,launch_date VARCHAR(20)  		--//  ENCODE lzo
	,largest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,middle_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,smallest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,company VARCHAR(200)  		--//  ENCODE lzo
	,sku_english VARCHAR(200)  		--//  ENCODE lzo
	,sku_code VARCHAR(200)  		--//  ENCODE lzo
	,ps_category VARCHAR(200)  		--//  ENCODE lzo
	,ps_segment VARCHAR(200)  		--//  ENCODE lzo
	,ps_category_segment VARCHAR(200)  		--//  ENCODE lzo
	,country_l1 VARCHAR(200)  		--//  ENCODE lzo
	,regional_franchise_l2 VARCHAR(200)  		--//  ENCODE lzo
	,franchise_l3 VARCHAR(200)  		--//  ENCODE lzo
	,brand_l4 VARCHAR(200)  		--//  ENCODE lzo
	,sub_category_l5 VARCHAR(200)  		--//  ENCODE lzo
	,platform_l6 VARCHAR(200)  		--//  ENCODE lzo
	,variance_l7 VARCHAR(200)  		--//  ENCODE lzo
	,pack_size_l8 VARCHAR(200)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_promotion_plans;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE lzo
	,photo_field VARCHAR(255)  		--//  ENCODE lzo
	,reason_field VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_promotions;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,promotion_type VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE lzo
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_service_levels;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,team VARCHAR(200)  		--//  ENCODE lzo
	,visit_frequency VARCHAR(10)  		--//  ENCODE lzo
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE lzo
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_sku_audits;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_tasks;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE SGPSDL_RAW.sdl_pop6_sg_users;
CREATE TABLE IF NOT EXISTS SGPSDL_RAW.SDL_POP6_SG_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS SGPSDL_RAW.sdl_pop6_sg_users
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,userdb_id VARCHAR(255)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,first_name VARCHAR(50)  		--//  ENCODE lzo
	,last_name VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,authorisation_group VARCHAR(50)  		--//  ENCODE lzo
	,email_address VARCHAR(50)  		--//  ENCODE lzo
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(200)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(200)  		--//  ENCODE lzo
	,mobile_number VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;

--DROP TABLE THASDL_RAW.sdl_pop6_th_display_plans;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_displays;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,display_type VARCHAR(255)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE zstd
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_executed_visits;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE zstd
	,check_out_photo VARCHAR(200)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,superior_username VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE zstd
	,cancellation_note VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_general_audits;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,subsection_id VARCHAR(255)  		--//  ENCODE zstd
	,subsection VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_planned_visits;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_pop_lists;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_pop_lists
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pop_list VARCHAR(25)  		--//  ENCODE zstd
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,pop_list_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_pops;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_pops
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(255)  		--//  ENCODE zstd
	,business_unit_name VARCHAR(255)  		--//  ENCODE zstd
	,country VARCHAR(200)  		--//  ENCODE zstd
	,channel VARCHAR(200)  		--//  ENCODE zstd
	,retail_environment_ps VARCHAR(200)  		--//  ENCODE zstd
	,sales_group_name VARCHAR(200)  		--//  ENCODE zstd
	,customer VARCHAR(200)  		--//  ENCODE zstd
	,sales_group_code VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,external_store_code VARCHAR(200)  		--//  ENCODE zstd
	,territory_or_region VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_product_attribute_audits;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_product_lists_allocation;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_product_lists_allocation
(
	product_group_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_group VARCHAR(25)  		--//  ENCODE zstd
	,product_list_status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_list VARCHAR(100)  		--//  ENCODE zstd
	,product_list_code VARCHAR(100)  		--//  ENCODE zstd
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_attribute VARCHAR(200)  		--//  ENCODE zstd
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_product_lists_pops;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_product_lists_pops
(
	product_list VARCHAR(100)  		--//  ENCODE zstd
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_product_lists_products;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_product_lists_products
(
	product_list VARCHAR(100)  		--//  ENCODE zstd
	,product_list_code VARCHAR(100)  		--//  ENCODE zstd
	,productdb_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(150)  		--//  ENCODE zstd
	,msl_ranking VARCHAR(20)  		--//  ENCODE zstd
	,prod_grp_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_products;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_products
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,productdb_id VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(150)  		--//  ENCODE zstd
	,sku VARCHAR(150)  		--//  ENCODE zstd
	,unit_price NUMERIC(18,2)  		--//  ENCODE az64
	,display_order numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,launch_date VARCHAR(20)  		--//  ENCODE zstd
	,largest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,middle_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,smallest_uom_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,company VARCHAR(200)  		--//  ENCODE zstd
	,sku_english VARCHAR(200)  		--//  ENCODE zstd
	,sku_code VARCHAR(200)  		--//  ENCODE zstd
	,ps_category VARCHAR(200)  		--//  ENCODE zstd
	,ps_segment VARCHAR(200)  		--//  ENCODE zstd
	,ps_category_segment VARCHAR(200)  		--//  ENCODE zstd
	,country_l1 VARCHAR(200)  		--//  ENCODE zstd
	,regional_franchise_l2 VARCHAR(200)  		--//  ENCODE zstd
	,franchise_l3 VARCHAR(200)  		--//  ENCODE zstd
	,brand_l4 VARCHAR(200)  		--//  ENCODE zstd
	,sub_category_l5 VARCHAR(200)  		--//  ENCODE zstd
	,platform_l6 VARCHAR(200)  		--//  ENCODE zstd
	,variance_l7 VARCHAR(200)  		--//  ENCODE zstd
	,pack_size_l8 VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_promotion_plans;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE zstd
	,photo_field VARCHAR(255)  		--//  ENCODE zstd
	,reason_field VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_promotions;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,promotion_type VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE zstd
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE zstd
	,photo VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_rir_data;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_RIR_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_rir_data
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,facing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,is_eyelevel numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_service_levels;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,team VARCHAR(200)  		--//  ENCODE zstd
	,visit_frequency VARCHAR(10)  		--//  ENCODE zstd
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE zstd
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_sku_audits;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE THASDL_RAW.sdl_pop6_th_users;
CREATE TABLE IF NOT EXISTS THASDL_RAW.SDL_POP6_TH_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS THASDL_RAW.sdl_pop6_th_users
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,userdb_id VARCHAR(255)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,first_name VARCHAR(50)  		--//  ENCODE zstd
	,last_name VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,authorisation_group VARCHAR(50)  		--//  ENCODE zstd
	,email_address VARCHAR(50)  		--//  ENCODE zstd
	,longitude NUMERIC(18,5)  		--//  ENCODE az64
	,latitude NUMERIC(18,5)  		--//  ENCODE az64
	,business_units_id VARCHAR(200)  		--//  ENCODE zstd
	,business_unit_name VARCHAR(200)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,hashkey VARCHAR(200)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;

