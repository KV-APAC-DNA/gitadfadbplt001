create or replace TABLE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_PRINT_COMP_SPEND (
	MEDIUM VARCHAR(500),
	CATEGORY VARCHAR(500),
	COMP VARCHAR(500),
	COMPTTN VARCHAR(500),
	MONTH VARCHAR(500),
	WEEK VARCHAR(500),
	YR_MTH VARCHAR(500),
	CONCATE VARCHAR(500),
	SUPER_CATEGORY VARCHAR(500),
	PRODUCT_GROUP VARCHAR(500),
	ADVERTISER VARCHAR(500),
	PRODUCT VARCHAR(500),
	AD_MAIN_TYPE VARCHAR(500),
	AD_SUB_TYPE VARCHAR(500),
	PARENT_PUBLICATION VARCHAR(500),
	PUBLICATION VARCHAR(500),
	SUPPLEMENTARY VARCHAR(500),
	DATE VARCHAR(500),
	PAGENO VARCHAR(500),
	PAGE_TITLE VARCHAR(500),
	POSITION VARCHAR(500),
	AD_TYPE VARCHAR(500),
	AD_LANGUAGE VARCHAR(500),
	LOCATION VARCHAR(500),
	PAGE_SIDE VARCHAR(500),
	PUB_NATURE VARCHAR(500),
	PUB_GROUP VARCHAR(500),
	PUB_LANGUAGE VARCHAR(500),
	PUB_PERIODICITY VARCHAR(500),
	PUB_GENRE VARCHAR(500),
	ZONE VARCHAR(500),
	STATE VARCHAR(500),
	EDITION VARCHAR(500),
	SALES_PROMO VARCHAR(500),
	INNOVATION VARCHAR(500),
	FESTIVAL VARCHAR(500),
	AGENCY VARCHAR(500),
	COL VARCHAR(500),
	CM VARCHAR(500),
	AD_CM VARCHAR(500),
	VOL_CC VARCHAR(500),
	VOL_SQCM VARCHAR(500),
	TAM_COST VARCHAR(500),
	DD VARCHAR(500),
	MN VARCHAR(500),
	YR VARCHAR(500),
	DAY VARCHAR(500),
	HOUSE_ADS VARCHAR(500),
	ADS VARCHAR(500),
	PAGETAG VARCHAR(500),
	DISCOUNTING_FACTOR VARCHAR(500),
	ADJUSTED_COST_IN_CR VARCHAR(500),
	FILE_NAME VARCHAR(5000),
	LOAD_DATE DATE
);



create or replace TABLE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_MICROLEVEL_REACH (
	REPORT_NAME VARCHAR(100) NOT NULL,
	DATA_SOURCE VARCHAR(100) NOT NULL,
	MERGEID VARCHAR(100) NOT NULL,
	COUNTRY VARCHAR(100) NOT NULL,
	PLATFORM VARCHAR(100) NOT NULL,
	BRAND_NAME VARCHAR(100) NOT NULL,
	SUBBRAND VARCHAR(100) NOT NULL,
	CAMPAIGN_PHASE VARCHAR(100) NOT NULL,
	MARKET_NAME VARCHAR(100) NOT NULL,
	MARKET_CLUSTER VARCHAR(100) NOT NULL,
	CAMPAIGN_NAME_NEW VARCHAR(100) NOT NULL,
	HVA_TG_NAME VARCHAR(100) NOT NULL,
	AD_TYPE VARCHAR(100) NOT NULL,
	BUY_TYPE VARCHAR(100),
	CAMPAIGN_MONTH VARCHAR(100),
	MONTH_FILTER VARCHAR(100),
	REACH_SOURCE_FILE VARCHAR(100),
	REPORTING_STARTS VARCHAR(100),
	REPORTING_ENDS VARCHAR(100),
	CAMPAIGN_START VARCHAR(100),
	CAMPAIGN_END VARCHAR(100),
	CURRENCY VARCHAR(100),
	ACCOUNT_ID VARCHAR(5000),
	ACCOUNT_NAME VARCHAR(100),
	PARTNER_ID VARCHAR(5000),
	PARTNER VARCHAR(100),
	ADVERTISER_ID VARCHAR(5000),
	ADVERTISER VARCHAR(100),
	CAMPAIGN_ID VARCHAR(5000),
	CAMPAIGN_TAXONOMY VARCHAR(400),
	INSERTION_ORDER_ID VARCHAR(5000),
	AD_SET_ID VARCHAR(5000),
	IO_AD_SET_NAME_TAXONOMY VARCHAR(400),
	LINE_ITEM_TYPE VARCHAR(100),
	MEDIA_TYPE VARCHAR(100),
	BUYING_TYPE VARCHAR(100),
	OBJECTIVE VARCHAR(100),
	LATEST_UPDATE VARCHAR(100),
	REACH VARCHAR(5000),
	ESTM_REACH VARCHAR(5000),
	IMPRESSIONS VARCHAR(5000),
	ESTM_IMPRESSIONS VARCHAR(5000),
	SPENDS VARCHAR(5000),
	ESTM_MEDIA_SPENDS VARCHAR(5000),
	CLICKS VARCHAR(5000),
	ESTM_CLICKS VARCHAR(5000),
	VIDEO_VIEWS_THRUPLAYS VARCHAR(5000),
	ESTM_VIDEO_VIEWS_THRUPLAYS VARCHAR(5000),
	QUARTILE_VIDEO_VIEWS_1ST VARCHAR(5000),
	QUARTILE_VIDEO_VIEWS_2ND VARCHAR(5000),
	QUARTILE_VIDEO_VIEWS_3RD VARCHAR(5000),
	QUARTILE_VIDEO_VIEWS_4TH VARCHAR(5000),
	TRUEVIEW_VIEWS VARCHAR(5000),
	THRUPLAYS VARCHAR(5000),
	SECOND_VIDEO_PLAYS_3 VARCHAR(5000),
	COMPANION_VIEWS_VIDEO VARCHAR(5000),
	SKIPS_VIDEO VARCHAR(5000),
	BILLABLE_IMPRESSIONS VARCHAR(5000),
	MEDIA_COST_ADVERTISER_CURRENCY VARCHAR(5000),
	CLICKS_ALL VARCHAR(5000),
	PAGE_ENGAGEMENT VARCHAR(5000),
	POST_ENGAGEMENTS VARCHAR(5000),
	FOLLOWS_OR_LIKES VARCHAR(5000),
	FILE_NAME VARCHAR(10000),
	LOAD_DATE DATE
);



create or replace TABLE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_PLATFORMLEVEL_REACH (
	REPORT_NAME VARCHAR(100),
	DATA_SOURCE VARCHAR(100),
	MERGEID VARCHAR(200),
	COUNTRY VARCHAR(100),
	PLATFORM VARCHAR(100),
	BRAND_NAME VARCHAR(100),
	SUBBRAND VARCHAR(100),
	CAMPAIGN_PHASE VARCHAR(100),
	MARKET_NAME VARCHAR(100),
	MARKET_CLUSTER VARCHAR(100),
	CAMPAIGN_NAME_NEW VARCHAR(100),
	HVA_TG_NAME VARCHAR(100),
	AD_TYPE VARCHAR(100),
	BUY_TYPE VARCHAR(100),
	CAMPAIGN_MONTH VARCHAR(100),
	MONTH_FILTER DATE,
	REACH_SOURCE_FILE VARCHAR(100),
	REPORTING_STARTS DATE,
	REPORTING_ENDS DATE,
	CAMPAIGN_START DATE,
	CAMPAIGN_END DATE,
	CURRENCY VARCHAR(100),
	ACCOUNT_ID VARCHAR(100),
	ACCOUNT_NAME VARCHAR(100),
	PARTNER_ID VARCHAR(100),
	PARTNER VARCHAR(100),
	ADVERTISER_ID VARCHAR(100),
	ADVERTISER VARCHAR(1S00),
	CAMPAIGN_ID VARCHAR(100),
	CAMPAIGN_TAXONOMY VARCHAR(400),
	INSERTION_ORDER_ID VARCHAR(100),
	AD_SET_ID VARCHAR(100),
	IO_AD_SET_NAME_TAXONOMY VARCHAR(400),
	LINE_ITEM_TYPE VARCHAR(100),
	MEDIA_TYPE VARCHAR(100),
	BUYING_TYPE VARCHAR(100),
	OBJECTIVE VARCHAR(100),
	LATEST_UPDATE DATE,
	REACH VARCHAR(100),
	ESTM_REACH VARCHAR(100),
	IMPRESSIONS VARCHAR(100),
	ESTM_IMPRESSIONS VARCHAR(100),
	SPENDS VARCHAR(100),
	ESTM_MEDIA_SPENDS VARCHAR(100),
	CLICKS VARCHAR(100),
	ESTM_CLICKS VARCHAR(100),
	VIDEO_VIEWS_THRUPLAYS VARCHAR(100),
	ESTM_VIDEO_VIEWS_THRUPLAYS VARCHAR(100),
	QUARTILE_VIDEO_VIEWS_1ST VARCHAR(100),
	QUARTILE_VIDEO_VIEWS_2ND VARCHAR(100),
	QUARTILE_VIDEO_VIEWS_3RD VARCHAR(100),
	QUARTILE_VIDEO_VIEWS_4TH VARCHAR(100),
	TRUEVIEW_VIEWS VARCHAR(100),
	THRUPLAYS VARCHAR(100),
	SECOND_VIDEO_PLAYS_3 VARCHAR(100),
	COMPANION_VIEWS_VIDEO VARCHAR(100),
	SKIPS_VIDEO VARCHAR(100),
	BILLABLE_IMPRESSIONS VARCHAR(100),
	MEDIA_COST_ADVERTISER_CURRENCY VARCHAR(100),
	CLICKS_ALL VARCHAR(100),
	PAGE_ENGAGEMENT VARCHAR(100),
	POST_ENGAGEMENTS VARCHAR(100),
	FOLLOWS_OR_LIKES VARCHAR(100),
	FILE_NAME VARCHAR(100),
	LOAD_DATE DATE
);


update prod_dna_load.meta_raw.process set IS_ACTIVE='TRUE' where process_id=3030
update prod_dna_load.meta_raw.parameters set IS_ACTIVE='TRUE' where parameter_group_id=3030
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Print_Competitive' where parameter_id= 28577
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Print_Competitive' where parameter_id= 28582


update prod_dna_load.meta_raw.process set IS_ACTIVE='TRUE' where process_id=3028
update prod_dna_load.meta_raw.parameters set IS_ACTIVE='TRUE' where parameter_group_id=3028
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Micro_Level_Reach' where parameter_id=28545
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Micro_Level_Reach' where parameter_id=28550


update prod_dna_load.meta_raw.process set IS_ACTIVE='TRUE' where process_id=3029
update prod_dna_load.meta_raw.parameters set IS_ACTIVE='TRUE' where parameter_group_id=3029
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Platform_Level_Reach' where parameter_id=28561
update prod_dna_load.meta_raw.parameters set PARAMETER_VALUE='Platform_Level_Reach' where parameter_id=28566

alter table
PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_TV_GRP_SPENDS
modify column file_name varchar(100);
