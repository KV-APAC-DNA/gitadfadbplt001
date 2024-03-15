create or replace TABLE VNMSDL_RAW.SDL_MDS_VN_ALLCHANNEL_SISO_TARGET_SKU (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CYCLE NUMBER(31,0),
	"data type" VARCHAR(200),
	"jnj code" NUMBER(31,0),
	DESCRIPTION VARCHAR(200),
	"dksh - mti" NUMBER(31,7),
	"dksh - ecom" NUMBER(31,7),
	MTD NUMBER(31,7),
	"gt - tien thanh" NUMBER(31,7),
	"gt - duong anh" NUMBER(31,7),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500),
	OTC VARCHAR(100)
);

--DROP TABLE VNMSDL_RAW.sdl_mds_vn_customer_sales_organization;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_CUSTOMER_SALES_ORGANIZATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_customer_sales_organization
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
	,mtd_code NUMERIC(31,0)  		--//  ENCODE az64
	,mti_code NUMERIC(31,0)  		--//  ENCODE az64
	,customer_name VARCHAR(600)  		--//  ENCODE lzo
	,address VARCHAR(1200)  		--//  ENCODE lzo
	,district_name VARCHAR(200)  		--//  ENCODE lzo
	,code_ss VARCHAR(60)  		--//  ENCODE lzo
	,sales_supervisor VARCHAR(200)  		--//  ENCODE lzo
	,code_sr_pg VARCHAR(60)  		--//  ENCODE lzo
	,sales_man VARCHAR(400)  		--//  ENCODE lzo
	,status VARCHAR(60)  		--//  ENCODE lzo
	,dksh_jnj VARCHAR(200)  		--//  ENCODE lzo
	,rom VARCHAR(60)  		--//  ENCODE lzo
	,kam VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_customer_segmentation;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_CUSTOMER_SEGMENTATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_customer_segmentation
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
	,customer_segmentation_level_1_code VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_1_name VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_1_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,customer_segmentation_level_2_code VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_2_name VARCHAR(500)  		--//  ENCODE lzo
	,customer_segmentation_level_2_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_distributor_customers;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_DISTRIBUTOR_CUSTOMERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_distributor_customers
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
	,address VARCHAR(200)  		--//  ENCODE lzo
	,sub_channel_code VARCHAR(500)  		--//  ENCODE lzo
	,sub_channel_name VARCHAR(500)  		--//  ENCODE lzo
	,sub_channel_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,group_account_code VARCHAR(500)  		--//  ENCODE lzo
	,group_account_name VARCHAR(500)  		--//  ENCODE lzo
	,group_account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,account_code VARCHAR(500)  		--//  ENCODE lzo
	,account_name VARCHAR(500)  		--//  ENCODE lzo
	,account_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,ten_st_ddp VARCHAR(200)  		--//  ENCODE lzo
	,region_code VARCHAR(500)  		--//  ENCODE lzo
	,region_name VARCHAR(500)  		--//  ENCODE lzo
	,region_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,province_code VARCHAR(500)  		--//  ENCODE lzo
	,province_name VARCHAR(500)  		--//  ENCODE lzo
	,province_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,retail_environment VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_distributor_products;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_DISTRIBUTOR_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_distributor_products
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
	,barcode NUMERIC(31,0)  		--//  ENCODE az64
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE az64
	,franchise VARCHAR(500)  		--//  ENCODE lzo
	,category VARCHAR(500)  		--//  ENCODE lzo
	,sub_category VARCHAR(500)  		--//  ENCODE lzo
	,sub_brand VARCHAR(400)  		--//  ENCODE lzo
	,base_bundle VARCHAR(500)  		--//  ENCODE lzo
	,size VARCHAR(400)  		--//  ENCODE lzo
	,product_name VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ecom_nts;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_ECOM_NTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ecom_nts
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
	,year NUMERIC(31,0)  		--//  ENCODE az64
	,month NUMERIC(31,0)  		--//  ENCODE az64
	,market VARCHAR(200)  		--//  ENCODE lzo
	,gfo VARCHAR(200)  		--//  ENCODE lzo
	,need_state VARCHAR(200)  		--//  ENCODE lzo
	,brand VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,crncy_cd VARCHAR(200)  		--//  ENCODE lzo
	,nts_lcy NUMERIC(31,3)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ecom_product;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_ECOM_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ecom_product
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
	,dataset VARCHAR(200)  		--//  ENCODE lzo
	,cust_attr_1 VARCHAR(200)  		--//  ENCODE lzo
	,cust_attr_2 VARCHAR(200)  		--//  ENCODE lzo
	,upc VARCHAR(200)  		--//  ENCODE lzo
	,sku_name VARCHAR(510)  		--//  ENCODE lzo
	,prod_attr_1 VARCHAR(200)  		--//  ENCODE lzo
	,prod_attr_2 VARCHAR(200)  		--//  ENCODE lzo
	,prod_attr_3 VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ecom_target;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_ECOM_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ecom_target
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
	,platform VARCHAR(200)  		--//  ENCODE lzo
	,cycle NUMERIC(31,0)  		--//  ENCODE az64
	,target NUMERIC(31,1)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_gt_gts_ratio;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_GT_GTS_RATIO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_gt_gts_ratio
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
	,distributor VARCHAR(200)  		--//  ENCODE lzo
	,"from month" NUMERIC(31,0)  		--//  ENCODE az64
	,"to month" NUMERIC(31,0)  		--//  ENCODE az64
	,percentage NUMERIC(31,2)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_gt_msl_shoptype_mapping;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_GT_MSL_SHOPTYPE_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_gt_msl_shoptype_mapping
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
	,shoptype_code VARCHAR(200)  		--//  ENCODE lzo
	,shoptype_name VARCHAR(200)  		--//  ENCODE lzo
	,channel VARCHAR(200)  		--//  ENCODE lzo
	,subchannel VARCHAR(200)  		--//  ENCODE lzo
	,msl_subchannel VARCHAR(200)  		--//  ENCODE lzo
	,active NUMERIC(28,0)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_pos_customers;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_POS_CUSTOMERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_pos_customers
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
	,customer_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,store_code VARCHAR(200)  		--//  ENCODE lzo
	,customer_store_code VARCHAR(200)  		--//  ENCODE lzo
	,store_name VARCHAR(200)  		--//  ENCODE lzo
	,zone VARCHAR(200)  		--//  ENCODE lzo
	,district VARCHAR(200)  		--//  ENCODE lzo
	,format VARCHAR(200)  		--//  ENCODE lzo
	,plant VARCHAR(200)  		--//  ENCODE lzo
	,chain VARCHAR(200)  		--//  ENCODE lzo
	,status VARCHAR(200)  		--//  ENCODE lzo
	,note_closed_store VARCHAR(200)  		--//  ENCODE lzo
	,store_name_2 VARCHAR(200)  		--//  ENCODE lzo
	,wh VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_pos_products;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_POS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_pos_products
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
	,customer_sku VARCHAR(200)  		--//  ENCODE lzo
	,barcode VARCHAR(200)  		--//  ENCODE lzo
	,customer VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_price_products;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_PRICE_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_price_products
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
	,jnj_sap_code NUMERIC(31,0)  		--//  ENCODE az64
	,franchise VARCHAR(60)  		--//  ENCODE lzo
	,brand VARCHAR(200)  		--//  ENCODE lzo
	,sku VARCHAR(500)  		--//  ENCODE lzo
	,bar_code VARCHAR(200)  		--//  ENCODE lzo
	,pc_per_case NUMERIC(31,2)  		--//  ENCODE az64
	,price NUMERIC(31,5)  		--//  ENCODE az64
	,product_id_concung VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ps_store_tagging;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_PS_STORE_TAGGING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ps_store_tagging
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
	,parent_customer VARCHAR(200)  		--//  ENCODE lzo
	,store_code VARCHAR(200)  		--//  ENCODE lzo
	,store_name VARCHAR(200)  		--//  ENCODE lzo
	,status VARCHAR(200)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ps_targets;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_PS_TARGETS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ps_targets
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
	,re VARCHAR(200)  		--//  ENCODE lzo
	,kpi VARCHAR(200)  		--//  ENCODE lzo
	,attribute_1 VARCHAR(200)  		--//  ENCODE lzo
	,attribute_2 VARCHAR(200)  		--//  ENCODE lzo
	,value NUMERIC(31,3)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_ps_weights;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_PS_WEIGHTS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_ps_weights
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
	,re VARCHAR(200)  		--//  ENCODE lzo
	,kpi VARCHAR(200)  		--//  ENCODE lzo
	,weight NUMERIC(31,3)  		--//  ENCODE az64
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_sku_benchmarks;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_SKU_BENCHMARKS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_sku_benchmarks
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
	,jj_upc VARCHAR(200)  		--//  ENCODE lzo
	,jj_sku_description VARCHAR(510)  		--//  ENCODE lzo
	,jj_packsize NUMERIC(31,3)  		--//  ENCODE az64
	,jj_target NUMERIC(31,3)  		--//  ENCODE az64
	,variance NUMERIC(31,3)  		--//  ENCODE az64
	,comp_upc VARCHAR(200)  		--//  ENCODE lzo
	,comp_sku_description VARCHAR(510)  		--//  ENCODE lzo
	,comp_packsize NUMERIC(31,3)  		--//  ENCODE az64
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
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_mds_vn_topdoor_storetype_mapping;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_MDS_VN_TOPDOOR_STORETYPE_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_mds_vn_topdoor_storetype_mapping
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
	,storetype VARCHAR(200)  		--//  ENCODE lzo
	,group_hierarchy VARCHAR(200)  		--//  ENCODE lzo
	,top_door_group VARCHAR(500)  		--//  ENCODE lzo
	,enterdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,enterusername VARCHAR(200)  		--//  ENCODE lzo
	,enterversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,lastchgdatetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastchgusername VARCHAR(200)  		--//  ENCODE lzo
	,lastchgversionnumber numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,validationstatus VARCHAR(500)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_raw_spiral_mti_offtake;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_RAW_SPIRAL_MTI_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_raw_spiral_mti_offtake
(
	stt VARCHAR(100)  		--//  ENCODE lzo
	,area VARCHAR(100)  		--//  ENCODE lzo
	,channelname VARCHAR(100)  		--//  ENCODE lzo
	,customername VARCHAR(100)  		--//  ENCODE lzo
	,shopcode VARCHAR(100)  		--//  ENCODE lzo
	,shopname VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,supcode VARCHAR(100)  		--//  ENCODE lzo
	,supname VARCHAR(100)  		--//  ENCODE lzo
	,year VARCHAR(100)  		--//  ENCODE lzo
	,month VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,productname VARCHAR(100)  		--//  ENCODE lzo
	,franchise VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,cate VARCHAR(100)  		--//  ENCODE lzo
	,sub_cat VARCHAR(100)  		--//  ENCODE lzo
	,sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,quantity VARCHAR(100)  		--//  ENCODE lzo
	,amount VARCHAR(100)  		--//  ENCODE lzo
	,amountusd VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,crtd_name TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;

--DROP TABLE VNMSDL_RAW.sdl_spiral_mti_offtake;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_SPIRAL_MTI_OFFTAKE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_spiral_mti_offtake
(
	stt VARCHAR(100)  		--//  ENCODE lzo
	,area VARCHAR(100)  		--//  ENCODE lzo
	,channelname VARCHAR(100)  		--//  ENCODE lzo
	,customername VARCHAR(100)  		--//  ENCODE lzo
	,shopcode VARCHAR(100)  		--//  ENCODE lzo
	,shopname VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(100)  		--//  ENCODE lzo
	,supcode VARCHAR(100)  		--//  ENCODE lzo
	,supname VARCHAR(100)  		--//  ENCODE lzo
	,year VARCHAR(100)  		--//  ENCODE lzo
	,month VARCHAR(100)  		--//  ENCODE lzo
	,barcode VARCHAR(100)  		--//  ENCODE lzo
	,productname VARCHAR(100)  		--//  ENCODE lzo
	,franchise VARCHAR(100)  		--//  ENCODE lzo
	,brand VARCHAR(100)  		--//  ENCODE lzo
	,cate VARCHAR(100)  		--//  ENCODE lzo
	,sub_cat VARCHAR(100)  		--//  ENCODE lzo
	,sub_brand VARCHAR(100)  		--//  ENCODE lzo
	,size VARCHAR(100)  		--//  ENCODE lzo
	,quantity VARCHAR(100)  		--//  ENCODE lzo
	,amount VARCHAR(100)  		--//  ENCODE lzo
	,amountusd VARCHAR(100)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dksh_daily_sales;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DKSH_DAILY_SALES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dksh_daily_sales
(
	group_ds VARCHAR(255)  		--//  ENCODE lzo
	,category VARCHAR(255)  		--//  ENCODE lzo
	,material VARCHAR(255)  		--//  ENCODE lzo
	,materialdescription VARCHAR(255)  		--//  ENCODE lzo
	,syslot VARCHAR(255)  		--//  ENCODE lzo
	,batchno VARCHAR(255)  		--//  ENCODE lzo
	,exp_date VARCHAR(255)  		--//  ENCODE lzo
	,total numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,hcm VARCHAR(255)  		--//  ENCODE lzo
	,vsip VARCHAR(255)  		--//  ENCODE lzo
	,langha VARCHAR(255)  		--//  ENCODE lzo
	,thanhtri VARCHAR(255)  		--//  ENCODE lzo
	,danang VARCHAR(255)  		--//  ENCODE lzo
	,values_lc NUMERIC(24,10)  		--//  ENCODE az64
	,reason VARCHAR(255)  		--//  ENCODE lzo
	,file_date VARCHAR(255)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,file_name VARCHAR(255)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_call_details;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_CALL_DETAILS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_call_details
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,visit_date VARCHAR(30)  		--//  ENCODE zstd
	,checkin_time VARCHAR(30)  		--//  ENCODE zstd
	,checkout_time VARCHAR(30)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,distance VARCHAR(30)  		--//  ENCODE zstd
	,ordervisit VARCHAR(30)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_customer_dim;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_CUSTOMER_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_customer_dim
(
	sap_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,outlet_name VARCHAR(100)  		--//  ENCODE zstd
	,address_1 VARCHAR(200)  		--//  ENCODE zstd
	,address_2 VARCHAR(200)  		--//  ENCODE zstd
	,telephone VARCHAR(20)  		--//  ENCODE zstd
	,fax VARCHAR(20)  		--//  ENCODE zstd
	,city VARCHAR(50)  		--//  ENCODE zstd
	,postcode VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,channel_group VARCHAR(30)  		--//  ENCODE zstd
	,sub_channel VARCHAR(20)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,gps_lat VARCHAR(30)  		--//  ENCODE zstd
	,gps_long VARCHAR(30)  		--//  ENCODE zstd
	,status CHAR(1)  		--//  ENCODE zstd
	,district VARCHAR(30)  		--//  ENCODE zstd
	,province VARCHAR(30)  		--//  ENCODE zstd
	,crt_date VARCHAR(50)  		--//  ENCODE zstd
	,date_off VARCHAR(50)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
	,shop_type VARCHAR(100)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_d_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_D_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_d_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date VARCHAR(30)  		--//  ENCODE zstd
	,invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,order_no VARCHAR(30)  		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sales_route_id VARCHAR(30)  		--//  ENCODE zstd
	,sale_route_name VARCHAR(100)  		--//  ENCODE zstd
	,sales_group VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,material_code VARCHAR(50)  		--//  ENCODE zstd
	,uom VARCHAR(30)  		--//  ENCODE zstd
	,gross_price VARCHAR(100)  		--//  ENCODE zstd
	,orderqty VARCHAR(30)  		--//  ENCODE zstd
	,quantity VARCHAR(30)  		--//  ENCODE zstd
	,total_sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,discount VARCHAR(100)  		--//  ENCODE zstd
	,total_sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,line_number VARCHAR(10)  		--//  ENCODE zstd
	,promotion_id VARCHAR(50)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_distributor_dim;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_DISTRIBUTOR_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_distributor_dim
(
	territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(10)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,dstrbtr_address VARCHAR(200)  		--//  ENCODE zstd
	,longitude VARCHAR(20)  		--//  ENCODE zstd
	,latitude VARCHAR(20)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_forecast;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_FORECAST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_forecast
(
	cycle VARCHAR(10)  		--//  ENCODE zstd
	,channel VARCHAR(30)  		--//  ENCODE zstd
	,territory_dist VARCHAR(100)  		--//  ENCODE zstd
	,warehouse VARCHAR(100)  		--//  ENCODE zstd
	,franchise VARCHAR(100)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,forecastso_mil VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_h_sellout_sales_fact;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_H_SELLOUT_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_h_sellout_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,outlet_id VARCHAR(30)  		--//  ENCODE zstd
	,order_date VARCHAR(30)  		--//  ENCODE zstd
	,invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,order_no VARCHAR(30)  		--//  ENCODE zstd
	,invoice_no VARCHAR(30)  		--//  ENCODE zstd
	,sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,total_discount VARCHAR(100)  		--//  ENCODE zstd
	,invoice_discount VARCHAR(100)  		--//  ENCODE zstd
	,sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_history_saleout;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_HISTORY_SALEOUT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_history_saleout
(
	user_id VARCHAR(100)  		--//  ENCODE zstd
	,rsm_name VARCHAR(200)  		--//  ENCODE zstd
	,group_jb VARCHAR(50)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,dmsproduct_group VARCHAR(200)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,dmsproductid VARCHAR(100)  		--//  ENCODE zstd
	,sku_name VARCHAR(100)  		--//  ENCODE zstd
	,tax VARCHAR(10)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,sellout_afvat_bfdisc VARCHAR(100)  		--//  ENCODE zstd
	,sellout_afvat_afdisc VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_kpi;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_KPI		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_kpi
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,saleman_code VARCHAR(30)  		--//  ENCODE zstd
	,saleman_name VARCHAR(50)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,export_date VARCHAR(30)  		--//  ENCODE zstd
	,kpi_type VARCHAR(20)  		--//  ENCODE zstd
	,target_value VARCHAR(100)  		--//  ENCODE zstd
	,actual_value VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_kpi_sellin_sellthrgh;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_KPI_SELLIN_SELLTHRGH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_kpi_sellin_sellthrgh
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(10)  		--//  ENCODE zstd
	,dstrbtr_name VARCHAR(100)  		--//  ENCODE zstd
	,cycle VARCHAR(10)  		--//  ENCODE zstd
	,ordertype VARCHAR(30)  		--//  ENCODE zstd
	,sellin_tg VARCHAR(100)  		--//  ENCODE zstd
	,sellin_ac VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_msl;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_MSL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_msl
(
	msl_id VARCHAR(20)  		--//  ENCODE zstd
	,sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,from_cycle VARCHAR(10)  		--//  ENCODE zstd
	,to_cycle VARCHAR(10)  		--//  ENCODE zstd
	,product_id VARCHAR(50)  		--//  ENCODE zstd
	,prouct_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_msl_temp;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_MSL_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_msl_temp
(
	msl_id VARCHAR(20)  		--//  ENCODE lzo
	,sub_channel VARCHAR(50)  		--//  ENCODE lzo
	,from_cycle VARCHAR(10)  		--//  ENCODE lzo
	,to_cycle VARCHAR(10)  		--//  ENCODE lzo
	,product_id VARCHAR(20)  		--//  ENCODE lzo
	,prouct_name VARCHAR(100)  		--//  ENCODE lzo
	,active VARCHAR(5)  		--//  ENCODE lzo
	,groupmsl VARCHAR(100)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_order_promotion;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_ORDER_PROMOTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_order_promotion
(
	branch_id VARCHAR(30)  		--//  ENCODE zstd
	,pro_id VARCHAR(50)  		--//  ENCODE zstd
	,ord_no VARCHAR(20)  		--//  ENCODE zstd
	,line_ref VARCHAR(10)  		--//  ENCODE zstd
	,disc_type VARCHAR(5)  		--//  ENCODE zstd
	,break_by VARCHAR(5)  		--//  ENCODE zstd
	,disc_break_line_ref VARCHAR(10)  		--//  ENCODE zstd
	,disct_bl_amt VARCHAR(100)  		--//  ENCODE zstd
	,disct_bl_qty VARCHAR(20)  		--//  ENCODE zstd
	,free_item_code VARCHAR(20)  		--//  ENCODE zstd
	,free_item_qty VARCHAR(20)  		--//  ENCODE zstd
	,disc_amt VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_product_dim;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_PRODUCT_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_product_dim
(
	product_code VARCHAR(50)  		--//  ENCODE zstd
	,product_name VARCHAR(100)  		--//  ENCODE zstd
	,productcodesap VARCHAR(40)  		--//  ENCODE zstd
	,productnamesap VARCHAR(100)  		--//  ENCODE zstd
	,unit VARCHAR(10)  		--//  ENCODE zstd
	,tax_rate VARCHAR(10)  		--//  ENCODE zstd
	,weight VARCHAR(20)  		--//  ENCODE zstd
	,volume VARCHAR(20)  		--//  ENCODE zstd
	,groupjb VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,variant VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(200)  		--//  ENCODE zstd
	,active VARCHAR(10)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_promotion_list;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_PROMOTION_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_promotion_list
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,promotion_id VARCHAR(50)  		--//  ENCODE zstd
	,promotion_name VARCHAR(200)  		--//  ENCODE zstd
	,promotion_desc VARCHAR(200)  		--//  ENCODE zstd
	,start_date VARCHAR(30)  		--//  ENCODE zstd
	,end_date VARCHAR(30)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_sales_org_dim;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_SALES_ORG_DIM		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_sales_org_dim
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_id VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_name VARCHAR(100)  		--//  ENCODE zstd
	,sup_code VARCHAR(30)  		--//  ENCODE zstd
	,salesrep_crtdate VARCHAR(50)  		--//  ENCODE zstd
	,salesrep_dateoff VARCHAR(50)  		--//  ENCODE zstd
	,sup_name VARCHAR(100)  		--//  ENCODE zstd
	,sup_active VARCHAR(5)  		--//  ENCODE zstd
	,sup_crtdate VARCHAR(50)  		--//  ENCODE zstd
	,sup_dateoff VARCHAR(50)  		--//  ENCODE zstd
	,asm_id VARCHAR(50)  		--//  ENCODE zstd
	,asm_name VARCHAR(100)  		--//  ENCODE zstd
	,active VARCHAR(5)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_sales_stock_fact;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_SALES_STOCK_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_sales_stock_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,cntry_code VARCHAR(10)  		--//  ENCODE zstd
	,wh_code VARCHAR(10)  		--//  ENCODE zstd
	,date VARCHAR(30)  		--//  ENCODE zstd
	,material_code VARCHAR(50)  		--//  ENCODE zstd
	,bat_number VARCHAR(20)  		--//  ENCODE zstd
	,expiry_date VARCHAR(30)  		--//  ENCODE zstd
	,quantity VARCHAR(20)  		--//  ENCODE zstd
	,uom VARCHAR(10)  		--//  ENCODE zstd
	,amount VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_sales_stock_fact_temp;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_SALES_STOCK_FACT_TEMP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_sales_stock_fact_temp
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE lzo
	,cntry_code VARCHAR(10)  		--//  ENCODE lzo
	,wh_code VARCHAR(10)  		--//  ENCODE lzo
	,date VARCHAR(30)  		--//  ENCODE lzo
	,material_code VARCHAR(30)  		--//  ENCODE lzo
	,bat_number VARCHAR(20)  		--//  ENCODE lzo
	,expiry_date VARCHAR(30)  		--//  ENCODE lzo
	,quantity VARCHAR(20)  		--//  ENCODE lzo
	,uom VARCHAR(10)  		--//  ENCODE lzo
	,amount VARCHAR(20)  		--//  ENCODE lzo
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_sellthrgh_sales_fact;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_SELLTHRGH_SALES_FACT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_sellthrgh_sales_fact
(
	dstrbtr_id VARCHAR(30)  		--//  ENCODE zstd
	,dstrbtr_type VARCHAR(30)  		--//  ENCODE zstd
	,mapped_spk VARCHAR(30)  		--//  ENCODE zstd
	,doc_number VARCHAR(30)  		--//  ENCODE zstd
	,ref_number VARCHAR(30)  		--//  ENCODE zstd
	,receipt_date VARCHAR(30)  		--//  ENCODE zstd
	,order_type VARCHAR(10)  		--//  ENCODE zstd
	,vat_invoice_number VARCHAR(30)  		--//  ENCODE zstd
	,vat_invoice_note VARCHAR(30)  		--//  ENCODE zstd
	,vat_invoice_date VARCHAR(30)  		--//  ENCODE zstd
	,pon_number VARCHAR(50)  		--//  ENCODE zstd
	,line_ref VARCHAR(20)  		--//  ENCODE zstd
	,product_code VARCHAR(50)  		--//  ENCODE zstd
	,unit VARCHAR(10)  		--//  ENCODE zstd
	,quantity VARCHAR(20)  		--//  ENCODE zstd
	,price VARCHAR(30)  		--//  ENCODE zstd
	,amount VARCHAR(30)  		--//  ENCODE zstd
	,tax_amount VARCHAR(30)  		--//  ENCODE zstd
	,tax_id VARCHAR(30)  		--//  ENCODE zstd
	,tax_rate VARCHAR(30)  		--//  ENCODE zstd
	,values VARCHAR(30)  		--//  ENCODE zstd
	,line_discount VARCHAR(30)  		--//  ENCODE zstd
	,doc_discount VARCHAR(100)  		--//  ENCODE zstd
	,status VARCHAR(1)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_dms_yearly_target;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_DMS_YEARLY_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_dms_yearly_target
(
	year VARCHAR(10)  		--//  ENCODE zstd
	,kpi VARCHAR(100)  		--//  ENCODE zstd
	,category VARCHAR(200)  		--//  ENCODE zstd
	,target VARCHAR(100)  		--//  ENCODE zstd
	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('CURRENT_TIMESTAMP()'::varchar)::timestamp without time zone)		--// 	,curr_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT convert_timezone(('SGT'::character varying)::text, ('now'::character varying)::timestamp without time zone) ENCODE zstd //  ENCODE zstd // character varying
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,source_file_name VARCHAR(256)  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_gt_topdoor_target;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_GT_TOPDOOR_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_gt_topdoor_target
(
	td_id VARCHAR(100)  		--//  ENCODE lzo
	,td_zone VARCHAR(50)  		--//  ENCODE lzo
	,from_cycle VARCHAR(10)  		--//  ENCODE lzo
	,to_cycle VARCHAR(10)  		--//  ENCODE lzo
	,distributor_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_code VARCHAR(100)  		--//  ENCODE lzo
	,customer_name VARCHAR(200)  		--//  ENCODE lzo
	,shoptype VARCHAR(100)  		--//  ENCODE lzo
	,target_value NUMERIC(18,2)  		--//  ENCODE az64
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,file_name VARCHAR(200)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)
		--// DISTSTYLE EVEN
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_answers;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_ANSWERS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_answers
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value NUMERIC(20,0)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,oos NUMERIC(20,0)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_branch;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_BRANCH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_branch
(
	parent_cust_code VARCHAR(255)  		--//  ENCODE zstd
	,parent_cust_name VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,branch_name VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,sales_group VARCHAR(255)  		--//  ENCODE zstd
	,region VARCHAR(255)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,city VARCHAR(255)  		--//  ENCODE zstd
	,district VARCHAR(255)  		--//  ENCODE zstd
	,trade_type VARCHAR(255)  		--//  ENCODE zstd
	,store_prioritization VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(32,16)  		--//  ENCODE zstd
	,longitude NUMERIC(32,16)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_choices;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_CHOICES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_choices
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(20,0)  		--//  ENCODE zstd
	,channel_name VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,sku_group VARCHAR(255)  		--//  ENCODE zstd
	,rep_param VARCHAR(255)  		--//  ENCODE zstd
	,putup_id VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,score NUMERIC(20,0)  		--//  ENCODE zstd
	,sfa NUMERIC(20,0)  		--//  ENCODE zstd
	,brand_id VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_cpg;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_CPG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_cpg
(
	slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,visitdate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_customer_visited;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_CUSTOMER_VISITED		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_customer_visited
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,branch_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,created_date VARCHAR(255)  		--//  ENCODE zstd
	,visit_date VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_ise_header;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_ISE_HEADER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_ise_header
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ise_desc VARCHAR(255)  		--//  ENCODE zstd
	,channel_code VARCHAR(255)  		--//  ENCODE zstd
	,channel_desc VARCHAR(255)  		--//  ENCODE zstd
	,startdate VARCHAR(255)  		--//  ENCODE zstd
	,enddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_notes;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_NOTES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_notes
(
	cust_code VARCHAR(255)  		--//  ENCODE zstd
	,slsper_id VARCHAR(255)  		--//  ENCODE zstd
	,shop_code VARCHAR(255)  		--//  ENCODE zstd
	,ise_id VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_seq NUMERIC(20,0)  		--//  ENCODE zstd
	,answer_value VARCHAR(255)  		--//  ENCODE zstd
	,createddate VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_interface_question;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_INTERFACE_QUESTION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_interface_question
(
	ise_id VARCHAR(255)  		--//  ENCODE zstd
	,channel_id NUMERIC(10,0)  		--//  ENCODE zstd
	,channel VARCHAR(255)  		--//  ENCODE zstd
	,ques_no NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_desc VARCHAR(255)  		--//  ENCODE zstd
	,standard_ques NUMERIC(10,0)  		--//  ENCODE zstd
	,ques_class_code VARCHAR(255)  		--//  ENCODE zstd
	,ques_class_desc VARCHAR(255)  		--//  ENCODE zstd
	,weigh NUMERIC(10,0)  		--//  ENCODE zstd
	,total_score NUMERIC(10,0)  		--//  ENCODE zstd
	,answer_code VARCHAR(255)  		--//  ENCODE zstd
	,answer_desc VARCHAR(255)  		--//  ENCODE zstd
	,franchise_code VARCHAR(255)  		--//  ENCODE zstd
	,franchise_name VARCHAR(255)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_mds_log;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_MDS_LOG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_mds_log
(
	status VARCHAR(100)  		--//  ENCODE zstd
	,result VARCHAR(65535)  		--//  ENCODE zstd
	,job_name VARCHAR(100)  		--//  ENCODE zstd
	,query VARCHAR(65535)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellin_coop;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLIN_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellin_coop
(
	sku VARCHAR(20)  		--//  ENCODE lzo
	,idescr VARCHAR(200)  		--//  ENCODE lzo
	,vendor VARCHAR(50)  		--//  ENCODE lzo
	,asname VARCHAR(200)  		--//  ENCODE lzo
	,imfgr VARCHAR(50)  		--//  ENCODE lzo
	,store VARCHAR(50)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellin_dksh;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLIN_DKSH		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellin_dksh
(
	supplier_code VARCHAR(10)  		--//  ENCODE zstd
	,supplier_name VARCHAR(100)  		--//  ENCODE zstd
	,plant VARCHAR(10)  		--//  ENCODE zstd
	,productid VARCHAR(20)  		--//  ENCODE zstd
	,product VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,sellin_category VARCHAR(100)  		--//  ENCODE zstd
	,product_group VARCHAR(100)  		--//  ENCODE zstd
	,product_sub_group VARCHAR(100)  		--//  ENCODE zstd
	,unit_of_measurement VARCHAR(10)  		--//  ENCODE zstd
	,custcode VARCHAR(20)  		--//  ENCODE zstd
	,customer VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(50)  		--//  ENCODE zstd
	,region VARCHAR(20)  		--//  ENCODE zstd
	,zone VARCHAR(20)  		--//  ENCODE zstd
	,channel VARCHAR(10)  		--//  ENCODE zstd
	,sellin_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,cust_group VARCHAR(50)  		--//  ENCODE zstd
	,billing_no VARCHAR(20)  		--//  ENCODE zstd
	,invoice_date VARCHAR(20)  		--//  ENCODE zstd
	,qty_include_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,qty_exclude_foc numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,foc VARCHAR(100)  		--//  ENCODE zstd
	,net_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,tax NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,net_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,gross_amount_w_vat NUMERIC(20,5)  		--//  ENCODE az64
	,list_price_wo_vat NUMERIC(20,5)  		--//  ENCODE az64
	,vendor_lot VARCHAR(100)  		--//  ENCODE zstd
	,order_type VARCHAR(20)  		--//  ENCODE zstd
	,red_invoice_no VARCHAR(20)  		--//  ENCODE zstd
	,expiry_date VARCHAR(20)  		--//  ENCODE zstd
	,order_no VARCHAR(20)  		--//  ENCODE zstd
	,order_date VARCHAR(20)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,sellout_sub_channel VARCHAR(50)  		--//  ENCODE zstd
	,group_account VARCHAR(50)  		--//  ENCODE zstd
	,account VARCHAR(50)  		--//  ENCODE zstd
	,name_st_or_ddp VARCHAR(100)  		--//  ENCODE zstd
	,zone_or_area VARCHAR(20)  		--//  ENCODE zstd
	,franchise VARCHAR(50)  		--//  ENCODE zstd
	,sellout_category VARCHAR(50)  		--//  ENCODE zstd
	,sub_cat VARCHAR(50)  		--//  ENCODE zstd
	,sub_brand VARCHAR(50)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,base_or_bundle VARCHAR(20)  		--//  ENCODE zstd
	,size VARCHAR(20)  		--//  ENCODE zstd
	,key_chain VARCHAR(20)  		--//  ENCODE zstd
	,status VARCHAR(20)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellin_target;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLIN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellin_target
(
	mtd_code VARCHAR(20)  		--//  ENCODE zstd
	,mti_code VARCHAR(20)  		--//  ENCODE zstd
	,target NUMERIC(20,5)  		--//  ENCODE az64
	,sellin_cycle numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sellin_year VARCHAR(10)  		--//  ENCODE zstd
	,visit VARCHAR(10)  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_aeon;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_AEON		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_aeon
(
	store VARCHAR(20)  		--//  ENCODE lzo
	,department VARCHAR(30)  		--//  ENCODE lzo
	,supplier_code VARCHAR(50)  		--//  ENCODE lzo
	,supplier_name VARCHAR(60)  		--//  ENCODE lzo
	,item VARCHAR(30)  		--//  ENCODE lzo
	,item_name VARCHAR(200)  		--//  ENCODE lzo
	,sales_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_bhx;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_BHX		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_bhx
(
	pro_code VARCHAR(50)  		--//  ENCODE lzo
	,pro_name VARCHAR(200)  		--//  ENCODE lzo
	,cust_code VARCHAR(20)  		--//  ENCODE lzo
	,cust_name VARCHAR(200)  		--//  ENCODE lzo
	,cat_store VARCHAR(60)  		--//  ENCODE lzo
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_con_cung;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_CON_CUNG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_con_cung
(
	delivery_code VARCHAR(50)  		--//  ENCODE zstd
	,store VARCHAR(255)  		--//  ENCODE zstd
	,product_code VARCHAR(20)  		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_coop;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_COOP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_coop
(
	thang VARCHAR(20)  		--//  ENCODE zstd
	,desc_a VARCHAR(20)  		--//  ENCODE zstd
	,idept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isdept numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,iclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,isclas numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,tenvt VARCHAR(255)  		--//  ENCODE zstd
	,brand_spm VARCHAR(50)  		--//  ENCODE zstd
	,madv VARCHAR(20)  		--//  ENCODE zstd
	,sumoflg numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,sumofttbvnckhd NUMERIC(20,5)  		--//  ENCODE az64
	,store VARCHAR(50)  		--//  ENCODE zstd
	,sales_amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_guardian;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_GUARDIAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_guardian
(
	serial_no numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,store_code VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(20)  		--//  ENCODE zstd
	,barcode VARCHAR(20)  		--//  ENCODE zstd
	,description_vietnamese VARCHAR(255)  		--//  ENCODE zstd
	,brand VARCHAR(100)  		--//  ENCODE zstd
	,division VARCHAR(50)  		--//  ENCODE zstd
	,department VARCHAR(50)  		--//  ENCODE zstd
	,category VARCHAR(50)  		--//  ENCODE zstd
	,sub_category VARCHAR(50)  		--//  ENCODE zstd
	,sales_supplier numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,amount NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_lotte;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_LOTTE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_lotte
(
	str VARCHAR(10)  		--//  ENCODE zstd
	,str_nm VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_1 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_2 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_3 VARCHAR(50)  		--//  ENCODE zstd
	,cat_nm_4 VARCHAR(50)  		--//  ENCODE zstd
	,prod_cd VARCHAR(100)  		--//  ENCODE zstd
	,sale_cd VARCHAR(100)  		--//  ENCODE zstd
	,prod_nm VARCHAR(100)  		--//  ENCODE zstd
	,ven VARCHAR(100)  		--//  ENCODE zstd
	,ven_nm VARCHAR(100)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,tot_sale_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_mega;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_MEGA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_mega
(
	site_no VARCHAR(20)  		--//  ENCODE zstd
	,site_name VARCHAR(255)  		--//  ENCODE zstd
	,period VARCHAR(20)  		--//  ENCODE zstd
	,art_no VARCHAR(20)  		--//  ENCODE zstd
	,art_sv_name VARCHAR(255)  		--//  ENCODE zstd
	,suppl_no VARCHAR(20)  		--//  ENCODE zstd
	,suppl_name VARCHAR(255)  		--//  ENCODE zstd
	,sale_qty numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cogs_amt NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_mt_sellout_vinmart;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_MT_SELLOUT_VINMART		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_mt_sellout_vinmart
(
	store VARCHAR(20)  		--//  ENCODE zstd
	,store_name VARCHAR(255)  		--//  ENCODE zstd
	,mch5_mc VARCHAR(20)  		--//  ENCODE zstd
	,mc VARCHAR(100)  		--//  ENCODE zstd
	,article VARCHAR(20)  		--//  ENCODE zstd
	,article_name VARCHAR(255)  		--//  ENCODE zstd
	,manufacturer VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,pos_quantity numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,pos_revenue NUMERIC(20,5)  		--//  ENCODE az64
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_oneview_otc;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_ONEVIEW_OTC		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_oneview_otc
(
	plant VARCHAR(10)  		--//  ENCODE zstd
	,principalcode VARCHAR(100)  		--//  ENCODE zstd
	,principal VARCHAR(100)  		--//  ENCODE zstd
	,product VARCHAR(100)  		--//  ENCODE zstd
	,productname VARCHAR(100)  		--//  ENCODE zstd
	,kunnr VARCHAR(100)  		--//  ENCODE zstd
	,name1 VARCHAR(100)  		--//  ENCODE zstd
	,name2 VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(100)  		--//  ENCODE zstd
	,province VARCHAR(100)  		--//  ENCODE zstd
	,zterm VARCHAR(100)  		--//  ENCODE zstd
	,kdgrp VARCHAR(100)  		--//  ENCODE zstd
	,custgroup VARCHAR(100)  		--//  ENCODE zstd
	,region VARCHAR(100)  		--//  ENCODE zstd
	,district VARCHAR(100)  		--//  ENCODE zstd
	,vbeln VARCHAR(100)  		--//  ENCODE zstd
	,billingdate VARCHAR(100)  		--//  ENCODE zstd
	,reason VARCHAR(100)  		--//  ENCODE zstd
	,qty VARCHAR(100)  		--//  ENCODE zstd
	,dgle VARCHAR(100)  		--//  ENCODE zstd
	,pernr VARCHAR(100)  		--//  ENCODE zstd
	,vat VARCHAR(100)  		--//  ENCODE zstd
	,suom VARCHAR(100)  		--//  ENCODE zstd
	,custpayto VARCHAR(100)  		--//  ENCODE zstd
	,tt NUMERIC(21,5)  		--//  ENCODE zstd
	,nguyengia VARCHAR(100)  		--//  ENCODE zstd
	,ttv NUMERIC(21,5)  		--//  ENCODE zstd
	,discount NUMERIC(21,5)  		--//  ENCODE zstd
	,device_code VARCHAR(100)  		--//  ENCODE zstd
	,device VARCHAR(100)  		--//  ENCODE zstd
	,order_no VARCHAR(100)  		--//  ENCODE zstd
	,orginv VARCHAR(100)  		--//  ENCODE zstd
	,batch VARCHAR(100)  		--//  ENCODE zstd
	,charge VARCHAR(100)  		--//  ENCODE zstd
	,contact_name VARCHAR(100)  		--//  ENCODE zstd
	,userid VARCHAR(100)  		--//  ENCODE zstd
	,billinginst VARCHAR(100)  		--//  ENCODE zstd
	,distchannel VARCHAR(100)  		--//  ENCODE zstd
	,redinv VARCHAR(100)  		--//  ENCODE zstd
	,serial VARCHAR(100)  		--//  ENCODE zstd
	,potext VARCHAR(100)  		--//  ENCODE zstd
	,expdate VARCHAR(100)  		--//  ENCODE zstd
	,ret_so VARCHAR(100)  		--//  ENCODE zstd
	,vat_code VARCHAR(100)  		--//  ENCODE zstd
	,sodoc_date VARCHAR(100)  		--//  ENCODE zstd
	,itemnotes VARCHAR(100)  		--//  ENCODE zstd
	,mg1 VARCHAR(100)  		--//  ENCODE zstd
	,year VARCHAR(100)  		--//  ENCODE zstd
	,month VARCHAR(100)  		--//  ENCODE zstd
	,channel VARCHAR(100)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id VARCHAR(100)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
--<<Error - UNKNOWN DISTSTYLE>>
;
--DROP TABLE VNMSDL_RAW.sdl_vn_product_mapping;
CREATE OR REPLACE TABLE VNMSDL_RAW.SDL_VN_PRODUCT_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE VNMSDL_RAW.sdl_vn_product_mapping
(
	putupid VARCHAR(255)  		--//  ENCODE zstd
	,barcode VARCHAR(255)  		--//  ENCODE zstd
	,productname VARCHAR(2000)  		--//  ENCODE zstd
	,filename VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
)
--<<Error - UNKNOWN DISTSTYLE>>
;


use schema THASDL_RAW ;

create or replace TABLE SDL_CBD_GT_INVENTORY_REPORT_FACT (
	DATE VARCHAR(20),
	CLIENTCD_NAME VARCHAR(200),
	PRODUCT_CODE VARCHAR(50),
	PRODUCT_NAME VARCHAR(200),
	BASEUOM VARCHAR(10),
	EXPIRED VARCHAR(20),
	"1-90days" VARCHAR(20),
	"91-180days" VARCHAR(20),
	"181-365days" VARCHAR(20),
	">365days" VARCHAR(20),
	TOTAL_QTY VARCHAR(20),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_CBD_GT_SALES_REPORT_FACT (
	BU VARCHAR(50),
	CLIENT VARCHAR(50),
	SUB_CLIENT VARCHAR(50),
	PRODUCT_CODE VARCHAR(50),
	PRODUCT_NAME VARCHAR(200),
	BILLING_NO VARCHAR(50),
	BILLING_DATE VARCHAR(50),
	BATCH_NO VARCHAR(50),
	EXPIRY_DATE VARCHAR(50),
	CUSTOMER_CODE VARCHAR(50),
	CUSTOMER_NAME VARCHAR(16777216),
	DISTRIBUTION_CHANNEL VARCHAR(50),
	CUSTOMER_GROUP VARCHAR(100),
	PROVINCE VARCHAR(50),
	SALES_QTY VARCHAR(50),
	FOC_QTY VARCHAR(50),
	NET_PRICE VARCHAR(50),
	NET_SALES VARCHAR(50),
	SALES_REP_NO VARCHAR(50),
	ORDER_NO VARCHAR(50),
	RETURN_REASON VARCHAR(200),
	PAYMENT_TERM VARCHAR(50),
	FILENAME VARCHAR(255),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_CBD_GT_SALES_REPORT_FACT_TEMP (
	HASHKEY VARCHAR(32),
	BU VARCHAR(50),
	CLIENT VARCHAR(50),
	SUB_CLIENT VARCHAR(50),
	PRODUCT_CODE VARCHAR(50),
	PRODUCT_NAME VARCHAR(200),
	BILLING_NO VARCHAR(50),
	BILLING_DATE DATE,
	BATCH_NO VARCHAR(50),
	EXPIRY_DATE DATE,
	CUSTOMER_CODE VARCHAR(50),
	CUSTOMER_NAME VARCHAR(100),
	DISTRIBUTION_CHANNEL VARCHAR(50),
	CUSTOMER_GROUP VARCHAR(100),
	PROVINCE VARCHAR(50),
	SALES_QTY NUMBER(18,4),
	FOC_QTY NUMBER(18,4),
	NET_PRICE NUMBER(18,4),
	NET_SALES NUMBER(18,4),
	SALES_REP_NO VARCHAR(50),
	ORDER_NO VARCHAR(50),
	RETURN_REASON VARCHAR(200),
	PAYMENT_TERM VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_JNJ_CONSUMERREACH_711 (
	ID VARCHAR(255),
	CDATE VARCHAR(255),
	RETAIL VARCHAR(255),
	RETAILNAME VARCHAR(255),
	RETAILBRANCH VARCHAR(255),
	RETAILPROVINCE VARCHAR(255),
	JJSKUBARCODE VARCHAR(255),
	JJSKUNAME VARCHAR(255),
	JJCORE VARCHAR(255),
	DISTRIBUTION VARCHAR(255),
	STATUS VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
create or replace TABLE SDL_JNJ_CONSUMERREACH_CVS (
	ID VARCHAR(255),
	CDATE VARCHAR(255),
	RETAIL VARCHAR(255),
	RETAILNAME VARCHAR(255),
	RETAILBRANCH VARCHAR(255),
	RETAILPROVINCE VARCHAR(255),
	JJSKUBARCODE VARCHAR(255),
	JJSKUNAME VARCHAR(255),
	JJCORE VARCHAR(255),
	DISTRIBUTION VARCHAR(255),
	STATUS VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
create or replace TABLE SDL_JNJ_CONSUMERREACH_SFM (
	ID VARCHAR(255),
	CDATE VARCHAR(255),
	RETAIL VARCHAR(255),
	RETAILNAME VARCHAR(255),
	RETAILBRANCH VARCHAR(255),
	RETAILPROVINCE VARCHAR(255),
	JJSKUBARCODE VARCHAR(255),
	JJSKUNAME VARCHAR(255),
	JJCORE VARCHAR(255),
	DISTRIBUTION VARCHAR(255),
	STATUS VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
create or replace TABLE SDL_JNJ_MER_COP (
	COP_DATE VARCHAR(255),
	EMP_ADDRESS_PC VARCHAR(255),
	PC_NAME VARCHAR(255),
	SURVEY_NAME VARCHAR(255),
	EMP_ADDRESS_SUPERVISOR VARCHAR(255),
	SUPERVISOR_NAME VARCHAR(255),
	COP_PRIORITY VARCHAR(255),
	START_DATE VARCHAR(255),
	END_DATE VARCHAR(255),
	AREA VARCHAR(255),
	CHANNEL VARCHAR(255),
	ACCOUNT VARCHAR(255),
	STORE_ID VARCHAR(255),
	STORE_NAME VARCHAR(255),
	QUESTION VARCHAR(500),
	ANSWER VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255),
	ACTIVITY VARCHAR(255),
	CATEGORY VARCHAR(255),
	BRAND VARCHAR(255),
	COMPLIANCE VARCHAR(255)
);
create or replace TABLE SDL_JNJ_MER_SHARE_OF_SHELF (
	SOS_DATE VARCHAR(255),
	MERCHANDISER_NAME VARCHAR(255),
	SUPERVISOR_NAME VARCHAR(255),
	AREA VARCHAR(255),
	CHANNEL VARCHAR(255),
	ACCOUNT VARCHAR(255),
	STORE_ID VARCHAR(255),
	STORE_NAME VARCHAR(255),
	CATEGORY VARCHAR(255),
	AGENCY VARCHAR(255),
	BRAND VARCHAR(255),
	SIZE VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
create or replace TABLE SDL_JNJ_OSA_OOS_REPORT (
	OSA_OOS_DATE VARCHAR(255),
	WEEK VARCHAR(255),
	EMP_ADDRESS_PC VARCHAR(255),
	PC_NAME VARCHAR(255),
	EMP_ADDRESS_SUPERVISOR VARCHAR(255),
	SUPERVISOR_NAME VARCHAR(255),
	AREA VARCHAR(255),
	CHANNEL VARCHAR(255),
	ACCOUNT VARCHAR(255),
	STORE_ID VARCHAR(255),
	STORE_NAME VARCHAR(255),
	SHOP_TYPE VARCHAR(255),
	BRAND VARCHAR(255),
	CATEGORY VARCHAR(255),
	BARCODE VARCHAR(255),
	SKU VARCHAR(255),
	MSL_PRICE_TAG VARCHAR(255),
	OOS VARCHAR(255),
	OOS_REASON VARCHAR(255),
	RUN_ID NUMBER(14,0),
	FILE_NAME VARCHAR(255),
	YEARMO VARCHAR(255)
);
create or replace TABLE SDL_LA_GT_CUSTOMER (
	DISTRIBUTORID VARCHAR(10),
	ARCODE VARCHAR(20),
	ARNAME VARCHAR(500),
	ARADDRESS VARCHAR(500),
	TELEPHONE VARCHAR(150),
	FAX VARCHAR(150),
	CITY VARCHAR(500),
	REGION VARCHAR(20),
	SALEDISTRICT VARCHAR(10),
	SALEOFFICE VARCHAR(10),
	SALEGROUP VARCHAR(10),
	ARTYPECODE VARCHAR(10),
	SALEEMPLOYEE VARCHAR(150),
	SALENAME VARCHAR(250),
	BILLNO VARCHAR(500),
	BILLMOO VARCHAR(250),
	BILLSOI VARCHAR(255),
	BILLROAD VARCHAR(255),
	BILLSUBDIST VARCHAR(30),
	BILLDISTRICT VARCHAR(30),
	BILLPROVINCE VARCHAR(30),
	BILLZIPCODE VARCHAR(50),
	ACTIVESTATUS NUMBER(18,0),
	ROUTESTEP1 VARCHAR(10),
	ROUTESTEP2 VARCHAR(10),
	ROUTESTEP3 VARCHAR(10),
	ROUTESTEP4 VARCHAR(10),
	ROUTESTEP5 VARCHAR(10),
	ROUTESTEP6 VARCHAR(10),
	ROUTESTEP7 VARCHAR(10),
	LATITUDE VARCHAR(10),
	LONGITUDE VARCHAR(10),
	ROUTESTEP10 VARCHAR(10),
	STORE VARCHAR(200),
	PRICELEVEL VARCHAR(50),
	SALESAREANAME VARCHAR(150),
	BRANCHCODE VARCHAR(50),
	BRANCHNAME VARCHAR(150),
	FREQUENCYOFVISIT VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_INVENTORY_FACT (
	RECDATE VARCHAR(200),
	DISTRIBUTORID VARCHAR(200),
	WHCODE VARCHAR(200),
	PRODUCTCODE VARCHAR(200),
	QTY VARCHAR(200),
	AMOUNT VARCHAR(200),
	BATCHNO VARCHAR(200),
	EXPIRYDATE VARCHAR(200),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_ROUTE_DETAIL (
	HASHKEY VARCHAR(500),
	ROUTE_ID VARCHAR(50),
	CUSTOMER_ID VARCHAR(50),
	ROUTE_NO VARCHAR(10),
	SALEUNIT VARCHAR(100),
	SHIP_TO VARCHAR(50),
	CONTACT_PERSON VARCHAR(100),
	CREATED_DATE VARCHAR(20),
	FILE_UPLOAD_DATE DATE,
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_ROUTE_DETAIL_TEMP (
	HASHKEY VARCHAR(500),
	ROUTE_ID VARCHAR(50),
	CUSTOMER_ID VARCHAR(50),
	ROUTE_NO VARCHAR(10),
	SALEUNIT VARCHAR(100),
	SHIP_TO VARCHAR(50),
	CONTACT_PERSON VARCHAR(100),
	CREATED_DATE VARCHAR(20),
	FILE_UPLOAD_DATE DATE,
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_ROUTE_HEADER (
	HASHKEY VARCHAR(500),
	ROUTE_ID VARCHAR(50),
	ROUTE_NAME VARCHAR(100),
	ROUTE_DESC VARCHAR(200),
	IS_ACTIVE VARCHAR(5),
	ROUTESALE VARCHAR(100),
	SALEUNIT VARCHAR(100),
	ROUTE_CODE VARCHAR(50),
	DESCRIPTION VARCHAR(100),
	LAST_UPDATED_DATE VARCHAR(20),
	FILE_UPLOAD_DATE DATE,
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_ROUTE_HEADER_TEMP (
	HASHKEY VARCHAR(500),
	ROUTE_ID VARCHAR(50),
	ROUTE_NAME VARCHAR(100),
	ROUTE_DESC VARCHAR(200),
	IS_ACTIVE VARCHAR(5),
	ROUTESALE VARCHAR(100),
	SALEUNIT VARCHAR(100),
	ROUTE_CODE VARCHAR(50),
	DESCRIPTION VARCHAR(100),
	LAST_UPDATED_DATE VARCHAR(20),
	FILE_UPLOAD_DATE DATE,
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_SALES_ORDER_FACT (
	SALEUNIT VARCHAR(50),
	ORDERID VARCHAR(50),
	ORDERDATE VARCHAR(50),
	CUSTOMER_ID VARCHAR(50),
	CUSTOMER_NAME VARCHAR(200),
	CITY VARCHAR(100),
	REGION VARCHAR(100),
	SALEDISTRICT VARCHAR(100),
	SALEOFFICE VARCHAR(100),
	SALEGROUP VARCHAR(100),
	CUSTOMERTYPE VARCHAR(200),
	STORETYPE VARCHAR(200),
	SALETYPE VARCHAR(100),
	SALESEMPLOYEE VARCHAR(100),
	SALENAME VARCHAR(100),
	PRODUCTID VARCHAR(50),
	PRODUCTNAME VARCHAR(300),
	MEGABRAND VARCHAR(100),
	BRAND VARCHAR(100),
	BASEPRODUCT VARCHAR(100),
	VARIANT VARCHAR(100),
	PUTUP VARCHAR(100),
	PRICEREF VARCHAR(100),
	BACKLOG VARCHAR(100),
	QTY VARCHAR(50),
	SUBAMT1 VARCHAR(50),
	DISCOUNT VARCHAR(50),
	SUBAMT2 VARCHAR(50),
	DISCOUNTBTLINE VARCHAR(50),
	TOTALBEFOREVAT VARCHAR(50),
	TOTAL VARCHAR(50),
	NO VARCHAR(50),
	CANCELED VARCHAR(50),
	DOCUMENTID VARCHAR(50),
	RETURN_REASON VARCHAR(100),
	PROMOTIONCODE VARCHAR(100),
	PROMOTIONCODE1 VARCHAR(100),
	PROMOTIONCODE2 VARCHAR(100),
	PROMOTIONCODE3 VARCHAR(100),
	PROMOTIONCODE4 VARCHAR(100),
	PROMOTIONCODE5 VARCHAR(100),
	PROMOTION_CODE VARCHAR(100),
	PROMOTION_CODE2 VARCHAR(100),
	PROMOTION_CODE3 VARCHAR(100),
	AVGDISCOUNT VARCHAR(50),
	ORDERTYPE VARCHAR(50),
	APPROVERSTATUS VARCHAR(50),
	PRICELEVEL VARCHAR(100),
	OPTIONAL3 VARCHAR(100),
	DELIVERYDATE VARCHAR(20),
	ORDERTIME VARCHAR(20),
	SHIPTO VARCHAR(100),
	BILLTO VARCHAR(100),
	DELIVERYROUTEID VARCHAR(50),
	APPROVED_DATE VARCHAR(20),
	APPROVED_TIME VARCHAR(20),
	REF_15 VARCHAR(100),
	PAYMENTTYPE VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_SALES_ORDER_FACT_TEMP (
	HASHKEY VARCHAR(32),
	SALEUNIT VARCHAR(50),
	ORDERID VARCHAR(50),
	ORDERDATE DATE,
	CUSTOMER_ID VARCHAR(50),
	CUSTOMER_NAME VARCHAR(200),
	CITY VARCHAR(100),
	REGION VARCHAR(100),
	SALEDISTRICT VARCHAR(100),
	SALEOFFICE VARCHAR(100),
	SALEGROUP VARCHAR(100),
	CUSTOMERTYPE VARCHAR(200),
	STORETYPE VARCHAR(200),
	SALETYPE VARCHAR(100),
	SALESEMPLOYEE VARCHAR(100),
	SALENAME VARCHAR(100),
	PRODUCTID VARCHAR(50),
	PRODUCTNAME VARCHAR(300),
	MEGABRAND VARCHAR(100),
	BRAND VARCHAR(100),
	BASEPRODUCT VARCHAR(100),
	VARIANT VARCHAR(100),
	PUTUP VARCHAR(100),
	PRICEREF VARCHAR(100),
	BACKLOG VARCHAR(100),
	QTY NUMBER(18,4),
	SUBAMT1 NUMBER(18,4),
	DISCOUNT NUMBER(18,4),
	SUBAMT2 NUMBER(18,4),
	DISCOUNTBTLINE NUMBER(18,4),
	TOTALBEFOREVAT NUMBER(18,4),
	TOTAL NUMBER(18,4),
	SALES_ORDER_LINE_NO VARCHAR(50),
	CANCELED VARCHAR(50),
	DOCUMENTID VARCHAR(50),
	RETURN_REASON VARCHAR(100),
	PROMOTIONCODE VARCHAR(100),
	PROMOTIONCODE1 VARCHAR(100),
	PROMOTIONCODE2 VARCHAR(100),
	PROMOTIONCODE3 VARCHAR(100),
	PROMOTIONCODE4 VARCHAR(100),
	PROMOTIONCODE5 VARCHAR(100),
	PROMOTION_CODE VARCHAR(100),
	PROMOTION_CODE2 VARCHAR(100),
	PROMOTION_CODE3 VARCHAR(100),
	AVGDISCOUNT NUMBER(18,4),
	ORDERTYPE VARCHAR(50),
	APPROVERSTATUS VARCHAR(50),
	PRICELEVEL VARCHAR(100),
	OPTIONAL3 VARCHAR(100),
	DELIVERYDATE DATE,
	ORDERTIME VARCHAR(20),
	SHIPTO VARCHAR(100),
	BILLTO VARCHAR(100),
	DELIVERYROUTEID VARCHAR(50),
	APPROVED_DATE DATE,
	APPROVED_TIME VARCHAR(20),
	REF_15 VARCHAR(100),
	PAYMENTTYPE VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_SCHEDULE (
	EMPLOYEE_ID VARCHAR(50),
	ROUTE_ID VARCHAR(50),
	SCHEDULE_DATE VARCHAR(20),
	APPROVED VARCHAR(5),
	SALEUNIT VARCHAR(20),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_SELLOUT_FACT (
	DISTRIBUTORID VARCHAR(10),
	ORDERNO VARCHAR(255),
	ORDERDATE VARCHAR(20),
	ARCODE VARCHAR(20),
	ARNAME VARCHAR(500),
	CITY VARCHAR(255),
	REGION VARCHAR(20),
	SALEDISTRICT VARCHAR(10),
	SALEOFFICE VARCHAR(255),
	SALEGROUP VARCHAR(255),
	ARTYPECODE VARCHAR(20),
	SALEEMPLOYEE VARCHAR(255),
	SALENAME VARCHAR(350),
	PRODUCTCODE VARCHAR(25),
	PRODUCTDESC VARCHAR(300),
	MEGABRAND VARCHAR(10),
	BRAND VARCHAR(10),
	BASEPRODUCT VARCHAR(20),
	VARIANT VARCHAR(10),
	PUTUP VARCHAR(10),
	GROSSPRICE NUMBER(19,6),
	QTY NUMBER(19,6),
	SUBAMT1 NUMBER(19,6),
	DISCOUNT NUMBER(19,6),
	SUBAMT2 NUMBER(19,6),
	DISCOUNTBTLINE NUMBER(19,6),
	TOTALBEFOREVAT NUMBER(19,6),
	TOTAL NUMBER(19,6),
	LINENUMBER NUMBER(18,0),
	ISCANCEL NUMBER(18,0),
	CNDOCNO VARCHAR(255),
	CNREASONCODE VARCHAR(255),
	PROMOTIONHEADER1 VARCHAR(255),
	PROMOTIONHEADER2 VARCHAR(255),
	PROMOTIONHEADER3 VARCHAR(255),
	PROMODESC1 VARCHAR(255),
	PROMODESC2 VARCHAR(255),
	PROMODESC3 VARCHAR(255),
	PROMOCODE1 VARCHAR(255),
	PROMOCODE2 VARCHAR(255),
	PROMOCODE3 VARCHAR(255),
	AVGDISCOUNT NUMBER(18,4),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_LA_GT_VISIT (
	ID_SALE VARCHAR(50),
	SALE_NAME VARCHAR(50),
	ID_CUSTOMER VARCHAR(50),
	CUSTOMER_NAME VARCHAR(200),
	DATE_PLAN VARCHAR(20),
	TIME_PLAN VARCHAR(20),
	DATE_VISI VARCHAR(20),
	TIME_VISI VARCHAR(20),
	OBJECT VARCHAR(200),
	VISIT_END VARCHAR(20),
	VISIT_TIME VARCHAR(20),
	REGIONCODE VARCHAR(50),
	AREACODE VARCHAR(50),
	BRANCHCODE VARCHAR(50),
	SALEUNIT VARCHAR(50),
	TIME_SURVEY_IN VARCHAR(50),
	TIME_SURVEY_OUT VARCHAR(50),
	COUNT_SURVEY VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_MDS_LCM_DISTRIBUTOR_TARGET_SALES (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	SALEOFFICE VARCHAR(200),
	SALEGROUP VARCHAR(200),
	TARGET NUMBER(31,0),
	PERIOD VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_LCM_DISTRIBUTOR_TARGET_SALES_RE (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	RE VARCHAR(200),
	TARGET NUMBER(31,0),
	PERIOD VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_CBD_ITEM_MASTER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	FRANCHISE VARCHAR(200),
	"sub brand" VARCHAR(200),
	VARIANT VARCHAR(200),
	"main brand" VARCHAR(200),
	"sap code 1" NUMBER(28,0),
	"sap code 2" NUMBER(28,0),
	"sap code 3" NUMBER(28,0),
	"sap code 4" NUMBER(28,0),
	"hs code" VARCHAR(200),
	"2022/suggested hs code" VARCHAR(200),
	"CAT. (ext)" VARCHAR(200),
	"individual barcode" NUMBER(28,0),
	"dksh code" VARCHAR(200),
	"dksh code 1" NUMBER(28,0),
	"dksh code 2" NUMBER(28,0),
	"product description" VARCHAR(200),
	"product name" VARCHAR(200),
	"active 2022" VARCHAR(200),
	"status 23" VARCHAR(200),
	"new sap jnj" VARCHAR(200),
	"new description jnj" VARCHAR(200),
	"status 2023" VARCHAR(200),
	"stock ea" NUMBER(28,0),
	"unit per case" NUMBER(28,0),
	"stock cse" NUMBER(28,6),
	"avg sellout per month" NUMBER(28,6),
	DOS VARCHAR(200),
	"duty fee (int)" NUMBER(28,2),
	"tax (int)" NUMBER(28,3),
	COO_ABBREV VARCHAR(200),
	"country of origin" VARCHAR(200),
	GTS NUMBER(28,6),
	NIV NUMBER(28,6),
	"duty fee (ext)" NUMBER(28,2),
	"tax (ext)" NUMBER(28,1),
	COMMENTS VARCHAR(200),
	"ccd comments" VARCHAR(262),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_COP (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	YEAR NUMBER(31,0),
	MONTH NUMBER(31,0),
	NO_OF_STORE NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_CUSTOMER_HIERARCHY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CHANNEL_CODE VARCHAR(500),
	CHANNEL_NAME VARCHAR(500),
	CHANNEL_ID NUMBER(18,0),
	RE_CODE VARCHAR(500),
	RE_NAME VARCHAR(500),
	RE_ID NUMBER(18,0),
	SUB_RE_CODE VARCHAR(500),
	SUB_RE_NAME VARCHAR(500),
	SUB_RE_ID NUMBER(18,0),
	REGION_CODE VARCHAR(500),
	REGION_NAME VARCHAR(500),
	REGION_ID NUMBER(18,0),
	CUSTOMER_NAME_CODE VARCHAR(500),
	CUSTOMER_NAME_NAME VARCHAR(500),
	CUSTOMER_NAME_ID NUMBER(18,0),
	SEGMENTATION_CODE VARCHAR(500),
	SEGMENTATION_NAME VARCHAR(500),
	SEGMENTATION_ID NUMBER(18,0),
	CUSTOMER_SEGMENTATION_LEVEL_2_CODE VARCHAR(500),
	CUSTOMER_SEGMENTATION_LEVEL_2_NAME VARCHAR(500),
	CUSTOMER_SEGMENTATION_LEVEL_2_ID NUMBER(18,0),
	STATUS_CODE VARCHAR(500),
	STATUS_NAME VARCHAR(500),
	STATUS_ID NUMBER(18,0),
	CDM_CODE VARCHAR(500),
	CDM_NAME VARCHAR(500),
	CDM_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_CUSTOMER_PRODUCT_CODE (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ACCOUNT VARCHAR(200),
	ITEM NUMBER(31,0),
	DESCRIPTION VARCHAR(200),
	BARCODE VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_CUSTOMER_RSP (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ACCOUNT_CODE VARCHAR(500),
	ACCOUNT_NAME VARCHAR(500),
	ACCOUNT_ID NUMBER(18,0),
	BARCODE VARCHAR(200),
	RSP NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500),
	VALID_FROM TIMESTAMP_NTZ(9),
	VALID_TO TIMESTAMP_NTZ(9),
	ACCOUNT VARCHAR(200)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_CN_REASON (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CN_EN_DESC VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_LIST (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	STATUS_CODE VARCHAR(500),
	STATUS_NAME VARCHAR(500),
	STATUS_ID NUMBER(18,0),
	REGION_CODE VARCHAR(500),
	REGION_NAME VARCHAR(500),
	REGION_ID NUMBER(18,0),
	SAP_CUSTOMER_CODE VARCHAR(200),
	SHIP_TO VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_MSL (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	BARCODE VARCHAR(200),
	PRODUCTNAME VARCHAR(200),
	ARTYPECODE_CODE VARCHAR(500),
	ARTYPECODE_NAME VARCHAR(500),
	ARTYPECODE_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_PRODUCT_GROUP (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	SAP_CODE VARCHAR(200),
	PRODUCT_GROUP_CODE VARCHAR(500),
	PRODUCT_GROUP_NAME VARCHAR(500),
	PRODUCT_GROUP_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_TARGET_DISTRIBUTION (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	PERIOD NUMBER(31,0),
	TARGET NUMBER(31,0),
	PRODUCT_GROUP_NAME_CODE VARCHAR(500),
	PRODUCT_GROUP_NAME_NAME VARCHAR(500),
	PRODUCT_GROUP_NAME_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_TARGET_SALES (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	SALEOFFICE VARCHAR(200),
	SALEGROUP VARCHAR(200),
	TARGET NUMBER(31,0),
	PERIOD NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_DISTRIBUTOR_TARGET_SALES_RE (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	RE_CODE VARCHAR(500),
	RE_NAME VARCHAR(500),
	RE_ID NUMBER(18,0),
	TARGET NUMBER(31,0),
	PERIOD NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_ECOM_PRODUCT (
	ID NUMBER(10,0),
	MUID VARCHAR(40),
	VERSIONNAME VARCHAR(300),
	VERSIONNUMBER NUMBER(10,0),
	VERSION_ID NUMBER(10,0),
	VERSIONFLAG VARCHAR(300),
	NAME VARCHAR(1500),
	CODE VARCHAR(1500),
	CHANGETRACKINGMASK NUMBER(10,0),
	DATASET VARCHAR(600),
	CUST_ATTR_1 VARCHAR(600),
	CUST_ATTR_2 VARCHAR(600),
	UPC VARCHAR(600),
	SKU_NAME VARCHAR(1530),
	PROD_ATTR_1 VARCHAR(600),
	PROD_ATTR_3 VARCHAR(600),
	PROD_ATTR_2 VARCHAR(600),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(600),
	ENTERVERSIONNUMBER NUMBER(10,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(600),
	LASTCHGVERSIONNUMBER NUMBER(10,0),
	VALIDATIONSTATUS VARCHAR(1500)
);
create or replace TABLE SDL_MDS_TH_FF_ACCOUNT_HIERACHY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ACCOUNT_NUMBER VARCHAR(200),
	ACCOUNT_DESCRIPTION VARCHAR(200),
	ACCOUNT_GROUP VARCHAR(200),
	ACCOUNT_BUCKET VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_FF_CUSTOMER_HIERACHY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	SAP_CUSTOMER_CODE VARCHAR(200),
	SAP_CUSTOMER_NAME VARCHAR(200),
	CHANNEL VARCHAR(200),
	SUB_CHANNEL VARCHAR(200),
	ACCOUNT VARCHAR(200),
	CUSTOMER_SEGMENTATION VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_FF_FORECAST (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ACCOUNT VARCHAR(200),
	BRAND VARCHAR(200),
	ACCOUNTING_GROUP VARCHAR(200),
	YEAR_MONTH NUMBER(28,0),
	FORECAST NUMBER(28,6),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500),
	CHANNEL VARCHAR(200),
	FORECAST_TYPE VARCHAR(200)
);
create or replace TABLE SDL_MDS_TH_FF_PRODUCT_HIERACHY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	BW_PRODUCT_MAJOR VARCHAR(200),
	FF_BRAND VARCHAR(200),
	FF_CATEGORY VARCHAR(200),
	BRAND_SEGMENT VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_GT_KPI_TARGET (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	YEAR NUMBER(31,0),
	KPI_CODE VARCHAR(200),
	KPI_DESC VARCHAR(200),
	RE_CODE VARCHAR(200),
	RE_DESC VARCHAR(200),
	JAN NUMBER(31,1),
	FEB NUMBER(31,1),
	MAR NUMBER(31,1),
	APR NUMBER(31,1),
	MAY NUMBER(31,1),
	JUN NUMBER(31,1),
	JUL NUMBER(31,1),
	AUG NUMBER(31,1),
	SEP NUMBER(31,1),
	OCT NUMBER(31,1),
	NOV NUMBER(31,1),
	DEC NUMBER(31,1),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_GT_MSL (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	SAPCODE VARCHAR(200),
	"product name" VARCHAR(200),
	BARCODE VARCHAR(200),
	AR_TYP_GRP VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_GT_PRODUCTIVE_CALL_TARGET (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	SALEUNIT VARCHAR(200),
	SALEID VARCHAR(200),
	MONTH NUMBER(31,0),
	YEAR NUMBER(31,0),
	PRODUCT NUMBER(31,0),
	TARGET NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_GT_SCOPE (
	CALL VARCHAR(200),
	CHANA_CUST_LOAD VARCHAR(200),
	CHANGETRACKINGMASK NUMBER(18,0),
	CODE VARCHAR(500),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	ID NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	MSL VARCHAR(200),
	MUID VARCHAR(36),
	NAME VARCHAR(500),
	OTIF VARCHAR(200),
	SELLOUT VARCHAR(200),
	STORES VARCHAR(200),
	VALIDATIONSTATUS VARCHAR(500),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0)
);
create or replace TABLE SDL_MDS_TH_HTC_CUSTOMER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	ARADDRESS VARCHAR(200),
	TELEPHONE VARCHAR(200),
	FAX VARCHAR(200),
	CITY VARCHAR(200),
	REGION VARCHAR(200),
	SALEDISTRICT VARCHAR(200),
	SALEOFFICE VARCHAR(200),
	SALEGROUP VARCHAR(200),
	ARTYPECODE VARCHAR(200),
	SALEEMPLOYEE NUMBER(31,0),
	SALENAME VARCHAR(200),
	BILLNO VARCHAR(200),
	BILLMOO VARCHAR(200),
	BILLSOI VARCHAR(200),
	BILLROAD VARCHAR(200),
	BILLSUBDIST VARCHAR(200),
	BILLDISTRICT VARCHAR(200),
	BILLPROVINCE VARCHAR(200),
	BILLZIPCODE VARCHAR(200),
	ACTIVESTATUS NUMBER(31,0),
	ROUTESTEP1 VARCHAR(200),
	ROUTESTEP2 VARCHAR(200),
	ROUTESTEP3 VARCHAR(200),
	ROUTESTEP4 VARCHAR(200),
	ROUTESTEP5 VARCHAR(200),
	ROUTESTEP6 VARCHAR(200),
	ROUTESTEP7 VARCHAR(200),
	ROUTESTEP8 VARCHAR(200),
	ROUTESTEP9 VARCHAR(200),
	ROUTESTEP10 VARCHAR(200),
	STORE VARCHAR(200),
	PRICELEVEL VARCHAR(200),
	SALESAREANAME VARCHAR(200),
	BRANCHCODE VARCHAR(200),
	BRANCHNAME VARCHAR(200),
	FREQUENCYOFVISIT NUMBER(31,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_HTC_INVENTORY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DATE TIMESTAMP_NTZ(9),
	COMPANY VARCHAR(200),
	WHCODE VARCHAR(200),
	PRODUCTCODE NUMBER(31,0),
	QTY NUMBER(31,7),
	AMOUNT NUMBER(31,7),
	LOTNUMBER VARCHAR(200),
	EXPIRYDATE VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_HTC_SELLOUT (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DISTRIBUTORID VARCHAR(200),
	INVOICE VARCHAR(200),
	DATE_INVOICE TIMESTAMP_NTZ(9),
	CUSTOMER_CODE VARCHAR(200),
	CUSTOMER_NAME VARCHAR(200),
	CITY VARCHAR(200),
	REGION VARCHAR(200),
	SALEDISTRICT VARCHAR(200),
	SALEOFFICE VARCHAR(200),
	SALEAREACODE VARCHAR(200),
	ARTYPECODE VARCHAR(200),
	SALEEMPLOYEE VARCHAR(200),
	SALENAME VARCHAR(200),
	PRODUCT_CODE NUMBER(31,0),
	PRODUCT_NAME VARCHAR(200),
	MEGABRAND VARCHAR(200),
	BRAND VARCHAR(200),
	BASEPRODUCT VARCHAR(200),
	VARIANT VARCHAR(200),
	PUTUP VARCHAR(200),
	GROSSPRICE NUMBER(31,2),
	QTY_PCS NUMBER(31,0),
	"sum of subamt" NUMBER(31,3),
	DISCOUNT_AMOUNT NUMBER(31,5),
	SUB_AMT_2 NUMBER(31,7),
	DISCOUNT_BTLINE NUMBER(31,0),
	TOTALBEFOREVAT NUMBER(31,7),
	TOTAL NUMBER(31,2),
	LINENUMBER NUMBER(31,0),
	ISCANCEL NUMBER(31,0),
	CNDOCNO VARCHAR(200),
	CNREASONCODE VARCHAR(200),
	PROMO_ID VARCHAR(200),
	PROMOTIONCODE1 VARCHAR(200),
	PROMOTIONCODE2 VARCHAR(200),
	PROMOTIONCODE3 VARCHAR(200),
	PROMOTIONCODE4 VARCHAR(200),
	PROMOTIONCODE5 VARCHAR(200),
	PROMOTION_CODE VARCHAR(200),
	PROMOTION_CODE2 VARCHAR(200),
	PROMOTION_CODE3 VARCHAR(200),
	BTDISC VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_LCM_EXCHANGE_RATE (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CNTRY_KEY VARCHAR(200),
	CNTRY_NM VARCHAR(200),
	FROM_CCY VARCHAR(200),
	TO_CCY VARCHAR(200),
	VALID_FROM TIMESTAMP_NTZ(9),
	VALID_TO TIMESTAMP_NTZ(9),
	EXCH_RATE NUMBER(28,6),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_MAPPING_LOCAL_BRAND (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CATEGORY_CODE VARCHAR(500),
	CATEGORY_NAME VARCHAR(500),
	CATEGORY_ID NUMBER(18,0),
	LOCALBRAND_CODE VARCHAR(500),
	LOCALBRAND_NAME VARCHAR(500),
	LOCALBRAND_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_MARKET_SHARE_DISTRIBUTION (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	MEASURE VARCHAR(200),
	CATEGORY VARCHAR(200),
	YEAR_MONTH NUMBER(31,0),
	VALUE NUMBER(31,1),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_MT_BRANCH_MASTER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	BRANCHCODE VARCHAR(200),
	ACCOUNT_CODE VARCHAR(500),
	ACCOUNT_NAME VARCHAR(500),
	ACCOUNT_ID NUMBER(18,0),
	BRANCHTYPE_CODE VARCHAR(500),
	BRANCHTYPE_NAME VARCHAR(500),
	BRANCHTYPE_ID NUMBER(18,0),
	ALLSTORETYPE_CODE VARCHAR(500),
	ALLSTORETYPE_NAME VARCHAR(500),
	ALLSTORETYPE_ID NUMBER(18,0),
	PROVINCECODE_CODE VARCHAR(500),
	PROVINCECODE_NAME VARCHAR(500),
	PROVINCECODE_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_MT_WEBSCRAPING_ITEM_MASTER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(200),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(200),
	NAME VARCHAR(1000),
	CODE VARCHAR(1000),
	CHANGETRACKINGMASK NUMBER(18,0),
	UNIVERSAL_SKU_NUMBER NUMBER(28,0),
	STANDARD_NAME VARCHAR(400),
	SOURCE VARCHAR(400),
	SKU_NAME VARCHAR(400),
	JJ_COMP VARCHAR(400),
	SITE_SKUID VARCHAR(400),
	URL VARCHAR(800),
	JJ_SAPCODE VARCHAR(400),
	BRAND VARCHAR(400),
	MANUFACTURER VARCHAR(400),
	CATEGORY_JNJ VARCHAR(400),
	SUB_CATEGORY_JNJ VARCHAR(400),
	PH1 VARCHAR(400),
	PH2 VARCHAR(400),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(400),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(400),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(1000)
);
create or replace TABLE SDL_MDS_TH_MYANMAR_CUSTOMER_MASTER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	"customer code" VARCHAR(200),
	"customer name" VARCHAR(200),
	"customer re" VARCHAR(200),
	TYPE VARCHAR(200),
	"sales office" VARCHAR(200),
	"sales group" VARCHAR(200),
	"distributor name" VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_MYM_PRODUCT_MASTER (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	"item NO." VARCHAR(200),
	"product description" VARCHAR(200),
	"sap code" NUMBER(28,0),
	"sap name" VARCHAR(200),
	BRAND VARCHAR(200),
	FRANCHISE VARCHAR(200),
	SIZE NUMBER(28,0),
	"dz per case" NUMBER(28,1),
	"unit per case" NUMBER(28,0),
	PRICE_PER_CASE_USD NUMBER(28,6),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_NPI (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	STARTDATE TIMESTAMP_NTZ(9),
	ENDDATE TIMESTAMP_NTZ(9),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_ONE_JNJ_DATA (
	ACTUAL_VALUE VARCHAR(1530),
	ADD_INFO VARCHAR(1530),
	CHANGETRACKINGMASK NUMBER(18,0),
	CODE VARCHAR(1500),
	COMPLIANCE_DEFINITION VARCHAR(600),
	DATE_LEVEL_CODE VARCHAR(1500),
	DATE_LEVEL_ID NUMBER(18,0),
	DATE_LEVEL_NAME VARCHAR(1500),
	DATE_VALUE VARCHAR(600),
	DEFAULT_VIEW_FLAG VARCHAR(6),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(600),
	ENTERVERSIONNUMBER NUMBER(18,0),
	FUNCTIONAL_AREA VARCHAR(600),
	ID NUMBER(18,0),
	KPI VARCHAR(600),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(600),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	LEVEL_1 VARCHAR(600),
	LEVEL_1_DEF VARCHAR(600),
	LEVEL_2 VARCHAR(600),
	LEVEL_2_DEF VARCHAR(600),
	MUID VARCHAR(108),
	NAME VARCHAR(1500),
	REF_VALUE VARCHAR(600),
	SECTOR_FUNCTION_CODE VARCHAR(1500),
	SECTOR_FUNCTION_ID NUMBER(18,0),
	SECTOR_FUNCTION_NAME VARCHAR(1500),
	SUBJECT VARCHAR(600),
	VALIDATIONSTATUS VARCHAR(1500),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(300),
	VERSIONNAME VARCHAR(300),
	VERSIONNUMBER NUMBER(18,0)
);
create or replace TABLE SDL_MDS_TH_PRODUCT_MASTER (
	ACTIVESTATUS_CODE VARCHAR(1500),
	ACTIVESTATUS_ID NUMBER(18,0),
	ACTIVESTATUS_NAME VARCHAR(1500),
	BARCODE VARCHAR(600),
	BARCODEACTIVESTATUS VARCHAR(600),
	BARCODEREMARK VARCHAR(600),
	BRANDCODE VARCHAR(600),
	BUYPRICE1 NUMBER(31,2),
	BUYPRICE2 NUMBER(31,2),
	BUYPRICE3 NUMBER(31,6),
	BUYPRICE4 NUMBER(31,6),
	CANMAKEPO VARCHAR(600),
	CATEORYCODE VARCHAR(600),
	CHANGETRACKINGMASK NUMBER(18,0),
	CODE VARCHAR(1500),
	COLORCODE VARCHAR(600),
	COSTTYPE NUMBER(31,0),
	CREATEDATE TIMESTAMP_NTZ(9),
	DEFBUYSHELF VARCHAR(600),
	DEFBUYUNITCODE_CODE VARCHAR(1500),
	DEFBUYUNITCODE_ID NUMBER(18,0),
	DEFBUYUNITCODE_NAME VARCHAR(1500),
	DEFBUYWHCODE VARCHAR(600),
	DEFSALESHELF VARCHAR(600),
	DEFSALEUNITCODE_CODE VARCHAR(1500),
	DEFSALEUNITCODE_ID NUMBER(18,0),
	DEFSALEUNITCODE_NAME VARCHAR(1500),
	DEFSALEWHCODE VARCHAR(600),
	DEFSTKUNITCODE_CODE VARCHAR(1500),
	DEFSTKUNITCODE_ID NUMBER(18,0),
	DEFSTKUNITCODE_NAME VARCHAR(1500),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(600),
	ENTERVERSIONNUMBER NUMBER(18,0),
	EXCEPTAX NUMBER(31,0),
	EXCEPTIONFLAG VARCHAR(3),
	FORMATCODE VARCHAR(600),
	FORMQTY NUMBER(31,2),
	GROUPCODE VARCHAR(600),
	ID NUMBER(18,0),
	IS_HERO VARCHAR(600),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(600),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	MUID VARCHAR(108),
	MYCLASS_CODE VARCHAR(1500),
	MYCLASS_ID NUMBER(18,0),
	MYCLASS_NAME VARCHAR(1500),
	MYDESCRIPTION VARCHAR(600),
	MYGRADE_CODE VARCHAR(1500),
	MYGRADE_ID NUMBER(18,0),
	MYGRADE_NAME VARCHAR(1500),
	MYSIZE NUMBER(31,0),
	NAME VARCHAR(1500),
	NAME2 VARCHAR(600),
	PARENTCODE VARCHAR(600),
	PRICEREMARK VARCHAR(600),
	RETAILER_UNIT_CONVERSION NUMBER(31,0),
	SALEPRICE1 NUMBER(31,0),
	SALEPRICE2 NUMBER(31,0),
	SALEPRICE3 NUMBER(31,0),
	SALEPRICE4 NUMBER(31,0),
	STOCKTYPE NUMBER(31,0),
	TAXTYPE NUMBER(31,0),
	TOQTY NUMBER(31,0),
	TRANSPORTTIME NUMBER(31,0),
	TYPECODE VARCHAR(600),
	U1PRICE1 NUMBER(31,0),
	U1PRICE2 NUMBER(31,2),
	U1PRICE3 NUMBER(31,2),
	U2PRICE1 NUMBER(31,0),
	U2PRICE2 NUMBER(31,0),
	U2PRICE3 NUMBER(31,0),
	U3PRICE1 NUMBER(31,0),
	U3PRICE2 NUMBER(31,0),
	U3PRICE3 NUMBER(31,0),
	U4PRICE1 VARCHAR(600),
	U4PRICE2 VARCHAR(600),
	U4PRICE3 VARCHAR(600),
	UNIT1_CODE VARCHAR(1500),
	UNIT1_ID NUMBER(18,0),
	UNIT1_NAME VARCHAR(1500),
	UNIT1R1 NUMBER(31,0),
	UNIT1R2 NUMBER(31,0),
	UNIT2_CODE VARCHAR(1500),
	UNIT2_ID NUMBER(18,0),
	UNIT2_NAME VARCHAR(1500),
	UNIT2R1 NUMBER(31,0),
	UNIT2R2 NUMBER(31,0),
	UNIT3_CODE VARCHAR(1500),
	UNIT3_ID NUMBER(18,0),
	UNIT3_NAME VARCHAR(1500),
	UNIT3R1 NUMBER(31,0),
	UNIT3R2 NUMBER(31,0),
	UNIT4 VARCHAR(600),
	UNIT4R1 VARCHAR(600),
	UNIT4R2 VARCHAR(600),
	UNITTYPE NUMBER(31,0),
	VALIDATIONSTATUS VARCHAR(1500),
	VENDERCODE VARCHAR(600),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(300),
	VERSIONNAME VARCHAR(300),
	VERSIONNUMBER NUMBER(18,0),
	WEIGHT VARCHAR(600),
	LOCAL_BRAND VARCHAR(600),
	LOCAL_CATEGORY VARCHAR(600)
);
create or replace TABLE SDL_MDS_TH_PS_STORE (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	DATASET VARCHAR(200),
	CHANNEL VARCHAR(200),
	RETAIL_ENVIRONMENT VARCHAR(200),
	STATE VARCHAR(200),
	CUSTOMER_CODE VARCHAR(200),
	CUSTOMER_NAME VARCHAR(200),
	STORE_CODE VARCHAR(200),
	STORE_NAME VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_PS_TARGETS (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CHANNEL VARCHAR(200),
	RE VARCHAR(200),
	KPI VARCHAR(200),
	ATTRIBUTE_1 VARCHAR(200),
	ATTRIBUTE_2 VARCHAR(200),
	VALUE NUMBER(31,3),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500),
	VALID_FROM TIMESTAMP_NTZ(9),
	VALID_TO TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_MDS_TH_PS_WEIGHTS (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CHANNEL VARCHAR(200),
	RE VARCHAR(200),
	KPI VARCHAR(200),
	WEIGHT NUMBER(31,3),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_PS_WEIGHT_BY_ACCOUNT (
	ID NUMBER(18,0),
	MUID VARCHAR(108),
	VERSIONNAME VARCHAR(300),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(300),
	NAME VARCHAR(1500),
	CODE VARCHAR(1500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CHANNEL_CODE VARCHAR(1500),
	CHANNEL_NAME VARCHAR(1500),
	CHANNEL_ID NUMBER(18,0),
	STORETYPE_CODE VARCHAR(1500),
	STORETYPE_NAME VARCHAR(1500),
	STORETYPE_ID NUMBER(18,0),
	STOREREFERENCE_CODE VARCHAR(1500),
	STOREREFERENCE_NAME VARCHAR(1500),
	STOREREFERENCE_ID NUMBER(18,0),
	ACCOUNT_WEIGHT NUMBER(31,2),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(600),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(600),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(1500)
);
create or replace TABLE SDL_MDS_TH_REF_CITY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	REGION_CODE VARCHAR(500),
	REGION_NAME VARCHAR(500),
	REGION_ID NUMBER(18,0),
	CITYENGLISH VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_DISTRIBUTOR_CUSTOMER_GROUP (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_DISTRIBUTOR_DISTRIBUTION_LIST (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_DISTRIBUTOR_ITEM_UNIT (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	NAME2 VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_DISTRIBUTOR_MYCLASS (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_DISTRICT (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_PRODUCT_CATEGORY (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_PRODUCT_LOCAL_BRAND (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	CATEGORY_CODE VARCHAR(500),
	CATEGORY_NAME VARCHAR(500),
	CATEGORY_ID NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_REGION (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_REF_SUB_DISTRICT (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_ROLLING_FORECAST (
	ID NUMBER(18,0),
	MUID VARCHAR(36),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(18,0),
	VERSION_ID NUMBER(18,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(18,0),
	"fiscal_year/period" NUMBER(31,0),
	"j&j_fiscal_week" VARCHAR(200),
	COMPANY_CODE NUMBER(31,0),
	VALUE_TYPE VARCHAR(200),
	VERSION NUMBER(31,0),
	MANUAL_TYPE VARCHAR(200),
	CURRENCY VARCHAR(200),
	SALES_ORGANIZATION NUMBER(31,0),
	DISTRIBUTION_CHANNEL NUMBER(31,0),
	DIVISION NUMBER(31,0),
	MATERIAL NUMBER(31,0),
	"b1_mega-brand" VARCHAR(200),
	B2_BRAND VARCHAR(200),
	B3_BASE_PRODUCT VARCHAR(200),
	B4_VARIANT VARCHAR(200),
	"b5_put-up" VARCHAR(200),
	"operating_group" VARCHAR(200),
	"franchise_group" VARCHAR(200),
	FRANCHISE VARCHAR(200),
	PRODUCT_FRANCHISE VARCHAR(200),
	PRODUCT_MAJOR VARCHAR(200),
	PRODUCT_MINOR VARCHAR(200),
	CURRENT_SALES_EMPLOYEE VARCHAR(200),
	CUSTOMER_NUMBER NUMBER(31,0),
	LOCAL_CUSTOMER_GRP_1 VARCHAR(200),
	LOCAL_CUSTOMER_GRP_2 VARCHAR(200),
	LOCAL_CUSTOMER_GRP_3 VARCHAR(200),
	LOCAL_CUSTOMER_GRP_4 VARCHAR(200),
	LOCAL_CUSTOMER_GRP_5 VARCHAR(200),
	LOCAL_CUSTOMER_GRP_6 VARCHAR(200),
	SALES_TARGET NUMBER(31,5),
	QUANTITY VARCHAR(200),
	UNIT VARCHAR(200),
	FISCAL_VARIANT VARCHAR(200),
	FISCAL_YEAR NUMBER(31,0),
	COUNTRY VARCHAR(200),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(18,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(18,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MDS_TH_SKU_BENCHMARKS (
	ID NUMBER(10,0),
	MUID VARCHAR(40),
	VERSIONNAME VARCHAR(100),
	VERSIONNUMBER NUMBER(10,0),
	VERSION_ID NUMBER(10,0),
	VERSIONFLAG VARCHAR(100),
	NAME VARCHAR(500),
	CODE VARCHAR(500),
	CHANGETRACKINGMASK NUMBER(10,0),
	JJ_UPC VARCHAR(200),
	JJ_SKU_DESCRIPTION VARCHAR(510),
	JJ_PACKSIZE NUMBER(28,3),
	JJ_TARGET NUMBER(28,3),
	VARIANCE NUMBER(28,3),
	COMP_UPC VARCHAR(200),
	COMP_SKU_DESCRIPTION VARCHAR(510),
	COMP_PACKSIZE NUMBER(28,3),
	VALID_FROM TIMESTAMP_NTZ(9),
	VALID_TO TIMESTAMP_NTZ(9),
	ENTERDATETIME TIMESTAMP_NTZ(9),
	ENTERUSERNAME VARCHAR(200),
	ENTERVERSIONNUMBER NUMBER(10,0),
	LASTCHGDATETIME TIMESTAMP_NTZ(9),
	LASTCHGUSERNAME VARCHAR(200),
	LASTCHGVERSIONNUMBER NUMBER(10,0),
	VALIDATIONSTATUS VARCHAR(500)
);
create or replace TABLE SDL_MYM_GT_INVENTORY_REPORT_FACT (
	ITEM_NO VARCHAR(50),
	ITEM_DESCRIPTION VARCHAR(200),
	QTY_SOLD VARCHAR(50),
	STOCK VARCHAR(50),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_MYM_GT_SALES_REPORT_FACT (
	ITEM_NO VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	QTY_SOLD VARCHAR(50),
	FOC_QTY VARCHAR(50),
	TOTAL VARCHAR(200),
	PERIOD VARCHAR(50),
	CUSTOMER_GROUP VARCHAR(100),
	CUSTOMER_CODE VARCHAR(50),
	CUSTOMER_NAME VARCHAR(100),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_MYM_GT_SALES_REPORT_FACT_TEMP (
	HASHKEY VARCHAR(32),
	ITEM_NO VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	QTY_SOLD NUMBER(18,4),
	FOC_QTY NUMBER(18,4),
	TOTAL NUMBER(18,4),
	PERIOD VARCHAR(50),
	CUSTOMER_GROUP VARCHAR(100),
	CUSTOMER_CODE VARCHAR(50),
	CUSTOMER_NAME VARCHAR(100),
	FILENAME VARCHAR(50),
	RUN_ID VARCHAR(14),
	CRT_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);


);
create or replace TABLE SDL_TH_CIW_ACCOUNT_LOOKUP (
	YEAR NUMBER(18,0),
	AREA VARCHAR(50),
	CATEGORY VARCHAR(256),
	ACCOUNT_NAME VARCHAR(256),
	ACCOUNT_NUM VARCHAR(50) NOT NULL,
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (ACCOUNT_NUM)
);
create or replace TABLE SDL_TH_DMS_CHANA_CUSTOMER_DIM (
	DISTRIBUTORID VARCHAR(10) NOT NULL,
	ARCODE VARCHAR(20) NOT NULL,
	ARNAME VARCHAR(500),
	ARADDRESS VARCHAR(500),
	TELEPHONE VARCHAR(150),
	FAX VARCHAR(150),
	CITY VARCHAR(500),
	REGION VARCHAR(20),
	SALEDISTRICT VARCHAR(10),
	SALEOFFICE VARCHAR(10),
	SALEGROUP VARCHAR(10),
	ARTYPECODE VARCHAR(10),
	SALEEMPLOYEE VARCHAR(150),
	SALENAME VARCHAR(250),
	BILLNO VARCHAR(500),
	BILLMOO VARCHAR(250),
	BILLSOI VARCHAR(255),
	BILLROAD VARCHAR(255),
	BILLSUBDIST VARCHAR(30),
	BILLDISTRICT VARCHAR(30),
	BILLPROVINCE VARCHAR(30),
	BILLZIPCODE VARCHAR(50),
	ACTIVESTATUS NUMBER(18,0),
	ROUTESTEP1 VARCHAR(10),
	ROUTESTEP2 VARCHAR(10),
	ROUTESTEP3 VARCHAR(10),
	ROUTESTEP4 VARCHAR(10),
	ROUTESTEP5 VARCHAR(10),
	ROUTESTEP6 VARCHAR(10),
	ROUTESTEP7 VARCHAR(10),
	ROUTESTEP8 VARCHAR(10),
	ROUTESTEP9 VARCHAR(10),
	ROUTESTEP10 VARCHAR(10),
	STORE VARCHAR(200),
	PRICELEVEL VARCHAR(50),
	SALESAREANAME VARCHAR(150),
	BRANCHCODE VARCHAR(50),
	BRANCHNAME VARCHAR(150),
	FREQUENCYOFVISIT VARCHAR(50),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_DMS_CUSTOMER_DIM (
	DISTRIBUTORID VARCHAR(10),
	ARCODE VARCHAR(20),
	ARNAME VARCHAR(500),
	ARADDRESS VARCHAR(500),
	TELEPHONE VARCHAR(150),
	FAX VARCHAR(150),
	CITY VARCHAR(500),
	REGION VARCHAR(20),
	SALEDISTRICT VARCHAR(10),
	SALEOFFICE VARCHAR(10),
	SALEGROUP VARCHAR(10),
	ARTYPECODE VARCHAR(10),
	SALEEMPLOYEE VARCHAR(150),
	SALENAME VARCHAR(250),
	BILLNO VARCHAR(500),
	BILLMOO VARCHAR(250),
	BILLSOI VARCHAR(255),
	BILLROAD VARCHAR(255),
	BILLSUBDIST VARCHAR(30),
	BILLDISTRICT VARCHAR(30),
	BILLPROVINCE VARCHAR(30),
	BILLZIPCODE VARCHAR(50),
	ACTIVESTATUS VARCHAR(200),
	ROUTESTEP1 VARCHAR(10),
	ROUTESTEP2 VARCHAR(10),
	ROUTESTEP3 VARCHAR(10),
	ROUTESTEP4 VARCHAR(10),
	ROUTESTEP5 VARCHAR(10),
	ROUTESTEP6 VARCHAR(10),
	ROUTESTEP7 VARCHAR(10),
	ROUTESTEP8 VARCHAR(100),
	ROUTESTEP9 VARCHAR(100),
	ROUTESTEP10 VARCHAR(10),
	STORE VARCHAR(200),
	SOURCEFILE VARCHAR(255),
	OLD_CUSTID VARCHAR(255),
	MODIFYDATE VARCHAR(25),
	CURR_DATE DATE,
	RUN_ID NUMBER(18,0),
	SOURCE_FILE_NAME VARCHAR(200)
);
create or replace TABLE SDL_TH_DMS_INVENTORY_FACT (
	RECDATE VARCHAR(200),
	DISTRIBUTORID VARCHAR(200),
	WHCODE VARCHAR(200),
	PRODUCTCODE VARCHAR(200),
	QTY VARCHAR(200),
	AMOUNT VARCHAR(200),
	BATCHNO VARCHAR(200),
	EXPIRYDATE VARCHAR(200),
	CURR_DATE DATE,
	RUN_ID NUMBER(18,0),
	SOURCE_FILE_NAME VARCHAR(200)
);
create or replace TABLE SDL_TH_DMS_SELLOUT_FACT (
	DISTRIBUTORID VARCHAR(10),
	ORDERNO VARCHAR(255),
	ORDERDATE VARCHAR(255),
	ARCODE VARCHAR(20),
	ARNAME VARCHAR(500),
	CITY VARCHAR(255),
	REGION VARCHAR(20),
	SALEDISTRICT VARCHAR(10),
	SALEOFFICE VARCHAR(255),
	SALEGROUP VARCHAR(255),
	ARTYPECODE VARCHAR(20),
	SALEEMPLOYEE VARCHAR(255),
	SALENAME VARCHAR(350),
	PRODUCTCODE VARCHAR(25),
	PRODUCTDESC VARCHAR(300),
	MEGABRAND VARCHAR(10),
	BRAND VARCHAR(10),
	BASEPRODUCT VARCHAR(20),
	VARIANT VARCHAR(10),
	PUTUP VARCHAR(10),
	GROSSPRICE NUMBER(19,6),
	QTY NUMBER(19,6),
	SUBAMT1 NUMBER(19,6),
	DISCOUNT NUMBER(19,6),
	SUBAMT2 NUMBER(19,6),
	DISCOUNTBTLINE NUMBER(19,6),
	TOTALBEFOREVAT NUMBER(19,6),
	TOTAL NUMBER(19,6),
	LINENUMBER VARCHAR(255),
	ISCANCEL VARCHAR(255),
	CNDOCNO VARCHAR(255),
	CNREASONCODE VARCHAR(255),
	PROMOTIONHEADER1 VARCHAR(255),
	PROMOTIONHEADER2 VARCHAR(255),
	PROMOTIONHEADER3 VARCHAR(255),
	PROMODESC1 VARCHAR(255),
	PROMODESC2 VARCHAR(255),
	PROMODESC3 VARCHAR(255),
	PROMOCODE1 VARCHAR(255),
	PROMOCODE2 VARCHAR(255),
	PROMOCODE3 VARCHAR(255),
	AVGDISCOUNT NUMBER(18,4),
	CURR_DATE DATE,
	RUN_ID NUMBER(18,0),
	SOURCE_FILE_NAME VARCHAR(200)
);
create or replace TABLE SDL_TH_DTSCNREASON (
	CN_REASON VARCHAR(5),
	CN_TH_DESC VARCHAR(250),
	CN_EN_DESC VARCHAR(250),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_DTSCUSTGROUP (
	AR_TYP_CD VARCHAR(10) NOT NULL,
	GRP_NM VARCHAR(100),
	AR_TYP_GRP VARCHAR(10),
	TYP_GRP_NM VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (AR_TYP_CD)
);
create or replace TABLE SDL_TH_DTSCUSTOMER (
	DSTRBTR_ID VARCHAR(10) NOT NULL,
	AR_CD VARCHAR(20) NOT NULL,
	AR_NM VARCHAR(500),
	AR_ADRES VARCHAR(500),
	TEL_PHN VARCHAR(150),
	FAX VARCHAR(150),
	CITY VARCHAR(500),
	REGION VARCHAR(20),
	AR_TYP_CD VARCHAR(10),
	SLS_DIST VARCHAR(10),
	SLS_OFFICE VARCHAR(10),
	SLS_GRP VARCHAR(10),
	SLS_EMP VARCHAR(150),
	SLS_NM VARCHAR(250),
	SRC_FILE VARCHAR(255),
	BILL_NO VARCHAR(500),
	BILL_MOO VARCHAR(250),
	BILL_SOI VARCHAR(255),
	BILL_RD VARCHAR(255),
	BILL_SUBDIST VARCHAR(30),
	BILL_DIST VARCHAR(30),
	BILL_PRVNCE VARCHAR(30),
	BILL_ZIP_CD VARCHAR(50),
	OLD_CUST_ID VARCHAR(25) NOT NULL,
	MODIFY_DT TIMESTAMP_NTZ(9),
	ROUTESTEP1 VARCHAR(10),
	ROUTESTEP2 VARCHAR(10),
	ROUTESTEP3 VARCHAR(10),
	ROUTESTEP4 VARCHAR(10),
	ROUTESTEP5 VARCHAR(10),
	ROUTESTEP6 VARCHAR(10),
	ROUTESTEP7 VARCHAR(10),
	ROUTESTEP8 VARCHAR(10),
	ROUTESTEP9 VARCHAR(10),
	ROUTESTEP10 VARCHAR(10),
	ACTV_STATUS NUMBER(18,0),
	CUST_TYPE VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DSTRBTR_ID, AR_CD, OLD_CUST_ID)
);
create or replace TABLE SDL_TH_DTSDISTRIBUTOR (
	DSTRBTR_ID VARCHAR(10) NOT NULL,
	DIST_NM VARCHAR(100),
	COST_LVL NUMBER(18,0),
	STATUS VARCHAR(10),
	REGION VARCHAR(20),
	CNTRY VARCHAR(20),
	CURNT_DIST VARCHAR(10),
	INV_DAY NUMBER(3,0),
	DSTRBTR_CD VARCHAR(255),
	DSTRBTR_FEE FLOAT,
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DSTRBTR_ID)
);
create or replace TABLE SDL_TH_DTSDISTRICT (
	DIST VARCHAR(10) NOT NULL,
	DIST_NM VARCHAR(100),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DIST)
);
create or replace TABLE SDL_TH_DTSINVENTORYBAL (
	DSTRBTR_ID VARCHAR(10) NOT NULL,
	REC_DT TIMESTAMP_NTZ(9) NOT NULL,
	WH_CD VARCHAR(20) NOT NULL,
	PROD_CD VARCHAR(25) NOT NULL,
	QTY NUMBER(19,6) DEFAULT 0,
	AMT NUMBER(19,6) DEFAULT 0,
	MEGA_BRND VARCHAR(10),
	BRND VARCHAR(10),
	BASE_PROD VARCHAR(20),
	VRNT VARCHAR(10),
	PUT_UP VARCHAR(10),
	ISUPDT_AMT NUMBER(18,0),
	SRC_FILE VARCHAR(250),
	CRT_DATE TIMESTAMP_NTZ(9),
	UPDT_DATE TIMESTAMP_NTZ(9),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DSTRBTR_ID, REC_DT, WH_CD, PROD_CD)
);
create or replace TABLE SDL_TH_DTSITEMMASTER (
	ITEM_CD VARCHAR(50) NOT NULL,
	NAME1 VARCHAR(500),
	NAME2 VARCHAR(500),
	DEFSTK_UNT_CD VARCHAR(10),
	PROD_GRP_ID VARCHAR(10),
	GRP_CD VARCHAR(10),
	TYP_CD VARCHAR(10),
	CATGRY_CD VARCHAR(10),
	BRND_CD VARCHAR(10),
	FORMAT_CD VARCHAR(10),
	BARCD VARCHAR(20),
	SLS_PRC1 NUMBER(19,6),
	SLS_PRC2 NUMBER(19,6),
	SLS_PRC3 NUMBER(19,6),
	SLS_PRC4 NUMBER(19,6),
	TAX_TYP NUMBER(18,0),
	FROM_QTY NUMBER(19,6),
	TO_QTY NUMBER(19,6),
	PRC_REMARK VARCHAR(100),
	COLOR_CD VARCHAR(100),
	MYGRADE VARCHAR(40),
	MYCLASS VARCHAR(20),
	UNIT_TYP NUMBER(18,0),
	COST_TYP NUMBER(18,0),
	STOCK_TYP NUMBER(18,0),
	DEF_SLS_UNT_CD VARCHAR(10),
	DEF_BUY_UNT_CD VARCHAR(10),
	MYSIZE VARCHAR(40),
	WEIGHT NUMBER(19,6),
	EXCEPTAX NUMBER(18,0),
	MYDESC VARCHAR(255),
	ACTV_STATUS NUMBER(18,0),
	BARCD_ACTV_STATUS NUMBER(18,0),
	BARCD_REMARK VARCHAR(400),
	CANMAKEPO NUMBER(18,0),
	BUY_PRC1 NUMBER(19,6),
	BUY_PRC2 NUMBER(19,6),
	BUY_PRC3 NUMBER(19,6),
	BUY_PRC4 NUMBER(19,6),
	TRNSPRT_TM NUMBER(18,0),
	VEND_CD VARCHAR(10),
	DEF_SLS_WH_CD VARCHAR(10),
	DEF_BUY_WH_CD VARCHAR(10),
	DEF_SLS_SHELF VARCHAR(10),
	DEF_BUY_SHELF VARCHAR(10),
	COST_LEVEL1 NUMBER(19,6),
	COST_LEVEL2 NUMBER(19,6),
	COST_LEVEL3 NUMBER(19,6),
	COST_LEVEL4 NUMBER(19,6),
	UOM VARCHAR(10),
	UNIT_PER_CASE NUMBER(18,0),
	ISEXCEPTION VARCHAR(1),
	ISFORVMR NUMBER(18,0),
	TH_SHRT_NM VARCHAR(200),
	EN_SHRT_NM VARCHAR(200),
	CRT_DT TIMESTAMP_NTZ(9),
	UPDT_DT TIMESTAMP_NTZ(9),
	IS_HERO VARCHAR(1),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (ITEM_CD)
);
create or replace TABLE SDL_TH_DTSITEMUNIT (
	CODE VARCHAR(10) NOT NULL,
	NAME1 VARCHAR(50),
	NAME2 VARCHAR(50),
	MSREPL_TRAN_VER VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (CODE)
);
create or replace TABLE SDL_TH_DTSJJPROMOTION (
	PROMO_ID VARCHAR(50),
	PROMO_NM VARCHAR(500),
	PROMO_REMARK VARCHAR(800),
	DATEBEGIN TIMESTAMP_NTZ(9),
	DATEEND TIMESTAMP_NTZ(9),
	FLAG_ENBLD VARCHAR(1),
	MDFD_DATE TIMESTAMP_NTZ(9),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_DTSMCL (
	BARCD VARCHAR(50),
	PROD_NM VARCHAR(50),
	AR_TYP_CD VARCHAR(10),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_DTSNPI (
	PROD_CD VARCHAR(25),
	PROD_NM VARCHAR(50),
	STARTDATE TIMESTAMP_NTZ(9),
	ENDDATE TIMESTAMP_NTZ(9),
	UPDT_DT TIMESTAMP_NTZ(9),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_DTSREGION (
	REGION VARCHAR(20) NOT NULL,
	REGION_DESC VARCHAR(100),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (REGION)
);
create or replace TABLE SDL_TH_DTSSALEDISTRICT (
	SLS_DIST VARCHAR(10) NOT NULL,
	CITY VARCHAR(100),
	REGION VARCHAR(20),
	CITY_ENG VARCHAR(100),
	BLNG_TO_DSTRBTR VARCHAR(5),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (SLS_DIST)
);
create or replace TABLE SDL_TH_DTSSALETRANS (
	DSTRBTR_ID VARCHAR(10) NOT NULL,
	ORDER_NO VARCHAR(255) NOT NULL,
	ORDER_DT TIMESTAMP_NTZ(9) NOT NULL,
	AR_CD VARCHAR(20) NOT NULL,
	AR_NM VARCHAR(500),
	CITY VARCHAR(255),
	REGION VARCHAR(20),
	AR_TYP_CD VARCHAR(20),
	SLS_DIST VARCHAR(10),
	SLS_OFFICE VARCHAR(255),
	SLS_GRP VARCHAR(255),
	SLS_EMP VARCHAR(255),
	SLS_NM VARCHAR(350),
	PROD_CD VARCHAR(25),
	PROD_DESC VARCHAR(300),
	MEGA_BRND VARCHAR(10),
	BRND VARCHAR(10),
	BASE_PROD_CD VARCHAR(20),
	VRNT_CD VARCHAR(10),
	PUT_UP VARCHAR(10),
	GRS_PRC NUMBER(19,6),
	QTY NUMBER(19,6),
	SUBAMT1 NUMBER(19,6),
	DISCNT NUMBER(19,6),
	SUBAMT2 NUMBER(19,6),
	DISCNT_BT_LN NUMBER(19,6),
	TOTAL_BFR_VAT NUMBER(19,6),
	TOTAL NUMBER(19,6),
	LINE_NUM NUMBER(18,0) NOT NULL,
	ISCALCREDEEM NUMBER(18,0),
	REDEEM_AMT NUMBER(19,6),
	ISCANCEL NUMBER(18,0),
	SRC_FILE VARCHAR(250),
	CN_DOC_NO VARCHAR(255),
	CN_REASON_CD VARCHAR(255),
	UPDT_DT TIMESTAMP_NTZ(9),
	PROM_HEADER1 VARCHAR(255),
	PROM_HEADER2 VARCHAR(255),
	PROM_HEADER3 VARCHAR(255),
	PROM_DESC1 VARCHAR(255),
	PROM_DESC2 VARCHAR(255),
	PROM_DESC3 VARCHAR(255),
	PROM_CD1 VARCHAR(255),
	PROM_CD2 VARCHAR(255),
	PROM_CD3 VARCHAR(255),
	AVG_DISCNT NUMBER(18,4),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DSTRBTR_ID, ORDER_NO, ORDER_DT, AR_CD, LINE_NUM)
);
create or replace TABLE SDL_TH_DTSSUBDISTRICT (
	SUB_DIST VARCHAR(10) NOT NULL,
	SUB_DIST_NM VARCHAR(100),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (SUB_DIST)
);
create or replace TABLE SDL_TH_GT_MSL_DISTRIBUTION (
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	DISTRIBUTOR_ID VARCHAR(10),
	DISTRIBUTOR_NAME VARCHAR(150),
	RE_CODE VARCHAR(20),
	RE_NAME VARCHAR(100),
	STORE_ID VARCHAR(50),
	STORE_NAME VARCHAR(150),
	SALES_REP_ID VARCHAR(50),
	SALES_REP_NAME VARCHAR(150),
	CATEGORY_CODE VARCHAR(50),
	CATEGORY VARCHAR(100),
	BRAND_CODE VARCHAR(50),
	BRAND VARCHAR(100),
	BARCODE VARCHAR(50),
	PRODUCT_DESCRIPTION VARCHAR(150),
	SURVEY_DATE DATE,
	NO_DISTRIBUTION VARCHAR(10),
	OSA VARCHAR(10),
	OOS VARCHAR(10),
	OOS_REASON VARCHAR(255),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_GT_ROUTE (
	HASHKEY VARCHAR(500),
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	ROUTEID VARCHAR(50),
	NAME VARCHAR(100),
	ROUTE_DESCRIPTION VARCHAR(100),
	ISACTIVE VARCHAR(10),
	ROUTESALE VARCHAR(50),
	SALEUNIT VARCHAR(50),
	ROUTECODE VARCHAR(50),
	DESCRIPTION VARCHAR(100),
	LAST_UPDATED_DATE DATE,
	FILENAME VARCHAR(100),
	FILE_UPLOADED_DATE DATE,
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_GT_ROUTE_DETAIL (
	HASHKEY VARCHAR(500),
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	ROUTEID VARCHAR(50),
	CUSTOMERID VARCHAR(50),
	ROUTENO VARCHAR(50),
	SALEUNIT VARCHAR(50),
	SHIP_TO VARCHAR(50),
	CONTACT_PERSON VARCHAR(100),
	CREATED_DATE DATE,
	FILENAME VARCHAR(100),
	FILE_UPLOADED_DATE DATE,
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_GT_SALES_ORDER (
	HASHKEY VARCHAR(500),
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	SALEUNIT VARCHAR(10),
	ORDERID VARCHAR(50),
	ORDERDATE DATE,
	CUSTOMER_ID VARCHAR(50),
	CUSTOMER_NAME VARCHAR(150),
	CITY VARCHAR(50),
	REGION VARCHAR(50),
	SALEDISTRICT VARCHAR(50),
	SALEOFFICE VARCHAR(50),
	SALEGROUP VARCHAR(50),
	CUSTOMERTYPE VARCHAR(50),
	STORETYPE VARCHAR(50),
	SALETYPE VARCHAR(50),
	SALESEMPLOYEE VARCHAR(50),
	SALENAME VARCHAR(150),
	PRODUCTID VARCHAR(50),
	PRODUCTNAME VARCHAR(150),
	MEGABRAND VARCHAR(50),
	BRAND VARCHAR(50),
	BASEPRODUCT VARCHAR(50),
	VARIANT VARCHAR(50),
	PUTUP VARCHAR(50),
	PRICEREF NUMBER(18,6),
	BACKLOG NUMBER(18,6),
	QTY NUMBER(18,6),
	SUBAMT1 NUMBER(18,6),
	DISCOUNT NUMBER(18,6),
	SUBAMT2 NUMBER(18,6),
	DISCOUNTBTLINE NUMBER(18,6),
	TOTALBEFOREVAT NUMBER(18,6),
	TOTAL NUMBER(18,6),
	SALES_ORDER_LINE_NO VARCHAR(10),
	CANCELLED VARCHAR(10),
	DOCUMENTID VARCHAR(50),
	RETURN_REASON VARCHAR(150),
	PROMOTIONCODE VARCHAR(50),
	PROMOTIONCODE1 VARCHAR(50),
	PROMOTIONCODE2 VARCHAR(50),
	PROMOTIONCODE3 VARCHAR(50),
	PROMOTIONCODE4 VARCHAR(50),
	PROMOTIONCODE5 VARCHAR(50),
	PROMOTION_CODE VARCHAR(50),
	PROMOTION_CODE2 VARCHAR(50),
	PROMOTION_CODE3 VARCHAR(50),
	AVGDISCOUNT NUMBER(18,6),
	ORDERTYPE VARCHAR(10),
	APPROVERSTATUS VARCHAR(10),
	PRICELEVEL VARCHAR(10),
	OPTIONAL DATE,
	DELIVERYDATE DATE,
	ORDERTIME VARCHAR(50),
	SHIPTO VARCHAR(50),
	BILLTO VARCHAR(50),
	DELIVERYROUTEID VARCHAR(50),
	APPROVED_DATE DATE,
	APPROVED_TIME VARCHAR(50),
	REF_15 VARCHAR(255),
	PAYMENTTYPE VARCHAR(50),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_GT_SCHEDULE (
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	EMPLOYEEID VARCHAR(50),
	ROUTEID VARCHAR(50),
	SCHEDULE_DATE DATE,
	APPROVED VARCHAR(10),
	SALEUNIT VARCHAR(50),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_GT_VISIT (
	CNTRY_CD VARCHAR(5),
	CRNCY_CD VARCHAR(5),
	ID_SALE VARCHAR(50),
	SALE_NAME VARCHAR(50),
	ID_CUSTOMER VARCHAR(50),
	CUSTOMER_NAME VARCHAR(100),
	DATE_PLAN DATE,
	TIME_PLAN VARCHAR(50),
	DATE_VISI DATE,
	TIME_VISI VARCHAR(50),
	OBJECT VARCHAR(100),
	VISIT_END DATE,
	VISIT_TIME VARCHAR(50),
	REGIONCODE VARCHAR(50),
	AREACODE VARCHAR(50),
	BRANCHCODE VARCHAR(50),
	SALEUNIT VARCHAR(50),
	TIME_SURVEY_IN VARCHAR(50),
	TIME_SURVEY_OUT VARCHAR(50),
	COUNT_SURVEY VARCHAR(50),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_JBP_COP (
	DIST_ID VARCHAR(20) NOT NULL,
	YEAR NUMBER(18,0) NOT NULL,
	MONTH NUMBER(18,0) NOT NULL,
	STR_COUNT NUMBER(18,0),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DIST_ID, YEAR, MONTH)
);
create or replace TABLE SDL_TH_JBP_MARKET_SHARE_DIST (
	MEASURE VARCHAR(100),
	CATEGORY VARCHAR(256),
	YEAR_MONTH NUMBER(18,0) NOT NULL,
	VALUE NUMBER(20,5),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_JBP_ROLLING_FORECAST (
	FISC_YR_PER NUMBER(18,0),
	FISC_WK NUMBER(18,0),
	CO_CD VARCHAR(4),
	VAL_TYPE VARCHAR(20),
	VRSN VARCHAR(20),
	MANUAL_TYP VARCHAR(20),
	CRNCY_KEY VARCHAR(5),
	SLS_ORG VARCHAR(4),
	DSTR_CHNL VARCHAR(4),
	DVSN VARCHAR(4),
	MATL_NUM VARCHAR(18),
	B1_MEGA_BRND VARCHAR(10),
	B2_BRND VARCHAR(10),
	B3_BASE_PROD VARCHAR(10),
	B4_VAR VARCHAR(10),
	B5_PUT_UP VARCHAR(10),
	OPER_GRP VARCHAR(18),
	FRANCHISE_GRP VARCHAR(20),
	FRANCHISE VARCHAR(20),
	PROD_FRAN VARCHAR(18),
	PROD_MAJOR VARCHAR(18),
	PROD_MINOR VARCHAR(18),
	SLS_EMP_CURR VARCHAR(20),
	CUST_NUM VARCHAR(10),
	LOCAL_CUST_GRP1 VARCHAR(20),
	LOCAL_CUST_GRP2 VARCHAR(20),
	LOCAL_CUST_GRP3 VARCHAR(20),
	LOCAL_CUST_GRP4 VARCHAR(20),
	LOCAL_CUST_GRP5 VARCHAR(20),
	LOCAL_CUST_GRP6 VARCHAR(20),
	SLS_TARGET NUMBER(20,5),
	QTY NUMBER(20,5),
	UNIT VARCHAR(10),
	FISC_YR_VAR VARCHAR(10),
	FISC_YR NUMBER(18,0),
	CTRY_KEY VARCHAR(5),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_MT_7_11 (
	PARTNER_CODE VARCHAR(100),
	PARTNER_NAME VARCHAR(100),
	PARTNER_GLN VARCHAR(20),
	PARTNER_INVENTORY_LOCATION VARCHAR(100),
	SUPPLIER_CODE VARCHAR(20),
	SUPPLIER_NAME VARCHAR(50),
	SUPPLIER_GLN VARCHAR(20),
	MESSAGE_DATE VARCHAR(20),
	INVENTORY_REPORT_DATE VARCHAR(20),
	DATE_TYPE VARCHAR(20),
	LINE_ITEM_NUMBER NUMBER(5,0),
	MATERIAL_NUMBER VARCHAR(20),
	EAN_ITEM_CODE VARCHAR(20),
	EAN_PACK_CODE VARCHAR(20),
	CUSTOMER_ITEM_CODE VARCHAR(20),
	INVENTORY_LOCATION VARCHAR(10),
	UNIT_OF_MEASURE VARCHAR(30),
	QTY_PER_PACK NUMBER(10,0),
	TOTAL_QTY_ONHAND NUMBER(15,0),
	ACTUAL_ONHAND_STOCK_QTY NUMBER(15,0),
	QTY_IN_TRANSIT NUMBER(15,0),
	SALES_QTY NUMBER(15,0),
	EXPECTED_SALES_QTY NUMBER(15,0),
	SHORT_SHIPPED_QTY NUMBER(15,0),
	ITEM_PRICE_TYPE VARCHAR(20),
	ITEM_PRICE NUMBER(15,3),
	ITEM_PRICE_UNIT VARCHAR(20),
	PRICE_CURRENCY VARCHAR(10),
	CRT_DTTM TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(14)
);
create or replace TABLE SDL_TH_MT_BIGC (
	REPORT_CODE VARCHAR(100),
	SUPPLIER VARCHAR(500),
	BUSINESS_FORMAT VARCHAR(500),
	COMPARE VARCHAR(500),
	STORE VARCHAR(500),
	TRANSACTION_DATE VARCHAR(20),
	LY_COMPARE_DATE VARCHAR(20),
	REPORT_DATE VARCHAR(20),
	DIVISION VARCHAR(500),
	DEPARTMENT VARCHAR(500),
	SUBDEPARTMENT VARCHAR(500),
	CLASS VARCHAR(500),
	SUBCLASS VARCHAR(500),
	BARCODE VARCHAR(500),
	ARTICLE VARCHAR(500),
	ARTICLE_NAME VARCHAR(500),
	BRAND VARCHAR(500),
	MODEL VARCHAR(500),
	SALE_AMT_TY_BAHT NUMBER(16,4),
	SALE_AMT_LY_BAHT NUMBER(16,4),
	SALE_AMT_VAR NUMBER(16,4),
	SALE_QTY_TY NUMBER(16,4),
	SALE_QTY_LY NUMBER(16,4),
	SALE_QTY_VAR NUMBER(16,4),
	STOCK_TY_BAHT NUMBER(16,4),
	STOCK_LY_BAHT NUMBER(16,4),
	STOCK_VAR NUMBER(16,4),
	STOCK_QTY_TY NUMBER(16,4),
	STOCK_QTY_LY NUMBER(16,4),
	STOCK_QTY_VAR NUMBER(16,4),
	DAY_ON_HAND_TY NUMBER(16,4),
	DAY_ON_HAND_LY NUMBER(16,4),
	DAY_ON_HAND_DIFF NUMBER(16,4),
	FILE_NAME VARCHAR(200),
	CRT_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE SDL_TH_MT_MAKRO (
	TRANSACTION_DATE VARCHAR(50),
	SUPPLIER_NUMBER VARCHAR(50),
	LOCATION_NUMBER VARCHAR(100),
	LOCATION_NAME VARCHAR(500),
	CLASS_NUMBER VARCHAR(100),
	SUBCLASS_NUMBER VARCHAR(100),
	ITEM_NUMBER VARCHAR(100),
	BARCODE VARCHAR(100),
	ITEM_DESC VARCHAR(500),
	EOH_QTY VARCHAR(50),
	ORDER_IN_TRANSIT_QTY VARCHAR(50),
	PACK_TYPE VARCHAR(50),
	MAKRO_UNIT VARCHAR(50),
	AVG_NET_SALES_QTY VARCHAR(50),
	NET_SALES_QTY_YTD VARCHAR(50),
	LAST_RECV_DT VARCHAR(50),
	LAST_SOLD_DT VARCHAR(50),
	STOCK_COVER_DAYS VARCHAR(20),
	NET_SALES_QTY_MTD VARCHAR(50),
	DAY_1 VARCHAR(50),
	DAY_2 VARCHAR(50),
	DAY_3 VARCHAR(50),
	DAY_4 VARCHAR(50),
	DAY_5 VARCHAR(50),
	DAY_6 VARCHAR(50),
	DAY_7 VARCHAR(50),
	DAY_8 VARCHAR(50),
	DAY_9 VARCHAR(50),
	DAY_10 VARCHAR(50),
	DAY_11 VARCHAR(50),
	DAY_12 VARCHAR(50),
	DAY_13 VARCHAR(50),
	DAY_14 VARCHAR(50),
	DAY_15 VARCHAR(50),
	DAY_16 VARCHAR(50),
	DAY_17 VARCHAR(50),
	DAY_18 VARCHAR(50),
	DAY_19 VARCHAR(50),
	DAY_20 VARCHAR(50),
	DAY_21 VARCHAR(50),
	DAY_22 VARCHAR(50),
	DAY_23 VARCHAR(50),
	DAY_24 VARCHAR(50),
	DAY_25 VARCHAR(50),
	DAY_26 VARCHAR(50),
	DAY_27 VARCHAR(50),
	DAY_28 VARCHAR(50),
	DAY_29 VARCHAR(50),
	DAY_30 VARCHAR(50),
	DAY_31 VARCHAR(50),
	FILE_NAME VARCHAR(100),
	CRTD_DTM TIMESTAMP_NTZ(9)
);

create or replace TABLE SDL_TH_MT_PRICE_DATA (
	COMPANY VARCHAR(50),
	DATE TIMESTAMP_NTZ(9),
	BRAND VARCHAR(50),
	MANUFACTURER VARCHAR(100),
	PRODUCT_NAME VARCHAR(400),
	SKU_ID VARCHAR(200),
	LIST_PRICE NUMBER(20,5),
	PRICE NUMBER(20,5),
	CATEGORY_JNJ VARCHAR(100),
	SUB_CATEGORY_JNJ VARCHAR(100),
	CATEGORY VARCHAR(200),
	SUB_CATEGORY VARCHAR(400),
	URL VARCHAR(400),
	REVIEW_SCORE NUMBER(20,5),
	REVIEW_QTY NUMBER(20,5),
	DISCOUNT_DEPTH NUMBER(20,5),
	SOURCE VARCHAR(20),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_MT_TOPS (
	PARTNER_CODE VARCHAR(100),
	PARTNER_NAME VARCHAR(100),
	PARTNER_GLN VARCHAR(20),
	PARTNER_INVENTORY_LOCATION VARCHAR(100),
	SUPPLIER_CODE VARCHAR(20),
	SUPPLIER_NAME VARCHAR(50),
	SUPPLIER_GLN VARCHAR(20),
	MESSAGE_DATE VARCHAR(20),
	INVENTORY_REPORT_DATE VARCHAR(20),
	DATE_TYPE VARCHAR(20),
	LINE_ITEM_NUMBER NUMBER(5,0),
	MATERIAL_NUMBER VARCHAR(20),
	EAN_ITEM_CODE VARCHAR(20),
	EAN_PACK_CODE VARCHAR(20),
	CUSTOMER_ITEM_CODE VARCHAR(20),
	INVENTORY_LOCATION VARCHAR(20),
	UNIT_OF_MEASURE VARCHAR(30),
	QTY_PER_PACK NUMBER(10,0),
	TOTAL_QTY_ONHAND NUMBER(15,0),
	ACTUAL_ONHAND_STOCK_QTY NUMBER(15,0),
	QTY_IN_TRANSIT NUMBER(15,0),
	SALES_QTY NUMBER(15,0),
	EXPECTED_SALES_QTY NUMBER(15,0),
	SHORT_SHIPPED_QTY NUMBER(15,0),
	ITEM_PRICE_TYPE VARCHAR(20),
	ITEM_PRICE NUMBER(15,3),
	ITEM_PRICE_UNIT VARCHAR(20),
	PRICE_CURRENCY VARCHAR(10),
	CRT_DTTM TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100),
	RUN_ID VARCHAR(14)
);
create or replace TABLE SDL_TH_MT_WATSONS (
	DIV VARCHAR(50),
	DEPT VARCHAR(50),
	CLASS VARCHAR(50),
	SUBCLASS VARCHAR(100),
	ITEM VARCHAR(100),
	ITEM_DESC VARCHAR(200),
	NON_SLOW VARCHAR(100),
	NON_SLOW2 VARCHAR(100),
	FINANCE_STATUS VARCHAR(200),
	CREATE_DATETIME VARCHAR(50),
	PRIM_SUPPLIER VARCHAR(100),
	OLD_SUPP_NO VARCHAR(100),
	SUPP_DESC VARCHAR(200),
	LEAD_TIME VARCHAR(50),
	UNIT_COST VARCHAR(100),
	UNIT_RETAIL_ZONE5 VARCHAR(100),
	ITEM_STATUS VARCHAR(100),
	STATUS_WH VARCHAR(100),
	STATUS_WH_UPDATE_DATE VARCHAR(50),
	STATUS_STORE VARCHAR(100),
	STATUS_STORE_UPDATE_DATE VARCHAR(50),
	STATUS_XDOCK VARCHAR(50),
	STATUS_XDOCK_UPDATE_DATE VARCHAR(50),
	SOURCE_METHOD VARCHAR(100),
	SOURCE_WH VARCHAR(100),
	POG VARCHAR(100),
	PRODUCT_TYPE VARCHAR(200),
	LABEL_UDA VARCHAR(100),
	BRAND VARCHAR(100),
	ITEM_TYPE VARCHAR(100),
	RETURN_POLICY VARCHAR(100),
	RETURN_TYPE VARCHAR(100),
	WH_WAC VARCHAR(100),
	IN_TAX VARCHAR(50),
	TAX_RATE VARCHAR(50),
	STOCK_CAT VARCHAR(50),
	ORDER_FLAG VARCHAR(50),
	NEW_ITEM_13WEEK VARCHAR(50),
	DEACTIVATE_DATE VARCHAR(50),
	WH_ON_ORDER VARCHAR(50),
	FIRST_RCV VARCHAR(50),
	PROMO_MONTH VARCHAR(50),
	SALES_TW VARCHAR(50),
	NET_AMT VARCHAR(50),
	NET_COST VARCHAR(50),
	SALE_AVG_QTY_13WEEKS NUMBER(16,4),
	SALE_AVG_AMT_13WEEKS NUMBER(16,4),
	SALE_AVG_COST13WEEKS NUMBER(16,4),
	NET_QTY_YTD NUMBER(16,4),
	NET_AMT_YTD NUMBER(16,4),
	NET_COST_YTD NUMBER(16,4),
	TURN_WK VARCHAR(100),
	WH_SOH NUMBER(16,4),
	STORE_TOTAL_STOCK NUMBER(16,4),
	TOTAL_STOCK_QTY NUMBER(16,4),
	WH_STOCK_AMT NUMBER(16,4),
	STORE_TOTAL_STOCK_AMT NUMBER(16,4),
	TOTAL_STOCK_XVAT NUMBER(16,4),
	PRO2 VARCHAR(50),
	DISC VARCHAR(50),
	PRO22 VARCHAR(50),
	PRO2_PERT_DISC VARCHAR(50),
	FIRST_DATE_SMS VARCHAR(50),
	AGING_SMS VARCHAR(50),
	GROUP_W VARCHAR(50),
	WIN VARCHAR(50),
	POG_2 VARCHAR(50),
	FILE_NAME VARCHAR(100),
	DATE VARCHAR(10)
);

create or replace TABLE SDL_TH_PRODUCTGROUPING (
	PROD_CD VARCHAR(25) NOT NULL,
	PROD_GRP VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (PROD_CD)
);
create or replace TABLE SDL_TH_SFMC_BOUNCE_DATA (
	OYB_ACCOUNT_ID VARCHAR(20),
	JOB_ID VARCHAR(20),
	LIST_ID VARCHAR(10),
	BATCH_ID VARCHAR(10),
	SUBSCRIBER_ID VARCHAR(20),
	SUBSCRIBER_KEY VARCHAR(100),
	EVENT_DATE TIMESTAMP_NTZ(9),
	IS_UNIQUE VARCHAR(10),
	DOMAIN VARCHAR(50),
	BOUNCE_CATEGORY_ID VARCHAR(10),
	BOUNCE_CATEGORY VARCHAR(30),
	BOUNCE_SUBCATEGORY_ID VARCHAR(10),
	BOUNCE_SUBCATEGORY VARCHAR(30),
	BOUNCE_TYPE_ID VARCHAR(10),
	BOUNCE_TYPE VARCHAR(30),
	SMTP_BOUNCE_REASON VARCHAR(1000),
	SMTP_MESSAGE VARCHAR(200),
	SMTP_CODE VARCHAR(10),
	TRIGGERER_SEND_DEFINITION_OBJECT_ID VARCHAR(50),
	TRIGGERED_SEND_CUSTOMER_KEY VARCHAR(10),
	EMAIL_SUBJECT VARCHAR(200),
	BCC_EMAIL VARCHAR(50),
	EMAIL_NAME VARCHAR(100),
	EMAIL_ID VARCHAR(20),
	EMAIL_ADDRESS VARCHAR(100),
	FILE_NAME VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_CHILDREN_DATA (
	PARENT_KEY VARCHAR(200),
	CHILD_NM VARCHAR(200),
	CHILD_BIRTH_MNTH VARCHAR(100),
	CHILD_BIRTH_YEAR VARCHAR(10),
	CHILD_GENDER VARCHAR(100),
	CHILD_NUMBER VARCHAR(30),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_CLICK_DATA (
	OYB_ACCOUNT_ID VARCHAR(50),
	JOB_ID VARCHAR(50),
	LIST_ID VARCHAR(50),
	BATCH_ID VARCHAR(50),
	SUBSCRIBER_ID VARCHAR(50),
	SUBSCRIBER_KEY VARCHAR(100),
	EVENT_DATE TIMESTAMP_NTZ(9),
	DOMAIN VARCHAR(50),
	URL VARCHAR(1000),
	LINK_NAME VARCHAR(200),
	LINK_CONTENT VARCHAR(1000),
	IS_UNIQUE VARCHAR(10),
	EMAIL_NAME VARCHAR(100),
	EMAIL_SUBJECT VARCHAR(200),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_COMPLAINT_DATA (
	OYB_ACCOUNT_ID VARCHAR(50),
	JOB_ID VARCHAR(50),
	LIST_ID VARCHAR(50),
	BATCH_ID VARCHAR(50),
	SUBSCRIBER_ID VARCHAR(50),
	SUBSCRIBER_KEY VARCHAR(100),
	EVENT_DATE TIMESTAMP_NTZ(9),
	IS_UNIQUE VARCHAR(10),
	DOMAIN VARCHAR(50),
	EMAIL_SUBJECT VARCHAR(200),
	EMAIL_NAME VARCHAR(100),
	EMAIL_ID VARCHAR(100),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_CONSUMER_MASTER (
	FIRST_NAME VARCHAR(200),
	LAST_NAME VARCHAR(200),
	MOBILE_NUM VARCHAR(30),
	MOBILE_CNTRY_CD VARCHAR(10),
	BIRTHDAY_MNTH VARCHAR(100),
	BIRTHDAY_YEAR VARCHAR(10),
	ADDRESS_1 VARCHAR(255),
	ADDRESS_2 VARCHAR(255),
	ADDRESS_CITY VARCHAR(100),
	ADDRESS_ZIPCODE VARCHAR(30),
	SUBSCRIBER_KEY VARCHAR(100),
	WEBSITE_UNIQUE_ID VARCHAR(150),
	SOURCE VARCHAR(100),
	MEDIUM VARCHAR(100),
	BRAND VARCHAR(200),
	ADDRESS_CNTRY VARCHAR(100),
	CAMPAIGN_ID VARCHAR(100),
	CREATED_DATE TIMESTAMP_NTZ(9),
	UPDATED_DATE TIMESTAMP_NTZ(9),
	UNSUBSCRIBE_DATE TIMESTAMP_NTZ(9),
	EMAIL VARCHAR(100),
	FULL_NAME VARCHAR(200),
	LAST_LOGON_TIME TIMESTAMP_NTZ(9),
	REMAINING_POINTS NUMBER(10,4),
	REDEEMED_POINTS NUMBER(10,4),
	TOTAL_POINTS NUMBER(10,4),
	GENDER VARCHAR(20),
	LINE_ID VARCHAR(50),
	LINE_NAME VARCHAR(200),
	LINE_EMAIL VARCHAR(100),
	LINE_CHANNEL_ID VARCHAR(50),
	ADDRESS_REGION VARCHAR(100),
	TIER VARCHAR(100),
	OPT_IN_FOR_COMMUNICATION VARCHAR(100),
	HAVE_KID VARCHAR(20),
	AGE NUMBER(18,0),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_CONSUMER_MASTER_ADDITIONAL (
	SUBSCRIBER_KEY VARCHAR(100),
	ATTRIBUTE_NAME VARCHAR(100),
	ATTRIBUTE_VALUE VARCHAR(100),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_OPEN_DATA (
	OYB_ACCOUNT_ID VARCHAR(50),
	JOB_ID VARCHAR(50),
	LIST_ID VARCHAR(30),
	BATCH_ID VARCHAR(30),
	SUBSCRIBER_ID VARCHAR(50),
	SUBSCRIBER_KEY VARCHAR(100),
	EMAIL_NAME VARCHAR(100),
	EMAIL_SUBJECT VARCHAR(200),
	BCC_EMAIL VARCHAR(50),
	EMAIL_ID VARCHAR(30),
	EVENT_DATE TIMESTAMP_NTZ(9),
	DOMAIN VARCHAR(50),
	IS_UNIQUE VARCHAR(10),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_SENT_DATA (
	OYB_ACCOUNT_ID VARCHAR(50),
	JOB_ID VARCHAR(50),
	LIST_ID VARCHAR(30),
	BATCH_ID VARCHAR(30),
	SUBSCRIBER_ID VARCHAR(50),
	SUBSCRIBER_KEY VARCHAR(100),
	EVENT_DATE TIMESTAMP_NTZ(9),
	DOMAIN VARCHAR(50),
	TRIGGERER_SEND_DEFINITION_OBJECT_ID VARCHAR(50),
	TRIGGERED_SEND_CUSTOMER_KEY VARCHAR(10),
	EMAIL_NAME VARCHAR(100),
	EMAIL_SUBJECT VARCHAR(200),
	EMAIL_ID VARCHAR(30),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_SFMC_UNSUBSCRIBE_DATA (
	OYB_ACCOUNT_ID VARCHAR(50),
	JOB_ID VARCHAR(50),
	LIST_ID VARCHAR(30),
	BATCH_ID VARCHAR(30),
	SUBSCRIBER_ID VARCHAR(50),
	SUBSCRIBER_KEY VARCHAR(100),
	EVENT_DATE TIMESTAMP_NTZ(9),
	DOMAIN VARCHAR(50),
	EMAIL_NAME VARCHAR(100),
	EMAIL_SUBJECT VARCHAR(200),
	EMAIL_ID VARCHAR(30),
	IS_UNIQUE VARCHAR(10),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);
create or replace TABLE SDL_TH_TARGET_DISTRIBUTION (
	DSTRBTR_ID VARCHAR(10),
	PERIOD VARCHAR(6),
	TARGET NUMBER(18,0),
	UPDT_DATE TIMESTAMP_NTZ(9),
	PROD_NM VARCHAR(50),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE SDL_TH_TARGET_SALES (
	DSTRBTR_ID VARCHAR(10) NOT NULL,
	SLS_OFFICE VARCHAR(10) NOT NULL,
	SLS_GRP VARCHAR(10) NOT NULL,
	TARGET NUMBER(18,0),
	PERIOD VARCHAR(6) NOT NULL,
	TGT_DATE TIMESTAMP_NTZ(9),
	CRT_DATE TIMESTAMP_NTZ(9),
	CDL_DTTM VARCHAR(255),
	CURR_DATE TIMESTAMP_NTZ(9),
	primary key (DSTRBTR_ID, SLS_OFFICE, SLS_GRP, PERIOD)
);
create or replace TABLE SDL_TH_TESCO_TRANSDATA (
	CREATION_DATE VARCHAR(50),
	SUPPLIER_ID VARCHAR(20) NOT NULL,
	SUPPLIER_NAME VARCHAR(300),
	WAREHOUSE VARCHAR(50) NOT NULL,
	DELIVERY_POINT_NAME VARCHAR(300),
	IR_DATE DATE NOT NULL,
	EANSKU VARCHAR(100),
	ARTICLE_ID FLOAT,
	SPN VARCHAR(50),
	ARTICLE_NAME VARCHAR(300),
	STOCK FLOAT,
	SALES FLOAT,
	SALES_AMOUNT FLOAT,
	FILE_NAME VARCHAR(200),
	FOLDER_NAME VARCHAR(100)
);



create or replace TABLE XML_DATA (
	SRCDATA VARIANT
);
