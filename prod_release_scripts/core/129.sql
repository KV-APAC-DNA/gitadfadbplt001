create or replace TABLE PHLITG_INTEGRATION.ITG_PH_DYNA_PRODUCT_DIM (
	YEARMONTH VARCHAR(20),
	CUST_CD VARCHAR(20),
	ITEM_CD VARCHAR(30),
	ITEM_NM VARCHAR(150),
	SAP_ITEM_CD VARCHAR(30),
	CUST_SKU_GRP VARCHAR(150),
	CUST_CONV_FACTOR VARCHAR(20),
	CUST_ITEM_PRC NUMBER(10,2),
	LST_PERIOD VARCHAR(10),
	EARLY_BK_PERIOD VARCHAR(10),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);



CREATE OR REPLACE TABLE PHLWKS_INTEGRATION.WKS_PH_POS_DYNA_SALES (		--// CREATE TABLE os_wks.wks_ph_pos_dyna_sales (
    mnth_id varchar(255),		--//  ENCODE lzo // character varying
    sls_area varchar(255),		--//  ENCODE lzo // character varying
    plant varchar(255),		--//  ENCODE lzo // character varying
    cust_id varchar(255) ,		--// distkey //  ENCODE lzo // character varying
    old_cust_id varchar(255),		--//  ENCODE lzo // character varying
    cust_nm varchar(255),		--//  ENCODE lzo // character varying
    chnl varchar(255),		--//  ENCODE lzo // character varying
    sls_off varchar(255),		--//  ENCODE lzo // character varying
    sls_grp varchar(255),		--//  ENCODE lzo // character varying
    address varchar(255),		--//  ENCODE lzo // character varying
    city varchar(255),		--//  ENCODE lzo // character varying
    postal_cd varchar(255),		--//  ENCODE lzo // character varying
    dsm varchar(255),		--//  ENCODE lzo // character varying
    sman varchar(255),		--//  ENCODE lzo // character varying
    prin varchar(255),		--//  ENCODE lzo // character varying
    principal varchar(255),		--//  ENCODE lzo // character varying
    matl_grp varchar(255),		--//  ENCODE lzo // character varying
    matl_sub_grp varchar(255),		--//  ENCODE lzo // character varying
    brand varchar(255),		--//  ENCODE lzo // character varying
    uom_conv varchar(255),		--//  ENCODE lzo // character varying
    matl_num varchar(255),		--//  ENCODE lzo // character varying
    old_matl_num varchar(255),		--//  ENCODE lzo // character varying
    matl_desc varchar(255),		--//  ENCODE lzo // character varying
    sjan2022 varchar(255),		--//  ENCODE lzo // character varying
    sfeb2022 varchar(255),		--//  ENCODE lzo // character varying
    smar2022 varchar(255),		--//  ENCODE lzo // character varying
    sapr2022 varchar(255),		--//  ENCODE lzo // character varying
    smay2022 varchar(255),		--//  ENCODE lzo // character varying
    sjun2022 varchar(255),		--//  ENCODE lzo // character varying
    sjul2022 varchar(255),		--//  ENCODE lzo // character varying
    saug2022 varchar(255),		--//  ENCODE lzo // character varying
    ssep2022 varchar(255),		--//  ENCODE lzo // character varying
    soct2022 varchar(255),		--//  ENCODE lzo // character varying
    snov2022 varchar(255),		--//  ENCODE lzo // character varying
    sdec2022 varchar(255),		--//  ENCODE lzo // character varying
    sjan2023 varchar(255),		--//  ENCODE lzo // character varying
    sfeb2023 varchar(255),		--//  ENCODE lzo // character varying
    smar2023 varchar(255),		--//  ENCODE lzo // character varying
    sapr2023 varchar(255),		--//  ENCODE lzo // character varying
    smay2023 varchar(255),		--//  ENCODE lzo // character varying
    sjun2023 varchar(255),		--//  ENCODE lzo // character varying
    sjul2023 varchar(255),		--//  ENCODE lzo // character varying
    saug2023 varchar(255),		--//  ENCODE lzo // character varying
    ssep2023 varchar(255),		--//  ENCODE lzo // character varying
    soct2023 varchar(255),		--//  ENCODE lzo // character varying
    snov2023 varchar(255),		--//  ENCODE lzo // character varying
    sdec2023 varchar(255),		--//  ENCODE lzo // character varying
    qjan2022 varchar(255),		--//  ENCODE lzo // character varying
    qfeb2022 varchar(255),		--//  ENCODE lzo // character varying
    qmar2022 varchar(255),		--//  ENCODE lzo // character varying
    qapr2022 varchar(255),		--//  ENCODE lzo // character varying
    qmay2022 varchar(255),		--//  ENCODE lzo // character varying
    qjun2022 varchar(255),		--//  ENCODE lzo // character varying
    qjul2022 varchar(255),		--//  ENCODE lzo // character varying
    qaug2022 varchar(255),		--//  ENCODE lzo // character varying
    qsep2022 varchar(255),		--//  ENCODE lzo // character varying
    qoct2022 varchar(255),		--//  ENCODE lzo // character varying
    qnov2022 varchar(255),		--//  ENCODE lzo // character varying
    qdec2022 varchar(255),		--//  ENCODE lzo // character varying
    qjan2023 varchar(255),		--//  ENCODE lzo // character varying
    qfeb2023 varchar(255),		--//  ENCODE lzo // character varying
    qmar2023 varchar(255),		--//  ENCODE lzo // character varying
    qapr2023 varchar(255),		--//  ENCODE lzo // character varying
    qmay2023 varchar(255),		--//  ENCODE lzo // character varying
    qjun2023 varchar(255),		--//  ENCODE lzo // character varying
    qjul2023 varchar(255),		--//  ENCODE lzo // character varying
    qaug2023 varchar(255),		--//  ENCODE lzo // character varying
    qsep2023 varchar(255),		--//  ENCODE lzo // character varying
    qoct2023 varchar(255),		--//  ENCODE lzo // character varying
    qnov2023 varchar(255),		--//  ENCODE lzo // character varying
    qdec2023 varchar(255)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE AUTO;
