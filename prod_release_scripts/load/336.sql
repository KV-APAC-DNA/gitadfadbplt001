UPDATE meta_raw.parameters
set PARAMETER_VALUE = 'WEEKLY'
WHERE PARAMETER_ID = 7079;

CREATE TABLE IDNSDL_RAW.SDL_MDS_ID_LAV_MCS_LIST (		--// CREATE TABLE IDNSDL_RAW.sdl_mds_id_lav_mcs_list (
    id numeric(18,0),		--//  ENCODE az64 // integer
    muid varchar(36),		--//  ENCODE lzo // character varying
    versionname varchar(100),		--//  ENCODE lzo // character varying
    versionnumber numeric(18,0),		--//  ENCODE az64 // integer
    version_id numeric(18,0),		--//  ENCODE az64 // integer
    versionflag varchar(100),		--//  ENCODE lzo // character varying
    name varchar(500),		--//  ENCODE lzo // character varying
    code varchar(500),		--//  ENCODE lzo // character varying
    changetrackingmask numeric(18,0),		--//  ENCODE az64 // integer
    tiering varchar(200),		--//  ENCODE lzo // character varying
    sku varchar(300),		--//  ENCODE lzo // character varying
    year varchar(200),		--//  ENCODE lzo // character varying
    january varchar(200),		--//  ENCODE lzo // character varying
    february varchar(200),		--//  ENCODE lzo // character varying
    march varchar(200),		--//  ENCODE lzo // character varying
    april varchar(200),		--//  ENCODE lzo // character varying
    may varchar(200),		--//  ENCODE lzo // character varying
    june varchar(200),		--//  ENCODE lzo // character varying
    july varchar(200),		--//  ENCODE lzo // character varying
    august varchar(200),		--//  ENCODE lzo // character varying
    september varchar(200),		--//  ENCODE lzo // character varying
    october varchar(200),		--//  ENCODE lzo // character varying
    november varchar(200),		--//  ENCODE lzo // character varying
    december varchar(200),		--//  ENCODE lzo // character varying
    enterdatetime timestamp without time zone,		--//  ENCODE az64
    enterusername varchar(200),		--//  ENCODE lzo // character varying
    enterversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    lastchgdatetime timestamp without time zone,		--//  ENCODE az64
    lastchgusername varchar(200),		--//  ENCODE lzo // character varying
    lastchgversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    validationstatus varchar(500),		--//  ENCODE lzo // character varying
    rownum numeric(38,0)		--//  ENCODE az64 // bigint
)
;		--// DISTSTYLE AUTO;




CREATE TABLE IDNSDL_RAW.SDL_MDS_ID_LAV_PRODUCT_HIERARCHY (		--// CREATE TABLE IDNSDL_RAW.sdl_mds_id_lav_product_hierarchy (
    id numeric(18,0),		--//  ENCODE az64 // integer
    muid varchar(36),		--//  ENCODE lzo // character varying
    versionname varchar(100),		--//  ENCODE lzo // character varying
    versionnumber numeric(18,0),		--//  ENCODE az64 // integer
    version_id numeric(18,0),		--//  ENCODE az64 // integer
    versionflag varchar(100),		--//  ENCODE lzo // character varying
    name varchar(500),		--//  ENCODE lzo // character varying
    code varchar(500),		--//  ENCODE lzo // character varying
    changetrackingmask numeric(18,0),		--//  ENCODE az64 // integer
    sap_code varchar(200),		--//  ENCODE lzo // character varying
    sku_description varchar(500),		--//  ENCODE lzo // character varying
    franchise varchar(200),		--//  ENCODE lzo // character varying
    brand varchar(200),		--//  ENCODE lzo // character varying
    variant1 varchar(200),		--//  ENCODE lzo // character varying
    variant2 varchar(200),		--//  ENCODE lzo // character varying
    variant3 varchar(200),		--//  ENCODE lzo // character varying
    status varchar(200),		--//  ENCODE lzo // character varying
    put_up varchar(200),		--//  ENCODE lzo // character varying
    uom varchar(200),		--//  ENCODE lzo // character varying
    sap_upgrade varchar(200),		--//  ENCODE lzo // character varying
    sku_description2 varchar(500),		--//  ENCODE lzo // character varying
    price varchar(200),		--//  ENCODE lzo // character varying
    sku_class varchar(200),		--//  ENCODE lzo // character varying
    sap_code_mapping varchar(200),		--//  ENCODE lzo // character varying
    sku_description3 varchar(500),		--//  ENCODE lzo // character varying
    enterdatetime timestamp without time zone,		--//  ENCODE az64
    enterusername varchar(200),		--//  ENCODE lzo // character varying
    enterversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    lastchgdatetime timestamp without time zone,		--//  ENCODE az64
    lastchgusername varchar(200),		--//  ENCODE lzo // character varying
    lastchgversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    validationstatus varchar(500),		--//  ENCODE lzo // character varying
    effective_from varchar(200),		--//  ENCODE lzo // character varying
    effective_to varchar(200)		--//  ENCODE lzo // character varying
)
;		--// DISTSTYLE AUTO;
