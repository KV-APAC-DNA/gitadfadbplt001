CREATE OR REPLACE TABLE MYSSDL_RAW.SDL_MDS_MY_PS_MSL (		--// CREATE TABLE os_sdl.sdl_mds_my_ps_msl (
    id numeric(18,0),		--//  ENCODE az64 // integer
    muid varchar(36),		--//  ENCODE lzo // character varying
    versionname varchar(100),		--//  ENCODE lzo // character varying
    versionnumber numeric(18,0),		--//  ENCODE az64 // integer
    version_id numeric(18,0),		--//  ENCODE az64 // integer
    versionflag varchar(100),		--//  ENCODE lzo // character varying
    name varchar(500),		--//  ENCODE lzo // character varying
    code varchar(500),		--//  ENCODE lzo // character varying
    changetrackingmask numeric(18,0),		--//  ENCODE az64 // integer
    ean varchar(200),		--//  ENCODE lzo // character varying
    product_name varchar(510),		--//  ENCODE lzo // character varying
    enterdatetime timestamp without time zone,		--//  ENCODE az64
    enterusername varchar(200),		--//  ENCODE lzo // character varying
    enterversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    lastchgdatetime timestamp without time zone,		--//  ENCODE az64
    lastchgusername varchar(200),		--//  ENCODE lzo // character varying
    lastchgversionnumber numeric(18,0),		--//  ENCODE az64 // integer
    validationstatus varchar(500),		--//  ENCODE lzo // character varying
    valid_from timestamp without time zone,		--//  ENCODE az64
    valid_to timestamp without time zone		--//  ENCODE az64
)
;	
