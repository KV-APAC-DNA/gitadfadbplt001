CREATE OR REPLACE TABLE PCFSDL_RAW.SDL_CHW_ECOMM_DATA (		--// CREATE OR REPLACE TABLE PCFSDL_RAW.sdl_chw_ecomm_data (
    pfc varchar(20),		--//  ENCODE zstd // character varying
    skuname varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
    owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    periodid varchar(10),		--//  ENCODE zstd // character varying
    sales_online varchar(10),		--//  ENCODE zstd // character varying
    unit_online varchar(10),		--//  ENCODE zstd // character varying
    week_end varchar(20),		--//  ENCODE zstd // character varying
    file_name varchar(50),		--//  ENCODE zstd // character varying
    crt_dttm timestamp without time zone DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--//     crt_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE OR REPLACE TABLE PCFSDL_RAW.SDL_NATIONAL_ECOMM_DATA (		--// CREATE OR REPLACE TABLE PCFSDL_RAW.sdl_national_ecomm_data (
    pfc varchar(20),		--//  ENCODE zstd // character varying
    skuname varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
    owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    periodid varchar(10),		--//  ENCODE zstd // character varying
    sales_online varchar(10),		--//  ENCODE zstd // character varying
    unit_online varchar(10),		--//  ENCODE zstd // character varying
    week_end varchar(20),		--//  ENCODE zstd // character varying
    file_name varchar(50),		--//  ENCODE zstd // character varying
    crt_dttm timestamp without time zone DEFAULT ('CURRENT_TIMESTAMP()'::text)::timestamp without time zone		--//     crt_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64 //  ENCODE az64
)
;		--// DISTSTYLE AUTO;
