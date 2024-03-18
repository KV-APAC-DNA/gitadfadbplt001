CREATE OR REPLACE TABLE PCFSDL_RAW.sdl_chw_ecomm_data (
    pfc character varying(20) ENCODE zstd,
    skuname character varying(100) ENCODE zstd,
    nec1_desc character varying(100) ENCODE zstd,
    nec2_desc character varying(100) ENCODE zstd,
    nec3_desc character varying(100) ENCODE zstd,
    brand character varying(50) ENCODE zstd,
    owner character varying(20) ENCODE zstd,
    manufacturer character varying(50) ENCODE zstd,
    category character varying(50) ENCODE zstd,
    mat_year character varying(10) ENCODE zstd,
    periodid character varying(10) ENCODE zstd,
    sales_online character varying(10) ENCODE zstd,
    unit_online character varying(10) ENCODE zstd,
    week_end character varying(20) ENCODE zstd,
    file_name character varying(50) ENCODE zstd,
    crt_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
)
DISTSTYLE AUTO;

CREATE OR REPLACE TABLE PCFSDL_RAW.sdl_national_ecomm_data (
    pfc character varying(20) ENCODE zstd,
    skuname character varying(100) ENCODE zstd,
    nec1_desc character varying(100) ENCODE zstd,
    nec2_desc character varying(100) ENCODE zstd,
    nec3_desc character varying(100) ENCODE zstd,
    brand character varying(50) ENCODE zstd,
    owner character varying(20) ENCODE zstd,
    manufacturer character varying(50) ENCODE zstd,
    category character varying(50) ENCODE zstd,
    mat_year character varying(10) ENCODE zstd,
    periodid character varying(10) ENCODE zstd,
    sales_online character varying(10) ENCODE zstd,
    unit_online character varying(10) ENCODE zstd,
    week_end character varying(20) ENCODE zstd,
    file_name character varying(50) ENCODE zstd,
    crt_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
)
DISTSTYLE AUTO;
