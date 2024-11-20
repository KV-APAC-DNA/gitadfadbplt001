create or replace table PROD_dna_load.indsdl_raw.sdl_va_page_class(
FRANCHISE VARCHAR(50),
VA_NAME VARCHAR(100),
PAGE_NAME VARCHAR(50),
SUB_GROUP VARCHAR(50),
GROUP_NAME VARCHAR(50),
BRAND VARCHAR(50)
);

COPY INTO PROD_DNA_LOAD.INDSDL_RAW.sdl_va_page_class
FROM (
	SELECT *
	FROM '@PROD_DNA_LOAD.INDSDL_RAW.PROD_LOAD_STAGE_ADLS'
)
FILES = ('prd/Hcp_Edetailing/VA Page Classifications v2.1.csv') 
file_format= (TYPE=CSV SKIP_HEADER = 1)
ON_ERROR=ABORT_STATEMENT ;