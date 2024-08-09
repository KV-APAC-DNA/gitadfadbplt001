create or replace TABLE THASDL_RAW.SDL_POP6_TH_RIR_DATA_TEST (
	VISIT_ID VARCHAR(255),
    photo varchar(500),
    related_attribute varchar(255),
	SKU_ID VARCHAR(255),
	SKU VARCHAR(255),
	layer NUMBER(18,0),
	total_layer NUMBER(18,0),
    facing_of_this_layer NUMBER(18,0),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);
