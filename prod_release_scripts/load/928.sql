create or replace TABLE prod_dna_load.ntasdl_raw.sdl_tw_pos_cosmed_store_inventory (
	PRODUCT_CODE VARCHAR(50),
	PRODUCT_NAME VARCHAR(255),
	INVENTORY_QTY NUMBER(18,2),
	AVG_SALES_QTY NUMBER(18,2),
	INVENTORY_TURNOVER_DAYS NUMBER(18,2),
	FILE_NAME VARCHAR(50),
	MNTH_ID VARCHAR(50),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);

create or replace TABLE prod_dna_load.ntasdl_raw.sdl_tw_pos_cosmed_dc_inventory (
	BARCODE VARCHAR(255),
	PRODUCT_CODE VARCHAR(255),
	PRODUCT_NAME VARCHAR(255),
	NORTH_DC_STATUS VARCHAR(255),
	SOUTH_DC_STATUS VARCHAR(255),
	XIYUAN_DC_STATUS VARCHAR(255),
	IN_TRANSIT_QTY NUMBER(18,2),
	INVENTORY_QTY NUMBER(18,2),
	JM_NORTH_DC_QTY NUMBER(18,2),
	JM_SOUTH_DC_QTY NUMBER(18,2),
	JM_XIYUAN_DC_QTY NUMBER(18,2),
	INVENTORY_DATE VARCHAR(30),
	SUPPLIER_CODE VARCHAR(255),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM VARCHAR(255)
);