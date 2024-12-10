create or replace TRANSIENT TABLE PROD_DNA_CORE.THAWKS_INTEGRATION.TRATBL_sdl_th_watsons_weekly_pos__duplicate_test (
	FAILURE_REASON VARCHAR(25),
	FILE_NAME VARCHAR(255),
	Product_ID VARCHAR(100),
	year_month VARCHAR(50),
	year_week VARCHAR(50),
    store_format VARCHAR(200),
    store_segment VARCHAR(200)
);

create or replace TRANSIENT TABLE PROD_DNA_CORE.THAWKS_INTEGRATION.TRATBL_SDL_TH_MT_PRICE_DATA__NULL_TEST (
	FILE_NAME VARCHAR(255),
	FAILURE_REASON VARCHAR(24),
	Product_ID VARCHAR(100),
	year_month VARCHAR(50),
	year_week VARCHAR(50),
    store_format VARCHAR(200),
    store_segment VARCHAR(200)
);
