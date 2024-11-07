create or replace table PROD_DNA_CORE.aspedw_integration.v_rpt_copa_bkp_07112024 clone PROD_DNA_CORE.aspedw_integration.v_rpt_copa;
 
create or replace view PROD_DNA_CORE.aspedw_access.v_rpt_copa_bkp_07112024 as (
 select
        prev_fisc_yr_per as "prev_fisc_yr_per",
        latest_date as "latest_date",
        latest_fisc_yrmnth as "latest_fisc_yrmnth",
        fisc_yr as "fisc_yr",
        fisc_yr_per as "fisc_yr_per",
        fisc_day as "fisc_day",
        ctry_nm as "ctry_nm",
         "cluster",
        obj_crncy_co_obj as "obj_crncy_co_obj",
        "b1 mega-brand",
        "b2 brand",
        "b3 base product",
        "b4 variant",
        "b5 put-up",
        "prod h1 operating group",
        "prod h2 franchise group",
        "prod h3 franchise",
        "prod h4 product franchise",
        "prod h5 product major",
        "prod h6 product minor",
        "parent customer",
        banner as "banner",
        "banner format",
        channel as "channel",
        "go to model",
        "sub channel",
        "retail_env",
        nts_usd as "nts_usd",
        nts_lcy as "nts_lcy",
        gts_usd as "gts_usd",
        gts_lcy as "gts_lcy",
        eq_usd as "eq_usd",
        eq_lcy as "eq_lcy",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        nts_qty as "nts_qty",
        gts_qty as "gts_qty",
        eq_qty as "eq_qty",
        ord_pc_qty as "ord_pc_qty",
        unspp_qty as "unspp_qty",
        cust_num as "cust_num"
    from PROD_DNA_CORE.aspedw_integration.v_rpt_copa_bkp_07112024 );
	
create or replace TABLE PROD_DNA_CORE.IDNITG_INTEGRATION.ITG_CUSTOMERPL_SEGMENTATION_DIM (
	CODE VARCHAR(500),
	CUSTOMER_SEGMENT_LEVEL1 VARCHAR(500),
	CUSTOMER_SEGMENT_LEVEL2 VARCHAR(500)
);	
