delete from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE WHERE country_name='China Selfcare' AND 
mnth_id>= (case when (select param_value from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG where code='base_load_cn_otc')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG where code='base_load_cn_otc')::integer)), 'YYYYMM') END);

insert into PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE (

with wks_rpt_regional_cn_sc_sellout_offtake_npd as
(
    select * from PROD_DNA_CORE.chnwks_integration.wks_rpt_regional_cn_sc_sellout_offtake_npd
),
sdl_raw_sap_bw_price_list as
(
    select * from PROD_DNA_CORE.ASPITG_INTEGRATION.SDL_RAW_SAP_BW_PRICE_LIST
),
edw_sales_org_dim as
(
    select * from PROD_DNA_CORE.aspedw_integration.edw_sales_org_dim
),
itg_mds_ap_customer360_config as (
	select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
),
edw_list_price as
(
    select * from PROD_DNA_CORE.aspedw_integration.edw_list_price
)

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        'NA' AS channel,
        main.crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        main.updt_dttm::timestamp_ntz(9) as updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned 
        FROM wks_rpt_regional_cn_sc_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'CN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from wks_rpt_regional_cn_sc_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer
);
