delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE (
with edw_rpt_regional_sellout_offtake as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
),
edw_vw_cal_retail_excellence_dim as (
    select * from prod_dna_core.aspedw_integration.v_edw_vw_cal_Retail_excellence_dim
),
wks_tw_360_sellout_mapped_sku_cd as (
	select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_360_SELLOUT_MAPPED_SKU_CD
)

SELECT CNTRY_CD,
       MD5(nvl (SOLDTO_CODE,'stc') ||nvl (DISTRIBUTOR_CODE,'dc') ||nvl (DISTRIBUTOR_NAME,'dn') ||nvl (STORE_CODE,'sc') ||nvl (EAN,'ean') ||nvl (STORE_NAME,'sn') ||nvl (CHANNEL_NAME,'cn') ||nvl (SAP_PARENT_CUSTOMER_KEY,'spck') ||nvl (SAP_PARENT_CUSTOMER_DESCRIPTION,'spscd') ||nvl (SAP_CUSTOMER_CHANNEL_KEY,'scck') ||nvl (SAP_CUSTOMER_CHANNEL_DESCRIPTION,'sccd') ||nvl (SAP_CUSTOMER_SUB_CHANNEL_KEY,'scsck') ||nvl (SAP_SUB_CHANNEL_DESCRIPTION,'sscd') ||nvl (CUSTOMER_SEGMENT_KEY,'csk') ||nvl (CUSTOMER_SEGMENT_DESCRIPTION,'csd') ||nvl (SAP_GO_TO_MDL_KEY,'sgmk') ||nvl (SAP_GO_TO_MDL_DESCRIPTION,'sgmd') ||nvl (SAP_BANNER_KEY,'sbk') ||nvl (SAP_BANNER_DESCRIPTION,'sbd') ||nvl (SAP_BANNER_FORMAT_KEY,'sbfk') ||nvl (SAP_BANNER_FORMAT_DESCRIPTION,'sbfd') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||nvl (GLOBAL_PRODUCT_BRAND,'gpb') ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_VARIANT,'gpv') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') ||nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc') ||nvl (GLOBAL_PUT_UP_DESCRIPTION,'gpud') ||nvl (PKA_PRODUCT_KEY,'pk') ||nvl (PKA_PRODUCT_KEY_DESCRIPTION,'pkd')||nvl(store_type,'st')||nvl(Region,'rg')||nvl(zone_or_area,'ar')||nvl(store_grade,'sg')||nvl(sku_code,'sku_cd')||nvl(msl_product_desc,'msl_pd')) AS SELLOUT_DIM_KEY,
       CNTRY_NM,
       DATA_SRC,
       YEAR::numeric(18,0) as year,
       MNTH_ID::numeric(18,0) as MNTH_ID,
       SOLDTO_CODE,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME||'#'||ltrim(DISTRIBUTOR_CODE,'0') AS DISTRIBUTOR_NAME,
       --DISTRIBUTOR_NAME,
       STORE_CODE,
       STORE_NAME||'#'||ltrim(STORE_CODE,'0') AS STORE_NAME,
	   store_grade,
       list_price,
       --msl_product_desc,
       SKU_CODE,
       EAN,
       MSL_PRODUCT_DESC,
       --store_name||'#'||ltrim(store_code,'0') as STORE_NAME,
       RETAIL_ENVIRONMENT,
	   store_type,
	   Region,
       zone_or_area,
       CHANNEL_NAME,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_GO_TO_MDL_KEY,
       SAP_GO_TO_MDL_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       SUM(sales_value_list_price) AS sales_value_list_price,
       SUM(SO_SLS_QTY) AS SO_SLS_QTY,
       SUM(SO_SLS_VALUE) AS SO_SLS_VALUE,
       CAST(AVG(SO_SLS_QTY) AS DECIMAL(10,2)) AS SO_AVG_QTY
FROM (SELECT country_code AS CNTRY_CD,
             country_name AS CNTRY_NM,
             data_source AS DATA_SRC,
             YEAR,
             MNTH_ID,
             MAX(SOLDTO_CODE) OVER (PARTITION BY LTRIM(DISTRIBUTOR_CODE,'0') ORDER BY LENGTH(SOLDTO_CODE) DESC, cal_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SOLDTO_CODE,
			 --md5(distributor_name) as DISTRIBUTOR_CODE,
             case when data_source = 'POS' then MD5(DISTRIBUTOR_NAME)
             else LTRIM(DISTRIBUTOR_CODE,'0') 
             END AS DISTRIBUTOR_CODE,
             MAX(DISTRIBUTOR_NAME) OVER (PARTITION BY case when data_source = 'POS' then MD5(DISTRIBUTOR_NAME)
             else LTRIM(DISTRIBUTOR_CODE,'0') 
             END ORDER BY cal_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DISTRIBUTOR_NAME,
             LTRIM(STORE_CODE,'0') AS STORE_CODE,
             MAX(STORE_NAME) OVER (PARTITION BY LTRIM(STORE_CODE,'0') ORDER BY cal_date DESC, LENGTH(STORE_NAME) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STORE_NAME,
             LTRIM(msl_product_code,0) as EAN,
             MAX(list_price) over ( PARTITION BY LTRIM(ean,0) ORDER BY length(base.sku_code) DESC,cal_date desc,base.sku_code desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS list_price ,
			 --max(msl_product_desc) over ( PARTITION BY ltrim(ean,0) ORDER BY length(base.sku_code) DESC,cal_date desc,base.sku_code desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS msl_product_desc ,
			 MSCD.SKU_CODE,
             MSCD.MSL_PRODUCT_DESC AS MSL_PRODUCT_DESC,
             UPPER(retail_env) AS RETAIL_ENVIRONMENT,
			 store_type,
			 store_grade,
			 Region,
             zone_or_area,
             channel AS CHANNEL_NAME,
             SAP_PARENT_CUSTOMER_KEY,
             SAP_PARENT_CUSTOMER_DESCRIPTION,
             SAP_CUSTOMER_CHANNEL_KEY,
             SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             SAP_CUSTOMER_SUB_CHANNEL_KEY,
             SAP_SUB_CHANNEL_DESCRIPTION,
             SAP_GO_TO_MDL_KEY,
             SAP_GO_TO_MDL_DESCRIPTION,
             SAP_BANNER_KEY,
             SAP_BANNER_DESCRIPTION,
             SAP_BANNER_FORMAT_KEY,
             SAP_BANNER_FORMAT_DESCRIPTION,
             CUSTOMER_SEGMENT_KEY,
             CUSTOMER_SEGMENT_DESCRIPTION,
             GLOBAL_PRODUCT_FRANCHISE,
             GLOBAL_PRODUCT_BRAND,
             GLOBAL_PRODUCT_SUB_BRAND,
             GLOBAL_PRODUCT_VARIANT,
             GLOBAL_PRODUCT_SEGMENT,
             GLOBAL_PRODUCT_SUBSEGMENT,
             GLOBAL_PRODUCT_CATEGORY,
             GLOBAL_PRODUCT_SUBCATEGORY,
             GLOBAL_PUT_UP_DESCRIPTION,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,
             sellout_value_list_price AS sales_value_list_price,
             SELLOUT_SALES_QUANTITY AS SO_SLS_QTY,
             SELLOUT_SALES_VALUE AS SO_SLS_VALUE
      from  edw_rpt_regional_sellout_offtake base LEFT JOIN WKS_TW_360_SELLOUT_MAPPED_SKU_CD MSCD ON LTRIM (base.EAN,'0') = LTRIM (MSCD.EAN_NUM,'0')
	  WHERE COUNTRY_CODE = 'TW' and data_source in ('SELL-OUT', 'POS') 
	  and MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and SELLOUT_SALES_VALUE is not NULL 
	  ) SELLOUT
GROUP BY DISTRIBUTOR_CODE,
         DISTRIBUTOR_NAME,
         STORE_CODE,
         EAN,
         SOLDTO_CODE,
         YEAR,
         MNTH_ID,
         CNTRY_CD,
         CNTRY_NM,
         DATA_SRC,
         STORE_NAME,
         RETAIL_ENVIRONMENT,
		 store_type,
		 store_grade,
         list_price,
         msl_product_desc,
         sku_code,
	     Region,
         zone_or_area,
         CHANNEL_NAME,
         SAP_PARENT_CUSTOMER_KEY,
         SAP_PARENT_CUSTOMER_DESCRIPTION,
         SAP_CUSTOMER_CHANNEL_KEY,
         SAP_CUSTOMER_CHANNEL_DESCRIPTION,
         SAP_CUSTOMER_SUB_CHANNEL_KEY,
         SAP_SUB_CHANNEL_DESCRIPTION,
         SAP_GO_TO_MDL_KEY,
         SAP_GO_TO_MDL_DESCRIPTION,
         SAP_BANNER_KEY,
         SAP_BANNER_DESCRIPTION,
         SAP_BANNER_FORMAT_KEY,
         SAP_BANNER_FORMAT_DESCRIPTION,
         CUSTOMER_SEGMENT_KEY,
         CUSTOMER_SEGMENT_DESCRIPTION,
         GLOBAL_PRODUCT_FRANCHISE,
         GLOBAL_PRODUCT_BRAND,
         GLOBAL_PRODUCT_SUB_BRAND,
         GLOBAL_PRODUCT_VARIANT,
         GLOBAL_PRODUCT_SEGMENT,
         GLOBAL_PRODUCT_SUBSEGMENT,
         GLOBAL_PRODUCT_CATEGORY,
         GLOBAL_PRODUCT_SUBCATEGORY,
         GLOBAL_PUT_UP_DESCRIPTION,
         PKA_PRODUCT_KEY,
         PKA_PRODUCT_KEY_DESCRIPTION
);

--------------------------------------
delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_MSL_LIST;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_RE_MSL_LIST (
with edw_calendar_dim as (
    select * from prod_dna_core.aspedw_integration.edw_calendar_dim
),
edw_vw_cal_retail_excellence_dim as (
    select * from prod_dna_core.aspedw_integration.v_edw_vw_cal_retail_excellence_dim
),
itg_re_msl_input_definition as (
    select * from prod_dna_core.aspitg_integration.itg_re_msl_input_definition
),
wks_tw_base_re as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE
),
edw_vw_pop6_products as (
    --select * from {{ source('ntaedw_integration', 'edw_vw_pop6_products') }}
    select * from PROD_DNA_CORE.aspedw_integration.edw_vw_pop6_products
)

SELECT DISTINCT jj_year::numeric(18,0) as jj_year,
       jj_mnth_id::numeric(18,0) as jj_mnth_id,
       soldto_code,
       data_src,
       distributor_code,
       distributor_name,
       store_code,
       store_name,
       store_type,
       ean,
       store_grade,
       Sell_Out_Channel,
       retail_environment,
       market,
       cntry_cd,
       prod_hier_l1,
       prod_hier_l2,
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       prod_hier_l6,
       prod_hier_l7,
       prod_hier_l8,
       prod_hier_l9,
       sku_code,
       --MAX(msl_product_desc) OVER (PARTITION BY LTRIM(ean,0) ORDER BY LENGTH(msl_product_desc) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msl_product_desc,
       msl_product_desc,
       region,
       zone_or_area
FROM (SELECT DISTINCT SUBSTRING(base.jj_mnth_id,1,4) AS jj_year,
             base.jj_mnth_id,
             noo.soldto_code,
             noo.data_src,
             noo.distributor_code,
             noo.distributor_name,
             noo.distributor_name_new,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             LTRIM(base.sku_unique_identifier,0) AS ean,
             base.store_grade,
             noo.channel AS Sell_Out_Channel,
             UPPER(base.retail_environment) AS retail_environment,
             base.market,
             base.cntry_cd,
             epd.prod_hier_l1,
             epd.prod_hier_l2,
             epd.prod_hier_l3,
             epd.prod_hier_l4,
             epd.prod_hier_l5,
             prod_hier_l6,
             prod_hier_l7,
             prod_hier_l8,
             epd.prod_hier_l9,
             noo.sku_code,
             noo.msl_product_desc,
             noo.region,
             noo.zone_or_area,
             base.customer_name
      FROM (SELECT DISTINCT *,
                   CASE
                     WHEN msl.market = 'Taiwan' THEN 'TW'
                   END cntry_cd,
                   CASE
                     WHEN customer LIKE '%Carrefour%' THEN 'Carrefour'
                     WHEN customer LIKE '%PX%' THEN 'PX'
                     WHEN customer LIKE '%Watson%' THEN 'Watsons'
                     ELSE customer
                   END AS customer_name
            FROM itg_re_msl_input_definition msl
              LEFT JOIN (SELECT DISTINCT SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM
                         WHERE jj_mnth_id >= (SELECT last_17mnths FROM edw_vw_cal_Retail_excellence_Dim)
                         AND   jj_mnth_id <= (SELECT last_2mnths FROM edw_vw_cal_Retail_excellence_Dim)) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM') <= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= cal.jj_mnth_id
            WHERE UPPER(msl.market) = 'TAIWAN'
            --in ('Taiwan')	 and msl.retail_environment='MT'
            ) base
        LEFT JOIN (SELECT DISTINCT data_src,
                          distributor_code,
                          distributor_name,
                          CASE
                            WHEN distributor_name LIKE '%Carrefour%' THEN 'Carrefour'
                            WHEN distributor_name LIKE '%PX%' THEN 'PX'
                            WHEN distributor_name LIKE '%Watsons%' THEN 'Watsons'
                            ELSE distributor_name
                          END AS distributor_name_new,
                          store_code,
                          store_type,
                          region,
                          zone_or_area,
                          RETAIL_ENVIRONMENT,
                          store_grade,
                          CHANNEL_NAME AS channel,
                          CNTRY_CD,
                          soldto_code,
                          store_name,
                          msl_product_desc,
                          sku_code,
                          EAN
                   FROM WKS_TW_BASE_RE
                   --WHERE CNTRY_CD in  ('TW') and data_src='POS' and RETAIL_ENVIRONMENT='MT'
                   ) NOO
               ON UPPER (base.channel) = UPPER (noo.channel)
              AND UPPER(base.customer_name) = UPPER(noo.distributor_name_new)
              and UPPER(base.Retail_Environment) = UPPER(noo.Retail_Environment)
              AND UPPER(base.cntry_cd) = UPPER(noo.CNTRY_CD)
              AND TRIM (base.sku_unique_identifier) = TRIM (noo.EAN)
        LEFT JOIN (SELECT DISTINCT prd.country_l1 AS prod_hier_l1,
                          prd.regional_franchise_l2 AS prod_hier_l2,
                          prd.franchise_l3 AS prod_hier_l3,
                          prd.brand_l4 AS prod_hier_l4,
                          prd.sub_category_l5 AS prod_hier_l5,
                          prd.platform_l6 AS prod_hier_l6,
                          prd.variance_l7 AS prod_hier_l7,
                          prd.pack_size_l8 AS prod_hier_l8,
                          NULL AS prod_hier_l9,
                          prd.barcode
                   FROM edw_vw_pop6_products prd
                   WHERE cntry_cd = 'TW') epd ON UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.barcode))
      -- and row_no=1
    )
    WHERE STORE_CODE IS NOT NULL
    AND   DISTRIBUTOR_CODE IS NOT NULL
);
