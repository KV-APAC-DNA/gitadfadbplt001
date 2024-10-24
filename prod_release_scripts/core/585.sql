delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_360_SELLOUT_MAPPED_SKU_CD;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_360_SELLOUT_MAPPED_SKU_CD (
  
with edw_rpt_regional_sellout_offtake as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.V_EDW_VW_CAL_RETAIL_EXCELLENCE_DIM
)

select ean_num,sku_code,msl_product_desc from (SELECT DISTINCT ltrim(msl_product_code,'0') AS ean_num,
       LTRIM(sku_code,'0') AS sku_code,
	   msl_product_desc as msl_product_desc, 
       ROW_NUMBER() OVER (PARTITION BY ltrim(msl_product_code,0) ORDER BY cal_date DESC, LENGTH(LTRIM(sku_code,'0')) DESC) AS rno
FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
WHERE COUNTRY_CODE = 'TW'
AND UPPER(msl_product_desc) <> 'NA'
AND   data_source in ('SELL-OUT', 'POS')
and  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) WHERE rno = 1
);

-------------------

delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_BASE_RE (
 
with edw_rpt_regional_sellout_offtake as (
    select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.ASPEDW_INTEGRATION__EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
),
edw_vw_cal_retail_excellence_dim as (
    select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.V_EDW_VW_CAL_RETAIL_EXCELLENCE_DIM
),
wks_tw_360_sellout_mapped_sku_cd as (
	select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_TW_360_SELLOUT_MAPPED_SKU_CD
)

SELECT CNTRY_CD,
       MD5(nvl (SOLDTO_CODE,'stc') ||nvl (DISTRIBUTOR_CODE,'dc') ||nvl (DISTRIBUTOR_NAME,'dn') ||nvl (STORE_CODE,'sc') ||nvl (EAN,'ean') ||nvl (STORE_NAME,'sn') ||nvl (CHANNEL_NAME,'cn') ||nvl (SAP_PARENT_CUSTOMER_KEY,'spck') ||nvl (SAP_PARENT_CUSTOMER_DESCRIPTION,'spscd') ||nvl (SAP_CUSTOMER_CHANNEL_KEY,'scck') ||nvl (SAP_CUSTOMER_CHANNEL_DESCRIPTION,'sccd') ||nvl (SAP_CUSTOMER_SUB_CHANNEL_KEY,'scsck') ||nvl (SAP_SUB_CHANNEL_DESCRIPTION,'sscd') ||nvl (CUSTOMER_SEGMENT_KEY,'csk') ||nvl (CUSTOMER_SEGMENT_DESCRIPTION,'csd') ||nvl (SAP_GO_TO_MDL_KEY,'sgmk') ||nvl (SAP_GO_TO_MDL_DESCRIPTION,'sgmd') ||nvl (SAP_BANNER_KEY,'sbk') ||nvl (SAP_BANNER_DESCRIPTION,'sbd') ||nvl (SAP_BANNER_FORMAT_KEY,'sbfk') ||nvl (SAP_BANNER_FORMAT_DESCRIPTION,'sbfd') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||nvl (GLOBAL_PRODUCT_BRAND,'gpb') ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_VARIANT,'gpv') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') ||nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc') ||nvl (GLOBAL_PUT_UP_DESCRIPTION,'gpud') ||nvl (PKA_PRODUCT_KEY,'pk') ||nvl (PKA_PRODUCT_KEY_DESCRIPTION,'pkd')||nvl(store_type,'st')||nvl(Region,'rg')||nvl(zone_or_area,'ar')||nvl(store_grade,'sg')||nvl(sku_code,'sku_cd')||nvl(msl_product_desc,'msl_pd')) AS SELLOUT_DIM_KEY,
       CNTRY_NM,
       DATA_SRC,
       YEAR,
       MNTH_ID,
       SOLDTO_CODE,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME||'#'||ltrim(DISTRIBUTOR_CODE,'0') AS DISTRIBUTOR_NAME,
       --DISTRIBUTOR_NAME,
       STORE_CODE,
       STORE_NAME||'#'||ltrim(STORE_CODE,'0') AS STORE_NAME,
	   store_grade,
       list_price,
       --msl_product_desc,
       COALESCE(SKU_CODE, 'NA') AS SKU_CODE,
       EAN,
       COALESCE(MSL_PRODUCT_DESC, 'NA') AS MSL_PRODUCT_DESC,
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
