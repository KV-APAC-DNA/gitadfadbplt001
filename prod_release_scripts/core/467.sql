drop table PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HONG_KONG_REGIONAL_SELLOUT_BASE;

create table PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HONG_KONG_REGIONAL_SELLOUT_BASE as (

with edw_pos_fact as (
select * from PROD_DNA_CORE.NTAEDW_INTEGRATION.EDW_POS_FACT
),
edw_vw_os_time_dim as (
select * from PROD_DNA_CORE.SGPEDW_INTEGRATION.EDW_VW_OS_TIME_DIM
),
edw_ims_fact as (
select * from PROD_DNA_CORE.NTAEDW_INTEGRATION.EDW_IMS_FACT
),
itg_ims_dstr_cust_attr as (
select * from PROD_DNA_CORE.NTAITG_INTEGRATION.ITG_IMS_DSTR_CUST_ATTR
),
itg_mds_ap_customer360_config as (
select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
),
itg_query_parameters as (
select * from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_QUERY_PARAMETERS
),
edw_vw_pop6_store as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_VW_POP6_STORE
),
sdl_mds_hk_store_master as
(
    select * from PROD_DNA_LOAD.NTASDL_RAW.SDL_MDS_HK_STORE_MASTER
)

SELECT
BASE.data_src,
BASE.cntry_cd,
BASE.cntry_nm,
BASE.year,
BASE.mnth_id,
BASE.day,
BASE.univ_year,
BASE.univ_month,
BASE.soldto_code,
BASE.distributor_code,
BASE.distributor_name,
BASE.store_cd,
BASE.store_name,
BASE.store_type,
BASE.ean,
BASE.matl_num,
BASE.Customer_Product_Desc,
BASE.region,
BASE.zone_or_area,
BASE.so_sls_qty,
BASE.so_sls_value,
BASE.msl_product_code,
--BASE.msl_product_desc,
BASE.store_grade,
UPPER(BASE.retail_env) as retail_env,
channel,
convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
convert_timezone('UTC',current_timestamp()) AS updt_dttm
FROM
(
--POS
SELECT 'POS' AS DATA_SRC,
       'HK' AS 	CNTRY_CD,
       'Hong Kong' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
	   POS.pos_dt AS DAY,
	   time_dim.cal_year::INT  as univ_year,
	   time_dim.cal_mnth_no::INT as univ_month,
	   sold_to_party as SOLDTO_CODE,
       'NA' AS DISTRIBUTOR_CODE,
       POS.sls_grp AS DISTRIBUTOR_NAME,
       POS.str_cd AS STORE_CD,
       POS.str_nm AS STORE_NAME,
	   POS.store_type,
       ean_num AS EAN,
       POS.matl_num AS MATL_NUM,
	   vend_prod_nm AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   POS.sls_qty as SO_SLS_QTY, 
	   POS.sls_amt as SO_SLS_VALUE,
	   'NA' as msl_product_code,
		--'NA' as msl_product_desc,
		'NA'as store_grade,
		'NA' as retail_env,
        'NA' as channel
      FROM  edw_pos_fact POS
	  left join edw_vw_os_time_dim as time_dim
	on POS.pos_dt = time_dim.cal_date 
	WHERE ctry_cd = 'HK' and crncy_cd = 'HKD' 
	and not (sls_qty = 0 and sls_amt = 0)
	
UNION ALL

SELECT 'SELL-OUT' AS DATA_SRC,
       'HK' AS 	CNTRY_CD,
       'Hong Kong' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
       ims_txn_dt AS DAY,
	   time_dim.cal_year::INT  as univ_year,
	   time_dim.cal_mnth_no::INT as univ_month,
       sls.dstr_cd as SOLDTO_CODE,
       sls.dstr_cd AS DISTRIBUTOR_CODE,
       sls.dstr_nm AS DISTRIBUTOR_NAME,
       sls.cust_cd AS STORE_CD,
       sls.cust_nm AS STORE_NAME,
	   dc.dstr_cust_clsn1 as store_type,
       sls.ean_num AS EAN,
       sls.prod_cd AS MATL_NUM,
	   prod_nm AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   (sls_qty - rtrn_qty) as SO_SLS_QTY, 
	   (sls_amt - rtrn_amt) as SO_SLS_VALUE,
	   sls.ean_num as msl_product_code,
		--prod_nm as msl_product_desc,
		--pop.store_grade as store_grade,
		--pop.retail_env as retail_env
        str_mstr.store_grade as store_grade,
		str_mstr.retail_env as retail_env,
		channel
from edw_ims_fact sls LEFT JOIN edw_vw_os_time_dim time_dim
on sls.ims_txn_dt = time_dim.cal_date
left join (SELECT DISTINCT dstr_cd, "replace"("replace"("replace"("replace"(dstr_cust_cd::text, 'Indirect'::text, ''::text),
	  'Direct'::text, ''::text), 'Indi'::text, ''::text), 'Dire'::text, ''::text) AS dstr_cust_cd, 
	dstr_cust_clsn1 FROM itg_ims_dstr_cust_attr where ctry_cd = 'HK'
	) dc on sls.dstr_cd = dc.dstr_cd and sls.cust_cd = dc.dstr_cust_cd
--LEFT JOIN (SELECT DISTINCT sales_group_name AS store_grade, channel AS retail_env, POP_CODE FROM edw_vw_pop6_store
             --WHERE cntry_cd = 'HK' AND   UPPER(sales_group_name) IN --('GENERAL TRADE','GENERAL TRADE B/C')) 
			 --(select parameter_value from itg_query_parameters where country_code='HK' and parameter_name='sales_group_name')) pop 
	 --ON LTRIM(SUBSTRING (pop.pop_code,3),0) = LTRIM(sls.cust_cd,0) 
  LEFT JOIN (SELECT DISTINCT sales_group_name_name AS store_grade,
                    "retail environment (ps)_name" AS retail_env, channel_code AS channel,
                    distributor_customer_number
             FROM SDL_MDS_HK_Store_master where retail_env IS NOT NULL) str_mstr ON LTRIM (str_mstr.distributor_customer_number,'0') = LTRIM (sls.cust_cd,'0')
where ctry_cd = 'HK' and crncy_cd = 'HKD'  and not (sls.prod_cd like '1U%' OR sls.prod_cd like 'COUNTER TOP%' OR sls.prod_cd like 'DUMPBIN%' OR sls.prod_cd IS NULL OR sls.prod_cd = '') 
	  )BASE
WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_hk')='ALL' THEN '190001' ELSE to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -(30::integer)), 'YYYYMM')
END)
);

------------------------------------------

drop table PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HONG_KONG_REGIONAL_SELLOUT;

create table PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HONG_KONG_REGIONAL_SELLOUT as (

with wks_hong_kong_regional_sellout_base as (
select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.NTAWKS_INTEGRATION__WKS_HONG_KONG_REGIONAL_SELLOUT_BASE
),
edw_vw_os_time_dim as (
select * from PROD_DNA_CORE.SGPEDW_INTEGRATION.EDW_VW_OS_TIME_DIM
),
edw_material_dim as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_MATERIAL_DIM
),
edw_product_key_attributes as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_PRODUCT_KEY_ATTRIBUTES
),
edw_gch_producthierarchy as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_GCH_PRODUCTHIERARCHY
),
edw_gch_customerhierarchy as (
select * from PROD_DNA_CORE.aspedw_integration.edw_gch_customerhierarchy
),
edw_customer_sales_dim as (
select * from PROD_DNA_CORE.aspedw_integration.edw_customer_sales_dim
),
edw_customer_base_dim as (
select * from PROD_DNA_CORE.aspedw_integration.edw_customer_base_dim
),
edw_company_dim as (
select * from PROD_DNA_CORE.aspedw_integration.edw_company_dim
),
edw_dstrbtn_chnl as (
select * from PROD_DNA_CORE.aspedw_integration.edw_dstrbtn_chnl
),
edw_sales_org_dim as (
select * from PROD_DNA_CORE.aspedw_integration.edw_sales_org_dim
),
edw_code_descriptions as (
select * from PROD_DNA_CORE.aspedw_integration.edw_code_descriptions
),
edw_subchnl_retail_env_mapping as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_SUBCHNL_RETAIL_ENV_MAPPING
),
edw_code_descriptions_manual as (
select * from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_CODE_DESCRIPTIONS_MANUAL
),
edw_customer_sales_dim as (
select * from PROD_DNA_CORE.aspedw_integration.edw_customer_sales_dim
),
vw_edw_reg_exch_rate as (
select * from PROD_DNA_CORE.aspedw_integration.vw_edw_reg_exch_rate
)

SELECT 
	YEAR,
	QRTR_NO,
	MNTH_ID,
	mnth_no,
	CAL_DATE,
	univ_year,
	univ_month,
	COUNTRY_CODE,	   
	COUNTRY_NAME,
	DATA_SOURCE,
	SOLDTO_CODE,
	DISTRIBUTOR_CODE,
	DISTRIBUTOR_NAME,
	STORE_CODE,
	STORE_NAME,
	store_type,
	DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
	DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
	DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
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
	RETAIL_ENVIRONMENT,
	REGION,
	ZONE_OR_AREA,
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
	EAN,
	SKU_CODE,
	SKU_DESCRIPTION,
	--GREENLIGHT_SKU_FLAG,
	CASE WHEN PKA_PRODUCT_KEY IN ('N/A','NA') THEN 'NA'
		ELSE PKA_PRODUCT_KEY END AS PKA_PRODUCT_KEY,
	CASE WHEN PKA_PRODUCT_KEY_DESCRIPTION IN ('N/A','NA') THEN 'NA'
		ELSE PKA_PRODUCT_KEY_DESCRIPTION END AS PKA_PRODUCT_KEY_DESCRIPTION,	
	--TRIM(NVL (NULLIF(PRODUCT.SLS_ORG,''),'NA')) AS SLS_ORG,
	Customer_Product_Desc,
	FROM_CURRENCY,
	TO_CURRENCY,
	EXCHANGE_RATE,
	SELLOUT_SALES_QUANTITY,
	SELLOUT_SALES_VALUE,
	SELLOUT_SALES_VALUE_USD,
	msl_product_code,
	msl_product_desc,
	store_grade,
	retail_env,
    channel,
	crtd_dttm,
	updt_dttm
FROM
(SELECT  
		CAST(TIME.YEAR AS VARCHAR(10)) AS YEAR,
	   CAST(TIME.QRTR_NO AS VARCHAR(14)) AS QRTR_NO,
	   CAST(TIME.MNTH_ID AS VARCHAR(21)) AS MNTH_ID,
	   CAST(TIME.MNTH_NO AS VARCHAR(10)) AS mnth_no,
	   SELLOUT.DAY  AS CAL_DATE,
	   SELLOUT.univ_year  AS univ_year,
	   SELLOUT.univ_month  AS univ_month,
	   SELLOUT.CNTRY_CD AS COUNTRY_CODE,	   
	   SELLOUT.CNTRY_NM AS COUNTRY_NAME,
	   SELLOUT.DATA_SRC AS DATA_SOURCE,
	   TRIM(NVL (NULLIF(SELLOUT.SOLDTO_CODE,''),'NA')) AS SOLDTO_CODE,
	   DISTRIBUTOR_CODE,
	   DISTRIBUTOR_NAME,
	   STORE_CD AS STORE_CODE,
	   TRIM(STORE_NAME) AS STORE_NAME,
	   TRIM(NVL (NULLIF(SELLOUT.store_type,''),'NA')) AS store_type,
	   'NA' AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
       'NA' AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
       'NA' AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
	   TRIM(NVL (NULLIF(CUST.SAP_PRNT_CUST_KEY,''),'NA')) AS SAP_PARENT_CUSTOMER_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_PRNT_CUST_DESC,''),'NA'))) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.SAP_CUST_CHNL_KEY,''),'NA')) AS SAP_CUSTOMER_CHANNEL_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_CUST_CHNL_DESC,''),'NA'))) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.SAP_CUST_SUB_CHNL_KEY,''),'NA')) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_SUB_CHNL_DESC,''),'NA'))) AS SAP_SUB_CHANNEL_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_GO_TO_MDL_DESC,''),'NA'))) AS SAP_GO_TO_MDL_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.SAP_BNR_KEY,''),'NA')) AS SAP_BANNER_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_BNR_DESC,''),'NA'))) AS SAP_BANNER_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.SAP_BNR_FRMT_KEY,''),'NA')) AS SAP_BANNER_FORMAT_KEY,
	   UPPER(TRIM(NVL (NULLIF(CUST.SAP_BNR_FRMT_DESC,''),'NA'))) AS SAP_BANNER_FORMAT_DESCRIPTION,
	   TRIM(NVL (NULLIF(CUST.RETAIL_ENV,''),'NA')) AS RETAIL_ENVIRONMENT,
	   --TRIM(NVL (NULLIF(CUST.REGION,''),'NA')) AS REGION,
	   --TRIM(NVL (NULLIF(CUST.ZONE_OR_AREA,''),'NA')) AS ZONE_OR_AREA,
	   TRIM(NVL (NULLIF(CUST.CUST_SEGMT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,
	   TRIM(NVL (NULLIF(CUST.CUST_SEGMENT_DESC,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION, 
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_FRNCHSE,''),'NA')) AS GLOBAL_PRODUCT_FRANCHISE,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_BRND,''),'NA')) AS GLOBAL_PRODUCT_BRAND,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUB_BRND,''),'NA')) AS GLOBAL_PRODUCT_SUB_BRAND,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_VRNT,''),'NA')) AS GLOBAL_PRODUCT_VARIANT,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SGMNT,''),'NA')) AS GLOBAL_PRODUCT_SEGMENT,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBSGMNT,''),'NA')) AS GLOBAL_PRODUCT_SUBSEGMENT,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_CTGRY,''),'NA')) AS GLOBAL_PRODUCT_CATEGORY,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBCTGRY,''),'NA')) AS GLOBAL_PRODUCT_SUBCATEGORY,
	   TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_PUT_UP_DESC,''),'NA')) AS GLOBAL_PUT_UP_DESCRIPTION,
	   SELLOUT.EAN AS EAN,
	   TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
	   UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))) AS SKU_DESCRIPTION,
	   --TRIM(NVL (NULLIF(PRODUCT.GREENLIGHT_SKU_FLAG,''),'NA')) AS GREENLIGHT_SKU_FLAG,
	   CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA'))
		ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) END AS PKA_PRODUCT_KEY,
	   CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productdesc,''),'NA'))
		ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END AS PKA_PRODUCT_KEY_DESCRIPTION,	
	   --TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) AS PKA_PRODUCT_KEY,
	   --TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) AS PKA_PRODUCT_KEY_DESCRIPTION,
	   --TRIM(NVL (NULLIF(PRODUCT.SLS_ORG,''),'NA')) AS SLS_ORG,
	   TRIM(NVL (NULLIF(SELLOUT.Customer_Product_Desc,''),'NA')) AS Customer_Product_Desc,
	   TRIM(NVL (NULLIF(SELLOUT.region,''),'NA')) AS REGION,
       TRIM(NVL (NULLIF(SELLOUT.zone_or_area,''),'NA')) AS ZONE_OR_AREA,
	   CAST('HKD' AS VARCHAR) AS FROM_CURRENCY,
	   'USD' AS TO_CURRENCY,
	   --C.EXCH_RATE AS EXCHANGE_RATE,
	   (C.EXCH_RATE/(C.from_ratio*C.to_ratio))::NUMERIC(15,5) as EXCHANGE_RATE,
		SUM(SO_SLS_QTY) SELLOUT_SALES_QUANTITY,
		SUM(SO_SLS_VALUE) AS SELLOUT_SALES_VALUE,
		SUM(SO_SLS_VALUE*(C.EXCH_RATE/(C.from_ratio*C.to_ratio)))::NUMERIC(38,11) SELLOUT_SALES_VALUE_USD,
		TRIM(NVL (NULLIF(SELLOUT.msl_product_code,''),'NA')) AS msl_product_code,
		CASE WHEN SELLOUT.DATA_SRC='POS' THEN 'NA'
        ELSE
        (CASE WHEN (UPPER(PRODUCT.PKA_PACKAGE) IN ('MIX PACK', 'ASSORTED PACK') OR PRODUCT.PKA_PACKAGE IS NULL) THEN UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')))
        ELSE (CASE WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
            WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
            WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productdesc,''),'NA'))
            ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END)
        END) END AS msl_product_desc,
		TRIM(NVL (NULLIF(SELLOUT.store_grade,''),'NA')) AS store_grade,
		TRIM(NVL (NULLIF(SELLOUT.retail_env,''),'NA')) AS retail_env,
        TRIM(NVL (NULLIF(SELLOUT.channel,''),'NA')) AS channel,
	   SELLOUT.crtd_dttm,
	   SELLOUT.updt_dttm
FROM WKS_HONG_KONG_REGIONAL_SELLOUT_BASE SELLOUT

LEFT JOIN (SELECT DISTINCT CAL_YEAR AS YEAR,
       CAL_QRTR_NO AS QRTR_NO,
       CAL_MNTH_ID AS MNTH_ID,
       CAL_MNTH_NO AS MNTH_NO
FROM EDW_VW_OS_TIME_DIM) TIME
ON SELLOUT.MNTH_ID=TIME.MNTH_ID


--product selection
LEFT JOIN (SELECT DISTINCT 

          --EMD.matl_num AS SAP_MATL_NUM,
		  sls_base.matl_num AS SAP_MATL_NUM,
          EMD.MATL_DESC AS SAP_MAT_DESC,
          EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
          EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
          --EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
          --EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
          EMD.PRODH1 AS SAP_PROD_SGMT_CD,
          EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
          --EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
          EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
          --EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
          EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
          --EMD.SAP_BRND_CD AS SAP_BRND_CD,
          EMD.BRND_DESC AS SAP_BRND_DESC,
          --EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
          EMD.VARNT_DESC AS SAP_VRNT_DESC,
          --EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
          EMD.PUT_UP_DESC AS SAP_PUT_UP_DESC,
          EMD.PRODH2 AS SAP_GRP_FRNCHSE_CD,
          EMD.PRODH2_TXTMD AS SAP_GRP_FRNCHSE_DESC,
          EMD.PRODH3 AS SAP_FRNCHSE_CD,
          EMD.PRODH3_TXTMD AS SAP_FRNCHSE_DESC,
          EMD.PRODH4 AS SAP_PROD_FRNCHSE_CD,
          EMD.PRODH4_TXTMD AS SAP_PROD_FRNCHSE_DESC,
          EMD.PRODH5 AS SAP_PROD_MJR_CD,
          EMD.PRODH5_TXTMD AS SAP_PROD_MJR_DESC,
          EMD.PRODH5 AS SAP_PROD_MNR_CD,
          EMD.PRODH5_TXTMD AS SAP_PROD_MNR_DESC,
          EMD.PRODH6 AS SAP_PROD_HIER_CD,
          EMD.PRODH6_TXTMD AS SAP_PROD_HIER_DESC,
		  --EMD.SLS_ORG AS SLS_ORG,
		  --EMD.greenlight_sku_flag AS greenlight_sku_flag,
		  NVL(EMD.pka_product_key, pka.pka_productkey) as  pka_product_key,
		  NVL(EMD.pka_product_key_description, pka.pka_productdesc) as pka_product_key_description,
		  --EMD.pka_product_key AS pka_product_key,
		  --EMD.pka_product_key_description AS pka_product_key_description,
          egph."region" AS GPH_REGION,
          EGPH.regional_franchise AS GPH_REG_FRNCHSE,
          EGPH.regional_franchise_group AS GPH_REG_FRNCHSE_GRP,
          EGPH.GCPH_FRANCHISE AS GPH_PROD_FRNCHSE,
          EGPH.GCPH_BRAND AS GPH_PROD_BRND,
          EGPH.GCPH_SUBBRAND AS GPH_PROD_SUB_BRND,
          EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
          EGPH.GCPH_NEEDSTATE AS GPH_PROD_NEEDSTATE,
          EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
          EGPH.GCPH_SUBCATEGORY AS GPH_PROD_SUBCTGRY,
          EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT,
          EGPH.GCPH_SUBSEGMENT AS GPH_PROD_SUBSGMNT,
          EGPH.PUT_UP_CODE AS GPH_PROD_PUT_UP_CD,
          EGPH.PUT_UP_DESCRIPTION AS GPH_PROD_PUT_UP_DESC,
          EGPH.SIZE AS GPH_PROD_SIZE,
          EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM,
          EMD.PKA_PACKAGE_DESC AS PKA_PACKAGE,
          row_number() over( partition by sap_matl_num order by sap_matl_num) rnk           
          FROM 
		   
		  --edw_material_dim EMD,
          --EDW_GCH_PRODUCTHIERARCHY EGPH
          --WHERE LTRIM(EMD.MATL_NUM,0) = LTRIM(EGPH.MATERIALNUMBER(+),0)
		  
    WKS_HONG_KONG_REGIONAL_SELLOUT_BASE sls_base
	left join edw_material_dim EMD on ltrim(sls_base.matl_num, '0') = ltrim(EMD.matl_num, '0')
	left join (
		select a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
		FROM ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
				 FROM edw_product_key_attributes
				WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
		JOIN ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date,
			  row_number() OVER(
				PARTITION BY ctry_nm, ean_upc
				ORDER BY lst_nts DESC) AS row_number
				 FROM edw_product_key_attributes
				WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null
				) b
		ON a.ctry_nm = 'Hong Kong' and b.ctry_nm = 'Hong Kong' and a.ean_upc = b.ean_upc and a.sku = b.sku
		AND b.latest_nts_date = a.nts_date AND b.row_number = 1
	) pka on ltrim(sls_base.ean, '0') = pka.ean_upc
	--left join edw_gch_producthierarchy EGPH
	left join (select * from edw_gch_producthierarchy where materialnumber <> '0') EGPH
	on nvl(ltrim(pka.sku, '0'), ltrim(sls_base.matl_num, '0')) = ltrim(EGPH.materialnumber, '0') 		 
    --WHERE  EMD.PROD_HIER_CD <> ''
	  ) product
 ON LTRIM(SELLOUT.matl_num,'0') = LTRIM(product.sap_matl_num,'0') and rnk=1
 
 --product key attribute selection
 LEFT JOIN
 (SELECT a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
	FROM ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
	JOIN ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date, 
      row_number() OVER( 
        PARTITION BY ctry_nm, ean_upc
        ORDER BY lst_nts DESC) AS row_number
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null
        ) b
	ON a.ctry_nm = b.ctry_nm AND a.ean_upc = b.ean_upc AND a.sku = b.sku
	AND b.latest_nts_date = a.nts_date AND b.row_number = 1 and a.ctry_nm = 'Hong Kong') PROD_KEY1
ON ltrim(SELLOUT.matl_num, '0') = ltrim(PROD_KEY1.sku, '0') 

LEFT JOIN
 (SELECT a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
	FROM ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
	JOIN ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date, 
      row_number() OVER( 
        PARTITION BY ctry_nm, ean_upc
        ORDER BY lst_nts DESC) AS row_number
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null
        ) b
	ON a.ctry_nm = b.ctry_nm AND a.ean_upc = b.ean_upc AND a.sku = b.sku
	AND b.latest_nts_date = a.nts_date AND b.row_number = 1 and a.ctry_nm = 'Hong Kong') PROD_KEY2
ON ltrim(SELLOUT.ean, '0') = ltrim(PROD_KEY2.ean_upc, '0') 
 
 ---customer selection
LEFT JOIN (SELECT * FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
       ECBD.CUST_NM AS SAP_CUST_NM,
       ECSD.SLS_ORG AS SAP_SLS_ORG,
       ECD.COMPANY AS SAP_CMP_ID,
       ECD.CTRY_KEY AS SAP_CNTRY_CD,
       ECD.CTRY_NM AS SAP_CNTRY_NM,
       ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
       CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC,
       ECSD.CHNL_KEY AS SAP_CUST_CHNL_KEY,
       CDDES_CHNL.CODE_DESC AS SAP_CUST_CHNL_DESC,
       ECSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY,
       CDDES_SUBCHNL.CODE_DESC AS SAP_SUB_CHNL_DESC,
       ECSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY,
       CDDES_GTM.CODE_DESC AS SAP_GO_TO_MDL_DESC,
       ECSD.BNR_KEY AS SAP_BNR_KEY,
       CDDES_BNRKEY.CODE_DESC AS SAP_BNR_DESC,
       ECSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY,
       CDDES_BNRFMT.CODE_DESC AS SAP_BNR_FRMT_DESC,
       SUBCHNL_RETAIL_ENV.RETAIL_ENV,
       --REGZONE.REGION_NAME AS REGION,
       --REGZONE.ZONE_NAME AS ZONE_OR_AREA,
       EGCH.GCGH_REGION AS GCH_REGION,
       EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
       EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
       EGCH.GCGH_MARKET AS GCH_MARKET,
       EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
       ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
       CODES_SEGMENT.code_desc AS cust_segment_desc,
       --ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY,cust_segmt_key ASC NULLS LAST) AS RANK
	   ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY NULLIF(ECSD.PRNT_CUST_KEY,''),NULLIF(ECSD.BNR_KEY,''),NULLIF(ECSD.BNR_FRMT_KEY,''),NULLIF(ECSD.SEGMT_KEY,'') ASC NULLS LAST) AS RANK
FROM EDW_GCH_CUSTOMERHIERARCHY EGCH,
     EDW_CUSTOMER_SALES_DIM ECSD,
     EDW_CUSTOMER_BASE_DIM ECBD,
     EDW_COMPANY_DIM ECD,
     EDW_DSTRBTN_CHNL EDC,
     EDW_SALES_ORG_DIM ESOD,
     EDW_CODE_DESCRIPTIONS CDDES_PCK,
     EDW_CODE_DESCRIPTIONS CDDES_BNRKEY,
     EDW_CODE_DESCRIPTIONS CDDES_BNRFMT,
     EDW_CODE_DESCRIPTIONS CDDES_CHNL,
     EDW_CODE_DESCRIPTIONS CDDES_GTM,
     EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL,
     EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV,
     edw_code_descriptions_manual CODES_SEGMENT,
     (SELECT DISTINCT CUST_NUM,REC_CRT_DT,PRNT_CUST_KEY,ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC)RN from EDW_CUSTOMER_SALES_DIM) A
WHERE EGCH.CUSTOMER(+) = ECBD.CUST_NUM
AND   ECSD.CUST_NUM = ECBD.CUST_NUM

AND   A.CUST_NUM = ECSD.CUST_NUM
AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
AND   ECSD.SLS_ORG = ESOD.SLS_ORG
AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
AND   A.RN=1
AND   UPPER(TRIM(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
AND   CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
AND   UPPER(TRIM(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
AND   CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
AND   UPPER(TRIM(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
AND   CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
AND   UPPER(TRIM(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
AND   CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
AND   UPPER(TRIM(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
AND   CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
AND   UPPER(TRIM(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
AND   CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
--AND   LTRIM(ECSD.CUST_NUM,'0') = REGZONE.CUSTOMER_CODE(+)
AND   CODES_SEGMENT.code_type(+) = 'Customer Segmentation Key'
AND   CODES_SEGMENT.CODE(+) = ECSD.segmt_key)
WHERE RANK = 1) CUST
ON LTRIM(SELLOUT.SOLDTO_CODE,'0')=LTRIM(CUST.SAP_CUST_ID,'0')

LEFT JOIN 

(SELECT *
  FROM vw_edw_reg_exch_rate
  WHERE cntry_key = 'HK'
  AND   TO_CCY = 'USD'
  AND   JJ_MNTH_ID = (SELECT MAX(JJ_MNTH_ID) FROM vw_edw_reg_exch_rate)
) C
 ON   UPPER(SELLOUT.CNTRY_NM) = UPPER(C.CNTRY_NM)  
            
where  C.FROM_CCY = 'HKD' 
GROUP BY 
	  TIME.YEAR,
	  TIME.QRTR_NO,
	  SELLOUT.DAY,
	  univ_year,
	  univ_month,
	  SELLOUT.CNTRY_CD,	   
	  SELLOUT.CNTRY_NM,
	  TIME.MNTH_ID,
	  TIME.MNTH_NO,
	  CONCAT(CAST(TIME.MNTH_ID AS VARCHAR(21)),'01'),
	  SELLOUT.DATA_SRC,
	  SELLOUT.SOLDTO_CODE, 
	  DISTRIBUTOR_CODE,
	  DISTRIBUTOR_NAME,
	  STORE_CD,
	  STORE_NAME,
	  STORE_TYPE,
	  DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
	  DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2 ,
	  DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3 ,
	  CUST.SAP_PRNT_CUST_KEY, 
	  CUST.SAP_PRNT_CUST_DESC, 
	  CUST.SAP_CUST_CHNL_KEY, 
	  CUST.SAP_CUST_CHNL_DESC, 
	  CUST.SAP_CUST_SUB_CHNL_KEY, 
	  CUST.SAP_SUB_CHNL_DESC, 
	  CUST.SAP_GO_TO_MDL_KEY, 
	  CUST.SAP_GO_TO_MDL_DESC, 
	  CUST.SAP_BNR_KEY, 
	  CUST.SAP_BNR_DESC, 
	  CUST.SAP_BNR_FRMT_KEY, 
	  CUST.SAP_BNR_FRMT_DESC, 
	  CUST.RETAIL_ENV, 
	  --CUST.REGION, 
	  --CUST.ZONE_OR_AREA, 
	  CUST.CUST_SEGMT_KEY, 
	  CUST.CUST_SEGMENT_DESC, 
	  PRODUCT.GPH_PROD_FRNCHSE, 
	  PRODUCT.GPH_PROD_BRND, 
	  PRODUCT.GPH_PROD_SUB_BRND, 
	  PRODUCT.GPH_PROD_VRNT, 
	  PRODUCT.GPH_PROD_SGMNT, 
	  PRODUCT.GPH_PROD_SUBSGMNT, 
	  PRODUCT.GPH_PROD_CTGRY, 
	  PRODUCT.GPH_PROD_SUBCTGRY, 
	  PRODUCT.GPH_PROD_PUT_UP_DESC, 
	  SELLOUT.EAN,
	  PRODUCT.SAP_MATL_NUM, 
	  PRODUCT.SAP_MAT_DESC, 
	  --PRODUCT.GREENLIGHT_SKU_FLAG, 
	  --PRODUCT.PKA_PRODUCT_KEY, 
	  --PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,
	  CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA'))
		ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) END,
	  CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
		WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productdesc,''),'NA'))
		ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END,
        PRODUCT.PKA_PACKAGE,
		  --PRODUCT.SLS_ORG,
		  SELLOUT.customer_product_desc,
		  SELLOUT.region, 
		  SELLOUT.zone_or_area,
		  --C.EXCH_RATE  
		(C.EXCH_RATE/(C.from_ratio*C.to_ratio)),
		SELLOUT.msl_product_code,
		--SELLOUT.msl_product_desc,
		SELLOUT.store_grade,
		SELLOUT.retail_env,
        SELLOUT.channel,
		  SELLOUT.crtd_dttm,
		  SELLOUT.updt_dttm		  
HAVING NOT (SUM(SELLOUT.so_sls_value) = 0 and SUM(SELLOUT.so_sls_qty) = 0)) 
);


