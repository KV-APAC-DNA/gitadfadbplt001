delete from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_RE_MSL_INPUT_DEFINITION;

insert into PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_RE_MSL_INPUT_DEFINITION (
with source as (
    select * from PROD_DNA_LOAD.ASPSDL_RAW.SDL_MDS_MDS_REDS_MARKET_MSL 
)
    SELECT TO_CHAR ((start_ddmmyyyy :: DATE),'DD/MM/YYYY') AS start_date,
       TO_CHAR ((end_ddmmyyyy :: DATE),'DD/MM/YYYY') AS end_date,
       market,
       region,
       zone,
       retail_environment_code AS retail_environment,
       channel,
       sub_channel,
       customer,
       store_grade_code AS store_grade,
       unique_identifier_mapping_code AS unique_identifier_mapping,
       sku_unique_identifier,
       sku_description,
       sku_code,
       product_key,
	   'NA' AS msl_final,
	   active_status_code,
	   sourceexistenceflag_code
    FROM source
);
