delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VIETNAM_REGIONAL_SELLOUT_BASE where data_src = 'POS';

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VIETNAM_REGIONAL_SELLOUT_BASE (
SELECT 'POS' AS DATA_SRC,
    'VN' AS    CNTRY_CD,
    'Vietnam' AS CNTRY_NM,
    TIME_DIM."year"::INT AS YEAR,
    time_dim.mnth_id::INT AS MNTH_ID,
     TO_DATE(pos.year||lpad(pos.month,2,'00')||'01','YYYYMMDD') AS DAY,
     time_dim.cal_year::INT as univ_year,
     time_dim.cal_mnth_no::INT as univ_month,
     pos.parameter_value as SOLDTO_CODE,
    'NA' AS DISTRIBUTOR_CODE,
    customername AS DISTRIBUTOR_NAME,
    shopcode AS STORE_CD,
    shopname AS STORE_NAME,
     channelname AS store_type,
    nvl(pos.barcode,'NA') AS EAN,
    nvl(vtdp.jnj_sap_code,matsls.matl_num) AS MATL_NUM,
     productname AS Customer_Product_Desc,
     stt AS region,
     area AS zone_or_area,
     pos.quantity::numeric(38,23) as SO_SLS_QTY,
     pos.amount::numeric(38,23) as SO_SLS_VALUE,
      nvl(pos.barcode,'NA') as msl_product_code,
      channelname as retail_env,
      'MT' as channel,
      current_timestamp() as crtd_dttm,
	current_timestamp() as updt_dttm
   FROM (select * from PROD_DNA_CORE.VNMITG_INTEGRATION.ITG_SPIRAL_MTI_OFFTAKE pos_inner,(select parameter_value from PROD_DNA_CORE.SGPITG_INTEGRATION.ITG_QUERY_PARAMETERS
   where country_code = 'VN' and parameter_name = 'vn_dksh_soldto_code')) pos
    LEFT JOIN (select * from (select distinct barcode, jnj_sap_code, row_number() OVER (PARTITION BY barcode order by jnj_sap_code DESC) rnk from PROD_DNA_CORE.VNMEDW_INTEGRATION.EDW_VW_VN_MT_DIST_PRODUCTS) where rnk=1) vtdp ON LTRIM(pos.barcode,'0') = LTRIM(vtdp.barcode,'0')
    LEFT JOIN (select matl_num,ean_num as matsls_ean from
       (Select ltrim(matl_num,'0') as matl_num,ltrim(ean_num,'0') as ean_num, row_number() over (partition by ltrim(ean_num,'0') order by crt_dttm desc) as rn
         from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM where sls_org in
          (select distinct sls_org from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_SALES_ORG_DIM where sls_org_co_cd in
            (select distinct co_cd from PROD_DNA_CORE.ASPEDW_INTEGRATION.EDW_COMPANY_DIM where crncy_key = 'VND' and ctry_key = 'VN' and ctry_group = 'Vietnam'))
      and (ean_num != 'N/A' and nullif(ean_num,'') is not null)) where rn = 1) matsls 
      ON LTRIM(matsls.matsls_ean,'0') = LTRIM(pos.barcode,'0')
    LEFT JOIN PROD_DNA_CORE.SGPEDW_INTEGRATION.EDW_VW_OS_TIME_DIM time_dim on TO_DATE(pos.year||lpad(pos.month,2,'00')|| '01','YYYYMMDD') = time_dim.cal_date
where NOT (nvl(so_sls_value, 0) = 0 and nvl(so_sls_qty, 0) = 0) AND day > (select to_date(param_value,'YYYY-MM-DD') from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG where code='min_date') 
AND mnth_id>= (case when (select param_value from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG where code='base_load_vn')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG where code='base_load_vn')::integer)), 'YYYYMM')
end));
