insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
with table_ as (
select
    'BWA_ZC_SD' as source_table_name,
    'vw_stg_sdl_sap_bw_zc_sd' as source_view_name,
    'itg_invc_sls' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_zc_sd
where file_name in (select distinct file_name from aspitg_integration.itg_invc_sls)
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
