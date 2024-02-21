create or replace table  aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES (
    source_table_name varchar(200),
    source_view_name varchar(200),
    target_table_name varchar(200),
    act_file_name varchar(500),
    inserted_on timestamp_ntz(9),
    is_deleted boolean default 'False'
);


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_SALES' as source_table_name,
    'vw_stg_sdl_sap_bw_sales' as source_view_name,
    'itg_sales_order_fact' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_sales
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
