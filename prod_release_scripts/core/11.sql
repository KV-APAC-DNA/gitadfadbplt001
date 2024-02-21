create  table if not exists  aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES (
    source_table_name varchar(200),
    source_view_name varchar(200),
    target_table_name varchar(200),
    act_file_name varchar(500),
    inserted_on timestamp_ntz(9),
    is_deleted boolean default 'False'
);




truncate table aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES;
--==========================--====================================itg_sales_order_fact--=====================================

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

--==========================--====================================sdl_raw_sap_bw_sales--=====================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_SALES' as source_table_name,
    'vw_stg_sdl_sap_bw_sales' as source_view_name,
    'sdl_raw_sap_bw_sales' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_sales
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
--===================================================itg_delivery_fact--==========================--===========================


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_DELIVERY' as source_table_name,
    'vw_stg_sdl_sap_bw_delivery' as source_view_name,
    'itg_delivery_fact' as target_table_name,
    filename as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_delivery
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;




--=============================================sdl_raw_sap_bw_delivery--===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_DELIVERY' as source_table_name,
    'vw_stg_sdl_sap_bw_delivery' as source_view_name,
    'sdl_raw_sap_bw_delivery' as target_table_name,
    filename as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_delivery
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

--=============================================itg_billing_fact--===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING' as source_table_name,
    'vw_stg_sdl_sap_bw_billing' as source_view_name,
    'itg_billing_fact' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_billing
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

--=============================================sdl_raw_sap_bw_billing--===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING' as source_table_name,
    'vw_stg_sdl_sap_bw_billing' as source_view_name,
    'sdl_raw_sap_bw_billing' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_billing
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
--=============================================itg_copa_trans--===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_COPA10' as source_table_name,
    'VW_STG_SDL_SAP_BW_ZOCOPA10' as source_view_name,
    'itg_copa_trans' as target_table_name,
    file_name as act_file_name
from aspitg_integration.VW_STG_SDL_SAP_BW_ZOCOPA10
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
----=============================================itg_invnt--===================================


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_INVENTORY' as source_table_name,
    'vw_stg_sdl_sap_bw_inventory' as source_view_name,
    'itg_invnt' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_inventory
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

----=============================================itg_invnt--===================================


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_INVENTORY' as source_table_name,
    'vw_stg_sdl_sap_bw_inventory' as source_view_name,
    'itg_invnt' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_bw_inventory
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING_COND' as source_table_name,
    'vw_stg_sdl_sap_billing_condition' as source_view_name,
    'itg_sap_billing_condition' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_billing_condition
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
--=============================================sdl_raw_sap_billing_condition--===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING_COND' as source_table_name,
    'vw_stg_sdl_sap_billing_condition' as source_view_name,
    'sdl_raw_sap_billing_condition' as target_table_name,
    file_name as act_file_name
from aspitg_integration.vw_stg_sdl_sap_billing_condition
group by act_file_name
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

