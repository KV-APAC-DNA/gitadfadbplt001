--For PROD RUN FOR MAHIMA:


--============================================DQ Framework =======================================================
create or replace view core_integration.vw_failed_tests_on_models as (
with failed_tests_models as (
select replace(replace(split(split(node_id,'.')[2],'__')[0],'TRATBL_'),'"') as model_name,* from core_integration.test_executions where status='fail'
order by run_started_at desc
)
select model_name,run_started_at,query_completed_at,status from failed_tests_models 
);


create table if not exists core_integration.dbtjobs_test_cdc_metadata 
(   job_name string,
    tempId string,
    models string,
    cdc_timestamp timestamp_ntz(9)
);

--Insert statements to add later
--insert into core_integration.dbtjobs_test_cdc_metadata values('job1','1','(\'vw_stg_sdl_sap_ecc_company_code_text\',\'vw_stg_sdl_sap_ecc_company\')','0');

--call core_integration.SP_updatecdc_failed_tests_models('1','(\'vw_stg_sdl_sap_ecc_company_code_text\',\'vw_stg_sdl_sap_ecc_company\')')
--call core_integration.SP_failed_tests_models('1','(\'vw_stg_sdl_sap_ecc_company_code_text\',\'vw_stg_sdl_sap_ecc_company\')')


CREATE OR REPLACE PROCEDURE core_integration.SP_updatecdc_failed_tests_models(tempId VARCHAR,models varchar)
RETURNS string
LANGUAGE SQL
AS
DECLARE
  final_query string;
  res RESULTSET;
BEGIN
final_query := '
update core_integration.dbtjobs_test_cdc_metadata 
set cdc_timestamp=(select max(run_started_at) from  core_integration.vw_failed_tests_on_models where model_name in' || :models ||') where tempId=' || :tempId ||'
;
';
res := (execute immediate :final_query);
RETURN 'Stored Proc ran successfully';
END;

create TABLE IF NOT EXISTS CORE_INTEGRATION.DBTTEST_TABLES_METADATA (
	MODEL VARCHAR(16777216),
	QUERY VARCHAR(16777216)
);

--Insert statements to add later


CREATE OR REPLACE PROCEDURE core_integration.SP_failed_tests_models(tempId VARCHAR,models varchar)
RETURNS TABLE(model_name string,status string)
LANGUAGE SQL
AS
DECLARE
  failed_test_models string; 
  cdc_timestamp_tbl string;
  final_query string;
  res RESULTSET;
BEGIN
  failed_test_models := 'select * from core_integration.vw_failed_tests_on_models where model_name in ' || :models; 
  cdc_timestamp_tbl := 'select * from core_integration.dbtjobs_test_cdc_metadata where tempId =' || :tempId;

final_query := '
with failed_test_models as ('  || :failed_test_models || '),
cdc_timestamp_tbl as (
' || :cdc_timestamp_tbl ||
')
select distinct model_name,status 
from failed_test_models ftm 
join cdc_timestamp_tbl ctt on ftm.run_started_at > ctt.cdc_timestamp
;
';
res := (execute immediate :final_query);
RETURN TABLE(res);
END;

--============================================Transactional tables logic =======================================================

create  table if not exists  aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES (
    source_table_name varchar(200),
    source_view_name varchar(200),
    target_table_name varchar(200),
    act_file_name varchar(500),
    inserted_on timestamp_ntz(9),
    is_deleted boolean default 'False'
);


--==============================================Travel retail sellout=============================================================

--Already added till now.
-- alter table aspitg_integration.itg_rg_travel_retail_sellout
-- add column hash_key varchar(32);

update aspitg_integration.itg_rg_travel_retail_sellout
set hash_key=md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) where ctry_cd in ('CN','SG','CM','HK');

update aspitg_integration.itg_rg_travel_retail_sellout
set hash_key=md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(door_name),''),'_',coalesce(upper(ctry_cd),''))) where ctry_cd in('KR');

--=============================================Transactional tables column addition====================================================================

alter table aspitg_integration.itg_copa_trans add column file_name varchar(255);
alter table aspitg_integration.itg_invnt add column file_name varchar(255);
alter table aspitg_integration.ITG_COPA17_TRANS add column file_name varchar(255);
alter table aspitg_integration.itg_invc_sls add column file_name varchar(255);

update aspitg_integration.itg_copa_trans set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.itg_invnt set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.ITG_COPA17_TRANS set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.itg_invc_sls set file_name='No file name in Legacy System' where file_name is null;

-- BWA_CDL_SALES -> vw_stg_sdl_sap_bw_sales -> itg_sales_order_fact
--                                             sdl_raw_sap_bw_sales
-- BWA_CDL_DELIVERY -> vw_stg_sdl_sap_bw_delivery -> itg_delivery_fact 
--                                                 sdl_raw_sap_bw_delivery
-- BWA_CDL_BILLING -> vw_stg_sdl_sap_bw_billing -> itg_billing_fact
--                                                 sdl_raw_sap_bw_billing
-- BWA_COPA10  -> vw_stg_sdl_sap_bw_cop10 -> itg_copa_trans
-- BWA_INVENTORY -> vw_stg_sdl_sap_bw_inventory -> itg_invnt
-- BWA_COPA17 -> vw_stg_sdl_sap_bw_copa17 -> itg_copa17_trans
-- BWA_CDL_BILLING_COND _> vw_stg_sdl_sap_billing_condition -> itg_sap_billing_condition
--                                                                 sdl_raw_sap_billing_condition
-- BWA_ZC_SD -> vw_stg_sdl_sap_bw_zc_sd -> itg_invc_sls

--==============================================================itg_sales_order_fact=====================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_SALES' as source_table_name,
    'vw_stg_sdl_sap_bw_sales' as source_view_name,
    'itg_sales_order_fact' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_sales_order_fact
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_sales_order_fact'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--==============================================================sdl_raw_sap_bw_sales=====================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_SALES' as source_table_name,
    'vw_stg_sdl_sap_bw_sales' as source_view_name,
    'sdl_raw_sap_bw_sales' as target_table_name,
    file_name as act_file_name
from aspitg_integration.sdl_raw_sap_bw_sales
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='sdl_raw_sap_bw_sales'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--===================================================itg_delivery_fact=====================================================


insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_DELIVERY' as source_table_name,
    'vw_stg_sdl_sap_bw_delivery' as source_view_name,
    'itg_delivery_fact' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_delivery_fact
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_delivery_fact'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================sdl_raw_sap_bw_delivery===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_DELIVERY' as source_table_name,
    'vw_stg_sdl_sap_bw_delivery' as source_view_name,
    'sdl_raw_sap_bw_delivery' as target_table_name,
    file_name as act_file_name
from aspitg_integration.sdl_raw_sap_bw_delivery
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='sdl_raw_sap_bw_delivery'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================itg_billing_fact===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING' as source_table_name,
    'vw_stg_sdl_sap_bw_billing' as source_view_name,
    'itg_billing_fact' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_billing_fact
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_billing_fact'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================sdl_raw_sap_bw_billing===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING' as source_table_name,
    'vw_stg_sdl_sap_bw_billing' as source_view_name,
    'sdl_raw_sap_bw_billing' as target_table_name,
    file_name as act_file_name
from aspitg_integration.sdl_raw_sap_bw_billing
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='sdl_raw_sap_bw_billing'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================itg_copa_trans===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_COPA10' as source_table_name,
    'vw_stg_sdl_sap_bw_cop10' as source_view_name,
    'itg_copa_trans' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_copa_trans
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_copa_trans'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================itg_invnt===================================

-- BWA_INVENTORY -> vw_stg_sdl_sap_bw_inventory -> itg_invnt

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_INVENTORY' as source_table_name,
    'vw_stg_sdl_sap_bw_inventory' as source_view_name,
    'itg_invnt' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_invnt
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_invnt'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 


--=============================================itg_invnt===================================

-- BWA_INVENTORY -> vw_stg_sdl_sap_bw_inventory -> itg_invnt

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_INVENTORY' as source_table_name,
    'vw_stg_sdl_sap_bw_inventory' as source_view_name,
    'itg_invnt' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_invnt
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_invnt'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 


--=============================================itg_copa17_trans===================================
-- BWA_COPA17 -> vw_stg_sdl_sap_bw_copa17 -> itg_copa17_trans

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_COPA17' as source_table_name,
    'vw_stg_sdl_sap_bw_copa17' as source_view_name,
    'itg_copa17_trans' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_copa17_trans
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_copa17_trans'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================itg_sap_billing_condition===================================
-- BWA_CDL_BILLING_COND _> vw_stg_sdl_sap_billing_condition -> itg_sap_billing_condition
--                                                                 sdl_raw_sap_billing_condition

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING_COND' as source_table_name,
    'vw_stg_sdl_sap_billing_condition' as source_view_name,
    'itg_sap_billing_condition' as target_table_name,
    file_name as act_file_name
from aspitg_integration.itg_sap_billing_condition
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='itg_sap_billing_condition'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 

--=============================================sdl_raw_sap_billing_condition===================================

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    'BWA_CDL_BILLING_COND' as source_table_name,
    'vw_stg_sdl_sap_billing_condition' as source_view_name,
    'sdl_raw_sap_billing_condition' as target_table_name,
    file_name as act_file_name
from aspitg_integration.sdl_raw_sap_billing_condition
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'False' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='sdl_raw_sap_billing_condition'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name) 
