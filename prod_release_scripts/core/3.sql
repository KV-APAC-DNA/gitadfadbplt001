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
alter table aspitg_integration.itg_rg_travel_retail_sellout
add column hash_key varchar(32);

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

truncate table aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES;
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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
=============================================itg_invnt===================================

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

--============================================itg_invnt===================================

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;
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
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'False' as is_deleted from table_ t;

-------------------------------------------------------------------------------------------

insert into core_integration.dbtjobs_test_cdc_metadata values('J_RG_Account_attr_CIW_Dim','1','(\'vw_stg_sdl_account_attr_ciw\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('J_RG_Account_attr_CIW_Dim','1','(\'vw_stg_sdl_account_attr_ciw\')','0');

insert into core_integration.dbtjobs_test_cdc_metadata values('sap_bw_master_etl_framework','3','(\'vw_stg_sdl_sap_bw_0account\', \'vw_stg_sdl_sap_ecc_account_text\', \'vw_stg_sdl_sap_ecc_material_plant\', \'vw_stg_sdl_sap_ecc_material_sales\', \'vw_stg_sdl_sap_bw_material_plant_text\', \'vw_stg_sdl_sap_bw_material_sales_text\', \'vw_stg_sdl_sap_bw_plant_attr\', \'vw_stg_sdl_sap_bw_plant_text\', \'vw_stg_sdl_sap_bw_dna_material_bomlist\')','0');

insert into core_integration.dbtjobs_test_cdc_metadata values('sap_ecc_master_etl_framework','4','(\'vw_stg_sdl_sap_ecc_customer_sales\', \'vw_stg_sdl_sap_ecc_material_text\',\'vw_stg_sdl_sap_ecc_base_product_text\',\'vw_stg_sdl_sap_ecc_brand_text\',\'vw_stg_sdl_sap_ecc_company_code_text\',\'vw_stg_sdl_sap_ecc_company\',\'vw_stg_sdl_sap_ecc_customer_text\',\'vw_stg_sdl_sap_ecc_mega_brand_text\',\'vw_stg_sdl_sap_ecc_customer_base\',\'vw_stg_sdl_sap_ecc_sales_org_text\',\'vw_stg_sdl_sap_ecc_sales_office_text\',\'vw_stg_sdl_sap_ecc_sales_org\',\'vw_stg_sdl_sap_ecc_profit_center\',\'vw_stg_sdl_sap_ecc_variant_text\',\'vw_stg_sdl_sap_ecc_tcurr\',\'vw_stg_sdl_sap_ecc_profit_center_text\',\'vw_stg_sdl_sap_ecc_put_up_text\',\'vw_stg_sdl_sap_ecc_sales_group_text\',\'vw_stg_sdl_sap_bw_strongholds_text\',\'vw_stg_sdl_sap_bw_time\',\'vw_stg_sdl_sap_ecc_marm \',\'vw_stg_sdl_sap_bw_country_code_text\',\'vw_stg_sdl_sap_bw_needstates_text\',\'vw_stg_sdl_code_descriptions\',\'vw_stg_sdl_dstrbtn_chnl\',\'vw_stg_sdl_prod_hier\',\'vw_stg_sdl_material_typ\',\'vw_stg_sdl_material_dim\',\'vw_stg_sdl_cust_sls_attr\',)','0');

insert into core_integration.dbtjobs_test_cdc_metadata values('rg_greenlight_mds','5','(\'sdl_mds_ap_greenlight_skus\')','0');

insert into core_integration.dbtjobs_test_cdc_metadata values('rg_travel_retail_files','8','(\'sdl_mds_apac_dcl_customers\',','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('rg_travel_retail_files','8','(\'sdl_mds_apac_dcl_customers\',
\'sdl_mds_apac_dcl_products\',
\'sdl_mds_apac_dcl_targets\',\'sdl_mds_apac_dcl_products
\',\'sdl_mds_apac_dcl_products\',\'sdl_rg_travel_retail_sales_stock\',\'sdl_rg_travel_retail_cdfg\', \'sdl_rg_travel_retail_cnsc\', \'sdl_rg_travel_retail_dfs\', \'sdl_rg_travel_retail_dfs_hainan\', \'sdl_rg_travel_retail_dufry_hainan\',\'sdl_mds_apac_dcl_customers\'
)','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('sg_master_sellout','10','(\'sdl_sg_zuellig_customer_mapping\',\'sdl_sg_zuellig_product_mapping\',\'itg_sg_ciw_mapping\',\'itg_sg_zuellig_sellout\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('sg_transaction_scan','11','(\'sdl_sg_scan_data_watsons\',\'sdl_sg_scan_data_dfi\',\'sdl_sg_scan_data_guardian\',\'sdl_sg_scan_data_marketplace\',\'sdl_sg_scan_data_scommerce\',\'sdl_sg_scan_data_redmart\',\'sdl_sg_scan_data_ntuc\',\'sdl_sg_scan_data_amazon\')','0');
insert into core_integration.dbtjobs_test_cdc_metadata values('sg_transaction_sellout','12','(\'sdl_sg_tp_closed_month\',\'sdl_sg_tp_closed_year_bal\',\'sdl_sg_zuellig_product_mapping\')
','0');
insert into core_integration.dbtjobs_test_cdc_metadata 
values('Global_Commercial_Hierarchy_Wrapper','6','','0');
insert into core_integration.dbtjobs_test_cdc_metadata 
values('sg_mds_to_dna_refresh','9','','0');

--===============================
--===============================
--===============================



insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_customer_sales','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_customer_sales__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_ecc_material_text','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_ecc_material_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_base_product_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_base_product_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_brand_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_brand_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_company_code_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_company_code_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_company','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_company__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_customer_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_customer_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_mega_brand_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_mega_brand_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_customer_base','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_customer_base__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_sales_org_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_sales_org_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_sales_office_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_sales_office_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_sales_org','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_sales_org__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_profit_center','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_profit_center__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_variant_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_variant_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_tcurr','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_tcurr__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_profit_center_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_profit_center_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_put_up_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_put_up_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_sales_group_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_sales_group_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_bw_strongholds_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_bw_strongholds_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_bw_time','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_bw_time__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_ecc_marm ','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_ecc_marm __duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_bw_country_code_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_bw_country_code_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_bw_needstates_text','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_bw_needstates_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_code_descriptions','select * from aspwks_integration.TRATBL_vw_stg_sdl_code_descriptions__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_dstrbtn_chnl','select * from aspwks_integration.TRATBL_vw_stg_sdl_dstrbtn_chnl__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_prod_hier','select * from aspwks_integration.TRATBL_vw_stg_sdl_prod_hier__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_material_typ','select * from aspwks_integration.TRATBL_vw_stg_sdl_material_typ__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_material_dim','select * from aspwks_integration.TRATBL_vw_stg_sdl_material_dim__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_cust_sls_attr','select * from aspwks_integration.TRATBL_vw_stg_sdl_cust_sls_attr__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_sap_bw_0account','select * from aspwks_integration.TRATBL_vw_stg_sdl_sap_bw_0account__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_ecc_account_text','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_ecc_account_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_ecc_material_plant','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_ecc_material_plant__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_ecc_material_sales','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_ecc_material_sales__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_bw_material_plant_text','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_bw_material_plant_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_bw_material_sales_text','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_bw_material_sales_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_bw_plant_attr','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_bw_plant_attr__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_bw_plant_text','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_bw_plant_text__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values(' vw_stg_sdl_sap_bw_dna_material_bomlist','select * from aspwks_integration.TRATBL_ vw_stg_sdl_sap_bw_dna_material_bomlist__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('vw_stg_sdl_account_attr_ciw','selecy * from aspwks_integration.TRATBL_vw_stg_sdl_account_attr_ciw__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_rg_ecom_plan','select * from aspwks_integration.TRATBL_sdl_mds_rg_ecom_plan__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_rg_ecom_plan__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_rg_profit_center_franchise_mapping','select * from aspwks_integration.TRATBL_sdl_mds_rg_profit_center_franchise_mapping__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_rg_profit_center_franchise_mapping__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_rg_ps_market_coverage','select * from aspwks_integration.TRATBL_sdl_mds_rg_ps_market_coverage__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_rg_ps_market_coverage__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_rg_ecom_digital_shelf_customer_mapping','select * from aspwks_integration.TRATBL_sdl_mds_rg_ecom_digital_shelf_customer_mapping__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_rg_ecom_digital_shelf_customer_mapping__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_ap_greenlight_skus','select * from aspwks_integration.TRATBL_sdl_mds_ap_greenlight_skus__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_ap_greenlight_skus__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_customers','select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_customers__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_customers__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_products','select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_targets','select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_targets__null_test union all
select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_targets__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_watsons','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_watsons__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_dfi','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_dfi__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_guardian','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_guardian__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_marketplace','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_marketplace__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_scommerce','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_scommerce__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_redmart','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_redmart__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_ntuc','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_ntuc__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_amazon','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_amazon__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_zuellig_customer_mapping','select * from sgpwks_integration.TRATBL_sdl_sg_zuellig_customer_mapping__null_test 
union all
select * from  sgpwks_integration.TRATBL_sdl_sg_zuellig_customer_mapping__duplicate_test
union all
select * from sgpwks_integration.TRATBL_sdl_sg_zuellig_customer_mapping__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_zuellig_sellout','select * from sgpwks_integration.TRATBL_sdl_sg_zuellig_sellout__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_zuellig_product_mapping','select * from sgpwks_integration.TRATBL_sdl_sg_zuellig_product_mapping__null_test union all select * from sgpwks_integration.TRATBL_sdl_sg_zuellig_product_mapping__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_tp_closed_month',' select * from sgpwks_integration.TRATBL_sdl_sg_tp_closed_month__null_test union all select * from sgpwks_integration.TRATBL_sdl_sg_tp_closed_month__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_tp_closed_year_bal','select * from sgpwks_integration.TRATBL_sdl_sg_tp_closed_year_bal__null_test union all select * from sgpwks_integration.TRATBL_sdl_sg_tp_closed_year_bal__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_watsons','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_watsons__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_dfi','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_dfi__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_guardian','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_guardian__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_marketplace','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_marketplace__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_scommerce','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_scommerce__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_redmart','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_redmart__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_ntuc','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_ntuc__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_sg_scan_data_amazon','select * from sgpwks_integration.TRATBL_sdl_sg_scan_data_amazon__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_cdfg','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_cdfg__null_test union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_cdfg_product__lookup_test union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_cdfg_channel__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_products','select * from  aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__null_test union all     select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_cnsc','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_cnsc__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_dfs','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_dfs__null_test  union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_dfs_product__lookup_test union all
select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_dfs_channel__lookup_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_dfs_hainan','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_dfs_hainan__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_dufry_hainan','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_dufry_hainan__null_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_customers','select * from  aspwks_integration.TRATBL_sdl_mds_apac_dcl_customers__null_test union all     select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_customers__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_mds_apac_dcl_products','select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__null_test   union all select * from aspwks_integration.TRATBL_sdl_mds_apac_dcl_products__duplicate_test;');
insert into CORE_INTEGRATION.DBTTEST_TABLES_METADATA values('sdl_rg_travel_retail_sales_stock','select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_sales_stock__null_test union all select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_sales_stock_product__lookup_test union all select * from aspwks_integration.TRATBL_sdl_rg_travel_retail_sales_stock_channel__lookup_test;');
