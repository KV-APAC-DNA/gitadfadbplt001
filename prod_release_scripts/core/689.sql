create or replace table PROD_DNA_CORE.ntaitg_integration.itg_sls_grp_to_customer_mapping_temp 
clone PROD_DNA_CORE.ntaitg_integration.itg_sls_grp_to_customer_mapping;

update PROD_DNA_CORE.ntaitg_integration.itg_sls_grp_to_customer_mapping_temp set sls_grp = 'A-Mart 愛買' 
where STRATEGY_CUSTOMER_HIERACHY_CODE = '900015';
