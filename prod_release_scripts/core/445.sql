create table PROD_DNA_CORE.PCFITG_INTEGRATION.ITG_CUST_CIW_PLAN_FACT_20241010 
clone 
PROD_DNA_CORE.PCFITG_INTEGRATION.ITG_CUST_CIW_PLAN_FACT;

ALTER TABLE PROD_DNA_CORE.PCFITG_INTEGRATION.ITG_CUST_CIW_PLAN_FACT 
ADD COLUMN target_type varchar(10);