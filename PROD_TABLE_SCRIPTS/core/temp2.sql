create table pcfitg_integration.itg_customer_sellout_bkp_10012024 as select * from pcfitg_integration.itg_customer_sellout;
delete from pcfitg_integration.itg_customer_sellout where nullif(dstr_prod_cd,'') is null and nullif(matl_num,'') is null and nullif(ean,'') is null and SAP_PARENT_CUSTOMER_DESC='SYMBION';
