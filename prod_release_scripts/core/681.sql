insert into INDITG_INTEGRATION.LKP_HARMONIZATION_UNIVERAL_EXCEPTION
select * from PROD_DNA_LOAD.INDSDL_RAW.LKP_HARMONIZATION_UNIVERAL_EXCEPTION;
 
insert into INDITG_INTEGRATION.lkp_harmonization_patterns
select * from PROD_DNA_LOAD.INDSDL_RAW.lkp_harmonization_patterns;
 
insert into INDITG_INTEGRATION.t_harmonization_rule_mstr
select * from PROD_DNA_LOAD.INDSDL_RAW.t_harmonization_rule_mstr;
