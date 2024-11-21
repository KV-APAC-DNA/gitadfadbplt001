create table prod_DNA_CORE.ntaitg_integration.itg_pos_21112024 as select * from prod_DNA_CORE.ntaitg_integration.itg_pos;
create table prod_DNA_CORE.ntaedw_integration.edw_pos_fact_21112024 as select * from prod_DNA_CORE.ntaedw_integration.edw_pos_fact;
create table prod_DNA_CORE.ntaitg_integration.sdl_raw_tw_pos_ec_21112024 as select * from prod_DNA_CORE.ntaitg_integration.sdl_raw_tw_pos_ec;
delete from prod_DNA_CORE.ntaitg_integration.itg_pos where SRC_SYS_CD='EC' and left(pos_dt,7)='2024-08';
delete from prod_DNA_CORE.ntaitg_integration.sdl_raw_tw_pos_ec where left(pos_date,7)>='2024-08';
