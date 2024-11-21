create table prod_DNA_CORE.ntaitg_integration.itg_pos_21112024_cosmed as select * from prod_DNA_CORE.ntaitg_integration.itg_pos;
delete from prod_DNA_CORE.ntaitg_integration.itg_pos where SRC_SYS_CD like 'Cosmed%' and ctry_cd='TW' and pos_dt in ('2024-09-16','2024-09-23','2024-09-30','2024-10-07');
insert into prod_DNA_CORE.ntaitg_integration.itg_pos select distinct * from prod_DNA_CORE.ntaitg_integration.itg_pos_21112024_cosmed where SRC_SYS_CD like 'Cosmed%' and ctry_cd='TW' and pos_dt in ('2024-09-16','2024-09-23','2024-09-30','2024-10-07');
