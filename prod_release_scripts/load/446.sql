use schema meta_raw;


create or replace table usecase_2006 as select * from usecase;
create or replace table process_2006 as select * from process;
create or replace table parameters_2006 as select * from parameters;

update usecase set category='ETL_XDM_TLRSR_LOAD' where usecase_id=250;

