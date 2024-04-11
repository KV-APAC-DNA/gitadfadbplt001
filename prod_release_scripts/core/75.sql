delete from mysitg_integration.itg_my_sellout_sales_fact
where year(CRTD_DTTM)<'2024';

insert into mysitg_integration.itg_my_sellout_sales_fact
select * from SNAPOSEITG_INTEGRATION.ITG_MY_SELLOUT_SALES_FACT_BACKUP
where year(CRTD_DTTM)<'2024';
