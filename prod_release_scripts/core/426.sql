create table aspitg_integration.itg_pop6_products_bkp_20241001 as select * fro aspitg_integration.itg_pop6_products;

create table aspitg_integration.itg_pop6_products_test as select * from aspitg_integration.itg_pop6_products;

truncate table aspitg_integration.itg_pop6_products_test;
