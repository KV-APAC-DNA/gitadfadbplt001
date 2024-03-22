alter table THAITG_INTEGRATION.ITG_MYM_GT_SALES_REPORT_FACT add column hash_key varchar(255);

update THAITG_INTEGRATION.ITG_MYM_GT_SALES_REPORT_FACT
set hash_key = md5(coalesce(UPPER(item_no), 'N/A') || coalesce(upper (customer_code), 'N/A') || coalesce(upper (customer_name), 'N/A'));

