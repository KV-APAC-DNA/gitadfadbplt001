delete from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VIETNAM_REGIONAL_SELLOUT_NPD;

insert into PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VIETNAM_REGIONAL_SELLOUT_NPD(
with wks_vietnam_regional_sellout as (
select * from PROD_DNA_CORE.DBT_CLOUD_PR_5458_1220.VNMWKS_INTEGRATION__WKS_VIETNAM_REGIONAL_SELLOUT
)
select *,
min(cal_date) over (partition by sap_parent_customer_key) as customer_min_date,
min(cal_date) over (partition by country_name) as market_min_date,
rank() over (partition by sap_parent_customer_key,pka_product_key order by cal_date) as rn_cus,
rank() over (partition by country_name,pka_product_key order by cal_date) as rn_mkt,
min(cal_date) over (partition by sap_parent_customer_key,pka_product_key) as customer_product_min_date, 
min(cal_date) over (partition by country_name,pka_product_key) as market_product_min_date
from wks_vietnam_regional_sellout);
