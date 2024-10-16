create table aspitg_integration.itg_pop6_products_bkp_20241016 as select * from aspitg_integration.itg_pop6_products;

update aspitg_integration.itg_pop6_products a set a.company = b.sku_english
from (select * from prod_dna_core.aspitg_integration.itg_pop6_products_bkp_20241016  where cntry_cd = 'TH' and active = 'Y') b
where a.productdb_id = b.productdb_id
and a.active = 'Y'  and a.cntry_cd  =  'TH'
and a.productdb_id <> 'CE6F8356AAB532657BEEB8B3B9F055AB';

update aspitg_integration.itg_pop6_products a set a.sku_english = b.company
from (select * from prod_dna_core.aspitg_integration.itg_pop6_products_bkp_20241016  where cntry_cd = 'TH' and active = 'Y') b
where a.productdb_id = b.productdb_id
and a.active = 'Y'  and a.cntry_cd  =  'TH'
and a.productdb_id <> 'CE6F8356AAB532657BEEB8B3B9F055AB';
