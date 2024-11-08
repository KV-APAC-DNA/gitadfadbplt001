create table aspitg_integration.itg_pop6_product_attribute_audits_bkp_20241108 as select * from aspitg_integration.itg_pop6_product_attribute_audits;

delete from aspitg_integration.itg_pop6_product_attribute_audits where cntry_cd = 'SG' and visit_id = 'A8FD3A31298FBA4C69D5876B216A1F67' and response like '56550000000%';

delete from aspedw_integration.edw_perfect_store where value like '56550000%' and country = 'Singapore'  and visitid = 'A8FD3A31298FBA4C69D5876B216A1F67';
