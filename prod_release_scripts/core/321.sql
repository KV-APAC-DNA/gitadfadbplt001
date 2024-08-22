 delete from prod_dna_core.dbt_cloud_pr_5458_963.aspitg_integration__itg_pop6_product_attribute_audits where cntry_cd = 'TH';

 Insert into prod_dna_core.dbt_cloud_pr_5458_963.aspitg_integration__itg_pop6_product_attribute_audits 
 select * from aspitg_integration.itg_pop6_product_attribute_audits where cntry_cd = 'TH';

delete from prod_dna_core.dbt_cloud_pr_5458_963.aspitg_integration__itg_pop6_sku_audits where cntry_cd = 'TH'
 and src_file_date in ('20240816', '20240817','20240818','20240819','20240820','20240821');
