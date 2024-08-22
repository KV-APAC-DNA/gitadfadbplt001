 delete from prod_dna_core.dbt_cloud_pr_5458_963.aspitg_integration__itg_pop6_sku_audits where cntry_cd = 'TH';

 Insert into prod_dna_core.dbt_cloud_pr_5458_963.aspitg_integration__itg_pop6_sku_audits 
 select * from aspitg_integration.itg_pop6_sku_audits where cntry_cd = 'TH';
