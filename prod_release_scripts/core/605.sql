UPDATE PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
   SET param_value = '30'
Where code in ('base_load_hk', 'base_load_vn', 'base_load_tw');
