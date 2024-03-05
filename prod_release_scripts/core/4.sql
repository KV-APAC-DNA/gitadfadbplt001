delete from prod_dna_core.aspwks_integration.sap_transactional_processed_files
where act_file_name in
(
'SAP_BW_CDL_SALES_20240228_120614.csv',
'SAP_BW_CDL_SALES_20240228_200548.csv',
'SAP_BW_CDL_SALES_20240228_160551.csv',
'SAP_BW_CDL_SALES_20240229_080549.csv',
'SAP_BW_CDL_SALES_20240228_140530.csv',
'SAP_BW_CDL_SALES_20240228_100544.csv',
'SAP_BW_CDL_SALES_20240228_180633.csv')
and target_table_name in ('sdl_raw_sap_bw_sales','itg_sales_order_fact');

delete from prod_dna_core.aspwks_integration.sap_transactional_processed_files
where act_file_name in(
'SAP_BW_CDL_BILLING_20240228_100536.csv',
'SAP_BW_CDL_BILLING_20240228_120609.csv',
'SAP_BW_CDL_BILLING_20240228_140511.csv',
'SAP_BW_CDL_BILLING_20240228_160531.csv',
'SAP_BW_CDL_BILLING_20240228_180632.csv',
'SAP_BW_CDL_BILLING_20240228_200550.csv',
'SAP_BW_CDL_BILLING_20240229_080541.csv') 
and target_table_name = 'itg_billing_fact';
