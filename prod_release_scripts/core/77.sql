delete from aspitg_integration.itg_copa_trans
where date(crt_dttm)>='2024-04-14';

delete from aspedw_integration.edw_copa_trans_fact 
where date(crt_dttm)>='2024-04-14';

delete from aspwks_integration.sap_transactional_processed_files
where target_table_name='itg_copa_trans' and act_file_name in (
'SAP_BW_COPA10_20240415_020448.csv',
'SAP_BW_COPA10_20240414_220517.csv',
'SAP_BW_COPA10_20240413_221051.csv',
'SAP_BW_COPA10_20240414_142126.csv',
'SAP_BW_COPA10_20240414_020214.csv',
'SAP_BW_COPA10_20240414_211131.csv'
);
