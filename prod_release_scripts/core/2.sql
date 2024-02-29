----itg_copa_trans data deletion----
delete from aspitg_integration.itg_copa_trans
where date(crt_dttm)>'2024-02-20' and file_name!='No file name in Legacy System';
----itg_copa_trans metadata deletion----
delete from aspwks_integration.sap_transactional_processed_files
where target_table_name='itg_copa_trans' and date(inserted_on)>='2024-02-22'
and act_file_name!='SAP_BW_COPA10_20240221_020315.csv';
----edw_copa_trans_fact data deletion----
delete from aspedw_integration.edw_copa_trans_fact
where date(crt_dttm)>='2024-02-21';
----itg_invc_sls data deletion----
delete from aspitg_integration.itg_invc_sls
where file_name!='No file name in Legacy System';
----itg_invnt data deletion----
delete from aspitg_integration.itg_invnt
where file_name!='No file name in Legacy System';
----itg_invc_sls metadata deletion----
delete from aspwks_integration.sap_transactional_processed_files
where target_table_name='itg_invc_sls'
and act_file_name!='No file name in Legacy System'
and inserted_on>='2024-02-22 04:27:33.600';
----itg_invnt metadata deletion----
delete from aspwks_integration.sap_transactional_processed_files
where target_table_name='itg_invnt'
and act_file_name!='No file name in Legacy System'
and inserted_on>='2024-02-22 02:06:12.776';
